import json
import time
import logging
import uuid
import jwt
import cloudscraper
import websockets
import hashlib
from typing import Dict, Any, AsyncGenerator, Optional
from datetime import datetime, timedelta

from fastapi import HTTPException
from fastapi.responses import StreamingResponse, JSONResponse

from app.core.config import settings
from app.providers.base_provider import BaseProvider
from app.utils.sse_utils import create_sse_data, create_chat_completion_chunk, DONE_CHUNK

logger = logging.getLogger(__name__)

class EngineLabsProvider(BaseProvider):
    def __init__(self):
        if not settings.CLERK_COOKIE:
            raise ValueError("配置错误: CLERK_COOKIE 必须在 .env 文件中设置。")
        
        self.scraper = cloudscraper.create_scraper()
        self.chat_url = "https://api.enginelabs.ai/engine-agent/chat"
        
        self.clerk_cookie = settings.CLERK_COOKIE.strip()
        self.session_id = "sess_34CF6rxgHrvboCirm3k5sVJL2LF"  # 从您的 cookie 提取，或动态获取
        self.organization_id = "org_34CG0TGEYHYPPUEVC6xiY7qAFUO"
        self.token_url = f"https://clerk.cto.new/v1/client/sessions/{self.session_id}/tokens?__clerk_api_version=2025-04-10"
        
        self.conversation_cache: Dict[str, str] = {}
        self.jwt_cache = {"token": None, "expires_at": None}  # 新增：JWT 缓存
        self.current_cookie_index = 0  # 新增：多 cookie 轮询（若启用）
        
        if settings.MULTI_COOKIES_FILE:  # 从 .env 启用
            self.load_multi_cookies()
        
        logger.info("EngineLabsProvider 已初始化 (小白凤凰版 v4.1)，上下文修复完成。")

    def load_multi_cookies(self):
        """从 cookies.txt 加载多 cookie（借鉴项目2）"""
        try:
            with open(settings.MULTI_COOKIES_FILE, 'r') as f:
                self.cookies_pool = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            logger.info(f"加载 {len(self.cookies_pool)} 个 cookie。")
        except FileNotFoundError:
            logger.warning("cookies.txt 未找到，使用单 cookie。")

    async def _get_fresh_jwt(self) -> str:
        """优化：缓存 + 重试"""
        now = datetime.now()
        if self.jwt_cache["token"] and self.jwt_cache["expires_at"] and now < self.jwt_cache["expires_at"] - timedelta(minutes=1):
            logger.info("使用缓存 JWT。")
            return self.jwt_cache["token"]
        
        for attempt in range(3):  # 重试 3 次
            try:
                headers = {
                    "Cookie": self.clerk_cookie if not hasattr(self, 'cookies_pool') else self.cookies_pool[self.current_cookie_index % len(self.cookies_pool)],
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Origin": "https://cto.new",
                    "Referer": "https://cto.new/",
                }
                form_data = {"organization_id": self.organization_id}
                response = self.scraper.post(self.token_url, headers=headers, data=form_data)
                response.raise_for_status()
                data = response.json()
                new_jwt = data.get("jwt")
                if new_jwt:
                    self.jwt_cache = {"token": new_jwt, "expires_at": now + timedelta(hours=1)}  # 假设 1 小时有效
                    logger.info("成功获取/刷新 JWT。")
                    return new_jwt
            except Exception as e:
                logger.warning(f"JWT 获取失败 (尝试 {attempt+1}): {e}")
                if hasattr(self, 'cookies_pool'):
                    self.current_cookie_index += 1  # 轮询下一个 cookie
                await asyncio.sleep(2 ** attempt)  # 指数退避
        raise HTTPException(status_code=500, detail="JWT 获取失败，重试耗尽。")

    def _get_conversation_fingerprint(self, messages: list) -> str:
        """优化：哈希全历史消息（修复上下文丢失）"""
        if not messages:
            return "empty"
        # 全历史（包括当前消息），确保唯一性
        history_str = json.dumps(messages, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(history_str.encode('utf-8')).hexdigest()

    def _build_full_prompt_fallback(self, messages: list) -> str:
        """Fallback：如 ID 失效，用项目1 方式拼接全 prompt"""
        parts = []
        for msg in messages:
            role = msg.get("role", "unknown")
            content = msg.get("content", "")
            if isinstance(content, list):
                content = "".join([item.get("text", "") for item in content if item.get("type") == "text"])
            parts.append(f"{role.upper()}:\n{content}\n\n")
        return "".join(parts).strip()

    async def chat_completion(self, request_data: Dict[str, Any]) -> StreamingResponse:
        model = request_data.get("model", settings.DEFAULT_MODEL)
        messages = request_data.get("messages", [])
        stream = request_data.get("stream", True)

        if not messages:
            raise HTTPException(400, "messages 不能为空")

        jwt_token = await self._get_fresh_jwt()
        user_id = jwt.decode(jwt_token, options={"verify_signature": False}).get("sub")

        # 上下文指纹（全消息）
        fingerprint = self._get_conversation_fingerprint(messages)
        if fingerprint in self.conversation_cache:
            chat_history_id = self.conversation_cache[fingerprint]
            logger.info(f"复用上下文 ID: {chat_history_id}")
            prompt = messages[-1].get("content", "")  # 新消息
        else:
            chat_history_id = str(uuid.uuid4())
            self.conversation_cache[fingerprint] = chat_history_id
            logger.info(f"新建上下文 ID: {chat_history_id}")
            prompt = self._build_full_prompt_fallback(messages)  # Fallback 全 prompt

        async def stream_generator() -> AsyncGenerator[bytes, None]:
            request_id = f"chatcmpl-{uuid.uuid4()}"
            websocket_uri = f"wss://api.enginelabs.ai/engine-agent/chat-histories/{chat_history_id}/buffer/stream?token={user_id}"
            
            headers = {
                "Authorization": f"Bearer {jwt_token}",
                "Content-Type": "application/json",
                "Origin": "https://cto.new",
                "Referer": f"https://cto.new/{chat_history_id}",
            }
            payload = {"prompt": prompt, "chatHistoryId": chat_history_id, "adapterName": model}
            
            # 触发上游
            trigger_response = self.scraper.post(self.chat_url, headers=headers, json=payload)
            trigger_response.raise_for_status()

            # WebSocket 流（优化：处理 thinking/chat 类型，添加 <think>）
            in_thinking = False
            async with websockets.connect(websocket_uri) as websocket:
                yield create_sse_data(create_chat_completion_chunk(request_id, model, ""))
                
                while True:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=30)  # 超时重试
                        data = json.loads(message)
                        
                        if data.get("type") == "state" and not data.get("state", {}).get("inProgress"):
                            break
                        
                        if data.get("type") == "update":
                            buffer_data = json.loads(data.get("buffer", "{}"))
                            buffer_type = buffer_data.get("type")
                            content = buffer_data.get("chat", {}).get("content", "")
                            
                            if content:
                                if buffer_type == "thinking":
                                    if not in_thinking:
                                        content = "<think>" + content
                                        in_thinking = True
                                elif buffer_type == "chat" and in_thinking:
                                    content = "</think>" + content
                                    in_thinking = False
                                
                                chunk = create_chat_completion_chunk(request_id, model, content)
                                yield create_sse_data(chunk)
                    except asyncio.TimeoutError:
                        logger.warning("WebSocket 超时，重连...")
                        break
                    except Exception as e:
                        logger.error(f"WebSocket 错误: {e}")
                        break
            
            yield create_sse_data(create_chat_completion_chunk(request_id, model, "", "stop"))
            yield DONE_CHUNK

        if stream:
            return StreamingResponse(stream_generator(), media_type="text/event-stream")
        else:
            # 非流式：收集全内容（简化版）
            full_content = ""
            async for chunk in stream_generator():
                if b"data: " in chunk:
                    data = json.loads(chunk.decode().split("data: ")[1].strip())
                    full_content += data["choices"][0]["delta"].get("content", "")
            return JSONResponse({
                "id": request_id, "object": "chat.completion", "created": int(time.time()),
                "model": model, "choices": [{"index": 0, "message": {"role": "assistant", "content": full_content}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            })

    async def get_models(self) -> JSONResponse:
        return JSONResponse({
            "object": "list",
            "data": [{"id": name, "object": "model", "created": int(time.time()), "owned_by": "enginelabs"} for name in settings.KNOWN_MODELS]
        })
