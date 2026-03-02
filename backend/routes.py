# backend/routes.py
"""
REST API + SSE endpoint.
Все URL начинаются с /api/...
"""
import asyncio
import json
from typing import AsyncIterator

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from backend.bot_engine import (
    Account, BotSettings, Post,
    bot, start_bot, stop_bot, save_state,
)
from backend.config import VERSION

router = APIRouter(prefix="/api")


# ── Models ───────────────────────────────────────────────────────────

class AccountIn(BaseModel):
    name: str
    cookies_file: str


class PostIn(BaseModel):
    url: str


class RunIn(BaseModel):
    model: str           = "llama3.2"
    max_parallel: int    = 3
    ollama_parallel: int = 1
    delay_min: float     = 5.0
    delay_max: float     = 15.0
    retries: int         = 3
    retry_delay: float   = 30.0
    mode: str            = "all"


# ── Health / meta ────────────────────────────────────────────────────

@router.get("/health")
async def health():
    return {"status": "ok"}


@router.get("/version")
async def version():
    return {"version": VERSION}


@router.get("/settings")
async def get_settings():
    return bot.last_settings


@router.put("/settings")
async def put_settings(body: RunIn):
    bot.last_settings = {
        "model": body.model,
        "max_parallel": body.max_parallel,
        "ollama_parallel": body.ollama_parallel,
        "delay_min": body.delay_min,
        "delay_max": body.delay_max,
        "retries": body.retries,
        "retry_delay": body.retry_delay,
        "mode": body.mode,
    }
    save_state()
    return {"ok": True}


# ── State (полный snapshot для фронта) ──────────────────────────────

@router.get("/state")
async def get_state():
    return bot.to_dict()


# ── Accounts ────────────────────────────────────────────────────────

@router.get("/accounts")
async def list_accounts():
    return [{"name": a.name, "cookies_file": a.cookies_file,
             "enabled": a.enabled, "status": a.status}
            for a in bot.accounts]


@router.post("/accounts", status_code=201)
async def add_account(body: AccountIn):
    if any(a.name == body.name for a in bot.accounts):
        raise HTTPException(400, f"Аккаунт '{body.name}' уже существует")
    bot.accounts.append(Account(name=body.name, cookies_file=body.cookies_file))
    save_state()
    return {"ok": True}


@router.delete("/accounts/{name}")
async def remove_account(name: str):
    before = len(bot.accounts)
    bot.accounts = [a for a in bot.accounts if a.name != name]
    if len(bot.accounts) == before:
        raise HTTPException(404, "Аккаунт не найден")
    save_state()
    return {"ok": True}


@router.patch("/accounts/{name}/toggle")
async def toggle_account(name: str):
    for a in bot.accounts:
        if a.name == name:
            a.enabled = not a.enabled
            save_state()
            return {"enabled": a.enabled}
    raise HTTPException(404, "Аккаунт не найден")


# ── Posts ────────────────────────────────────────────────────────────

@router.get("/posts")
async def list_posts():
    return [{"url": p.url, "shortcode": p.shortcode} for p in bot.posts]


@router.post("/posts", status_code=201)
async def add_post(body: PostIn):
    if any(p.url == body.url for p in bot.posts):
        raise HTTPException(400, "Пост уже добавлен")
    bot.posts.append(Post(url=body.url))
    save_state()
    return {"ok": True}


@router.delete("/posts/{idx}")
async def remove_post(idx: int):
    if idx < 0 or idx >= len(bot.posts):
        raise HTTPException(404, "Пост не найден")
    bot.posts.pop(idx)
    save_state()
    return {"ok": True}


# ── Run / Stop ───────────────────────────────────────────────────────

@router.post("/run")
async def run(body: RunIn):
    if bot.running:
        raise HTTPException(400, "Бот уже запущен")
    if not bot.accounts or not any(a.enabled for a in bot.accounts):
        raise HTTPException(400, "Нет активных аккаунтов")
    if not bot.posts:
        raise HTTPException(400, "Нет постов")

    settings = BotSettings(
        model=body.model,
        max_parallel=body.max_parallel,
        ollama_parallel=body.ollama_parallel,
        delay_min=body.delay_min,
        delay_max=body.delay_max,
        retries=body.retries,
        retry_delay=body.retry_delay,
        mode=body.mode,
    )
    bot.last_settings = {
        "model": body.model,
        "max_parallel": body.max_parallel,
        "ollama_parallel": body.ollama_parallel,
        "delay_min": body.delay_min,
        "delay_max": body.delay_max,
        "retries": body.retries,
        "retry_delay": body.retry_delay,
        "mode": body.mode,
    }
    save_state()
    start_bot(settings)
    bot.push_log("system",
        f"Запуск: режим={body.mode}, "
        f"аккаунтов={sum(1 for a in bot.accounts if a.enabled)}, "
        f"постов={len(bot.posts)}",
        "info")
    return {"ok": True}


@router.post("/stop")
async def stop():
    stop_bot()
    return {"ok": True}


# ── Progress ─────────────────────────────────────────────────────────

@router.get("/progress")
async def progress():
    return {
        "running": bot.running,
        "done":    bot.progress_done,
        "total":   bot.progress_total,
    }


# ── SSE log stream ───────────────────────────────────────────────────

@router.get("/logs/stream")
async def log_stream():
    """
    Server-Sent Events endpoint.
    Фронт подключается один раз и получает логи в реальном времени.
    """
    q = bot.subscribe()

    async def event_gen() -> AsyncIterator[str]:
        try:
            while True:
                try:
                    entry = await asyncio.wait_for(q.get(), timeout=15.0)
                    yield f"data: {json.dumps(entry, ensure_ascii=False)}\n\n"
                except asyncio.TimeoutError:
                    # keepalive ping чтобы соединение не разрывалось
                    yield ": ping\n\n"
        finally:
            bot.unsubscribe(q)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
