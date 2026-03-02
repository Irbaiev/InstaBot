# backend/app.py
"""
FastAPI application.
Раздаёт статику (frontend/) и REST API (/api/).
"""
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from backend.config import FRONTEND_DIR
from backend.bot_engine import load_state
from backend.routes import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_state()
    yield


# ── App ──────────────────────────────────────────────────────────────
app = FastAPI(
    title="InstaBot API",
    docs_url="/api/docs",    # Swagger UI — http://localhost:8765/api/docs
    redoc_url=None,
    lifespan=lifespan,
)

# ── CORS (нужен только для dev-режима когда фронт на другом порту) ───
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],     # в prod можно зажать до localhost:PORT
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── API routes ───────────────────────────────────────────────────────
app.include_router(router)

# ── Статика frontend/ ────────────────────────────────────────────────
if FRONTEND_DIR.exists():
    # Монтируем /assets если есть
    assets = FRONTEND_DIR / "assets"
    if assets.exists():
        app.mount("/assets", StaticFiles(directory=str(assets)), name="assets")

    @app.get("/")
    async def index():
        return FileResponse(str(FRONTEND_DIR / "index.html"))

    # Всё остальное → index.html (SPA fallback)
    @app.get("/{full_path:path}")
    async def spa_fallback(full_path: str):
        file = FRONTEND_DIR / full_path
        if file.exists() and file.is_file():
            return FileResponse(str(file))
        return FileResponse(str(FRONTEND_DIR / "index.html"))
