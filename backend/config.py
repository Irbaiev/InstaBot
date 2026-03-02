# backend/config.py
"""
Все настройки приложения.
Меняй прямо здесь или через переменные окружения (.env).
"""
import os
from pathlib import Path

# ── Пути ────────────────────────────────────────────────────────────
BASE_DIR     = Path(__file__).parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"
LOGS_DIR     = BASE_DIR / "logs"
COOKIES_DIR  = BASE_DIR / "cookies"
CONFIG_FILE  = BASE_DIR / "config.yaml"

LOGS_DIR.mkdir(exist_ok=True)
COOKIES_DIR.mkdir(exist_ok=True)

# ── HTTP-сервер ─────────────────────────────────────────────────────
HOST = os.getenv("HOST", "127.0.0.1")
PORT = int(os.getenv("PORT", "8765"))        # ← порт меняй здесь

# ── Окно pywebview ──────────────────────────────────────────────────
WINDOW_TITLE = "InstaBot"
WINDOW_W     = 1200
WINDOW_H     = 800
WINDOW_MIN_W = 900
WINDOW_MIN_H = 640

# ── Приложение ──────────────────────────────────────────────────────
VERSION = "1.0.0"
