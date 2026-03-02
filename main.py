#!/usr/bin/env python3
# main.py
"""
Точка входа.

Запуск (prod — открывает окно):
    python main.py

Запуск (dev — только сервер, открывает браузер):
    python main.py --dev

После старта сервера:
    Swagger UI → http://localhost:8765/api/docs
"""
import argparse
import os
import struct
import sys
import threading
import time
import webbrowser

import uvicorn

from backend.app import app
from backend.config import HOST, PORT, WINDOW_TITLE, WINDOW_W, WINDOW_H, WINDOW_MIN_W, WINDOW_MIN_H

# ── App icon ─────────────────────────────────────────────────────────

def _ensure_icon(path: str):
    """Генерирует ICO с Instagram-градиентом без сторонних библиотек."""
    if os.path.exists(path):
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    size = 64
    pixels = bytearray()
    cx = cy = (size - 1) / 2
    r_outer = size / 2

    for y in range(size - 1, -1, -1):      # BMP: снизу вверх
        for x in range(size):
            dist = ((x - cx) ** 2 + (y - cy) ** 2) ** 0.5
            if dist > r_outer - 0.5:
                pixels += b'\x00\x00\x00\x00'  # прозрачно за кругом
            else:
                # Instagram: фиолетовый (#833AB4) → оранжевый (#F77737)
                t = (x / (size - 1) + (1 - y / (size - 1))) / 2
                r = int(131 + (247 - 131) * t)
                g = int(58  + (119 - 58)  * t)
                b = int(180 + (55  - 180) * t)
                pixels += bytes([b, g, r, 255])  # BGRA

    bih = struct.pack('<IiiHHIIiiII',
        40, size, size, 1, 32, 0, size * size * 4, 0, 0, 0, 0)
    image_data = bih + bytes(pixels)

    direntry = struct.pack('<BBBBHHII',
        size, size, 0, 0, 1, 32, len(image_data), 6 + 16)
    icondir = struct.pack('<HHH', 0, 1, 1)

    with open(path, 'wb') as f:
        f.write(icondir + direntry + image_data)


ICON_PATH = os.path.join(os.path.dirname(__file__), 'frontend', 'assets', 'icon.ico')


# ── Server ───────────────────────────────────────────────────────────

_server: uvicorn.Server | None = None


def _run_server(dev: bool = False):
    global _server
    try:
        config = uvicorn.Config(
            app,
            host=HOST,
            port=PORT,
            log_level="info" if dev else "warning",
            reload=False,          # reload не работает внутри треда — используй dev-режим
        )
        _server = uvicorn.Server(config)
        _server.run()
    except Exception as e:
        print(f"[!] Ошибка запуска сервера: {e}")


def _wait_for_server(timeout: float = 15.0) -> bool:
    """Ждём пока uvicorn не поднимет сервер (проверяем флаг server.started)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _server is not None and _server.started:
            return True
        time.sleep(0.1)
    return False


def _stop_server():
    if _server:
        _server.should_exit = True


# ── pywebview window ─────────────────────────────────────────────────

class _JsApi:
    """Методы, доступные из JS через window.pywebview.api.*"""

    def pick_file(self):
        """Открывает диалог выбора файла, возвращает путь или None."""
        try:
            import tkinter as tk
            from tkinter import filedialog
            root = tk.Tk()
            root.withdraw()
            root.wm_attributes("-topmost", True)
            path = filedialog.askopenfilename(
                title="Выбери файл с куками",
                filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
            )
            root.destroy()
            return path or None
        except Exception:
            return None


def _run_window():
    try:
        import webview
    except ImportError:
        print("[!] pywebview не установлен: pip install pywebview")
        _stop_server()
        sys.exit(1)

    url = f"http://{HOST}:{PORT}/"

    window = webview.create_window(
        title     = WINDOW_TITLE,
        url       = url,
        width     = WINDOW_W,
        height    = WINDOW_H,
        min_size  = (WINDOW_MIN_W, WINDOW_MIN_H),
        resizable = True,
        js_api    = _JsApi(),
        # text_select = True,    # раскомментируй если нужно выделение текста
    )

    def on_closed():
        _stop_server()

    window.events.closed += on_closed

    _ensure_icon(ICON_PATH)

    def _set_win32_icon():
        """Принудительно ставит иконку через Win32 API (Windows)."""
        try:
            import ctypes
            hwnd = ctypes.windll.user32.GetForegroundWindow()
            icon = ctypes.windll.user32.LoadImageW(
                None, ICON_PATH, 1,  # IMAGE_ICON
                0, 0, 0x10 | 0x40   # LR_LOADFROMFILE | LR_DEFAULTSIZE
            )
            if icon:
                WM_SETICON = 0x0080
                ctypes.windll.user32.SendMessageW(hwnd, WM_SETICON, 0, icon)  # small
                ctypes.windll.user32.SendMessageW(hwnd, WM_SETICON, 1, icon)  # big
        except Exception:
            pass

    window.events.loaded += _set_win32_icon

    # gui='edgechromium' на Windows даёт лучший рендеринг
    # gui='gtk' на Linux, gui='cocoa' на Mac — выбирается автоматически
    webview.start(debug=False, icon=ICON_PATH)


# ── Entry ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="InstaBot Desktop App")
    parser.add_argument("--dev", action="store_true",
                        help="Dev-режим: запустить только сервер, открыть браузер")
    args = parser.parse_args()

    print(f"[InstaBot] Запуск сервера на http://{HOST}:{PORT} ...")

    # Запускаем сервер в фоновом треде
    server_thread = threading.Thread(target=_run_server,
                                     args=(args.dev,), daemon=True)
    server_thread.start()

    # Ждём пока поднимется
    if not _wait_for_server():
        print("[!] Сервер не ответил за 15 секунд — выход")
        sys.exit(1)

    print(f"[InstaBot] Сервер готов ✓")

    if args.dev:
        url = f"http://{HOST}:{PORT}/"
        print(f"[InstaBot] Dev-режим — открываю браузер: {url}")
        print(f"[InstaBot] Swagger UI: http://{HOST}:{PORT}/api/docs")
        webbrowser.open(url)
        # Держим процесс живым
        try:
            server_thread.join()
        except KeyboardInterrupt:
            print("\n[InstaBot] Остановка (Ctrl+C)")
            _stop_server()
    else:
        print(f"[InstaBot] Открываю окно приложения...")
        _run_window()
        print("[InstaBot] Окно закрыто, завершение")


if __name__ == "__main__":
    main()
