# backend/bot_engine.py
"""
Ядро бота: асинхронный движок с поддержкой SSE-логов.
Вся бизнес-логика здесь — UI и API её только запускают.
"""
import asyncio
import base64 as _base64
import json
import logging
import os
import random
import re
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import AsyncIterator

from backend.config import BASE_DIR, COOKIES_DIR, LOGS_DIR, CONFIG_FILE

# ── Optional deps ────────────────────────────────────────────────────
try:
    import aiohttp
    AIOHTTP_OK = True
except ImportError:
    AIOHTTP_OK = False

try:
    from PIL import Image as _PILImage
    import io as _io
    PIL_OK = True
except ImportError:
    PIL_OK = False

OLLAMA_URL = "http://127.0.0.1:11434/api/chat"

# ── Logging ──────────────────────────────────────────────────────────

def make_file_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(f"bot.{name}")
    if logger.handlers:
        return logger
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S")
    fh = logging.FileHandler(LOGS_DIR / f"{name}.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    eh = logging.FileHandler(LOGS_DIR / "errors.log", encoding="utf-8")
    eh.setLevel(logging.ERROR)
    eh.setFormatter(logging.Formatter(f"[{name}] %(asctime)s %(message)s", "%H:%M:%S"))
    logger.addHandler(eh)
    return logger


# ── Data models ──────────────────────────────────────────────────────

@dataclass
class Account:
    name: str
    cookies_file: str
    enabled: bool = True
    status: str = "idle"   # idle | running | ok | error


@dataclass
class Post:
    url: str

    @property
    def shortcode(self) -> str:
        m = re.search(r"/(?:p|reel|tv)/([A-Za-z0-9_-]+)", self.url)
        return m.group(1) if m else self.url

    @property
    def clean_url(self) -> str:
        """URL без UTM-параметров и лишних слэшей."""
        return self.url.split("?")[0].rstrip("/") + "/"


@dataclass
class RunResult:
    account: str
    ok: int = 0
    err: int = 0
    total: int = 0


@dataclass
class BotSettings:
    model: str         = "llama3.2"
    max_parallel: int  = 3
    ollama_parallel: int = 1   # сколько аккаунтов одновременно используют Ollama
    delay_min: float   = 5.0
    delay_max: float   = 15.0
    retries: int       = 3
    retry_delay: float = 30.0
    mode: str          = "all"   # all | round_robin
    stress_test: bool  = False


# Ollama семафор — контролирует сколько аккаунтов одновременно генерируют комментарий
_ollama_sem: asyncio.Semaphore | None = None

def _get_ollama_sem() -> asyncio.Semaphore:
    global _ollama_sem
    if _ollama_sem is None:
        _ollama_sem = asyncio.Semaphore(1)
    return _ollama_sem

def _reset_ollama_sem(parallel: int):
    global _ollama_sem
    _ollama_sem = asyncio.Semaphore(max(1, parallel))


# ── Global bot state ─────────────────────────────────────────────────

class BotState:
    def __init__(self):
        self.accounts: list[Account]     = []
        self.posts: list[Post]           = []
        self.results: dict[str, RunResult] = {}
        self.running: bool               = False
        self.stop_event: asyncio.Event | None = None
        self._task: asyncio.Task | None  = None
        self.progress_done: int          = 0
        self.progress_total: int         = 0
        self.last_settings: dict         = {}
        # SSE log broadcast
        self._subscribers: list[asyncio.Queue] = []

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=500)
        self._subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue):
        try:
            self._subscribers.remove(q)
        except ValueError:
            pass

    def push_log(self, acc: str, msg: str, level: str = "info"):
        import time
        entry = {"ts": time.strftime("%H:%M:%S"), "acc": acc,
                 "msg": msg, "level": level}
        for q in list(self._subscribers):
            try:
                q.put_nowait(entry)
            except asyncio.QueueFull:
                pass
        # also file-log
        make_file_logger(acc).info(msg)

    def to_dict(self) -> dict:
        return {
            "running":        self.running,
            "progress_done":  self.progress_done,
            "progress_total": self.progress_total,
            "accounts": [
                {"name": a.name, "cookies_file": a.cookies_file,
                 "enabled": a.enabled, "status": a.status}
                for a in self.accounts
            ],
            "posts": [{"url": p.url, "shortcode": p.shortcode} for p in self.posts],
            "results": {
                name: {"ok": r.ok, "err": r.err, "total": r.total}
                for name, r in self.results.items()
            },
        }


bot = BotState()


# ── Persistence (config.yaml) ─────────────────────────────────────────

def load_state():
    """Загружает аккаунты и посты из config.yaml при старте."""
    try:
        import yaml
    except ImportError:
        return
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except FileNotFoundError:
        return
    for acc in data.get("accounts", []):
        bot.accounts.append(Account(
            name=acc["name"],
            cookies_file=acc["cookies_file"],
            enabled=acc.get("enabled", True),
        ))
    for url in data.get("posts", []):
        bot.posts.append(Post(url=url))
    bot.last_settings = data.get("settings", {})


def save_state():
    """Сохраняет текущие аккаунты и посты в config.yaml."""
    try:
        import yaml
    except ImportError:
        return
    data = {
        "accounts": [
            {"name": a.name, "cookies_file": a.cookies_file, "enabled": a.enabled}
            for a in bot.accounts
        ],
        "posts": [p.url for p in bot.posts],
        "settings": bot.last_settings,
    }
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        yaml.dump(data, f, allow_unicode=True, default_flow_style=False)


# ── Instagram helpers ────────────────────────────────────────────────

BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":          "*/*",
    "Accept-Language": "ru-RU,ru;q=0.9",
    "X-IG-App-ID":     "936619743392459",
}

FALLBACK = {
    "beauty":  ["Очень красиво! 😍", "Вау, макияж огонь 🔥", "Выглядит шикарно!"],
    "food":    ["Выглядит очень вкусно! 😋", "М-м-м, аппетитно!", "Надо попробовать!"],
    "nature":  ["Какая красота! 😮", "Невероятный вид!", "Где это? Очень круто!"],
    "selfie":  ["Отличное фото! 🔥", "Очень круто выглядишь!", "Огонь! ❤️"],
    "gaming":  ["Красавчик! 🎮", "Топ игрок! 🔥", "Жёстко сыграно!", "Лучшее! 👏"],
    "sport":   ["Сила! 💪", "Топ результат!", "Так держать! 🔥", "Уважение! 👏"],
    "default": ["Огонь! 🔥", "Нравится!", "Круто!", "Класс! 👏", "Топ контент! ✨"],
}

# Категории: (ключевые слова EN, ключевые слова RU)
_CATEGORIES = [
    ("beauty",  ["lipstick", "makeup", "beauty", "mascara", "skincare", "eyeshadow", "blush", "foundation"],
                ["макияж", "помада", "тени", "скинкер", "бьюти", "косметик"]),
    ("food",    ["food", "eat", "dinner", "lunch", "breakfast", "recipe", "cook", "restaurant", "pizza", "burger", "sushi"],
                ["еда", "вкусно", "рецепт", "ресторан", "готовл", "обед", "ужин", "завтрак"]),
    ("nature",  ["sunset", "beach", "mountain", "ocean", "forest", "landscape", "nature", "travel", "sky"],
                ["закат", "пляж", "горы", "природа", "путешест", "лес", "море", "небо"]),
    ("gaming",  ["game", "gaming", "gamer", "minecraft", "fortnite", "valorant", "cs2", "pubg", "stream", "twitch", "playstation", "xbox"],
                ["игр", "геймер", "стрим", "киберспорт"]),
    ("sport",   ["gym", "fitness", "workout", "sport", "training", "run", "football", "basketball", "boxing", "crossfit"],
                ["спорт", "трениров", "качалк", "фитнес", "зал", "бег", "футбол", "баскетбол"]),
    ("selfie",  ["selfie", "ootd", "outfit", "fashion", "style", "portrait"],
                ["образ", "стиль", "селфи", "аутфит"]),
]


def fallback_comment(post_info: dict) -> str:
    """Шаблонный комментарий с учётом хэштегов и подписи."""
    caption = post_info.get("caption", "")
    hashtags = [h.lower() for h in post_info.get("hashtags", [])]
    cl = caption.lower() + " " + " ".join(hashtags)
    for category, en_keys, ru_keys in _CATEGORIES:
        if any(k in cl for k in en_keys) or any(k in cl for k in ru_keys):
            return random.choice(FALLBACK[category])
    return random.choice(FALLBACK["default"])


_VISION_PREFIXES = ("llava", "moondream", "bakllava", "qwen2-vl", "qwen2.5vl", "minicpm-v")

def _model_has_vision(model: str) -> bool:
    m = model.lower()
    # gemma3 4b/12b/27b — все vision, кроме 1b
    if "gemma3" in m and ":1b" not in m:
        return True
    return any(p in m for p in _VISION_PREFIXES)


async def _fetch_image_b64(url: str, session=None) -> str | None:
    """Скачивает изображение и возвращает base64-строку."""
    try:
        if session is not None:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    return _base64.b64encode(await r.read()).decode()
                make_file_logger("system").debug(f"Image fetch {r.status}: {url[:80]}")
        else:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status == 200:
                        return _base64.b64encode(await r.read()).decode()
    except Exception as e:
        make_file_logger("system").debug(f"Image fetch error: {e}")
    return None


async def _fetch_media_info(session, csrf: str, media_id: str) -> dict | None:
    """Один запрос к /media/{id}/info/ — кэшируемый результат для image/video/etc."""
    url = f"https://www.instagram.com/api/v1/media/{media_id}/info/"
    headers = {**BASE_HEADERS, "X-CSRFToken": csrf or ""}
    try:
        async with session.get(url, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                data = await r.json()
                items = data.get("items", [])
                if items:
                    return items[0]
    except Exception:
        pass
    return None


async def _fetch_media_image_url(session, csrf: str, media_id: str,
                                  _media_item: dict | None = None) -> str | None:
    """Получает прямую ссылку на изображение. Использует _media_item если передан."""
    item = _media_item or await _fetch_media_info(session, csrf, media_id)
    if not item:
        return None
    carousel = item.get("carousel_media", [])
    src = carousel[0] if carousel else item
    candidates = src.get("image_versions2", {}).get("candidates", [])
    if candidates:
        return candidates[0]["url"]
    return None


async def _fetch_top_comments(session, csrf: str, media_id: str,
                              count: int = 8) -> list[str]:
    """Получает топ-комментарии под постом для контекста в промпте."""
    url = (f"https://www.instagram.com/api/v1/media/{media_id}/comments/"
           f"?can_support_threading=true&permalink_enabled=false")
    headers = {**BASE_HEADERS, "X-CSRFToken": csrf or ""}
    try:
        async with session.get(url, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                data = await r.json()
                comments = data.get("comments", [])
                return [c["text"] for c in comments[:count] if c.get("text")]
    except Exception:
        pass
    return []


async def _fetch_video_url(session, csrf: str, media_id: str,
                            _media_item: dict | None = None) -> str | None:
    """Получает прямую ссылку на видео. Использует _media_item если передан."""
    item = _media_item or await _fetch_media_info(session, csrf, media_id)
    if not item:
        return None
    videos = item.get("video_versions", [])
    if videos:
        return videos[0]["url"]
    return None


async def _extract_video_frame(video_url: str, session=None,
                                seek_sec: float = 2.0) -> str | None:
    """Скачивает видео и извлекает кадр через ffmpeg. Возвращает base64."""
    if not shutil.which("ffmpeg"):
        return None
    tmp_video = tmp_frame = None
    try:
        fd, tmp_video = tempfile.mkstemp(suffix=".mp4")
        os.close(fd)
        tmp_frame = tmp_video + ".jpg"

        # Скачиваем видео
        async def _dl(s):
            async with s.get(video_url,
                             timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    return False
                with open(tmp_video, "wb") as f:
                    f.write(await r.read())
                return True

        if session is not None:
            if not await _dl(session):
                return None
        else:
            async with aiohttp.ClientSession() as s:
                if not await _dl(s):
                    return None

        # Извлекаем кадр: seek → 1 frame
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-ss", str(seek_sec), "-i", tmp_video,
            "-frames:v", "1", "-q:v", "2", tmp_frame, "-y",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.wait_for(proc.wait(), timeout=15.0)

        if os.path.exists(tmp_frame) and os.path.getsize(tmp_frame) > 100:
            with open(tmp_frame, "rb") as f:
                return _base64.b64encode(f.read()).decode()
    except Exception as e:
        make_file_logger("system").debug(f"Video frame extraction: {e}")
    finally:
        for p in (tmp_video, tmp_frame):
            if p:
                try:
                    os.unlink(p)
                except OSError:
                    pass
    return None


_ollama_session: aiohttp.ClientSession | None = None

def _get_ollama_session() -> aiohttp.ClientSession:
    global _ollama_session
    if _ollama_session is None or _ollama_session.closed:
        _ollama_session = aiohttp.ClientSession()
    return _ollama_session

async def _close_ollama_session():
    global _ollama_session
    if _ollama_session and not _ollama_session.closed:
        await _ollama_session.close()
        _ollama_session = None

async def _ollama_request(model: str, prompt: str,
                          timeout: float = 120.0,
                          image_b64: str | None = None,
                          max_tokens: int = 0) -> str:
    """Прямой HTTP запрос к Ollama API. Возвращает текст или бросает исключение."""
    message: dict = {"role": "user", "content": prompt}
    if image_b64:
        message["images"] = [image_b64]
    opts: dict = {"temperature": 0.9, "top_p": 0.95}
    if max_tokens > 0:
        opts["num_predict"] = max_tokens
    session = _get_ollama_session()
    async with session.post(
        OLLAMA_URL,
        json={"model": model, "messages": [message], "stream": False,
              "options": opts},
        timeout=aiohttp.ClientTimeout(total=timeout),
    ) as resp:
        if resp.status == 200:
            data = await resp.json()
            return data["message"]["content"]
        body = await resp.text()
        raise RuntimeError(f"HTTP {resp.status}: {body[:200]}")


async def _describe_image(model: str, image_b64: str) -> str:
    """Один запрос: описание картинки + OCR. Быстро и точно."""
    return await _ollama_request(model, (
        "Посмотри на изображение. Коротко:\n"
        "1) Что изображено (люди, место, действие)\n"
        "2) Прочитай ВЕСЬ текст на картинке если есть\n"
        "По-русски, только факты, 2-4 предложения."
    ), image_b64=image_b64, max_tokens=200)


_COMMENT_STYLES = [
    # (подход, длина_фраза, макс_предложений)
    ("просто коротко отреагируй — первое что в голову пришло", "1-2 предложения", 2),
    ("задай вопрос автору — тебе реально интересно", "1-2 предложения", 2),
    ("поделись коротким личным опытом по теме", "2-3 предложения", 3),
    ("выскажи мнение — что зацепило и почему", "2-3 предложения", 3),
    ("напиши развёрнутый отклик как другу", "3-4 предложения", 4),
    ("заметь деталь на фото/видео и отреагируй", "2-3 предложения", 3),
]

_PERSONAS = [
    "обычный подписчик, листаешь ленту",
    "давний фанат этого блогера",
    "случайный человек, пост попался в рекомендациях",
    "давний подписчик, следишь давно",
    "человек со схожими интересами",
]

# Few-shot примеры живых комментариев для обучения модели стилю
_FEW_SHOT_EXAMPLES = [
    "ааа ну вот это я понимаю контент",
    "жиза, у меня так же было на прошлой неделе",
    "подожди это реально?? я думал фотошоп",
    "вот это поворот, не ожидал честно",
    "а где это снято? хочу тоже туда",
    "ахах точно, знакомое чувство",
    "слушай а как ты этого добился? реально интересно",
    "у меня аж мурашки, атмосфера нереальная",
    "это прям то что мне нужно было сегодня увидеть",
    "напомнило мне одну историю, расскажу в лс",
    "ну ты даёшь, я бы так не смог",
    "кааайф, вот бы так каждый день",
    "а я как раз думал об этом вчера",
    "мощно получилось, видно что старался",
    "о, я тоже такое люблю, понимаю тебя",
]


def _build_prompt(post_info: dict, account_name: str = "") -> tuple[str, int]:
    """Возвращает (prompt, max_sentences)."""
    caption   = post_info.get("caption", "").strip()
    author    = post_info.get("author", "unknown")
    hashtags  = post_info.get("hashtags", [])
    post_type = post_info.get("post_type", "photo")
    top_comments = post_info.get("top_comments", [])

    type_ru = {"reel": "видео (Reel)", "igtv": "видео (IGTV)", "photo": "фото"}.get(post_type, "пост")
    tags_str = " ".join(f"#{h}" for h in hashtags[:15]) if hashtags else "нет"
    caption_str = f'"{caption}"' if caption else "(подпись отсутствует)"

    angle, length_hint, max_sent = random.choice(_COMMENT_STYLES)
    persona = random.choice(_PERSONAS)

    # 2 рандомных few-shot примера (меньше = быстрее)
    examples = random.sample(_FEW_SHOT_EXAMPLES, 2)
    examples_str = " | ".join(examples)

    # Контекст: реальные комментарии (до 5, коротко)
    comments_block = ""
    if top_comments:
        sample = random.sample(top_comments, min(5, len(top_comments)))
        comments_block = "\nКомменты под постом: " + " | ".join(c[:80] for c in sample) + "\n"

    # Описание картинки (заполняется на шаге 1 — _describe_image)
    vision_hint = ""
    img_desc = post_info.get("image_description", "")
    if img_desc:
        vision_hint = f"\nНа фото: {img_desc}\n"

    prompt = f"""Ты — {persona}. Комментарий под постом @{author} ({type_ru}).
Подпись: {caption_str}
{comments_block}{vision_hint}
{angle}. {length_hint}.
Примеры стиля: {examples_str}
Без хэштегов, без "круто/огонь/класс/топ", макс 1 эмодзи. Живой разговорный русский. Только комментарий:"""

    return prompt, max_sent


async def generate_comment(post_info: dict, model: str,
                           account_name: str = "",
                           image_b64: str | None = None) -> tuple[str, str]:
    """Возвращает (текст_комментария, источник) где источник: 'ollama' | 'fallback'."""
    if not AIOHTTP_OK:
        return fallback_comment(post_info), "fallback"
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            async with _get_ollama_sem():
                # Шаг 1: описание картинки (пропускаем если уже есть из кэша)
                if image_b64 and not post_info.get("image_description"):
                    description = await _describe_image(model, image_b64)
                    post_info["image_description"] = description.strip()
                    make_file_logger("system").debug(
                        f"Vision description: {description[:200]}")

                # Шаг 2: генерация комментария (по описанию, без картинки)
                prompt, max_sentences = _build_prompt(post_info, account_name)
                text = await _ollama_request(model, prompt, max_tokens=300)
            text = text.strip().strip("\"'«»")
            # Убираем типичные артефакты LLM
            for prefix in ("Комментарий:", "Comment:", "Ответ:"):
                if text.lower().startswith(prefix.lower()):
                    text = text[len(prefix):].strip()
            sentences = re.split(r'(?<=[.!?])\s+', text)
            text = " ".join(sentences[:max_sentences]).strip()
            if len(text) > 1500:
                text = text[:1497] + "..."
            return text, "ollama"
        except Exception as e:
            err_str = str(e)
            if ("503" in err_str or "connect" in err_str.lower()) and attempt < max_retries:
                make_file_logger("system").info(
                    f"Ollama 503, ретрай {attempt}/{max_retries} через 10с")
                await asyncio.sleep(10)
                continue
            make_file_logger("system").warning(f"Ollama error (model={model}): {e}")
            return fallback_comment(post_info), "fallback"


def resolve_cookies_path(filepath: str) -> str | None:
    raw = (filepath or "").strip().strip("\"'")
    if not raw:
        return None

    path = Path(raw).expanduser()
    candidates = [path]
    if not path.is_absolute():
        candidates.extend([
            BASE_DIR / path,
            COOKIES_DIR / path,
            COOKIES_DIR / path.name,
        ])

    checked = set()
    for candidate in candidates:
        key = str(candidate)
        if key in checked:
            continue
        checked.add(key)
        if candidate.is_file():
            return str(candidate)
    return None


def load_cookies(filepath: str):
    cookies, csrf, uid = {}, None, None
    resolved = resolve_cookies_path(filepath)
    if not resolved:
        return cookies, csrf, uid

    try:
        with open(resolved, "r", encoding="utf-8") as f:
            raw = f.read()
    except FileNotFoundError:
        return cookies, csrf, uid
    except UnicodeDecodeError:
        with open(resolved, "r", encoding="utf-8-sig", errors="replace") as f:
            raw = f.read()

    text = raw.strip()
    if not text:
        return cookies, csrf, uid

    # JSON export from browser extensions: [{"name":"...","value":"..."}]
    if text.startswith("[") or text.startswith("{"):
        try:
            data = json.loads(text)
            json_items = []
            if isinstance(data, list):
                json_items = data
            elif isinstance(data, dict):
                if isinstance(data.get("cookies"), list):
                    json_items = data["cookies"]
                elif isinstance(data.get("Cookies"), list):
                    json_items = data["Cookies"]
                else:
                    for name, value in data.items():
                        if isinstance(value, str):
                            cookies[str(name)] = value
            for item in json_items:
                if not isinstance(item, dict):
                    continue
                name = item.get("name")
                value = item.get("value")
                if isinstance(name, str) and isinstance(value, str):
                    cookies[name] = value
        except json.JSONDecodeError:
            pass

    # Netscape export format (.txt)
    if not cookies:
        for line in raw.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) < 7:
                continue
            name, value = parts[5], parts[6]
            cookies[name] = value

    csrf = cookies.get("csrftoken")
    uid = cookies.get("ds_user_id")
    return cookies, csrf, uid


async def verify_cookies(session) -> bool:
    try:
        async with session.get(
            "https://www.instagram.com/accounts/edit/",
            headers=BASE_HEADERS,
            timeout=aiohttp.ClientTimeout(total=10),
            allow_redirects=False,
        ) as r:
            return r.status == 200
    except Exception:
        return False


_SC_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_'


def _shortcode_to_media_id(shortcode: str) -> str:
    """Декодирует shortcode Instagram в media_id (детерминированный алгоритм)."""
    n = 0
    for char in shortcode:
        n = n * 64 + _SC_ALPHABET.index(char)
    return str(n)


async def get_post_info(session, csrf: str, shortcode: str, post_url: str = "") -> dict:
    url = post_url if post_url else f"https://www.instagram.com/p/{shortcode}/"
    headers = {**BASE_HEADERS, "X-CSRFToken": csrf or ""}
    async with session.get(url, headers=headers,
                           timeout=aiohttp.ClientTimeout(total=20)) as r:
        html = await r.text()

    # Сначала пробуем вытащить из HTML (старый формат страниц)
    media_id = None
    for pat in [r'"media_id"\s*:\s*"(\d+)"',
                r'"media_id"\s*:\s*(\d+)',
                r'"pk"\s*:\s*"?(\d+)"?']:
        m = re.search(pat, html)
        if m:
            media_id = m.group(1)
            break

    # Fallback: вычисляем из shortcode (работает для Reels и новых постов)
    if not media_id:
        try:
            media_id = _shortcode_to_media_id(shortcode)
        except (ValueError, IndexError):
            pass

    caption = ""
    m = re.search(r'<meta property="og:description" content="([^"]*)"', html)
    if m:
        caption = m.group(1)
        if " on Instagram: " in caption:
            caption = caption.split(" on Instagram: ", 1)[1]

    username = "unknown"
    m = re.search(r'"username"\s*:\s*"([^"]+)"', html)
    if m:
        username = m.group(1)

    # Хэштеги из подписи — основной сигнал о теме поста
    hashtags = re.findall(r'#([A-Za-z0-9а-яА-ЯёЁ_]+)', caption)

    # Тип поста из URL
    if "/reel/" in (post_url or ""):
        post_type = "reel"
    elif "/tv/" in (post_url or ""):
        post_type = "igtv"
    else:
        post_type = "photo"

    # Thumbnail для vision-анализа
    thumbnail_url = ""
    m = re.search(r'<meta property="og:image" content="([^"]*)"', html)
    if m:
        thumbnail_url = m.group(1)

    return {"shortcode": shortcode, "media_id": media_id,
            "caption": caption, "author": username,
            "hashtags": hashtags, "post_type": post_type,
            "thumbnail_url": thumbnail_url}


async def send_comment(session, csrf: str, media_id: str, text: str,
                       retries: int = 3, retry_delay: float = 30,
                       acc_name: str = "") -> tuple[bool, str]:
    """Отправляет комментарий. Возвращает (success, reason)."""
    endpoint = f"https://www.instagram.com/api/v1/web/comments/{media_id}/add/"
    headers = {
        **BASE_HEADERS,
        "Content-Type":    "application/x-www-form-urlencoded",
        "Origin":          "https://www.instagram.com",
        "Referer":         "https://www.instagram.com/",
        "X-CSRFToken":     csrf,
        "X-Requested-With":"XMLHttpRequest",
    }
    for attempt in range(1, retries + 1):
        try:
            async with session.post(
                endpoint, headers=headers,
                data={"comment_text": text},
                timeout=aiohttp.ClientTimeout(total=15)
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    if data.get("status") == "ok" or "comment" in data:
                        return True, "ok"
                    # Instagram вернул 200 но с ошибкой в теле
                    msg = data.get("message", str(data)[:200])
                    return False, f"API: {msg}"
                elif r.status == 429:
                    if attempt < retries:
                        make_file_logger(acc_name or "system").info(
                            f"429 rate limit, ретрай {attempt}/{retries} через {retry_delay}с")
                        await asyncio.sleep(retry_delay)
                        continue
                    return False, "429 rate limit (все ретраи)"
                else:
                    body = await r.text()
                    # Типичные ошибки Instagram
                    if r.status == 400:
                        if "feedback_required" in body:
                            return False, "feedback_required (временный бан)"
                        if "commenting is turned off" in body.lower() or \
                           "comments_disabled" in body or \
                           "commenting has been turned off" in body.lower():
                            return False, "comments_disabled"
                        if "spam" in body.lower():
                            return False, "spam_detected"
                    return False, f"HTTP {r.status}: {body[:150]}"
        except Exception as e:
            if attempt < retries:
                await asyncio.sleep(5)
            else:
                return False, f"Exception: {e}"
    return False, "все ретраи исчерпаны"


@dataclass
class CachedPostData:
    info: dict = field(default_factory=dict)
    image_b64: str | None = None
    img_source: str = ""
    comments_disabled: bool = False  # если True — все аккаунты пропускают этот пост


# ── Per-account worker ───────────────────────────────────────────────

async def account_worker(account: Account, posts: list[Post],
                         settings: BotSettings, result: RunResult,
                         post_cache: dict[str, CachedPostData] | None = None):
    acc = account.name
    bot.push_log(acc, f"Старт — {len(posts)} постов", "info")

    # ── Stress-test: симуляция без реальных запросов ──
    if settings.stress_test:
        for post in posts:
            if bot.stop_event and bot.stop_event.is_set():
                break
            bot.push_log(acc, f"[TEST] /p/{post.shortcode}")
            await asyncio.sleep(random.uniform(0.2, 0.6))
            ok = random.random() > 0.1
            comment = fallback_comment({})
            if ok:
                bot.push_log(acc, f'✓ «{comment}»', "ok")
                result.ok += 1
            else:
                bot.push_log(acc, "✗ Simulated 429", "error")
                result.err += 1
            result.total += 1
            bot.progress_done += 1
            await asyncio.sleep(random.uniform(settings.delay_min * 0.1,
                                               settings.delay_max * 0.1))
        account.status = "ok" if result.err == 0 else "error"
        bot.push_log(acc, f"Готово — ✓{result.ok} ✗{result.err}",
            "ok" if result.err == 0 else "warn")
        return

    cookies, csrf, uid = load_cookies(account.cookies_file)
    if not cookies:
        bot.push_log(acc, f"Файл куки не найден: {account.cookies_file}", "error")
        account.status = "error"
        result.err = len(posts)
        return

    bot.push_log(acc, f"Куки загружены ({len(cookies)} шт.), uid={uid}", "info")

    if not AIOHTTP_OK:
        # ── Simulation mode (aiohttp не установлен) ──
        for post in posts:
            if bot.stop_event and bot.stop_event.is_set():
                break
            bot.push_log(acc, f"[SIM] /p/{post.shortcode}", "info")
            await asyncio.sleep(random.uniform(0.4, 1.0))
            ok = random.random() > 0.15
            comment = fallback_comment({})
            if ok:
                bot.push_log(acc, f'✓ Комментарий: «{comment}»', "ok")
                result.ok += 1
            else:
                bot.push_log(acc, "✗ HTTP 429 — rate limit", "error")
                result.err += 1
            result.total += 1
            bot.progress_done += 1
            await asyncio.sleep(random.uniform(settings.delay_min * 0.1,
                                               settings.delay_max * 0.1))
        account.status = "ok" if result.err == 0 else "error"
        return

    # ── Real mode (с использованием кэша пре-фетча) ──────────────
    async with aiohttp.ClientSession() as session:
        for name, val in cookies.items():
            session.cookie_jar.update_cookies({name: val})

        if not await verify_cookies(session):
            bot.push_log(acc, "Куки просрочены — аккаунт пропущен", "error")
            account.status = "error"
            result.err = len(posts)
            return

        for post in posts:
            if bot.stop_event and bot.stop_event.is_set():
                break
            bot.push_log(acc, f"Обрабатываю /p/{post.shortcode}")
            try:
                # Используем кэш если есть, иначе фетчим заново
                cached = post_cache.get(post.shortcode) if post_cache else None
                if cached and cached.info.get("media_id"):
                    info = dict(cached.info)  # копия, чтобы не мутировать общий кэш
                    image_b64 = cached.image_b64
                else:
                    # Fallback: загружаем данные напрямую (если кэш пуст)
                    info = await get_post_info(session, csrf, post.shortcode, post.clean_url)
                    image_b64 = None

                if not info.get("media_id"):
                    bot.push_log(acc, f"Нет media_id для {post.shortcode}", "error")
                    result.err += 1
                    result.total += 1
                    bot.progress_done += 1
                    continue

                # Пропускаем посты с закрытыми комментариями
                if cached and cached.comments_disabled:
                    bot.push_log(acc, f"⊘ Комментарии закрыты — пропускаю", "warn")
                    result.total += 1
                    bot.progress_done += 1
                    continue

                tags_hint = " ".join(f"#{h}" for h in info.get("hashtags", [])[:5])
                bot.push_log(acc,
                    f"@{info['author']} · {info.get('post_type','photo')} · {tags_hint or 'нет тегов'}")

                # Генерируем уникальный комментарий (describe уже в кэше)
                comment, src = await generate_comment(info, settings.model, acc, image_b64)
                bot.push_log(acc, f"Комментарий [{src}]: «{comment}»")

                ok, reason = await send_comment(
                    session, csrf, info["media_id"], comment,
                    retries=settings.retries,
                    retry_delay=settings.retry_delay,
                    acc_name=acc,
                )
                if ok:
                    bot.push_log(acc, "✓ Отправлен", "ok")
                    result.ok += 1
                elif "comments_disabled" in reason:
                    bot.push_log(acc, f"⊘ Комментарии закрыты — пропускаю", "warn")
                    # Помечаем в кэше, чтобы другие аккаунты не пытались
                    if cached:
                        cached.comments_disabled = True
                elif "feedback_required" in reason:
                    bot.push_log(acc, f"⚠ Временный бан (feedback_required) — пропускаю оставшиеся посты", "error")
                    result.err += 1
                    break  # нет смысла продолжать с этого аккаунта
                else:
                    bot.push_log(acc, f"✗ Ошибка: {reason}", "error")
                    result.err += 1

            except Exception as e:
                bot.push_log(acc, f"✗ Exception: {e}", "error")
                result.err += 1

            result.total += 1
            bot.progress_done += 1
            delay = random.uniform(settings.delay_min, settings.delay_max)
            bot.push_log(acc, f"Пауза {delay:.1f}с")
            await asyncio.sleep(delay)

    account.status = "ok" if result.err == 0 else "error"
    bot.push_log(acc,
        f"Готово — ✓{result.ok} ✗{result.err}",
        "ok" if result.err == 0 else "warn")


# ── Dispatcher ───────────────────────────────────────────────────────

def assign_posts(accounts: list[Account], posts: list[Post],
                 mode: str) -> dict[str, list[Post]]:
    result = {a.name: [] for a in accounts}
    if mode == "all":
        for a in accounts:
            result[a.name] = list(posts)
    else:  # round_robin
        for i, p in enumerate(posts):
            result[accounts[i % len(accounts)].name].append(p)
    return result


# ── Pre-fetch: загрузка данных постов один раз для всех аккаунтов ──

async def _prefetch_single_post(post: Post, session, csrf: str,
                                 model: str, use_vision: bool) -> CachedPostData:
    """Загружает всю информацию о посте: info, top_comments, image, description."""
    cached = CachedPostData()
    try:
        info = await get_post_info(session, csrf, post.shortcode, post.clean_url)
        cached.info = info

        if not info.get("media_id"):
            return cached

        # Параллельно: top_comments + image
        tasks = []
        tasks.append(_fetch_top_comments(session, csrf, info["media_id"], count=10))

        img_coro = None
        if use_vision:
            if info.get("post_type") == "reel":
                img_coro = _prefetch_image_reel(session, csrf, info, post)
            else:
                img_coro = _prefetch_image_photo(session, csrf, info)

        if img_coro:
            tasks.append(img_coro)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # top_comments
        if not isinstance(results[0], Exception) and results[0]:
            info["top_comments"] = results[0]

        # image
        if len(results) > 1 and not isinstance(results[1], Exception) and results[1]:
            image_b64, img_source = results[1]
            cached.image_b64 = image_b64
            cached.img_source = img_source

        # Fallback og:image
        if use_vision and not cached.image_b64 and info.get("thumbnail_url"):
            cached.image_b64 = await _fetch_image_b64(info["thumbnail_url"], session)
            if cached.image_b64:
                cached.img_source = "og:image"

        # Vision описание — один раз, результат будет доступен всем аккаунтам
        if cached.image_b64:
            info["has_image"] = True
            try:
                async with _get_ollama_sem():
                    description = await _describe_image(model, cached.image_b64)
                    info["image_description"] = description.strip()
            except Exception as e:
                make_file_logger("system").debug(f"Vision describe error: {e}")

    except Exception as e:
        make_file_logger("system").warning(f"Prefetch error for {post.shortcode}: {e}")
    return cached


async def _prefetch_image_reel(session, csrf, info, post) -> tuple[str, str] | None:
    # Один запрос к media info — используем для video и fallback image
    media_item = await _fetch_media_info(session, csrf, info["media_id"])
    if media_item:
        video_url = await _fetch_video_url(session, csrf, info["media_id"], _media_item=media_item)
        if video_url:
            b64 = await _extract_video_frame(video_url, session)
            if b64:
                return b64, "видео-кадр (ffmpeg)"
        # fallback: image из того же media_item
        img_url = await _fetch_media_image_url(session, csrf, info["media_id"], _media_item=media_item)
        if img_url:
            b64 = await _fetch_image_b64(img_url, session)
            if b64:
                return b64, "media API"
    return None


async def _prefetch_image_photo(session, csrf, info) -> tuple[str, str] | None:
    media_item = await _fetch_media_info(session, csrf, info["media_id"])
    img_url = await _fetch_media_image_url(session, csrf, info["media_id"], _media_item=media_item)
    if img_url:
        b64 = await _fetch_image_b64(img_url, session)
        if b64:
            return b64, "media API"
    return None


async def _prefetch_all_posts(posts: list[Post], accounts: list[Account],
                               model: str) -> dict[str, CachedPostData]:
    """Загружает данные всех постов один раз. Использует куки первого рабочего аккаунта."""
    use_vision = _model_has_vision(model) and AIOHTTP_OK
    cache: dict[str, CachedPostData] = {}

    if not AIOHTTP_OK:
        return cache

    # Берём куки первого рабочего аккаунта для пре-фетча
    cookies, csrf, uid = None, None, None
    for acc in accounts:
        c, cs, u = load_cookies(acc.cookies_file)
        if c:
            cookies, csrf, uid = c, cs, u
            break

    if not cookies:
        return cache

    async with aiohttp.ClientSession() as session:
        for name, val in cookies.items():
            session.cookie_jar.update_cookies({name: val})

        if not await verify_cookies(session):
            return cache

        bot.push_log("system", f"Пре-фетч: загрузка данных {len(posts)} постов...", "info")

        # Параллельно загружаем все посты
        tasks = [_prefetch_single_post(p, session, csrf, model, use_vision) for p in posts]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for post, result in zip(posts, results):
            if isinstance(result, Exception):
                bot.push_log("system", f"Ошибка пре-фетча {post.shortcode}: {result}", "error")
                cache[post.shortcode] = CachedPostData()
            else:
                cache[post.shortcode] = result
                info = result.info
                if info.get("media_id"):
                    desc = "с картинкой" if result.image_b64 else "без картинки"
                    bot.push_log("system",
                        f"  {post.shortcode}: @{info.get('author','?')} {desc}")

    bot.push_log("system", f"Пре-фетч завершён ✓", "ok")
    return cache


# ── Main runner ──────────────────────────────────────────────────────

async def _warmup_ollama(model: str, timeout: float = 120.0):
    """Прогревает модель: загружает в память через /api/generate с keep_alive."""
    if not AIOHTTP_OK:
        return
    bot.push_log("system", f"Прогрев модели {model}...", "info")
    session = _get_ollama_session()
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        try:
            # Лёгкий запрос — просто загрузить модель в память без генерации
            async with session.post(
                "http://127.0.0.1:11434/api/generate",
                json={"model": model, "prompt": "", "keep_alive": "10m"},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as r:
                if r.status == 200:
                    bot.push_log("system", f"Модель {model} готова ✓", "ok")
                    return
                body = await r.text()
                raise RuntimeError(f"HTTP {r.status}: {body[:100]}")
        except Exception as e:
            if "503" in str(e) or "connect" in str(e).lower():
                await asyncio.sleep(5)
            else:
                make_file_logger("system").warning(f"Warmup error: {e}")
                return
    bot.push_log("system", f"Ollama недоступна, используется fallback", "warn")


async def _run_bot(settings: BotSettings):
    _reset_ollama_sem(settings.ollama_parallel)
    await _warmup_ollama(settings.model)

    enabled = [a for a in bot.accounts if a.enabled]
    posts_map = assign_posts(enabled, bot.posts, settings.mode)

    bot.results = {a.name: RunResult(account=a.name) for a in enabled}
    bot.progress_done = 0
    bot.progress_total = sum(len(v) for v in posts_map.values())

    # Пре-фетч: загружаем данные всех постов один раз
    post_cache = await _prefetch_all_posts(bot.posts, enabled, settings.model)

    for a in enabled:
        a.status = "running"

    sem = asyncio.Semaphore(settings.max_parallel)

    async def guarded(acc):
        async with sem:
            await account_worker(
                acc, posts_map[acc.name],
                settings, bot.results[acc.name],
                post_cache=post_cache,
            )

    await asyncio.gather(*[guarded(a) for a in enabled])

    await _close_ollama_session()
    bot.running = False
    total_ok  = sum(r.ok  for r in bot.results.values())
    total_err = sum(r.err for r in bot.results.values())
    bot.push_log("system",
        f"─── Завершено: ✓{total_ok} успешно  ✗{total_err} ошибок ───",
        "ok" if total_err == 0 else "warn")


def start_bot(settings: BotSettings):
    """Запускается из FastAPI endpoint в фоне."""
    if bot.running:
        return False
    bot.running = True
    bot.stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    bot._task = loop.create_task(_run_bot(settings))
    return True


def stop_bot():
    if bot.stop_event:
        bot.stop_event.set()
    bot.push_log("system", "Остановка по запросу пользователя", "warn")


def start_stress_test(num_accounts: int, num_posts: int, max_parallel: int):
    """Стресс-тест: создаёт фейковые аккаунты и прогоняет симуляцию."""
    if bot.running:
        return False

    fake_accounts = [Account(name=f"test_{i+1}", cookies_file="(тест)")
                     for i in range(num_accounts)]
    fake_posts = [Post(url=f"https://instagram.com/p/TEST{i}/")
                  for i in range(num_posts)]

    # Добавляем фейки в UI
    bot.accounts.extend(fake_accounts)

    settings = BotSettings(
        max_parallel=max_parallel,
        delay_min=1.0,
        delay_max=3.0,
        stress_test=True,
    )

    posts_map = assign_posts(fake_accounts, fake_posts, "all")
    bot.results = {a.name: RunResult(account=a.name) for a in fake_accounts}
    bot.progress_done = 0
    bot.progress_total = sum(len(v) for v in posts_map.values())
    bot.running = True
    bot.stop_event = asyncio.Event()

    for a in fake_accounts:
        a.status = "running"

    async def _run():
        start = asyncio.get_event_loop().time()
        sem = asyncio.Semaphore(max_parallel)

        async def guarded(acc):
            async with sem:
                await account_worker(
                    acc, posts_map[acc.name],
                    settings, bot.results[acc.name],
                )

        await asyncio.gather(*[guarded(a) for a in fake_accounts])

        elapsed = asyncio.get_event_loop().time() - start
        # Убираем фейковые аккаунты из UI
        bot.accounts = [a for a in bot.accounts
                        if not a.name.startswith("test_")]

        bot.running = False
        total_ok = sum(r.ok for r in bot.results.values())
        total_err = sum(r.err for r in bot.results.values())
        bot.push_log("system",
            f"─── Стресс-тест: {num_accounts} акк × {num_posts} постов = "
            f"{bot.progress_total} задач за {elapsed:.1f}с ───",
            "ok")

    loop = asyncio.get_event_loop()
    bot._task = loop.create_task(_run())
    return True
