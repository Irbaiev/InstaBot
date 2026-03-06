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


async def _fetch_media_image_url(session, csrf: str, media_id: str) -> str | None:
    """Получает прямую ссылку на изображение через Instagram media API."""
    url = f"https://www.instagram.com/api/v1/media/{media_id}/info/"
    headers = {**BASE_HEADERS, "X-CSRFToken": csrf or ""}
    try:
        async with session.get(url, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                data = await r.json()
                items = data.get("items", [])
                if items:
                    item = items[0]
                    # Carousel → первый элемент
                    carousel = item.get("carousel_media", [])
                    src = carousel[0] if carousel else item
                    candidates = src.get("image_versions2", {}).get("candidates", [])
                    if candidates:
                        return candidates[0]["url"]
    except Exception:
        pass
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


async def _fetch_video_url(session, csrf: str, media_id: str) -> str | None:
    """Получает прямую ссылку на видео через Instagram media API."""
    url = f"https://www.instagram.com/api/v1/media/{media_id}/info/"
    headers = {**BASE_HEADERS, "X-CSRFToken": csrf or ""}
    try:
        async with session.get(url, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                data = await r.json()
                items = data.get("items", [])
                if items:
                    item = items[0]
                    videos = item.get("video_versions", [])
                    if videos:
                        return videos[0]["url"]
    except Exception:
        pass
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


async def _ollama_request(model: str, prompt: str,
                          timeout: float = 120.0,
                          image_b64: str | None = None) -> str:
    """Прямой HTTP запрос к Ollama API. Возвращает текст или бросает исключение."""
    message: dict = {"role": "user", "content": prompt}
    if image_b64:
        message["images"] = [image_b64]
    async with aiohttp.ClientSession() as session:
        async with session.post(
            OLLAMA_URL,
            json={"model": model, "messages": [message], "stream": False,
                  "options": {"temperature": 0.9, "top_p": 0.95}},
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data["message"]["content"]
            body = await resp.text()
            raise RuntimeError(f"HTTP {resp.status}: {body[:200]}")


def _split_image_tiles(image_b64: str) -> list[str]:
    """Разрезает картинку на 3 горизонтальных тайла с перекрытием 10%.
    Возвращает список base64-строк. Если PIL недоступен — возвращает [оригинал]."""
    if not PIL_OK:
        return [image_b64]
    try:
        img = _PILImage.open(_io.BytesIO(_base64.b64decode(image_b64)))
        w, h = img.size
        # Для маленьких картинок нарезка бесполезна
        if h < 400:
            return [image_b64]
        overlap = int(h * 0.1)
        tile_h = h // 3
        tiles = []
        for i in range(3):
            top = max(0, i * tile_h - overlap)
            bot = min(h, (i + 1) * tile_h + overlap)
            tile = img.crop((0, top, w, bot))
            buf = _io.BytesIO()
            tile.save(buf, format="JPEG", quality=90)
            tiles.append(_base64.b64encode(buf.getvalue()).decode())
        return tiles
    except Exception:
        return [image_b64]


async def _describe_image(model: str, image_b64: str) -> str:
    """Шаг 1 vision: описание картинки + OCR через тайлы для полноты."""
    tiles = _split_image_tiles(image_b64)

    if len(tiles) == 1:
        # Одна картинка (маленькая или нет PIL) — один запрос
        prompt = (
            "Внимательно посмотри на это изображение.\n"
            "1. Опиши коротко что на нём: люди, предметы, место, действие\n"
            "2. Если на изображении есть ТЕКСТ — прочитай его ПОЛНОСТЬЮ и точно\n"
            "3. Пиши по-русски, коротко, только факты\n"
            "Описание:"
        )
        return await _ollama_request(model, prompt, image_b64=tiles[0])

    # Мульти-тайл: общее описание + OCR по частям
    descriptions = []

    # Запрос 1: общее описание по полной картинке
    general = await _ollama_request(model, (
        "Опиши коротко что на этом изображении: люди, предметы, место, действие. "
        "По-русски, только факты."
    ), image_b64=image_b64)
    descriptions.append(f"Общее: {general.strip()}")

    # Запросы 2-4: OCR по каждому тайлу
    labels = ["верхней", "центральной", "нижней"]
    for i, tile in enumerate(tiles):
        ocr = await _ollama_request(model, (
            f"Это {labels[i]} часть изображения. "
            "Прочитай ВЕСЬ текст который видишь — каждое слово, каждую строку. "
            "Если текста нет — напиши 'текста нет'. По-русски."
        ), image_b64=tile)
        text = ocr.strip()
        if text.lower() not in ("текста нет", "текста нет.", "нет текста", "нет текста."):
            descriptions.append(f"Текст ({labels[i]} часть): {text}")

    return "\n".join(descriptions)


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

    # Берём 3 рандомных few-shot примера
    examples = random.sample(_FEW_SHOT_EXAMPLES, 3)
    examples_str = "\n".join(f"- {e}" for e in examples)

    # Контекст: реальные комментарии под постом (показываем до 10 рандомных)
    comments_block = ""
    if top_comments:
        sample = random.sample(top_comments, min(10, len(top_comments)))
        comments_block = "\nДругие комментарии под этим постом (впишись в стиль):\n" + "\n".join(f"- {c}" for c in sample) + "\n"

    # Описание картинки (заполняется на шаге 1 — _describe_image)
    vision_hint = ""
    img_desc = post_info.get("image_description", "")
    if img_desc:
        vision_hint = (
            f"\nОписание изображения поста (что реально видно на фото/видео):\n"
            f"{img_desc}\n"
            f"\nРеагируй на то что изображено. Если на фото есть текст — учти его.\n"
        )

    prompt = f"""Ты — {persona}. Пишешь комментарий в Instagram под постом @{author}.

Пост: {type_ru}
Подпись: {caption_str}
Хэштеги: {tags_str}
{comments_block}{vision_hint}
Задача: {angle}
Длина: {length_hint}

Примеры стиля (НЕ копируй, пиши своё):
{examples_str}

ПРАВИЛА:
- Пиши по-русски, живым разговорным языком как реальный человек
- Можно сокращения, можно без точки в конце, можно начать с маленькой буквы
- НЕ используй слова: круто, огонь, класс, топ, супер, потрясающе, великолепно, замечательно
- НЕ ставь хэштеги
- Эмодзи максимум 1 штука и только если реально к месту
- НЕ начинай с обращения "привет" или "здравствуйте"
- Пиши как человек который реально залипает в инсту, а не как бот
- Только текст комментария, без пояснений

Комментарий:"""

    return prompt, max_sent


async def generate_comment(post_info: dict, model: str,
                           account_name: str = "",
                           image_b64: str | None = None) -> tuple[str, str]:
    """Возвращает (текст_комментария, источник) где источник: 'ollama' | 'fallback'."""
    if not AIOHTTP_OK:
        return fallback_comment(post_info), "fallback"
    try:
        # Шаг 1: если есть картинка — отдельный запрос на описание + OCR
        if image_b64:
            async with _get_ollama_sem():
                description = await _describe_image(model, image_b64)
            post_info["image_description"] = description.strip()
            make_file_logger("system").debug(f"Vision description: {description[:200]}")

        # Шаг 2: генерация комментария (уже без картинки, по описанию)
        prompt, max_sentences = _build_prompt(post_info, account_name)
        async with _get_ollama_sem():
            text = await _ollama_request(model, prompt)
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
                       retries: int = 3, retry_delay: float = 30) -> bool:
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
                    return data.get("status") == "ok" or "comment" in data
                elif r.status == 429:
                    await asyncio.sleep(retry_delay)
                else:
                    return False
        except Exception:
            if attempt < retries:
                await asyncio.sleep(5)
    return False


# ── Per-account worker ───────────────────────────────────────────────

async def account_worker(account: Account, posts: list[Post],
                         settings: BotSettings, result: RunResult):
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

    # ── Real mode ────────────────────────────────────────────────────
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
                info = await get_post_info(session, csrf, post.shortcode, post.clean_url)
                if not info.get("media_id"):
                    bot.push_log(acc, f"Нет media_id для {post.shortcode}", "error")
                    result.err += 1
                    result.total += 1
                    bot.progress_done += 1
                    continue

                tags_hint = " ".join(f"#{h}" for h in info.get("hashtags", [])[:5])
                bot.push_log(acc,
                    f"@{info['author']} · {info.get('post_type','photo')} · {tags_hint or 'нет тегов'}")

                # Загружаем топ-комментарии для контекста промпта (до 25 шт)
                if info.get("media_id"):
                    top_comments = await _fetch_top_comments(session, csrf, info["media_id"], count=25)
                    if top_comments:
                        info["top_comments"] = top_comments
                        bot.push_log(acc, f"Загружено {len(top_comments)} комментариев для контекста")

                # Загружаем изображение для vision-модели
                image_b64 = None
                img_source = ""
                if _model_has_vision(settings.model) and info.get("media_id"):
                    # Для Reels — пробуем извлечь кадр из видео через ffmpeg
                    if info.get("post_type") == "reel":
                        video_url = await _fetch_video_url(session, csrf, info["media_id"])
                        if video_url:
                            image_b64 = await _extract_video_frame(video_url, session)
                            if image_b64:
                                img_source = "видео-кадр (ffmpeg)"
                    # Обложка через media API
                    if not image_b64:
                        media_img_url = await _fetch_media_image_url(session, csrf, info["media_id"])
                        if media_img_url:
                            image_b64 = await _fetch_image_b64(media_img_url, session)
                            if image_b64:
                                img_source = "media API"
                    # Fallback на og:image
                    if not image_b64 and info.get("thumbnail_url"):
                        image_b64 = await _fetch_image_b64(info["thumbnail_url"], session)
                        if image_b64:
                            img_source = "og:image"
                    if image_b64:
                        info["has_image"] = True
                        bot.push_log(acc, f"Изображение загружено ✓ ({img_source})", "info")
                    else:
                        bot.push_log(acc, "Изображение недоступно — только текст", "warn")

                comment, src = await generate_comment(info, settings.model, acc, image_b64)
                bot.push_log(acc, f"Комментарий [{src}]: «{comment}»")

                ok = await send_comment(
                    session, csrf, info["media_id"], comment,
                    retries=settings.retries,
                    retry_delay=settings.retry_delay,
                )
                if ok:
                    bot.push_log(acc, "✓ Отправлен", "ok")
                    result.ok += 1
                else:
                    bot.push_log(acc, "✗ Ошибка при отправке", "error")
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


# ── Main runner ──────────────────────────────────────────────────────

async def _warmup_ollama(model: str, timeout: float = 120.0):
    """Прогревает модель перед стартом: ждёт пока загрузится в память (до timeout сек)."""
    if not AIOHTTP_OK:
        return
    bot.push_log("system", f"Прогрев модели {model}...", "info")
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        try:
            async with _get_ollama_sem():
                await _ollama_request(model, "Hi", timeout=30.0)
            bot.push_log("system", f"Модель {model} готова ✓", "ok")
            return
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

    for a in enabled:
        a.status = "running"

    sem = asyncio.Semaphore(settings.max_parallel)

    async def guarded(acc):
        async with sem:
            await account_worker(
                acc, posts_map[acc.name],
                settings, bot.results[acc.name]
            )

    await asyncio.gather(*[guarded(a) for a in enabled])

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
