# backend/bot_engine.py
"""
Ядро бота: асинхронный движок с поддержкой SSE-логов.
Вся бизнес-логика здесь — UI и API её только запускают.
"""
import asyncio
import logging
import os
import random
import re
from dataclasses import dataclass, field
from typing import AsyncIterator

from backend.config import LOGS_DIR, CONFIG_FILE

# ── Optional deps ────────────────────────────────────────────────────
try:
    import aiohttp
    AIOHTTP_OK = True
except ImportError:
    AIOHTTP_OK = False

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


_VISION_MODELS = {"llava", "llava:7b", "llava:13b", "llava:34b",
                  "moondream", "bakllava", "gemma3:12b", "gemma3:27b"}

def _model_has_vision(model: str) -> bool:
    return any(v in model.lower() for v in ("llava", "moondream", "bakllava")) \
           or model in _VISION_MODELS


async def _fetch_image_b64(url: str) -> str | None:
    """Скачивает изображение и возвращает base64-строку."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    import base64
                    data = await r.read()
                    return base64.b64encode(data).decode()
    except Exception:
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


_COMMENT_ANGLES = [
    "задай интересный вопрос автору по теме поста",
    "поделись коротким личным опытом или ассоциацией с темой",
    "выскажи своё мнение или удивление конкретной деталью",
    "сделай наблюдение о чём-то в посте, что тебя зацепило",
    "отреагируй эмоционально но конкретно — что именно тебя впечатлило и почему",
]


def _build_prompt(post_info: dict, account_name: str = "") -> str:
    caption   = post_info.get("caption", "").strip()
    author    = post_info.get("author", "unknown")
    hashtags  = post_info.get("hashtags", [])
    post_type = post_info.get("post_type", "photo")

    type_ru = {"reel": "видео (Reel)", "igtv": "видео (IGTV)", "photo": "фото"}.get(post_type, "пост")
    tags_str = " ".join(f"#{h}" for h in hashtags[:15]) if hashtags else "нет"
    caption_str = f'"{caption}"' if caption else "(подпись отсутствует)"
    angle = random.choice(_COMMENT_ANGLES)
    persona = f"Ты — живой подписчик Instagram{f' (аккаунт {account_name})' if account_name else ''}."

    return f"""{persona} Напиши живой, искренний комментарий под постом.

Автор: @{author}
Тип поста: {type_ru}
Подпись: {caption_str}
Хэштеги: {tags_str}

Твой подход к комментарию: {angle}

Правила:
- Пиши на русском языке, разговорным живым стилем
- 2-4 предложения — развёрнуто, но не многословно
- Реагируй конкретно на тему (хэштеги — подсказка если подпись пустая)
- Не используй хэштеги в самом комментарии
- Не пиши банальщину: "круто", "огонь", "класс", "топ", "нравится"
- Максимум 1-2 эмодзи, и только если уместно
- Не представляйся и не объясняй — просто напиши комментарий

Комментарий:"""


async def generate_comment(post_info: dict, model: str,
                           account_name: str = "") -> tuple[str, str]:
    """Возвращает (текст_комментария, источник) где источник: 'ollama' | 'fallback'."""
    if not AIOHTTP_OK:
        return fallback_comment(post_info), "fallback"
    prompt = _build_prompt(post_info, account_name)
    try:
        image_b64 = None
        thumbnail_url = post_info.get("thumbnail_url", "")
        if thumbnail_url and _model_has_vision(model):
            image_b64 = await _fetch_image_b64(thumbnail_url)

        async with _get_ollama_sem():
            text = await _ollama_request(model, prompt, image_b64=image_b64)
        text = text.strip().strip("\"'«»")
        # Берём не более 4 предложений
        sentences = re.split(r'(?<=[.!?])\s+', text)
        text = " ".join(sentences[:4]).strip()
        if len(text) > 700:
            text = text[:697] + "..."
        return text, "ollama"
    except Exception as e:
        make_file_logger("system").warning(f"Ollama error (model={model}): {e}")
        return fallback_comment(post_info), "fallback"


def load_cookies(filepath: str):
    cookies, csrf, uid = {}, None, None
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split("\t")
                if len(parts) >= 7:
                    name, value = parts[5], parts[6]
                    cookies[name] = value
                    if name == "csrftoken":
                        csrf = value
                    elif name == "ds_user_id":
                        uid = value
    except FileNotFoundError:
        pass
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
                comment, src = await generate_comment(info, settings.model, acc)
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
