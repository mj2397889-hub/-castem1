"""Telegram management, escrow verification, moderation, and multi-tenant custom response bot.

Features:
- Global Blacklist: قائمة وفحص وتحذير النصابين معمم في كل المجموعات.
- Isolated Primary Group: كروت الملاك والمشرفين الأساسيين ورابط الوساطة لشاتك فقط.
- Dynamic Roles: أوامر (أضف مشرف) و (أضف مالك) مع كروت مخصصة.
- Bot-Only Roles: أوامر (رفع ادمن / رفع مدير / رفع مميز) مع لوحة تحكم بالصلاحيات داخل البوت فقط.
- Full Moderation & Unrestriction: دعم كامل للمشرفين وأدمن البوت في الحظر، الكتم، وفك القيود.
- Interactive Buttons: نظام (أضف رد بزر) لإضافة أزرار تفاعلية وروابط.
- Message Pinning: دعم أمر (تثبيت / ثبت) بالرد.
- Real Group Admin Promotion: رفع وتنزيل المشرفين فعلياً في المجموعة بصلاحيات تيليجرام الرسمية.
- Broadcast Command: دعم أمر (انشر) لنشر الرسائل باسم البوت مع الإيموجي والتنسيقات.
- Prayer Times & Auto Lock: جلب مواقيت الصلاة لمصر وإغلاق الشات 15 دقيقة وفتحه تلقائياً.
- Manual Chat Lock/Unlock: أوامر (قفل الشات) و (فتح الشات) باسم المشرف المنفذ.
- Robust Roulette Engine: نظام روليت عربي مرن (روليت / تم) مزود بإيموجيات مخصصة.
- Accurate Message Purging: مسح دقيق وفوري للرسائل بالعدد الفعلي عبر Telethon.
- Dedicated Deep-Link Commands Guide: أمر (الأوامر) في الجروب مع زر يحول العضو للخاص بدليل منسق بالاقتباسات.
- Owner & Admin Immunity: حصانة كاملة للمطور والمشرفين من الحظر والكتم.
- Robust Lookup: نظام الكشف والرتب متاح ومفتوح لجميع الأعضاء بدون قيود.
- Cloud Persistence: Upstash Redis + Local JSON fallback.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
import random
import re
import atexit
import threading
import time
import weakref
from collections import deque
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from html import escape, unescape as html_unescape
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from telegram import ChatPermissions, InlineKeyboardButton, InlineKeyboardMarkup, Message, MessageEntity, Update
from telegram.constants import ChatMemberStatus, MessageEntityType, ParseMode
from telegram.error import RetryAfter, TelegramError
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from keep_alive import start_keep_alive
from telethon import TelegramClient

UPSTASH_URL = os.getenv("UPSTASH_REDIS_REST_URL", "https://more-snipe-201391.upstash.io").strip()
UPSTASH_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN", "gQAAAAAAAxKvAAIgcDIxMGY2NTZhZTY3MDU0ZmU0OTZjMmM4NDM1N2NkNGEyNQ").strip()

try:
    from upstash_redis import Redis
    redis_client = Redis(url=UPSTASH_URL, token=UPSTASH_TOKEN)
except Exception:
    redis_client = None

API_ID = int(os.getenv("TELEGRAM_API_ID", "24037585"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "4b9a092550123ce9a18f460532023094").strip()
BOT_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()

# الآيدي الخاص بشاتك الأساسي
PRIMARY_CHAT_ID = -1002170188795
PRIMARY_OWNER_ID = 8906807539

# إعدادات مواقيت الصلاة والمنطقة الزمنية
CAIRO_TZ = ZoneInfo("Africa/Cairo")
PRAYER_NAMES = {
    "Fajr": "الفجر",
    "Dhuhr": "الظهر",
    "Asr": "العصر",
    "Maghrib": "المغرب",
    "Isha": "العشاء",
}

telethon_client: TelegramClient | None = None

BASE_DIR = Path(__file__).resolve().parent
ESCROW_LINK = "https://t.me/+p0JZlmzzR0Q2YzNk"
ESCROW_IMAGE = "escrow.jpg"
RULES_LINK = "https://t.me/c/4357718598/77"

DEFAULT_DYNAMIC_REPLIES: dict[str, dict[str, Any]] = {}
DEFAULT_ALIASES: dict[str, str] = {}

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
)
logger = logging.getLogger("telegram_escrow_bot")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


# -------------------------------------------------------------
# Performance monitor (قياس زمني داخلي لكل عملية مهمة)
# -------------------------------------------------------------

class PerfMonitor:
    """Lightweight in-process timing statistics (count/avg/p95/max per operation)."""

    def __init__(self) -> None:
        self.stats: dict[str, dict[str, Any]] = {}
        self.slow_ms = float(os.getenv("SLOW_OP_MS", "500"))
        self.started = time.time()

    def record(self, name: str, ms: float) -> None:
        st = self.stats.get(name)
        if st is None:
            st = self.stats[name] = {"count": 0, "total": 0.0, "max": 0.0, "recent": deque(maxlen=300)}
        st["count"] += 1
        st["total"] += ms
        if ms > st["max"]:
            st["max"] = ms
        st["recent"].append(ms)
        if ms >= self.slow_ms:
            logger.warning("SLOW_OP %s took %.0f ms", name, ms)

    def report(self, top: int = 20, prefix: str | None = None) -> str:
        rows = []
        for name, st in self.stats.items():
            if prefix and not name.startswith(prefix):
                continue
            recent = sorted(st["recent"])
            p95 = recent[min(len(recent) - 1, int(len(recent) * 0.95))] if recent else 0.0
            rows.append((st["total"], name, st["count"], st["total"] / st["count"], p95, st["max"]))
        rows.sort(reverse=True)
        lines = [f"{'operation':<38}{'n':>6}{'avg':>8}{'p95':>8}{'max':>8}  (ms)"]
        for _total, name, n, avg, p95, mx in rows[:top]:
            lines.append(f"{name[:37]:<38}{n:>6}{avg:>8.1f}{p95:>8.1f}{mx:>8.1f}")
        if len(lines) == 1:
            lines.append("لا توجد بيانات بعد.")
        return "\n".join(lines)


perf = PerfMonitor()


@contextmanager
def timed(name: str):
    t0 = time.perf_counter()
    try:
        yield
    finally:
        perf.record(name, (time.perf_counter() - t0) * 1000.0)


_BG_TASKS: set[asyncio.Task] = set()


def spawn(coro) -> asyncio.Task:
    """Fire-and-forget helper that keeps a strong reference so tasks are never GC'd mid-flight."""
    task = asyncio.create_task(coro)
    _BG_TASKS.add(task)
    task.add_done_callback(_BG_TASKS.discard)
    return task


class TimedRequest(HTTPXRequest):
    """Times every Bot API HTTP call (one stat per Telegram method)."""

    async def do_request(self, *args, **kwargs):  # type: ignore[override]
        url = args[0] if args else kwargs.get("url", "")
        method = str(url).rsplit("/", 1)[-1] or "unknown"
        t0 = time.perf_counter()
        try:
            return await super().do_request(*args, **kwargs)
        finally:
            perf.record(f"tg.api.{method}", (time.perf_counter() - t0) * 1000.0)


# -------------------------------------------------------------
# Database Management — in-memory cache (source of truth at runtime) with
# non-blocking, coalesced write-behind to Upstash Redis + local JSON.
# Same keys and same JSON layout as before.
# -------------------------------------------------------------

_DB_CACHE: dict[str, Any] = {}
_PENDING_WRITES: dict[str, str] = {}
_PENDING_LOCK = threading.Lock()
_PERSIST_LOCK = threading.Lock()
_WRITE_EVENT = threading.Event()
_writer_thread: threading.Thread | None = None


def _db_read_backend(key: str) -> dict[str, Any]:
    if redis_client:
        try:
            val = redis_client.get(key)
            if val:
                return val if isinstance(val, dict) else json.loads(val)
        except Exception as e:
            logger.warning(f"Upstash GET failed for {key}: {e}")

    local_path = BASE_DIR / f"{key}.json"
    if local_path.exists():
        try:
            return json.loads(local_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def db_load(key: str) -> dict[str, Any]:
    """Return the cached object for key (loaded once from Redis/local file).

    The returned object is shared: callers mutate it and call db_save(key, obj),
    exactly like before, but reads no longer touch the network.
    """
    cached = _DB_CACHE.get(key)
    if cached is not None:
        return cached
    with timed("db.load_backend"):
        data = _db_read_backend(key)
    _DB_CACHE[key] = data
    return data


def _persist(key: str, payload: str) -> bool:
    ok = True
    t0 = time.perf_counter()
    if redis_client:
        try:
            redis_client.set(key, payload)
        except Exception as e:
            ok = False
            logger.warning(f"Upstash SET failed for {key}: {e}")
    perf.record("db.persist_redis", (time.perf_counter() - t0) * 1000.0)
    try:
        local_path = BASE_DIR / f"{key}.json"
        temp = local_path.with_suffix(".tmp")
        temp.write_text(payload, encoding="utf-8")
        temp.replace(local_path)
    except Exception:
        pass
    return ok


def _drain_pending() -> None:
    with _PERSIST_LOCK:
        with _PENDING_LOCK:
            batch = dict(_PENDING_WRITES)
            _PENDING_WRITES.clear()
        for key, payload in batch.items():
            if not _persist(key, payload):
                with _PENDING_LOCK:
                    _PENDING_WRITES.setdefault(key, payload)  # retry later unless a newer value arrived


def _writer_loop() -> None:
    while True:
        _WRITE_EVENT.wait()
        _WRITE_EVENT.clear()
        _drain_pending()
        with _PENDING_LOCK:
            retry = bool(_PENDING_WRITES)
        if retry:
            time.sleep(2.0)
            _WRITE_EVENT.set()


def _ensure_writer() -> None:
    global _writer_thread
    if _writer_thread is None or not _writer_thread.is_alive():
        _writer_thread = threading.Thread(target=_writer_loop, name="db-writer", daemon=True)
        _writer_thread.start()


def db_save(key: str, data: dict[str, Any]) -> None:
    """Update the cache immediately and persist in the background (coalesced per key)."""
    _DB_CACHE[key] = data
    try:
        payload = json.dumps(data, ensure_ascii=False)
    except Exception as e:
        logger.error(f"db_save serialization failed for {key}: {e}")
        return
    with _PENDING_LOCK:
        _PENDING_WRITES[key] = payload
    _ensure_writer()
    _WRITE_EVENT.set()


def flush_db() -> None:
    """Synchronously persist everything still pending (used at shutdown)."""
    try:
        _drain_pending()
    except Exception as e:
        logger.warning(f"flush_db failed: {e}")


atexit.register(flush_db)


def prefetch_db(keys: list[str]) -> None:
    """Load many keys in parallel at startup instead of one network round-trip each."""
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(db_load, keys))


# -------------------------------------------------------------
# Admin cache — one getChatAdministrators call serves every permission check
# -------------------------------------------------------------

ADMIN_CACHE_TTL = float(os.getenv("ADMIN_CACHE_TTL", "30"))
_ADMIN_CACHE: dict[int, tuple[float, dict[int, Any]]] = {}
_ADMIN_FETCH_LOCKS: dict[int, asyncio.Lock] = {}


def invalidate_admin_cache(chat_id: int | None = None) -> None:
    if chat_id is None:
        _ADMIN_CACHE.clear()
    else:
        _ADMIN_CACHE.pop(int(chat_id), None)


async def get_chat_admins_cached(bot, chat_id: int) -> dict[int, Any] | None:
    chat_id = int(chat_id)
    ent = _ADMIN_CACHE.get(chat_id)
    if ent and time.monotonic() - ent[0] < ADMIN_CACHE_TTL:
        return ent[1]
    lock = _ADMIN_FETCH_LOCKS.setdefault(chat_id, asyncio.Lock())
    async with lock:
        ent = _ADMIN_CACHE.get(chat_id)
        if ent and time.monotonic() - ent[0] < ADMIN_CACHE_TTL:
            return ent[1]
        try:
            with timed("perm.fetch_admins"):
                admins = await bot.get_chat_administrators(chat_id)
        except TelegramError:
            return None
        data = {m.user.id: m for m in admins}
        _ADMIN_CACHE[chat_id] = (time.monotonic(), data)
        return data


def _parse_admin_ids() -> set[int]:
    raw = os.getenv("ADMIN_USER_IDS", "")
    result: set[int] = {PRIMARY_OWNER_ID}
    for value in raw.split(","):
        value = value.strip()
        if value:
            try:
                result.add(int(value))
            except ValueError:
                pass
    return result


async def is_primary_owner(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if user_id in context.application.bot_data["admin_user_ids"]:
        return True
    primary_owners = context.application.bot_data.get("primary_owners", {})
    return str(user_id) in primary_owners


async def is_real_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat:
        return False
    if await is_primary_owner(user.id, context):
        return True
    if chat.type == "private":
        return False
    t0 = time.perf_counter()
    try:
        admins = await get_chat_admins_cached(context.bot, chat.id)
        if admins is not None:
            return user.id in admins
        try:
            member = await context.bot.get_chat_member(chat.id, user.id)
            return member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
        except TelegramError:
            return False
    finally:
        perf.record("perm.is_real_admin", (time.perf_counter() - t0) * 1000.0)


async def is_bot_owner(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if user_id == PRIMARY_OWNER_ID:
        return True
    primary_owners = context.application.bot_data.get("primary_owners", {})
    return str(user_id) in primary_owners


async def is_bot_internal_admin(user_id: int, chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Return True for a manager/admin registered inside this bot for this chat."""
    if await is_primary_owner(user_id, context):
        return True
    bot_admins_map = context.application.bot_data.get("bot_admins", {})
    entry = bot_admins_map.get(str(chat_id), {}).get(str(user_id))
    if not isinstance(entry, dict):
        return False
    role = str(entry.get("role", entry.get("title", ""))).lower()
    # All registered bot admins/managers are moderators for lock immunity and roulette.
    return True


async def is_management_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user, chat = update.effective_user, update.effective_chat
    if not user or not chat:
        return False
    return await is_real_admin(update, context) or await is_bot_internal_admin(user.id, chat.id, context)


async def check_bot_permission(update: Update, context: ContextTypes.DEFAULT_TYPE, perm_key: str) -> bool:
    if await is_real_admin(update, context):
        return True
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat:
        return False
    bot_admins_map = context.application.bot_data.get("bot_admins", {})
    chat_admins = bot_admins_map.get(str(chat.id), {})
    uid_str = str(user.id)
    if uid_str in chat_admins:
        return bool(chat_admins[uid_str].get("permissions", {}).get(perm_key, False))
    return False


async def resolve_username_to_user(username: str, context: ContextTypes.DEFAULT_TYPE):
    clean_u = username.lstrip("@").strip()
    lookup_u = clean_u.lower()
    user_map = context.application.bot_data.get("user_map", {})

    if f"@{lookup_u}" in user_map:
        uid = int(user_map[f"@{lookup_u}"])
        info = user_map.get(str(uid), {})
        return uid, clean_u, info.get("name", f"@{clean_u}")

    try:
        chat = await context.bot.get_chat(f"@{clean_u}")
        name = chat.full_name or f"@{clean_u}"
        user_map[str(chat.id)] = {"name": name, "username": clean_u}
        user_map[f"@{lookup_u}"] = str(chat.id)
        db_save("user_map", user_map)
        return chat.id, clean_u, name
    except Exception:
        pass

    global telethon_client
    if telethon_client and telethon_client.is_connected():
        try:
            entity = await telethon_client.get_entity(clean_u)
            if entity:
                uid = entity.id
                name = f"{getattr(entity, 'first_name', '') or ''} {getattr(entity, 'last_name', '') or ''}".strip() or clean_u
                user_map[str(uid)] = {"name": name, "username": clean_u}
                user_map[f"@{lookup_u}"] = str(uid)
                db_save("user_map", user_map)
                return uid, clean_u, name
        except Exception:
            pass

    return None, clean_u, f"@{clean_u}"


async def resolve_user_id_info(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> tuple[str, str | None]:
    user_map = context.application.bot_data.get("user_map", {})
    uid_str = str(user_id)

    # Cache first (no network on the hot moderation path); refresh from Telegram only on a miss.
    cached_entry = user_map.get(uid_str)
    if isinstance(cached_entry, dict) and cached_entry.get("name"):
        return cached_entry.get("name", uid_str), cached_entry.get("username") or None

    try:
        c_obj = await asyncio.wait_for(context.bot.get_chat(user_id), timeout=1.5)
        u_name = c_obj.full_name or uid_str
        u_uname = c_obj.username if c_obj.username else None
        user_map[uid_str] = {"name": u_name, "username": u_uname or ""}
        if u_uname:
            user_map[f"@{u_uname.lower()}"] = uid_str
        db_save("user_map", user_map)
        return u_name, u_uname
    except Exception:
        pass

    if uid_str in user_map:
        cached = user_map[uid_str]
        return cached.get("name", uid_str), cached.get("username") or None

    global telethon_client
    if telethon_client and telethon_client.is_connected():
        try:
            entity = await telethon_client.get_entity(user_id)
            if entity:
                u_name = f"{getattr(entity, 'first_name', '') or ''} {getattr(entity, 'last_name', '') or ''}".strip() or uid_str
                u_uname = entity.username if getattr(entity, 'username', None) else None
                user_map[uid_str] = {"name": u_name, "username": u_uname or ""}
                db_save("user_map", user_map)
                return u_name, u_uname
        except Exception:
            pass

    return uid_str, None


def get_wallet_emoji(phone: str) -> str:
    clean_p = phone.strip()
    if clean_p.startswith("010"):
        return '<tg-emoji emoji-id="5890939368956237177">🔴</tg-emoji> '
    elif clean_p.startswith("012"):
        return '<tg-emoji emoji-id="5891225499677496831">🟠</tg-emoji> '
    elif clean_p.startswith("015"):
        return '<tg-emoji emoji-id="5891052013063509921">🟣</tg-emoji> '
    elif clean_p.startswith("011"):
        return '<tg-emoji emoji-id="5891235511246264255">🟢</tg-emoji> '
    return "📱 "


def exact_trigger(text: str | None) -> str | None:
    if text is None:
        return None
    return {
        "@BRQ97": "owner_brq",
        "برق": "owner_brq",
        "@EG_ABDO": "owner_abdo",
        "عبدالرحمن": "owner_abdo",
        "عبد الرحمن": "owner_abdo",
        "عبدو": "owner_abdo",
        "@K_E_R_O_3S": "admin_kero",
        "كيرو": "admin_kero",
        "@Apo_Noaf": "admin_apo",
        "ابو نواف": "admin_apo",
        "@ZA_KA11": "admin_zaka",
        "زكا": "admin_zaka",
        "زيكا": "admin_zaka",
        "Dragon": "admin_dragon",
        "دراجون": "admin_dragon",
        "@EGPYT7STOR7": "admin_dragon",
        "غضب": "admin_ghadab",
        "@mohamed1GDB": "admin_ghadab",
        "mohamed1GDB": "admin_ghadab",
        "8751786478": "admin_ghadab",
        "رحمة": "admin_rahma",
        "رحمه": "admin_rahma",
        "@Rahoma_20": "admin_rahma",
        "6900964602": "admin_rahma",
    }.get(text.strip())


CARD_DETAILS: dict[str, dict[str, str]] = {
    "owner_brq": {
        "kind": "owner",
        "image": "brq.jpg",
        "caption": (
            '<tg-emoji emoji-id="5875344888165308204">👑</tg-emoji> <b>OWNER</b> <tg-emoji emoji-id="5875344888165308204">👑</tg-emoji>\n'
            '━━━━━━━━━━━━━━━━━━━━\n\n'
            'ايوه دا اليوزر الأساسي والمالك للشات المعاملة والضمان معاه مباشرة <tg-emoji emoji-id="5873005522558392818">✔️</tg-emoji>\n\n'
            '🔹 <b>يوزر المالك:</b> @BRQ97\n'
            '🔹 <b>الرابط الدائم:</b>\n'
            '<a href="tg://openmessage?user_id=8906807539">tg://openmessage?user_id=8906807539</a>\n\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
            '<tg-emoji emoji-id="5890787894049643800">⛔</tg-emoji> <b>غير مسئولين عن أي يوزر تاني لو اتعاملت مع منتحل.</b>'
        ),
        "link": "tg://openmessage?user_id=8906807539",
        "button_link": "https://t.me/BRQ97",
        "button": "تواصل",
    },
    "owner_abdo": {
        "kind": "owner",
        "image": "abdo.jpg",
        "caption": (
            '<tg-emoji emoji-id="5875344888165308204">👑</tg-emoji> <b>OWNER</b> <tg-emoji emoji-id="5875344888165308204">👑</tg-emoji>\n'
            '━━━━━━━━━━━━━━━━━━━━\n\n'
            'ايوه دا اليوزر الأساسي والمالك للشات المعاملة والضمان معاه مباشرة <tg-emoji emoji-id="5873005522558392818">✔️</tg-emoji>\n\n'
            '🔹 <b>يوزر المالك:</b> @EG_ABDO\n'
            '🔹 <b>الرابط الدائم:</b>\n'
            '<a href="tg://openmessage?user_id=7987845461">tg://openmessage?user_id=7987845461</a>\n\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
            '<tg-emoji emoji-id="5890787894049643800">⛔</tg-emoji> <b>غير مسئولين عن أي يوزر تاني لو اتعاملت مع منتحل.</b>'
        ),
        "link": "tg://openmessage?user_id=7987845461",
        "button_link": "https://t.me/EG_ABDO",
        "button": "تواصل",
    },
    "admin_kero": {
        "kind": "admin",
        "image": "kero.jpg",
        "caption": (
            '<tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji> <b>ADMIN</b> <tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji>\n'
            '━━━━━━━━━━━━━━━━━━━━\n\n'
            'مشرف وضامن معتمد داخل الشات، المعاملة والوساطة معاه بأمان تام <tg-emoji emoji-id="5891235511246264255">✔️</tg-emoji>\n\n'
            '🔹 <b>يوزر المشرف:</b> @K_E_R_O_3S\n'
            '🔹 <b>الرابط الدائم:</b>\n'
            '<a href="tg://openmessage?user_id=7445972479">tg://openmessage?user_id=7445972479</a>\n\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
            '<tg-emoji emoji-id="5890787894049643800">⛔</tg-emoji> <b>غير مسئولين عن أي يوزر تاني لو اتعاملت مع منتحل.</b>'
        ),
        "link": "tg://openmessage?user_id=7445972479",
        "button_link": "https://t.me/K_E_R_O_3S",
        "button": "تواصل",
    },
    "admin_apo": {
        "kind": "admin",
        "image": "apo_noaf.jpg",
        "caption": (
            '<tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji> <b>ADMIN</b> <tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji>\n'
            '━━━━━━━━━━━━━━━━━━━━\n\n'
            'مشرف وضامن معتمد داخل الشات، المعاملة والوساطة معاه بأمان تام <tg-emoji emoji-id="5891235511246264255">✔️</tg-emoji>\n\n'
            '🔹 <b>يوزر المشرف:</b> @Apo_Noaf\n'
            '🔹 <b>الرابط الدائم:</b>\n'
            '<a href="tg://openmessage?user_id=5967025652">tg://openmessage?user_id=5967025652</a>\n\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
            '<tg-emoji emoji-id="5890787894049643800">⛔</tg-emoji> <b>غير مسئولين عن أي يوزر تاني لو اتعاملت مع منتحل.</b>'
        ),
        "link": "tg://openmessage?user_id=5967025652",
        "button_link": "https://t.me/Apo_Noaf",
        "button": "تواصل",
    },
    "admin_zaka": {
        "kind": "admin",
        "image": "zaka.jpg",
        "caption": (
            '<tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji> <b>ADMIN</b> <tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji>\n'
            '━━━━━━━━━━━━━━━━━━━━\n\n'
            'مشرف وضامن معتمد داخل الشات، المعاملة والوساطة معاه بأمان تام <tg-emoji emoji-id="5891235511246264255">✔️</tg-emoji>\n\n'
            '🔹 <b>يوزر المشرف:</b> @ZA_KA11\n'
            '🔹 <b>الرابط الدائم:</b>\n'
            '<a href="tg://openmessage?user_id=1129739553">tg://openmessage?user_id=1129739553</a>\n\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
            '<tg-emoji emoji-id="5890787894049643800">⛔</tg-emoji> <b>غير مسئولين عن أي يوزر تاني لو اتعاملت مع منتحل.</b>'
        ),
        "link": "tg://openmessage?user_id=1129739553",
        "button_link": "https://t.me/ZA_KA11",
        "button": "تواصل",
    },
    "admin_dragon": {
        "kind": "admin",
        "image": "dragon.jpg",
        "caption": (
            '<tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji> <b>ADMIN</b> <tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji>\n'
            '━━━━━━━━━━━━━━━━━━━━\n\n'
            'مشرف وضامن معتمد داخل الشات، المعاملة والوساطة معاه بأمان تام <tg-emoji emoji-id="5891235511246264255">✔️</tg-emoji>\n\n'
            '🔹 <b>يوزر المشرف:</b> @EGPYT7STOR7\n'
            '🔹 <b>الرابط الدائم:</b>\n'
            '<a href="tg://openmessage?user_id=8930491568">tg://openmessage?user_id=8930491568</a>\n\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
            '<tg-emoji emoji-id="5890787894049643800">⛔</tg-emoji> <b>غير مسئولين عن أي يوزر تاني لو اتعاملت مع منتحل.</b>'
        ),
        "link": "tg://openmessage?user_id=8930491568",
        "button_link": "https://t.me/EGPYT7STOR7",
        "button": "تواصل",
    },
    "admin_ghadab": {
        "kind": "admin",
        "image": "ghadab.jpg",
        "caption": (
            '<tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji> <b>ADMIN</b> <tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji>\n'
            '━━━━━━━━━━━━━━━━━━━━\n\n'
            '<b>مشرف وضامن معتمد داخل الشات،\nالمعاملة والوساطة معاه بأمان تام <tg-emoji emoji-id="5891235511246264255">✔️</tg-emoji></b>\n\n'
            '🔹 <b>يوزر المشرف:</b> @mohamed1GDB\n'
            '🔹 <b>الرابط الدائم:</b>\n'
            '<a href="tg://openmessage?user_id=8751786478">tg://openmessage?user_id=8751786478</a>\n\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
            '<tg-emoji emoji-id="5890787894049643800">⛔</tg-emoji> <b>غير مسئولين عن أي يوزر تاني لو اتعاملت مع منتحل.</b>'
        ),
        "link": "tg://openmessage?user_id=8751786478",
        "button_link": "https://t.me/mohamed1GDB",
        "button": "تواصل",
    },
    "admin_rahma": {
        "kind": "admin",
        "image": "rahma.jpg",
        "caption": (
            '<tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji> <b>ADMIN</b> <tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji>\n'
            '━━━━━━━━━━━━━━━━━━━━\n\n'
            '<b>مشرفة وضامنة معتمدة داخل الشات،\nالمعاملة والوساطة معاها بأمان تام <tg-emoji emoji-id="5891235511246264255">✔️</tg-emoji></b>\n\n'
            '🔹 <b>يوزر المشرفة:</b> @Rahoma_20\n'
            '🔹 <b>الرابط الدائم:</b>\n'
            '<a href="tg://openmessage?user_id=6900964602">tg://openmessage?user_id=6900964602</a>\n\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
            '<tg-emoji emoji-id="5890787894049643800">⛔</tg-emoji> <b>غير مسئولين عن أي يوزر تاني لو اتعاملت مع منتحل.</b>'
        ),
        "link": "tg://openmessage?user_id=6900964602",
        "button_link": "https://t.me/Rahoma_20",
        "button": "تواصل",
    },
}


def _candidate_image(card_key: str) -> Path | None:
    image_name = CARD_DETAILS[card_key].get("image")
    if not image_name:
        return None
    image_path = BASE_DIR / image_name
    return image_path if image_path.is_file() else None


def _roulette_keyboard(chat_id: int, participant_count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    f"🎰 انضمام للسحب ({participant_count})",
                    callback_data=f"roulette_join:{chat_id}",
                    style="success",
                )
            ]
        ]
    )


def parse_duration(time_str: str) -> timedelta | None:
    match = re.match(r"^(\d+)([smhd])$", time_str.lower())
    if not match:
        return None
    val, unit = int(match.group(1)), match.group(2)
    if unit == "s":
        return timedelta(seconds=val)
    elif unit == "m":
        return timedelta(minutes=val)
    elif unit == "h":
        return timedelta(hours=val)
    elif unit == "d":
        return timedelta(days=val)
    return None


async def track_user_map(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return
    user_map = context.application.bot_data["user_map"]
    uid_str = str(user.id)
    uname = user.username if user.username else ""
    uname_key = uname.lower()
    changed = False

    if uid_str not in user_map or user_map[uid_str].get("username") != uname:
        user_map[uid_str] = {"name": user.full_name, "username": uname}
        changed = True

    if uname_key and user_map.get(f"@{uname_key}") != uid_str:
        user_map[f"@{uname_key}"] = uid_str
        changed = True

    if changed:
        db_save("user_map", user_map)


async def resolve_target_and_details(
    update: Update, context: ContextTypes.DEFAULT_TYPE, tokens: list[str]
) -> tuple[int | None, str | None, str, timedelta | None, str]:
    message = update.effective_message
    target_id: int | None = None
    target_username: str | None = None
    target_name: str | None = None
    duration: timedelta | None = None
    unused_tokens: list[str] = []

    full_text = message.text or message.caption or ""
    id_pattern = re.search(r"\b(\d{6,12})\b", full_text)
    if id_pattern:
        target_id = int(id_pattern.group(1))

    if message.reply_to_message and message.reply_to_message.from_user:
        target_user = message.reply_to_message.from_user
        if not target_id:
            target_id = target_user.id
        target_username = target_user.username if target_user.username else None
        target_name = f'<a href="tg://user?id={target_user.id}">{escape(target_user.full_name)}</a>'
        for token in tokens:
            dur = parse_duration(token)
            if dur and duration is None:
                duration = dur
            else:
                unused_tokens.append(token)
    else:
        if message.entities:
            for entity in message.entities:
                if entity.type == MessageEntityType.TEXT_MENTION and entity.user:
                    target_id = entity.user.id
                    target_username = entity.user.username if entity.user.username else None
                    target_name = f'<a href="tg://user?id={entity.user.id}">{escape(entity.user.full_name)}</a>'
                    break

        for token in tokens:
            dur = parse_duration(token)
            if dur and duration is None:
                duration = dur
                continue

            clean_token = token.strip().rstrip(",;.:")
            if clean_token.isdigit() and target_id == int(clean_token):
                continue

            if clean_token.startswith("@") and target_username is None:
                uname = clean_token.lstrip("@")
                resolved_id, resolved_uname, resolved_name = await resolve_username_to_user(uname, context)
                if resolved_id:
                    target_id = resolved_id
                    target_username = resolved_uname
                    target_name = f'<a href="tg://user?id={resolved_id}">{escape(resolved_name)}</a>'
                else:
                    target_username = uname
                continue

            unused_tokens.append(token)

    if target_id and not target_name:
        resolved_name, resolved_uname = await resolve_user_id_info(target_id, context)
        target_username = target_username or resolved_uname
        target_name = f'<a href="tg://user?id={target_id}">{escape(resolved_name)}</a>'

    if not target_name and target_username:
        target_name = f'<a href="https://t.me/{target_username}">@{target_username}</a>'

    reason = " ".join(unused_tokens).strip() or "لم يُحدد"
    return target_id, target_username, target_name or "", duration, reason


# -------------------------------------------------------------
# Accurate Status Detection (فحص دقيق لكافة الحالات)
# -------------------------------------------------------------

async def get_accurate_chat_status(chat_id: int, user_id: int, context: ContextTypes.DEFAULT_TYPE) -> str:
    """Return one concise, factual membership label for rank/status cards."""
    if not user_id:
        return "غير معروف"
    try:
        member = await context.bot.get_chat_member(chat_id=chat_id, user_id=int(user_id))
        status = member.status
        if status == ChatMemberStatus.OWNER:
            return "مالك"
        if status == ChatMemberStatus.ADMINISTRATOR:
            return "مشرف"
        if status == ChatMemberStatus.RESTRICTED:
            # الحكم على التقييد يعتمد على صلاحية إرسال الرسائل النصية فقط.
            # قيود الصور/الفيديو/الملفات/الصوت لا تُعتبر تقييدًا، وقيمة None ليست دليلًا على التقييد.
            can_send = getattr(member, "can_send_messages", None)
            if can_send is None:
                can_send = getattr(getattr(member, "permissions", None), "can_send_messages", None)
            if can_send is False:
                return "مقيد"
            if getattr(member, "is_member", True) is False:
                return "خارج المجموعة"
            return "عضو"
        if status in (ChatMemberStatus.BANNED, "kicked"):
            return "محظور"
        if status == ChatMemberStatus.LEFT:
            return "خارج المجموعة"
        return "عضو"
    except TelegramError:
        return "خارج المجموعة"
    except Exception:
        return "غير معروف"


# -------------------------------------------------------------
# Comprehensive Guide Text Builder
# -------------------------------------------------------------

def build_full_help_guide() -> str:
    return (
        '<tg-emoji emoji-id="5875344888165308204">👑</tg-emoji> <b>الـدلـيـل الـشـامـل لأوامـر وتـعـلـيـمـات الـبـوت</b> <tg-emoji emoji-id="5875344888165308204">👑</tg-emoji>\n'
        '━━━━━━━━━━━━━━━━━━━━\n\n'
        '<blockquote><b><tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji> أولاً: إدارة الرتب والإشراف المتقدم</b>\n'
        '• <code>رفع ادمن</code> / <code>تنزيل ادمن</code> ➔ تعيين أدمن داخل البوت بصلاحيات مخصصة (دون إشراف الجروب).\n'
        '• <code>رفع مدير</code> / <code>تنزيل مدير</code> ➔ ترقية مدير بالبوت بصلاحيات تحكم إدارية كاملة.\n'
        '• <code>رفع مميز</code> / <code>تنزيل مميز</code> ➔ منح رتبة العضو المميز كصفة شرفية.\n'
        '• <code>رفع مشرف</code> / <code>تنزيل مشرف</code> ➔ ترقية مشرف فعلي في تيليجرام بصلاحيات المجموعة.\n'
        '• <code>أضف مشرف</code> / <code>أضف مالك</code> ➔ إنشاء وتثبيت كارت تعريف رسمي بالشات.\n'
        '• <code>حظر</code>، <code>طرد</code>، <code>كتم</code> ➔ بالرد أو باليوزر/الآيدي (مع إمكانية تحديد مدة مثل <code>كتم 30m</code>).\n'
        '• <code>رفع القيود</code> / <code>فك الكتم</code> / <code>فك الحظر</code> ➔ إلغاء العقوبات وإعادة كامل الصلاحيات.\n'
        '• <code>المقيدين</code> / <code>مسح المقيدين</code> ➔ استعراض المكتومين وإلغاء تقييد الجميع دفعة واحدة.\n'
        '• <code>مسح [عدد]</code> ➔ حذف الرسائل الحية بدقة تامة وبدون أي نقص.\n'
        '• <code>تثبيت</code> أو <code>ثبت</code> ➔ تثبيت الرسالة فوراً بالرد عليها.</blockquote>\n\n'
        '<blockquote><b>🕌 ثانياً: مواقيت الصلاة والتحكم بالشات</b>\n'
        '• <code>قفل الشات</code> / <code>فتح الشات</code> ➔ إغلاق وفتح إرسال الرسائل يدوياً باسم المشرف المنفذ.\n'
        '• <code>تفعيل الصلاة</code> / <code>تعطيل الصلاة</code> ➔ تشغيل ميزة قفل الشات 15 دقيقة تلقائياً وقت الأذان.\n'
        '• <code>مواقيت الصلاة</code> ➔ استعلام مواعيد الأذان اليومية لمدينة القاهرة ومصر.</blockquote>\n\n'
        '<blockquote><b>🎰 ثالثاً: السحوبات والمسابقات (الروليت)</b>\n'
        '• <code>روليت [الجائزة]</code> ➔ إنشاء سحب تلقائي لفائز واحد.\n'
        '• <code>روليت [العدد] [الجائزة]</code> ➔ إنشاء سحب لعدة فائزين (مثال: <code>روليت 3 50ج</code> أو <code>روليت [2] كود</code>).\n'
        '• <code>تم</code> ➔ إنهاء السحب وإعلان الفائزين عشوائياً بالبطاقة الاحتفالية.</blockquote>\n\n'
        '<blockquote><b><tg-emoji emoji-id="5444989577422993015">🔍</tg-emoji> رابعاً: الاستعلامات والكشف (متاح للجميع)</b>\n'
        '• <code>كشف</code> ➔ فحص حساب العضو بالكامل وبيان رتبته وحالة ضمانه.\n'
        '• <code>رتبتي</code> / <code>رتبته</code> ➔ معرفة الرتبة الحالية في المجموعة والبوت.\n'
        '• <code>ايدي</code> ➔ عرض بطاقة الهوية الرقمية مع زر نسخ سريع للآيدي.</blockquote>\n\n'
        '<blockquote><b><tg-emoji emoji-id="5775979900649347911">💵</tg-emoji> خامساً: المعاملات، التجار، والأمان المالي</b>\n'
        '• <code>رفع تاجر [المبلغ]</code> / <code>تنزيل تاجر</code> ➔ تسجيل التاجر بسقف ضمان معتمد.\n'
        '• <code>التجار</code> ➔ استعراض قائمة التجار المعتمدين وسقوف ضمانهم.\n'
        '• <code>تسجيل [رقم الكاش] - [الاسم] - [المبلغ] - [السلعة]</code> ➔ توثيق مطالبة مالية.\n'
        '• <code>تم التحويل [رقم الكاش]</code> ➔ إرسال إيصال الدفع مع زر تأكيد الاستلام وتقييم التاجر.\n'
        '• <code>فحص #TX-XXXX</code> ➔ استعلام فوري عن تفاصيل المعاملة والتقييم المسجل.\n'
        '• <code>نصاب [اليوزر/الآيدي] [السبب]</code> ➔ الحظر الفوري والإدراج بالقائمة السوداء العامة.\n'
        '• <code>فحص</code> / <code>النصابين</code> ➔ الكشف والاطلاع على القائمة السوداء الموحدة.</blockquote>\n\n'
        '<blockquote><b>🔘 سادساً: التخصيص والنشر</b>\n'
        '• <code>انشر</code> أو <code>انشر النص</code> ➔ نشر تفاعلي أو مباشر باسم البوت.\n'
        '• <code>send</code> بالرد على رسالة ➔ إرسال المحتوى باسم البوت ردًا على العضو المستهدف.\n'
        '• <code>أضف رد</code> / <code>مسح رد</code> ➔ إدارة الردود التلقائية، بما فيها الفويس والصوت.\n'
        '• <code>أضف رد بزر</code> / <code>أضف زر</code> ➔ إنشاء رد تفاعلي بالطريقة الجديدة مع أزرار متعددة وألوان مختلفة.</blockquote>\n'
        '━━━━━━━━━━━━━━━━━━━━\n'
        '⚡ <i>بوت الإدارة والوساطة المعتمد — نظام حماية شامل ومستقر 100%.</i>'
    )


# -------------------------------------------------------------
# Prayer Times & Group Auto-Lock System (Background Loop Engine)
# -------------------------------------------------------------

async def fetch_prayer_timings() -> dict[str, str] | None:
    url = "https://api.aladhan.com/v1/timingsByCity?city=Cairo&country=Egypt&method=5"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    }
    try:
        async with httpx.AsyncClient(timeout=15, headers=headers, follow_redirects=True) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("data", {}).get("timings", {})
    except Exception as e:
        logger.error(f"Error fetching prayer timings API: {e}")
    return None


async def reopen_chat_after_prayer(bot, chat_id: int, prayer_name: str):
    await asyncio.sleep(900)  # الانتظار 15 دقيقة
    open_perms = ChatPermissions(
        can_send_messages=True,
        can_send_photos=True,
        can_send_videos=True,
        can_send_other_messages=True,
        can_send_polls=True,
        can_add_web_page_previews=True,
    )
    try:
        await bot.set_chat_permissions(chat_id=chat_id, permissions=open_perms)
        reopen_msg = (
            f'<tg-emoji emoji-id="5258476306152038031">🔓</tg-emoji> <b>تم فتح الشات الآن بعد انتهاء وقت صلاة {prayer_name}.</b>\n\n'
            'تقبل الله منا ومنكم صالح الأعمال، وبارك في أوقاتكم <tg-emoji emoji-id="5850481872244578228">🌸</tg-emoji>'
        )
        await bot.send_message(chat_id=chat_id, text=reopen_msg, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Failed to auto-reopen chat {chat_id}: {e}")


async def trigger_prayer_event(application: Application, p_name: str):
    prayer_chats = application.bot_data.get("prayer_chats", {})
    lock_perms = ChatPermissions(can_send_messages=False)

    for cid_str, active in list(prayer_chats.items()):
        if not active:
            continue
        try:
            chat_id = int(cid_str)
            
            call_text = (
                f'🕌 <b>حان الآن موعد صلاة {p_name}</b>\n\n'
                "أقبل على صلاتك بقلب خاشع مستشعرًا وقوفك بين يدي ملك الملوك، "
                'وداعيًا بما تشتهي في سجودك، رحمنا ورحمكم الله <tg-emoji emoji-id="5891258656825022695">💞</tg-emoji>'
            )
            await application.bot.send_message(chat_id=chat_id, text=call_text, parse_mode=ParseMode.HTML)

            await application.bot.set_chat_permissions(chat_id=chat_id, permissions=lock_perms)
            lock_announcement = (
                f'<tg-emoji emoji-id="5258476306152038031">🔒</tg-emoji> <b>تم قفل الشات تلقائياً بواسطة بوت الإدارة لحين انتهاء صلاة {p_name}.</b>\n\n'
                '⏳ <i>سيتم إعادة فتح الشات تلقائياً بعد 15 دقيقة.</i>'
            )
            await application.bot.send_message(chat_id=chat_id, text=lock_announcement, parse_mode=ParseMode.HTML)

            asyncio.create_task(reopen_chat_after_prayer(application.bot, chat_id, p_name))

        except Exception as e:
            logger.error(f"Error handling prayer auto-lock for chat {cid_str}: {e}")


async def prayer_times_background_loop(application: Application):
    logger.info("Prayer Times background worker started successfully.")
    cached_day = None
    today_prayers: dict[str, str] = {}
    triggered_today: set[str] = set()

    while True:
        try:
            now_cairo = datetime.now(CAIRO_TZ)
            current_day = now_cairo.strftime("%Y-%m-%d")

            if current_day != cached_day:
                timings = await fetch_prayer_timings()
                if timings:
                    today_prayers = timings
                    cached_day = current_day
                    triggered_today.clear()
                    application.bot_data["today_prayer_cache"] = today_prayers
                    logger.info(f"Prayer times cached for {current_day}")
                else:
                    await asyncio.sleep(60)
                    continue

            current_hm = now_cairo.strftime("%H:%M")

            for p_key, p_arabic in PRAYER_NAMES.items():
                p_time = today_prayers.get(p_key)
                if p_time and p_time == current_hm and p_key not in triggered_today:
                    triggered_today.add(p_key)
                    logger.info(f"Triggering prayer: {p_arabic} at {current_hm}")
                    asyncio.create_task(trigger_prayer_event(application, p_arabic))

            await asyncio.sleep(25)
        except Exception as err:
            logger.error(f"Error in prayer loop: {err}")
            await asyncio.sleep(30)


async def handle_prayer_and_lock_management(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.effective_message
    user = update.effective_user
    chat = update.effective_chat
    if not message or not user or not chat or chat.type == "private":
        return False

    raw_text = (message.text or message.caption or "").strip()
    if not raw_text:
        return False

    raw_text = apply_alias(raw_text, {"lock_chat", "unlock_chat"})
    prayer_chats = context.application.bot_data.setdefault("prayer_chats", db_load("prayer_chats"))

    if raw_text in ["قفل الشات", "قفل الجروب"]:
        # السماح لمالك البوت وأدمنيه الداخليين ومشرفي تيليجرام بإدارة القفل.
        if not await is_management_admin(update, context):
            return True

        # لا نغيّر صلاحيات تيليجرام العامة هنا؛ لأن ذلك يمنع حتى مديري البوت
        # الذين ليسوا مشرفين رسميين. بدلاً من ذلك نحذف رسائل الأعضاء أثناء القفل.
        locked = context.application.bot_data.setdefault("manual_chat_locks", db_load("manual_chat_locks"))
        locked[str(chat.id)] = {"locked": True, "by": user.id, "at": datetime.now(timezone.utc).isoformat()}
        db_save("manual_chat_locks", locked)
        admin_disp = user.mention_html()
        await message.reply_text(
            f'<tg-emoji emoji-id="5258476306152038031">🔒</tg-emoji> <b>تم قفل الشات بواسطة:</b> {admin_disp}\n\n'
            f'<i>سيتم حذف رسائل الأعضاء أثناء القفل، مع السماح للمشرفين ومديري البوت بالكلام.</i>',
            parse_mode=ParseMode.HTML,
        )
        return True

    if raw_text in ["فتح الشات", "فتح الجروب"]:
        # نفس صلاحيات القفل: أدمن البوت أو مشرف تيليجرام.
        if not await is_management_admin(update, context):
            return True

        locked = context.application.bot_data.setdefault("manual_chat_locks", db_load("manual_chat_locks"))
        locked.pop(str(chat.id), None)
        db_save("manual_chat_locks", locked)
        admin_disp = user.mention_html()
        await message.reply_text(
            f'<tg-emoji emoji-id="5258476306152038031">🔓</tg-emoji> <b>تم فتح الشات بواسطة:</b> {admin_disp}\n\n'
            f'<i>تم إيقاف القفل اليدوي، ويمكن للأعضاء التحدث من جديد.</i>',
            parse_mode=ParseMode.HTML,
        )
        return True

    if raw_text in ["تفعيل الصلاة", "تفعيل مواقيت الصلاة"]:
        if not await is_real_admin(update, context):
            return True

        prayer_chats[str(chat.id)] = True
        db_save("prayer_chats", prayer_chats)
        await message.reply_text(
            '🕌 <b>تم تفعيل ميزة مواقيت الصلاة في هذا الشات بنجاح!</b>\n\n'
            '⚡ <b>نظام التشغيل:</b>\n'
            '• سيتم إرسال رسالة الأذان التذكيرية في موعد كل صلاة.\n'
            '• سيتم إغلاق الشات تلقائياً لمدة <b>15 دقيقة</b> وإعادة فتحه بعد انتهاء الصلاة.',
            parse_mode=ParseMode.HTML,
        )
        return True

    if raw_text in ["تعطيل الصلاة", "ايقاف الصلاة", "إيقاف الصلاة"]:
        if not await is_real_admin(update, context):
            return True

        prayer_chats[str(chat.id)] = False
        db_save("prayer_chats", prayer_chats)
        await message.reply_text("✅ تم تعطيل تنبيهات مواقيت الصلاة والقفل التلقائي في هذا الشات.")
        return True

    if raw_text in ["مواقيت الصلاة", "مواعيد الصلاة"]:
        t_data = context.application.bot_data.get("today_prayer_cache")
        if not t_data:
            t_data = await fetch_prayer_timings()
            if t_data:
                context.application.bot_data["today_prayer_cache"] = t_data

        if t_data:
            p_lines = [
                "🕌 <b>مواقيت الصلاة لليوم بتوقيت مصر (القاهرة):</b>\n"
                "━━━━━━━━━━━━━━━━━━━━"
            ]
            for pk, pa in PRAYER_NAMES.items():
                p_lines.append(f"• <b>صلاة {pa}:</b> <code>{t_data.get(pk, '')}</code>")
            p_lines.append('━━━━━━━━━━━━━━━━━━━━\nتقبل الله منا ومنكم صالح الأعمال <tg-emoji emoji-id="5850481872244578228">🌸</tg-emoji>')
            await message.reply_text("\n".join(p_lines), parse_mode=ParseMode.HTML)
            return True
        else:
            await message.reply_text("تعذر جلب مواقيت الصلاة حالياً، جاري إعادة المحاولة تلقائياً.")
            return True

    return False


# -------------------------------------------------------------
# Handlers: Start Command in Private & Direct Deep-Linking
# -------------------------------------------------------------

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user
    chat = update.effective_chat
    if not message or not user or not chat or chat.type != "private":
        return

    # فحص المعاملات الممررة عبر الرابط (مثل: /start commands)
    if context.args and context.args[0] == "commands":
        await message.reply_text(
            build_full_help_guide(),
            parse_mode=ParseMode.HTML,
        )
        return

    bot_username = context.bot.username

    add_bot_url = f"https://t.me/{bot_username}?startgroup=true&admin=post_messages+edit_messages+delete_messages+restrict_members+invite_users+pin_messages"

    start_rows = [
            [InlineKeyboardButton("➕ أضفني إلى مجموعتك", url=add_bot_url, style="success")],
            [
                InlineKeyboardButton("👑 مطور البوت", url="https://t.me/BRQ97"),
                InlineKeyboardButton("📜 دليل الأوامر", callback_data="show_help_callback"),
            ],
            [InlineKeyboardButton("🌐 اللغة: العربية 🇪🇬", callback_data="lang_select_info")],
        ]
    if await is_bot_owner(user.id, context):
        start_rows.append([InlineKeyboardButton("👑 لوحة تحكم الأدمن", callback_data="profanity:open")])
    keyboard = InlineKeyboardMarkup(start_rows)

    welcome_text = (
        f'👋 <b>أهلاً بك يا</b> {user.mention_html()} <b>في بوت الإدارة والوساطة المعتمد!</b>\n'
        '━━━━━━━━━━━━━━━━━━━━\n\n'
        '<tg-emoji emoji-id="5890795783904565270">⚡</tg-emoji> <b>أنا حارس وضامن مجموعتك على تيليجرام:</b>\n\n'
        '<tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji> <b>نشر وتثبيت:</b> نشر رسائل باسم البوت وتثبيت الإعلانات.\n'
        '<tg-emoji emoji-id="5891235511246264255">🔨</tg-emoji> <b>إشراف ذكي:</b> رفع وتنزيل المشرفين فعلياً بالجروب وقفل وفتح الشات.\n'
        '🕌 <b>مواقيت الصلاة:</b> تنبيهات الأذان وقفل الشات 15 دقيقة تلقائياً أثناء الصلاة.\n'
        '<tg-emoji emoji-id="5890814398292826361">🚨</tg-emoji> <b>كشف النصابين:</b> قاعدة بيانات عامة وموحدة لحظر المحتالين فوراً.\n'
        '<tg-emoji emoji-id="5775979900649347911">💵</tg-emoji> <b>توثيق المعاملات:</b> تسجيل إيصالات الكاش وسقوف ضمان التجار.\n\n'
        '<i>اضغط على الزر الأخضر بالأسفل لإضافتي مشرفاً في جروبك والبدء فوراً:</i>'
    )

    await message.reply_text(
        welcome_text,
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
    )


async def handle_start_help_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return

    if query.data == "show_help_callback":
        await query.answer()
        await query.message.reply_text(build_full_help_guide(), parse_mode=ParseMode.HTML)

    elif query.data == "lang_select_info":
        await query.answer("اللغة الحالية المعتمدة هي اللغة العربية 🇪🇬", show_alert=True)


# -------------------------------------------------------------
# Handlers: Commands Deep-Link Trigger (أمر الأوامر للشات)
# -------------------------------------------------------------

async def handle_commands_menu_trigger(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.effective_message
    chat = update.effective_chat
    if not message or not chat:
        return False

    raw_text = (message.text or message.caption or "").strip()
    if raw_text not in ["اوامر", "أوامر", "الاوامر", "الأوامر", "/commands"]:
        return False

    if chat.type == "private":
        await message.reply_text(build_full_help_guide(), parse_mode=ParseMode.HTML)
        return True

    bot_username = context.bot.username
    deep_link_url = f"https://t.me/{bot_username}?start=commands"

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📜 معرفة أوامر البوت ➜",
                    url=deep_link_url,
                    style="success",
                )
            ]
        ]
    )

    guide_prompt = (
        '<blockquote><tg-emoji emoji-id="5890978075201509010">⭐</tg-emoji> <b>دليل أوامر البوت الشامل والتعليمات</b>\n\n'
        'للاطلاع على جميع أوامر الإشراف، الروليت، الرتب، الكشف، والمعاملات المالية بالشرح والتنسيق الكامل؛ اضغط على الزر بالأسفل للانتقال لخاص البوت:</blockquote>'
    )

    await message.reply_text(
        guide_prompt,
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
    )
    return True


# -------------------------------------------------------------
# Handlers: Broadcast / Publish Command (انشر)
# -------------------------------------------------------------

def _utf16_units(value: str) -> int:
    """Telegram entity offsets use UTF-16 code units."""
    return len(value.encode("utf-16-le")) // 2


def _entities_after_prefix(entities, prefix: str):
    """Copy and rebase message entities after removing a command prefix."""
    if not entities:
        return []
    removed = _utf16_units(prefix)
    result = []
    for entity in entities:
        start = int(entity.offset)
        end = start + int(entity.length)
        if end <= removed:
            continue
        # MessageEntity objects in python-telegram-bot are immutable. Build a
        # fresh entity with rebased UTF-16 offsets instead of assigning attributes.
        new_offset = max(0, start - removed)
        new_length = max(0, end - max(start, removed))
        if new_length:
            data = entity.to_dict()
            data["offset"] = new_offset
            data["length"] = new_length
            result.append(MessageEntity.de_json(data, None))
    return result


async def _send_message_content_as_bot(message, context, chat_id: int, *, reply_to_message_id: int | None = None,
                                       payload_text: str | None = None, entities=None):
    """Send the source message as the bot, preserving Telegram entities where possible."""
    kwargs = {"chat_id": chat_id}
    if reply_to_message_id is not None:
        kwargs["reply_to_message_id"] = reply_to_message_id

    is_media = any((message.photo, message.video, message.animation, message.voice,
                    message.audio, message.document, message.sticker, message.video_note))
    text_payload = payload_text if payload_text is not None else (message.text or "")
    caption_payload = payload_text if payload_text is not None else (message.caption or "")
    media_entities = entities if entities is not None else (message.caption_entities if message.caption is not None else message.entities)
    try:
        if message.photo:
            return await context.bot.send_photo(photo=message.photo[-1].file_id, caption=caption_payload or None,
                                                caption_entities=media_entities or None, **kwargs)
        if message.video:
            return await context.bot.send_video(video=message.video.file_id, caption=caption_payload or None,
                                                caption_entities=media_entities or None, **kwargs)
        if message.animation:
            return await context.bot.send_animation(animation=message.animation.file_id, caption=caption_payload or None,
                                                    caption_entities=media_entities or None, **kwargs)
        if message.voice:
            return await context.bot.send_voice(voice=message.voice.file_id, caption=caption_payload or None,
                                                caption_entities=media_entities or None, **kwargs)
        if message.audio:
            return await context.bot.send_audio(audio=message.audio.file_id, caption=caption_payload or None,
                                                caption_entities=media_entities or None, **kwargs)
        if message.document:
            return await context.bot.send_document(document=message.document.file_id, caption=caption_payload or None,
                                                   caption_entities=media_entities or None, **kwargs)
        if message.video_note:
            return await context.bot.send_video_note(video_note=message.video_note.file_id, **kwargs)
        if message.sticker:
            return await context.bot.send_sticker(sticker=message.sticker.file_id, **kwargs)
        if text_payload:
            return await context.bot.send_message(text=text_payload, entities=entities if entities is not None else (message.entities or None), **kwargs)
        raise ValueError("الرسالة لا تحتوي على محتوى مدعوم")
    except TelegramError:
        # Telegram's original copy path is the safest way to preserve custom entities,
        # but is only suitable when no command prefix needs to be removed.
        if payload_text is None and not reply_to_message_id:
            return await context.bot.copy_message(chat_id=chat_id, from_chat_id=message.chat_id,
                                                  message_id=message.message_id)
        raise


async def handle_send_as_bot(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message, user, chat = update.effective_message, update.effective_user, update.effective_chat
    if not message or not user or not chat or chat.type == "private":
        return False
    raw = (message.text or message.caption or "").strip()
    match = re.match(r"(?is)^send(?:\s+(.*))?$", raw)
    if not match:
        return False
    if not await is_real_admin(update, context):
        return True
    target = message.reply_to_message
    if not target:
        await message.reply_text("اعمل Reply على رسالة العضو المستهدف، ثم اكتب send وبعدها المحتوى.")
        return True
    # `send` alone may be followed by a separate content message; keep the target context.
    pending = context.application.bot_data.setdefault("pending_send_states", {})
    payload = (match.group(1) or "").strip()
    if not payload and not any((message.photo, message.video, message.animation, message.voice,
                                message.audio, message.document, message.sticker, message.video_note)):
        pending[user.id] = {"chat_id": chat.id, "target_message_id": target.message_id}
        try:
            await message.delete()
        except TelegramError:
            pass
        return True
    prefix_match = re.match(r"(?is)^(send\s*)", raw)
    prefix = prefix_match.group(1) if prefix_match else "send "
    source_entities = message.caption_entities if message.caption is not None else message.entities
    entities = _entities_after_prefix(source_entities, prefix) if payload else (source_entities or [])
    try:
        await _send_message_content_as_bot(message, context, chat.id, reply_to_message_id=target.message_id,
                                           payload_text=payload if payload else None, entities=entities)
        try:
            await message.delete()
        except TelegramError:
            pass
    except Exception as exc:
        logger.warning("send command failed: %s", exc, exc_info=True)
        await message.reply_text("تعذر إرسال المحتوى. راجع سجل الأخطاء وتأكد من صلاحيات البوت.")
    return True


async def handle_broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.effective_message
    user = update.effective_user
    chat = update.effective_chat
    if not message or not user or not chat:
        return False

    pending_broadcast = context.application.bot_data.setdefault("pending_broadcast_states", {})

    if user.id in pending_broadcast:
        state_data = pending_broadcast[user.id]
        if state_data.get("chat_id") != chat.id:
            return False

        raw_in = (message.text or message.caption or "").strip()
        if raw_in in ["الغاء", "إلغاء"]:
            del pending_broadcast[user.id]
            prompt_id = state_data.get("prompt_msg_id")
            if prompt_id:
                try:
                    await context.bot.delete_message(chat_id=chat.id, message_id=prompt_id)
                except Exception:
                    pass
            try:
                await message.delete()
            except Exception:
                pass
            await message.reply_text("تم إلغاء عملية النشر بنجاح.")
            return True

        prompt_id = state_data.get("prompt_msg_id")
        if prompt_id:
            try:
                await context.bot.delete_message(chat_id=chat.id, message_id=prompt_id)
            except Exception:
                pass

        try:
            await context.bot.copy_message(
                chat_id=chat.id,
                from_chat_id=chat.id,
                message_id=message.message_id
            )
            try:
                await message.delete()
            except Exception:
                pass
        except Exception:
            try:
                if message.text:
                    await context.bot.send_message(chat_id=chat.id, text=message.text_html or message.text, parse_mode=ParseMode.HTML)
                elif message.photo:
                    await context.bot.send_photo(chat_id=chat.id, photo=message.photo[-1].file_id, caption=message.caption_html or message.caption or "", parse_mode=ParseMode.HTML)
                elif message.video:
                    await context.bot.send_video(chat_id=chat.id, video=message.video.file_id, caption=message.caption_html or message.caption or "", parse_mode=ParseMode.HTML)
                elif message.animation:
                    await context.bot.send_animation(chat_id=chat.id, animation=message.animation.file_id, caption=message.caption_html or message.caption or "", parse_mode=ParseMode.HTML)
                elif message.sticker:
                    await context.bot.send_sticker(chat_id=chat.id, sticker=message.sticker.file_id)
                try:
                    await message.delete()
                except Exception:
                    pass
            except Exception as e:
                logger.error(f"Error copying broadcast message: {e}")

        del pending_broadcast[user.id]
        return True

    text = (message.text or message.caption or "").strip()
    # النشر المباشر: انشر <المحتوى>، مع إزاحة entities بعد حذف البادئة.
    direct_match = re.match(r"(?s)^انشر\s+(.+)$", text)
    if direct_match:
        if not await is_real_admin(update, context):
            return True
        prefix = text[:direct_match.start(1)]
        payload = direct_match.group(1)
        src_entities = message.caption_entities if message.caption is not None else message.entities
        entities = _entities_after_prefix(src_entities, prefix)
        try:
            await _send_message_content_as_bot(message, context, chat.id, payload_text=payload, entities=entities)
            try:
                await message.delete()
            except TelegramError:
                pass
        except Exception as exc:
            logger.warning("Direct publish failed: %s", exc, exc_info=True)
            await message.reply_text("تعذر نشر المحتوى؛ راجع سجل الأخطاء.")
        return True

    if text == "انشر":
        if not await is_real_admin(update, context):
            return True

        prompt_msg = await message.reply_text(
            '<tg-emoji emoji-id="5965443110077995385">📢</tg-emoji> <b>أرسل الآن الرسالة أو المنشور الذي تريد نشره في المجموعة:</b>\n\n'
            '<i>(يقبل النصوص المنسقة بكامل الإيموجيات، الصور، الفيديوهات، الملصقات)</i>\n'
            '<i>(أو اكتب إلغاء للتراجع)</i>',
            parse_mode=ParseMode.HTML,
        )

        pending_broadcast[user.id] = {
            "chat_id": chat.id,
            "prompt_msg_id": prompt_msg.message_id,
        }

        try:
            await message.delete()
        except Exception:
            pass

        return True

    return False


# -------------------------------------------------------------
# Handlers: Real Group Admin Promotion (رفع وتنزيل مشرف في المجموعة)
# -------------------------------------------------------------

GROUP_ADMIN_PERMISSION_LABELS = {
    "can_change_info": "تغيير معلومات المجموعة",
    "can_delete_messages": "حذف الرسائل",
    "can_restrict_members": "حظر وكتم الأعضاء",
    "can_invite_users": "دعوة المستخدمين عبر الرابط",
    "can_pin_messages": "تثبيت الرسائل",
    "can_manage_video_chats": "إدارة المحادثات الصوتية/الفيديو",
    "can_promote_members": "إضافة مشرفين جدد",
    "can_manage_topics": "إدارة الموضوعات",
    "can_post_messages": "نشر الرسائل في القنوات/المجموعات الداعمة",
    "can_edit_messages": "تعديل رسائل الآخرين في القنوات",
    "can_post_stories": "نشر القصص",
    "can_edit_stories": "تعديل القصص",
    "can_delete_stories": "حذف القصص",
    "can_manage_direct_messages": "إدارة الرسائل المباشرة",
    "can_manage_tags": "إدارة وسوم الأعضاء",
}


def build_group_admin_perm_markup(u_id: str, c_id: str, p: dict) -> InlineKeyboardMarkup:
    rows = []
    for perm_key, label in GROUP_ADMIN_PERMISSION_LABELS.items():
        enabled = bool(p.get(perm_key, False))
        # Green when enabled, red when disabled; make the state explicit in the label too.
        state = "🟢 مفعّلة" if enabled else "🔴 مقفولة"
        style = "success" if enabled else "danger"
        rows.append([InlineKeyboardButton(
            f"{label}: {state}",
            callback_data=f"gperm:{perm_key}:{u_id}:{c_id}",
            style=style,
        )])
    rows.append([InlineKeyboardButton(
        "💾 حفظ الصلاحيات ورفع المشرف",
        callback_data=f"gperm:apply:{u_id}:{c_id}",
        style="primary",
    )])
    return InlineKeyboardMarkup(rows)


async def handle_real_group_admin_commands(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.effective_message
    chat = update.effective_chat
    if not message or not chat or chat.type == "private":
        return False

    raw_text = (message.text or message.caption or "").strip()
    if not raw_text:
        return False

    lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]
    if not lines:
        return False

    is_promote = lines[0].startswith("رفع مشرف")
    is_demote = lines[0].startswith("تنزيل مشرف")

    if not is_promote and not is_demote:
        return False

    # The bot's primary owner may request promotion/demotion even if they do
    # not hold a Telegram admin role in this group. Telegram still enforces the
    # bot account's own rights and hierarchy when the API call is made.
    # Internal bot managers/admins do not get this bypass.
    actor = update.effective_user
    actor_is_owner = bool(actor and actor.id == PRIMARY_OWNER_ID)
    try:
        actor_member = await context.bot.get_chat_member(chat_id=chat.id, user_id=actor.id) if actor else None
        actor_has_group_right = bool(
            actor_member
            and (
                actor_member.status == ChatMemberStatus.OWNER
                or (
                    actor_member.status == ChatMemberStatus.ADMINISTRATOR
                    and bool(getattr(actor_member, "can_promote_members", False))
                )
            )
        )
    except TelegramError:
        actor_has_group_right = False
    actor_can_manage_admins = actor_is_owner or actor_has_group_right

    if not actor_can_manage_admins:
        await message.reply_text(
            "⛔ رفع وتنزيل مشرفي المجموعة متاح لمالك المجموعة، "
            "أو لمشرف في المجموعة لديه صلاحية إضافة مشرفين. رتبة مدير/أدمن البوت لا تكفي."
        )
        return True

    raw_tokens = raw_text.split()

    if is_demote:
        clean_tokens = [t for t in raw_tokens if t not in ["تنزيل", "مشرف"]]
        target_id, target_username, target_name, _, _ = await resolve_target_and_details(update, context, clean_tokens)

        if not target_id:
            await message.reply_text("⚠️ يرجى تحديد المشرف بالرد أو باليوزر أو بالآيدي لتنزيله من المجموعة.")
            return True

        if target_id == PRIMARY_OWNER_ID:
            await message.reply_text("⛔ لا يمكن تنزيل مالك ومطور البوت الأساسي.")
            return True

        # 1) نجلب بيانات العضو الفعلية من تيليجرام قبل محاولة التنزيل.
        try:
            target_member = await context.bot.get_chat_member(chat_id=chat.id, user_id=target_id)
        except TelegramError as e:
            await message.reply_text(
                f"تعذر جلب بيانات العضو من تيليجرام. السبب: {escape(str(getattr(e, 'message', None) or e))}",
                parse_mode=ParseMode.HTML,
            )
            return True

        if target_member.status == ChatMemberStatus.OWNER:
            await message.reply_text("⛔ لا يمكن تنزيل مالك المجموعة.")
            return True
        if target_member.status != ChatMemberStatus.ADMINISTRATOR:
            await message.reply_text("⚠️ هذا العضو ليس مشرفاً في المجموعة أصلاً.")
            return True

        # 2) هل يسمح تيليجرام للبوت بتعديل صلاحيات هذا المشرف؟
        # (المشرف الذي رفعه شخص آخر يدوياً قد لا يستطيع البوت تعديله).
        if getattr(target_member, "can_be_edited", None) is False:
            await message.reply_text(
                "⛔ تيليجرام لا يسمح للبوت بتعديل صلاحيات هذا المشرف، لأن البوت ليس هو من رفعه "
                "(أو أنه رُفع بواسطة مشرف آخر). لتنزيله: نزّله يدوياً من إعدادات المجموعة، "
                "أو اجعل من رفعه ينزّله، أو ارفعه من جديد عن طريق البوت."
            )
            return True

        # 3) التنزيل بتعطيل كل صلاحيات الإدارة.
        try:
            await context.bot.promote_chat_member(
                chat_id=chat.id,
                user_id=target_id,
                can_change_info=False,
                can_post_messages=False,
                can_edit_messages=False,
                can_delete_messages=False,
                can_invite_users=False,
                can_restrict_members=False,
                can_pin_messages=False,
                can_promote_members=False,
                can_manage_chat=False,
                can_manage_video_chats=False,
                can_manage_topics=False,
                can_post_stories=False,
                can_edit_stories=False,
                can_delete_stories=False,
                can_manage_direct_messages=False,
                can_manage_tags=False,
            )
            invalidate_admin_cache(chat.id)
            await message.reply_text(
                f'<tg-emoji emoji-id="5891235511246264255">✔️</tg-emoji> <b>تم تنزيل المشرف بنجاح!</b>\n'
                f'👤 <b>العضو:</b> {target_name}\n'
                f'تم تجريده من كافة صلاحيات الإشراف وإعادته كعضو عادي في المجموعة.',
                parse_mode=ParseMode.HTML,
            )
        except TelegramError as e:
            # نعرض سبب تيليجرام الحقيقي بدل رسالة عامة.
            err_text = str(getattr(e, "message", None) or e)
            low = err_text.lower()
            if "can't remove chat owner" in low:
                hint = "لا يمكن تعديل مالك المجموعة."
            elif "rights_not_modified" in low or "not modified" in low:
                hint = "صلاحيات هذا العضو لم تتغير (قد يكون غير مشرف بالفعل)."
            elif "not enough rights" in low or "chat_admin_required" in low:
                hint = "البوت لا يملك صلاحية «إضافة مشرفين جدد» في هذه المجموعة."
            elif "user_not_participant" in low or "participant_id_invalid" in low:
                hint = "العضو غير موجود في المجموعة."
            elif "can't demote chat creator" in low:
                hint = "لا يمكن تنزيل منشئ المجموعة."
            elif "user is an administrator of the chat" in low or "can't be edited" in low:
                hint = "تيليجرام لا يسمح للبوت بتعديل صلاحيات هذا المشرف."
            else:
                hint = None
            msg = "تعذر تنزيل المشرف."
            if hint:
                msg += f"\nالسبب: {hint}"
            msg += f"\nرد تيليجرام: {err_text}"
            await message.reply_text(escape(msg), parse_mode=ParseMode.HTML)
        return True

    clean_tokens = [t for t in raw_tokens if t not in ["رفع", "مشرف"]]
    target_id, target_username, target_name, _, _ = await resolve_target_and_details(update, context, clean_tokens)

    if not target_id:
        await message.reply_text("⚠️ يرجى تحديد العضو بالرد أو باليوزر أو بالآيدي لرفعه مشرفاً في المجموعة.")
        return True

    try:
        current_member = await context.bot.get_chat_member(chat_id=chat.id, user_id=target_id)
        if current_member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
            await message.reply_text("العضو مشرف بالفعل.")
            return True
    except TelegramError:
        pass

    pending_group_perms = context.application.bot_data.setdefault("pending_group_perms", {})
    state_key = f"{target_id}:{chat.id}"

    # All supported Telegram administrator rights are individually configurable.
    # Start with a limited baseline; the owner explicitly enables each extra right.
    default_perms = {key: False for key in GROUP_ADMIN_PERMISSION_LABELS}
    default_perms.update({
        "can_delete_messages": True,
        "can_restrict_members": True,
        "can_pin_messages": True,
        "can_invite_users": True,
        "target_name": target_name,
    })
    pending_group_perms[state_key] = default_perms

    await message.reply_text(
        f'<tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji> <b>لوحة ترقية مشرف رسمي في المجموعة</b> <tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji>\n'
        '━━━━━━━━━━━━━━━━━━━━\n\n'
        f'<tg-emoji emoji-id="5445092669522996408">👤</tg-emoji> <b>العضو:</b> {target_name}\n'
        f'<tg-emoji emoji-id="5935966809655088705">🪪</tg-emoji> <b>الآيدي:</b> <code>{target_id}</code>\n\n'
        f'<i>حدد الصلاحيات المطلوبة لهذا المشرف، ثم اضغط على زر التأكيد والرفع:</i>',
        parse_mode=ParseMode.HTML,
        reply_markup=build_group_admin_perm_markup(str(target_id), str(chat.id), default_perms),
    )
    return True


async def handle_group_admin_perm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data or not query.data.startswith("gperm:"):
        return

    parts = query.data.split(":")
    if len(parts) < 4:
        await query.answer("بيانات الطلب غير صحيحة.", show_alert=True)
        return
    action = parts[1]
    uid_str = parts[2]
    cid_str = parts[3]

    # Re-check authorization on every button press, including Save.
    # Only the primary bot owner gets the bypass; bot managers/admins do not.
    actor_is_owner = bool(query.from_user and query.from_user.id == PRIMARY_OWNER_ID)
    try:
        actor_member = await context.bot.get_chat_member(
            chat_id=int(cid_str), user_id=query.from_user.id
        ) if query.from_user else None
        actor_has_group_right = bool(
            actor_member
            and (
                actor_member.status == ChatMemberStatus.OWNER
                or (
                    actor_member.status == ChatMemberStatus.ADMINISTRATOR
                    and bool(getattr(actor_member, "can_promote_members", False))
                )
            )
        )
    except (TelegramError, ValueError):
        actor_has_group_right = False
    actor_can_manage_admins = actor_is_owner or actor_has_group_right

    if not actor_can_manage_admins:
        await query.answer(
            "المسموح مالك البوت الأساسي، أو مالك المجموعة، أو مشرف لديه صلاحية إضافة مشرفين. مدير/أدمن البوت لا يكفي.",
            show_alert=True,
        )
        return

    pending_group_perms = context.application.bot_data.setdefault("pending_group_perms", {})
    state_key = f"{uid_str}:{cid_str}"

    if state_key not in pending_group_perms:
        await query.answer("انتهت صلاحية هذه الجلسة، أعد كتابة الأمر مجدداً.", show_alert=True)
        return

    perm_data = pending_group_perms[state_key]

    if action == "apply":
        try:
            await context.bot.promote_chat_member(
                chat_id=int(cid_str),
                user_id=int(uid_str),
                can_change_info=bool(perm_data.get("can_change_info", False)),
                can_post_messages=bool(perm_data.get("can_post_messages", False)),
                can_edit_messages=bool(perm_data.get("can_edit_messages", False)),
                can_delete_messages=bool(perm_data.get("can_delete_messages", False)),
                can_invite_users=bool(perm_data.get("can_invite_users", False)),
                can_restrict_members=bool(perm_data.get("can_restrict_members", False)),
                can_pin_messages=bool(perm_data.get("can_pin_messages", False)),
                can_promote_members=bool(perm_data.get("can_promote_members", False)),
                can_manage_chat=True,
                can_manage_video_chats=bool(perm_data.get("can_manage_video_chats", False)),
                can_manage_topics=bool(perm_data.get("can_manage_topics", False)),
                can_post_stories=bool(perm_data.get("can_post_stories", False)),
                can_edit_stories=bool(perm_data.get("can_edit_stories", False)),
                can_delete_stories=bool(perm_data.get("can_delete_stories", False)),
                can_manage_direct_messages=bool(perm_data.get("can_manage_direct_messages", False)),
                can_manage_tags=bool(perm_data.get("can_manage_tags", False)),
            )
            del pending_group_perms[state_key]
            invalidate_admin_cache(int(cid_str))
            await query.answer("تم رفع المشرف في المجموعة بنجاح! ✅")
            await query.edit_message_text(
                f'<tg-emoji emoji-id="5812288471837975057">💯</tg-emoji> <b>تمت ترقية المشرف رسمياً في المجموعة!</b>\n'
                f'━━━━━━━━━━━━━━━━━━━━\n\n'
                f'👤 <b>المشرف:</b> {perm_data.get("target_name", uid_str)}\n'
                f'🪪 <b>الآيدي:</b> <code>{uid_str}</code>\n\n'
                f'⚡ <i>تم تطبيق صلاحيات تيليجرام التي اخترتها فقط لهذا المشرف.</i>',
                parse_mode=ParseMode.HTML,
                reply_markup=None,
            )
        except TelegramError:
            await query.answer("تعذر الرفع: تأكد أن البوت لديه صلاحية إضافة مشرفين.", show_alert=True)
        return

    if action not in GROUP_ADMIN_PERMISSION_LABELS:
        await query.answer("صلاحية غير معروفة.", show_alert=True)
        return
    perm_data[action] = not perm_data.get(action, False)
    await query.answer("تم تعديل الصلاحية")
    try:
        await query.edit_message_reply_markup(
            reply_markup=build_group_admin_perm_markup(uid_str, cid_str, perm_data)
        )
    except Exception:
        pass


# -------------------------------------------------------------
# Handlers: Bot Internal Roles (أدمن / مدير / مميز بالبوت فقط)
# -------------------------------------------------------------

def build_bot_role_perm_markup(u_id: str, c_id: str, role_title: str, p: dict) -> InlineKeyboardMarkup:
    del_icon = "✅" if p.get("can_purge") else "❌"
    ban_icon = "✅" if p.get("can_ban") else "❌"
    mute_icon = "✅" if p.get("can_mute") else "❌"
    pin_icon = "✅" if p.get("can_pin") else "❌"
    bl_icon = "✅" if p.get("can_blacklist") else "❌"

    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🗑️ مسح الرسائل: {del_icon}", callback_data=f"bperm:can_purge:{u_id}:{c_id}:{role_title}")],
        [InlineKeyboardButton(f"🚫 حظر الأعضاء: {ban_icon}", callback_data=f"bperm:can_ban:{u_id}:{c_id}:{role_title}")],
        [InlineKeyboardButton(f"🤐 كتم الأعضاء: {mute_icon}", callback_data=f"bperm:can_mute:{u_id}:{c_id}:{role_title}")],
        [InlineKeyboardButton(f"📌 تثبيت الرسائل: {pin_icon}", callback_data=f"bperm:can_pin:{u_id}:{c_id}:{role_title}")],
        [InlineKeyboardButton(f"🚨 إضافة نصابين: {bl_icon}", callback_data=f"bperm:can_blacklist:{u_id}:{c_id}:{role_title}")],
        [InlineKeyboardButton("💾 حفظ وتعيين الرتبة في البوت", callback_data=f"bperm:apply:{u_id}:{c_id}:{role_title}", style="success")],
    ])


async def handle_bot_internal_roles(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.effective_message
    chat = update.effective_chat
    if not message or not chat or chat.type == "private":
        return False

    raw_text = (message.text or message.caption or "").strip()
    if not raw_text:
        return False

    lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]
    if not lines:
        return False

    first_line = lines[0]
    cmds = ["رفع ادمن", "رفع أدمن", "رفع مدير", "رفع مميز", "تنزيل ادمن", "تنزيل أدمن", "تنزيل مدير", "تنزيل مميز"]
    matched_cmd = next((c for c in cmds if first_line.startswith(c)), None)
    if not matched_cmd:
        return False

    if not await is_real_admin(update, context):
        return True

    raw_tokens = raw_text.split()
    clean_tokens = [t for t in raw_tokens if t not in matched_cmd.split()]
    target_id, target_username, target_name, _, _ = await resolve_target_and_details(update, context, clean_tokens)

    if not target_id:
        await message.reply_text("⚠️ يرجى تحديد العضو بالرد أو باليوزر أو بالآيدي لتنفيذ الأمر.")
        return True

    bot_admins_map = context.application.bot_data.setdefault("bot_admins", db_load("bot_admins"))
    chat_admins = bot_admins_map.setdefault(str(chat.id), {})

    distinguished_map = context.application.bot_data.setdefault("distinguished_users", db_load("distinguished_users"))
    chat_dist = distinguished_map.setdefault(str(chat.id), {})

    # منع تكرار الترقية إذا كان العضو يحمل الرتبة بالفعل.
    if "مميز" in matched_cmd and "رفع" in matched_cmd and str(target_id) in chat_dist:
        await message.reply_text("العضو مميز بالفعل.")
        return True
    if ("ادمن" in matched_cmd or "أدمن" in matched_cmd or "مدير" in matched_cmd) and "رفع" in matched_cmd and str(target_id) in chat_admins:
        existing_role = chat_admins[str(target_id)].get("role", "أدمن")
        await message.reply_text(f"العضو {existing_role} بالفعل.")
        return True

    # معالجة أمر مميز
    if "مميز" in matched_cmd:
        if "رفع" in matched_cmd:
            chat_dist[str(target_id)] = {
                "name": target_name,
                "username": target_username or "",
                "promoted_by": update.effective_user.mention_html() if update.effective_user else "الأدمن",
                "date": datetime.now(timezone.utc).strftime("%Y-%m-%d")
            }
            distinguished_map[str(chat.id)] = chat_dist
            db_save("distinguished_users", distinguished_map)
            await message.reply_text(
                f'<tg-emoji emoji-id="5776113577211468831">⭐</tg-emoji> <b>ترقية عضو مميز</b> <tg-emoji emoji-id="5776113577211468831">⭐</tg-emoji>\n'
                f'━━━━━━━━━━━━━━━━━━━━\n\n'
                f'<tg-emoji emoji-id="5445092669522996408">👤</tg-emoji> <b>العضو:</b> {target_name}\n'
                f'<tg-emoji emoji-id="5935966809655088705">🪪</tg-emoji> <b>الآيدي:</b> <code>{target_id}</code>\n\n'
                f'🎉 تم منح العضو رتبة <b>عضو مميز الشات</b> بنجاح <tg-emoji emoji-id="5850481872244578228">🌸</tg-emoji>',
                parse_mode=ParseMode.HTML
            )
            return True
        else:
            if str(target_id) in chat_dist:
                del chat_dist[str(target_id)]
                distinguished_map[str(chat.id)] = chat_dist
                db_save("distinguished_users", distinguished_map)
                await message.reply_text(f"✅ تم تنزيل {target_name} من رتبة مميز بنجاح.")
            else:
                await message.reply_text("⚠️ هذا العضو ليس مميزاً بالفعل.")
            return True

    # معالجة تنزيل أدمن / مدير من البوت
    if "تنزيل" in matched_cmd:
        if str(target_id) in chat_admins:
            del chat_admins[str(target_id)]
            bot_admins_map[str(chat.id)] = chat_admins
            db_save("bot_admins", bot_admins_map)
            await message.reply_text(
                f'<tg-emoji emoji-id="5891235511246264255">✔️</tg-emoji> تم تنزيل {target_name} من رتبة البوت وتجريده من كافة الصلاحيات.',
                parse_mode=ParseMode.HTML
            )
        else:
            await message.reply_text("⚠️ هذا الشخص لا يمتلك أي رتبة خاصة في البوت.")
        return True

    # فتح لوحة الصلاحيات لرفع أدمن أو رفع مدير
    role_type = "مدير" if "مدير" in matched_cmd else "أدمن"
    icon = "💼" if role_type == "مدير" else "🛡️"

    pending_bot_perms = context.application.bot_data.setdefault("pending_bot_perms", {})
    state_key = f"{target_id}:{chat.id}"

    default_perms = {
        "can_purge": True,
        "can_ban": True,
        "can_mute": True,
        "can_pin": True,
        "can_blacklist": False,
        "target_name": target_name,
        "target_username": target_username,
        "role_type": role_type
    }
    pending_bot_perms[state_key] = default_perms

    await message.reply_text(
        f'{icon} <b>لوحة تعيين ({role_type} بالبوت فقط)</b> {icon}\n'
        f'━━━━━━━━━━━━━━━━━━━━\n\n'
        f'<tg-emoji emoji-id="5445092669522996408">👤</tg-emoji> <b>الحساب:</b> {target_name}\n'
        f'<tg-emoji emoji-id="5935966809655088705">🪪</tg-emoji> <b>الآيدي:</b> <code>{target_id}</code>\n\n'
        f'<i>حدد الصلاحيات التي ترغب بمنحها له في البوت، ثم اضغط حفظ وتعيين:</i>\n'
        f'*(لن يتم ترقيته في تيليجرام ولن تظهر رتبة بجانب اسمه في الجروب)*',
        parse_mode=ParseMode.HTML,
        reply_markup=build_bot_role_perm_markup(str(target_id), str(chat.id), role_type, default_perms)
    )
    return True


async def handle_bot_role_perm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data or not query.data.startswith("bperm:"):
        return

    if not await is_real_admin(update, context):
        await query.answer("هذا الإجراء متاح لمشرفي وملاك المجموعة فقط.", show_alert=True)
        return

    parts = query.data.split(":")
    action = parts[1]
    uid_str = parts[2]
    cid_str = parts[3]
    role_type = parts[4]

    pending_bot_perms = context.application.bot_data.setdefault("pending_bot_perms", {})
    state_key = f"{uid_str}:{cid_str}"

    if state_key not in pending_bot_perms:
        await query.answer("انتهت صلاحية هذه الجلسة، أعد كتابة الأمر مجدداً.", show_alert=True)
        return

    perm_data = pending_bot_perms[state_key]

    if action == "apply":
        bot_admins_map = context.application.bot_data.setdefault("bot_admins", db_load("bot_admins"))
        chat_admins = bot_admins_map.setdefault(cid_str, {})

        chat_admins[uid_str] = {
            "name": perm_data.get("target_name", uid_str),
            "username": perm_data.get("target_username", ""),
            "role": role_type,
            "permissions": {
                "can_purge": perm_data.get("can_purge", False),
                "can_ban": perm_data.get("can_ban", False),
                "can_mute": perm_data.get("can_mute", False),
                "can_pin": perm_data.get("can_pin", False),
                "can_blacklist": perm_data.get("can_blacklist", False),
            },
            "promoted_by": query.from_user.mention_html(),
            "date": datetime.now(timezone.utc).strftime("%Y-%m-%d")
        }
        bot_admins_map[cid_str] = chat_admins
        db_save("bot_admins", bot_admins_map)
        del pending_bot_perms[state_key]

        await query.answer(f"تم تعيين {role_type} بالبوت بنجاح! ✅")
        icon = "💼" if role_type == "مدير" else "🛡️"
        await query.edit_message_text(
            f'<tg-emoji emoji-id="5812288471837975057">💯</tg-emoji> <b>تم تعيين العضو بنجاح كـ ({role_type} في البوت)!</b>\n'
            f'━━━━━━━━━━━━━━━━━━━━\n\n'
            f'<tg-emoji emoji-id="5445092669522996408">👤</tg-emoji> <b>العضو:</b> {perm_data.get("target_name", uid_str)}\n'
            f'<tg-emoji emoji-id="5935966809655088705">🪪</tg-emoji> <b>الآيدي:</b> <code>{uid_str}</code>\n'
            f'{icon} <b>الرتبة:</b> {role_type} بالبوت فقط\n\n'
            f'⚡ <i>يمتلك الآن كافة الصلاحيات المحددة لدى البوت فقط بدون أن يكون مشرفاً بالمجموعة.</i>',
            parse_mode=ParseMode.HTML,
            reply_markup=None
        )
        return

    # تبديل حالة الصلاحية (True / False)
    perm_data[action] = not perm_data.get(action, False)
    await query.answer("تم تعديل الصلاحية")
    try:
        await query.edit_message_reply_markup(
            reply_markup=build_bot_role_perm_markup(uid_str, cid_str, role_type, perm_data)
        )
    except Exception:
        pass


# -------------------------------------------------------------
# Handlers: Dynamic Admin & Owner Cards
# -------------------------------------------------------------

async def handle_dynamic_admin_cards(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.effective_message
    user = update.effective_user
    chat = update.effective_chat
    if not message or not user or not chat:
        return False

    dynamic_admins_map = context.application.bot_data.setdefault("dynamic_admins", db_load("dynamic_admins"))
    chat_dynamic_admins = dynamic_admins_map.setdefault(str(chat.id), {})
    pending_card_states = context.application.bot_data.setdefault("pending_card_states", {})

    if user.id in pending_card_states:
        state_data = pending_card_states[user.id]
        if state_data.get("chat_id") != chat.id:
            return False

        step = state_data.get("step")
        raw_in = (message.text or message.caption or "").strip()
        if raw_in in ["الغاء", "إلغاء"]:
            del pending_card_states[user.id]
            await message.reply_text("تم إلغاء عملية إضافة الكارت بنجاح.")
            return True

        role_label = "المالك" if state_data.get("role") == "owner" else "المشرف"

        if step == "awaiting_username":
            raw_u = raw_in.lstrip("@")
            if not raw_u:
                await message.reply_text("يرجى إرسال يوزر صحيح (مثال: @username).")
                return True

            uid, clean_u, name = await resolve_username_to_user(raw_u, context)
            state_data["username"] = clean_u
            state_data["user_id"] = uid
            state_data["name"] = name
            state_data["step"] = "awaiting_alias"

            await message.reply_text(
                f'<tg-emoji emoji-id="5891235511246264255">✔️</tg-emoji> <b>تم تحديد اليوزر:</b> @{clean_u}\n\n'
                f'أرسل الآن <b>اسم {role_label} أو كلمة الرد بالشات</b> (مثال: أحمد، دراجون، زكا):\n'
                f'<i>(ليتم إرسال الكارت فور كتابة هذا الاسم بالشات أيضاً)</i>\n'
                f'<i>(أو اكتب إلغاء للتراجع)</i>',
                parse_mode=ParseMode.HTML,
            )
            return True

        elif step == "awaiting_alias":
            alias_word = raw_in.lower()
            if not alias_word:
                await message.reply_text("يرجى إرسال اسم أو كلمة رد صحيحة.")
                return True

            state_data["alias"] = alias_word
            state_data["step"] = "awaiting_photo"

            await message.reply_text(
                f'<tg-emoji emoji-id="5891235511246264255">✔️</tg-emoji> <b>تم تسجيل كلمة الرد:</b> « <code>{alias_word}</code> »\n\n'
                f'أرسل الآن <b>صورة كارت {role_label} المعتمدة</b> ليتم حفظ الكارت وتفعيله فوراً:\n'
                f'<i>(أو اكتب إلغاء للتراجع)</i>',
                parse_mode=ParseMode.HTML,
            )
            return True

        elif step == "awaiting_photo":
            if not message.photo:
                await message.reply_text(f"يرجى إرسال صورة كارت {role_label} (أو اكتب إلغاء).")
                return True

            photo_file_id = message.photo[-1].file_id
            clean_u = state_data["username"]
            alias_word = state_data.get("alias", "")
            uid = state_data.get("user_id")
            role_type = state_data.get("role", "admin")

            card_record = {
                "role": role_type,
                "username": clean_u,
                "alias": alias_word,
                "user_id": uid,
                "photo_id": photo_file_id,
                "name": state_data.get("name", f"@{clean_u}"),
                "added_by": user.mention_html(),
                "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            }

            chat_dynamic_admins[clean_u.lower()] = card_record
            if alias_word:
                chat_dynamic_admins[alias_word] = card_record

            dynamic_admins_map[str(chat.id)] = chat_dynamic_admins
            db_save("dynamic_admins", dynamic_admins_map)
            del pending_card_states[user.id]

            role_title = "OWNER" if role_type == "owner" else "ADMIN"
            await message.reply_text(
                f'<tg-emoji emoji-id="5812288471837975057">💯</tg-emoji> <b>تم إنشاء كارت {role_title} بنجاح في هذا الشات!</b>\n'
                f'━━━━━━━━━━━━━━━━━━━━\n\n'
                f'🔹 <b>اليوزر:</b> @{clean_u}\n'
                f'🔹 <b>كلمة الرد / الاسم:</b> {alias_word}\n'
                f'🔹 <b>الرتبة:</b> {role_title}\n\n'
                f'<i>ينزل الكارت الرسمي وزر التواصل كلما كُتب اليوزر أو الاسم بالشات.</i>',
                parse_mode=ParseMode.HTML,
            )
            return True

    text = (message.text or message.caption or "").strip()
    if not text:
        return False

    if text in ["اضف مشرف", "أضف مشرف"]:
        if not await is_management_admin(update, context):
            return True

        pending_card_states[user.id] = {"role": "admin", "step": "awaiting_username", "chat_id": chat.id}
        await message.reply_text(
            '<tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji> <b>إضافة كارت مشرف معتمد للمجموعة</b>\n\n'
            'أرسل الآن <b>يوزر المشرف</b> (مثال: @username):\n'
            '<i>(أو اكتب إلغاء للتراجع)</i>',
            parse_mode=ParseMode.HTML,
        )
        return True

    if text in ["اضف مالك", "أضف مالك", "اضف اونر", "أضف اونر"]:
        if not await is_management_admin(update, context):
            return True

        pending_card_states[user.id] = {"role": "owner", "step": "awaiting_username", "chat_id": chat.id}
        await message.reply_text(
            '<tg-emoji emoji-id="5875344888165308204">👑</tg-emoji> <b>إضافة كارت مالك (OWNER) معتمد للمجموعة</b>\n\n'
            'أرسل الآن <b>يوزر المالك</b> (مثال: @username):\n'
            '<i>(أو اكتب إلغاء للتراجع)</i>',
            parse_mode=ParseMode.HTML,
        )
        return True

    if text.startswith("مسح كارت") or text.startswith("حذف كارت"):
        if not await is_management_admin(update, context):
            return True

        prefix_cmd = "مسح كارت" if text.startswith("مسح كارت") else "حذف كارت"
        sub_text = text[len(prefix_cmd):].strip()
        for r_name in ["مشرف", "مالك", "اونر"]:
            if sub_text.startswith(r_name):
                sub_text = sub_text[len(r_name):].strip()

        target_trigger = sub_text.lstrip("@").lower().strip()
        if not target_trigger:
            await message.reply_text("يرجى كتابة يوزر أو اسم الكارت: <code>مسح كارت @username</code>", parse_mode=ParseMode.HTML)
            return True

        deleted = False
        if target_trigger in chat_dynamic_admins:
            card_obj = chat_dynamic_admins[target_trigger]
            u_key = card_obj.get("username", "").lower()
            a_key = card_obj.get("alias", "").lower()

            if u_key in chat_dynamic_admins:
                del chat_dynamic_admins[u_key]
            if a_key and a_key in chat_dynamic_admins:
                del chat_dynamic_admins[a_key]

            dynamic_admins_map[str(chat.id)] = chat_dynamic_admins
            db_save("dynamic_admins", dynamic_admins_map)
            deleted = True

        if deleted:
            await message.reply_text(f"✅ تم حذف الكارت الخاص بـ ({target_trigger}) بنجاح من هذا الشات.", parse_mode=ParseMode.HTML)
        else:
            await message.reply_text("⚠️ هذا اليوزر أو الاسم غير مسجل في كروت هذا الشات.")
        return True

    clean_text = text.lstrip("@").lower().rstrip(",;.:")
    if clean_text in chat_dynamic_admins:
        adm_data = chat_dynamic_admins[clean_text]
        uid = adm_data.get("user_id")
        u_disp = adm_data["username"]
        role_type = adm_data.get("role", "admin")

        # Keep the permanent link shown in the caption unchanged; alter only the contact button.
        link_val = f"tg://openmessage?user_id={uid}" if uid else f"https://t.me/{u_disp}"
        contact_link = f"https://t.me/{u_disp.lstrip('@')}" if u_disp and u_disp != "بدون يوزر" else None
        direct_link_html = f'<a href="{link_val}">{link_val}</a>' if uid else f'<a href="{link_val}">@{u_disp}</a>'

        if role_type == "owner":
            caption = (
                '<tg-emoji emoji-id="5875344888165308204">👑</tg-emoji> <b>OWNER</b> <tg-emoji emoji-id="5875344888165308204">👑</tg-emoji>\n'
                '━━━━━━━━━━━━━━━━━━━━\n\n'
                '<b>ايوه دا اليوزر الأساسي والمالك للشات المعاملة والضمان معاه مباشرة <tg-emoji emoji-id="5873005522558392818">✔️</tg-emoji></b>\n\n'
                f'🔹 <b>يوزر المالك:</b> @{u_disp}\n'
                f'🔹 <b>الرابط الدائم:</b>\n{direct_link_html}\n\n'
                '━━━━━━━━━━━━━━━━━━━━\n'
                '<tg-emoji emoji-id="5890787894049643800">⛔</tg-emoji> <b>غير مسئولين عن أي يوزر تاني لو اتعاملت مع منتحل.</b>'
            )
        else:
            caption = (
                '<tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji> <b>ADMIN</b> <tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji>\n'
                '━━━━━━━━━━━━━━━━━━━━\n\n'
                '<b>مشرف وضامن معتمد داخل الشات،\nالمعاملة والوساطة معاه بأمان تام <tg-emoji emoji-id="5891235511246264255">✔️</tg-emoji></b>\n\n'
                f'🔹 <b>يوزر المشرف:</b> @{u_disp}\n'
                f'🔹 <b>الرابط الدائم:</b>\n{direct_link_html}\n\n'
                '━━━━━━━━━━━━━━━━━━━━\n'
                '<tg-emoji emoji-id="5890787894049643800">⛔</tg-emoji> <b>غير مسئولين عن أي يوزر تاني لو اتعاملت مع منتحل.</b>'
            )

        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("تواصل ➜", url=contact_link, style="danger")]]
        ) if contact_link else None

        try:
            await message.reply_photo(
                photo=adm_data["photo_id"],
                caption=caption,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
            )
            return True
        except Exception:
            pass

    return False


# -------------------------------------------------------------
# Handlers: Moderation & Pinning (with Immunity & Bot-Admin Unban)
# -------------------------------------------------------------

async def handle_pin_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.effective_message
    chat = update.effective_chat
    if not message or not chat or chat.type == "private":
        return False

    raw_text = apply_alias((message.text or message.caption or "").strip(), {"pin"})
    if raw_text not in ["تثبيت", "ثبت"]:
        return False

    if not await check_bot_permission(update, context, "can_pin"):
        return True

    if not message.reply_to_message:
        await message.reply_text("⚠️ يرجى الرد على الرسالة المراد تثبيتها بالأمر (تثبيت).")
        return True

    try:
        await context.bot.pin_chat_message(
            chat_id=chat.id,
            message_id=message.reply_to_message.message_id,
            disable_notification=False
        )
        try:
            await message.delete()
        except Exception:
            pass
        return True
    except TelegramError:
        await message.reply_text("تعذر تثبيت الرسالة: تأكد من صلاحية التثبيت للبوت.")
        return True



_TELETHON_HISTORY_OK = True  # bots cannot read chat history through MTProto; learn that once and skip the wasted call


async def _safe_delete(msg) -> None:
    try:
        with timed("tg.delete_message"):
            await msg.delete()
    except Exception:
        pass


async def _finish_delete(task) -> None:
    if task is not None:
        await asyncio.gather(task, return_exceptions=True)


def _retry_after_seconds(exc: Exception) -> float:
    ra = getattr(exc, "retry_after", 1)
    try:
        ra = ra.total_seconds() if hasattr(ra, "total_seconds") else float(ra)
    except Exception:
        ra = 1.0
    return min(max(ra, 0.5), 8.0)


async def bulk_delete_messages(bot, chat_id: int, ids: list[int]) -> int:
    """Delete ids in safe batches of <=100 (Telegram limit), a few batches at a time,
    honouring FloodWait once per batch, falling back to per-message deletes for a failed batch."""
    ids = list(dict.fromkeys(i for i in ids if i and i > 0))  # never delete the same id twice
    if not ids:
        return 0
    chunks = [ids[i:i + 100] for i in range(0, len(ids), 100)]
    sem = asyncio.Semaphore(3)
    single_sem = asyncio.Semaphore(8)

    async def delete_one(mid: int) -> int:
        async with single_sem:
            try:
                await bot.delete_message(chat_id=chat_id, message_id=mid)
                return 1
            except RetryAfter as exc:
                await asyncio.sleep(_retry_after_seconds(exc))
                try:
                    await bot.delete_message(chat_id=chat_id, message_id=mid)
                    return 1
                except Exception:
                    return 0
            except Exception:
                return 0  # already deleted / too old / no right

    async def run(chunk: list[int]) -> int:
        async with sem:
            for attempt in range(2):
                try:
                    with timed("tg.delete_messages(batch)"):
                        await bot.delete_messages(chat_id=chat_id, message_ids=chunk)
                    return len(chunk)
                except RetryAfter as exc:
                    await asyncio.sleep(_retry_after_seconds(exc))
                except TelegramError:
                    break
                except Exception:
                    break
        results = await asyncio.gather(*(delete_one(m) for m in chunk))
        return sum(results)

    results = await asyncio.gather(*(run(c) for c in chunks))
    return sum(results)


def _build_moderation_tables():
    ban_cmds = {
        "حظر": "تم حظره وطرده من الشات نهائياً 🚫👋",
        "طرد": "تم طرده برا الشات نهائياً 🚪👋",
        "ديس": "تم استبعاده وطرده من الشات 🚪👋",
        "اركب القطر": "ركبته قطر 6 الا تلت 🚆👋",
        "طير انت": "ركبته الطيارة ✈️👋",
        "بالجزمة": "طردته بالجزمة 👞👋",
        "برا": "طردته برا الشات نهائياً 🚪👋",
    }

    mute_cmds = {
        "كتم": "تم كتم العضو وتقييده عن الإرسال 🤐🔇",
        "تقييد": "تم تقييد حساب العضو بنجاح 🤐🔒",
        "اقطع لسان واحذف": "قطعت لسانه ومسحت رسالته ✂️🤐",
        "اخرس واحذف": "خرسته ومسحت رسالته 🤐🗑️",
        "اسكت واحذف": "سكتّه ومسحت رسالته 🤐🗑️",
        "اقطع لسان": "قطعت لسانه ✂️🤐",
        "اخرس": "خرسته تماماً 🤐",
        "اسكت": "سكتّه وبلع ريقه 🤐",
    }

    unban_cmds = {
        "الغاء الحظر": "تم فك الحظر ورجوعه للشات بنجاح ✅",
        "فك الحظر": "تم فك الحظر ورجوعه للشات بنجاح ✅",
        "انزل من القطر": "نزلته من القطر وسامحناه 🤝",
        "سامحتك": "عفا الله عما سلف وتم فك الحظر 🤝",
    }

    unmute_cmds = {
        "الغاء الكتم": "تم فك الكتم عنه ويقدر يتكلم الآن 🔊",
        "فك الكتم": "تم فك الكتم عنه ويقدر يتكلم الآن 🔊",
        "اتكلم": "رجعتله لسانه وخلّيته ينطق 🗣️",
        "رجعله لسانه": "رجعنا له صوته ولسانه 🗣️✨",
    }

    lift_all_cmds = {
        "تنزيل الكل": "تم تنزيل العضو من جميع الرتب وإزالة القيود التي يديرها البوت.",
        "رفع القيود": "تم رفع جميع القيود وفك الحظر والكتم بنجاح 🔓✨",
        "فك القيود": "تم فك كافة القيود وتصفير العقوبات عنه بنجاح 🔓✨",
    }

    fun_cmds = [
        "رفع لقلبي",
        "تنزيل من قلبي",
        "رفع تيس",
        "رفع بضان",
        "رفع حوت",
    ]

    all_cmds = (
        list(lift_all_cmds.keys())
        + list(ban_cmds.keys())
        + list(mute_cmds.keys())
        + list(unban_cmds.keys())
        + list(unmute_cmds.keys())
        + fun_cmds
    )
    all_cmds.sort(key=len, reverse=True)
    return ban_cmds, mute_cmds, unban_cmds, unmute_cmds, lift_all_cmds, fun_cmds, all_cmds


_MOD_TABLES = _build_moderation_tables()


async def handle_moderation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.effective_message
    if not message or not update.effective_chat:
        return False

    chat = update.effective_chat
    if chat.type == "private":
        return False

    raw_text = (message.text or message.caption or "").strip()
    if not raw_text:
        return False

    ban_cmds, mute_cmds, unban_cmds, unmute_cmds, lift_all_cmds, fun_cmds, all_cmds = _MOD_TABLES

    lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]
    if not lines:
        return False

    lines = [apply_alias(ln, MOD_ALIAS_ACTIONS) for ln in lines]  # أوامر مضافة من اللوحة

    matched_cmd = None
    cmd_line_index = -1

    for idx, line in enumerate(lines):
        for cmd in all_cmds:
            if line == cmd or line.startswith(f"{cmd} "):
                matched_cmd = cmd
                cmd_line_index = idx
                break
        if matched_cmd:
            break

    if not matched_cmd:
        return False

    remainder_check = lines[cmd_line_index][len(matched_cmd):].strip()
    words_count = len(remainder_check.split())
    if not message.reply_to_message and words_count > 3:
        has_id_or_user = bool(re.search(r"(@[a-zA-Z0-9_]+|\b\d{6,12}\b)", remainder_check))
        if not has_id_or_user:
            return False

    # التحقق من الصلاحيات بما يمنح أدمن/مدير البوت إمكانية فك الكتم والحظر
    perm_req = "can_ban" if (matched_cmd in ban_cmds or matched_cmd in unban_cmds) else "can_mute"
    if matched_cmd in lift_all_cmds:
        # يكفي أن يمتلك إما صلاحية الحظر أو الكتم لرفع القيود
        has_lift_perm = await check_bot_permission(update, context, "can_ban") or await check_bot_permission(update, context, "can_mute")
        if not has_lift_perm:
            return True
    else:
        if not await check_bot_permission(update, context, perm_req):
            return True

    all_tokens: list[str] = []
    for idx, line in enumerate(lines):
        if idx == cmd_line_index:
            remainder_line = line[len(matched_cmd):].strip()
            if remainder_line:
                all_tokens.extend(remainder_line.split())
        else:
            all_tokens.extend(line.split())

    ar_duration, all_tokens = extract_duration_tokens(all_tokens)  # يومين / ساعتين / 3 ساعات ...
    target_id, _, target_name, duration, reason = await resolve_target_and_details(update, context, all_tokens)
    duration = ar_duration or duration

    if matched_cmd in fun_cmds:
        if not target_name:
            await message.reply_text("قم بالرد (Reply) على الشخص أو اكتب يوزره/آيديه.")
            return True

        admin_name = update.effective_user.mention_html() if update.effective_user else "الأدمن"
        if matched_cmd == "رفع لقلبي":
            await message.reply_text(f"ابسط يعم {target_name}، {admin_name} رفعك لقلبه ❤️🔥", parse_mode=ParseMode.HTML)
        elif matched_cmd == "تنزيل من قلبي":
            await message.reply_text(f"نزلك من قلبه ورماك في الشارع 💔🚪 {target_name}", parse_mode=ParseMode.HTML)
        elif matched_cmd == "رفع تيس":
            await message.reply_text(f"ابشر رفعت {target_name} تيس 🐐😂", parse_mode=ParseMode.HTML)
        elif matched_cmd == "رفع بضان":
            await message.reply_text(f"رسمياً تم ترقية {target_name} لأعلى رتبة بيض ف الجروب 🍳😂", parse_mode=ParseMode.HTML)
        elif matched_cmd == "رفع حوت":
            await message.reply_text(f"وسع للحوت الكبير {target_name} دخل الساحة 🐋🔥", parse_mode=ParseMode.HTML)
        return True

    if not target_id:
        await message.reply_text("⚠️ يرجى تحديد العضو بالرد (Reply) أو كتابة اليوزر أو الآيدي الخاص به.")
        return True

    # One getChatMember for the target, shared by every state check below (was up to 3 calls).
    _tm_state: dict[str, Any] = {}

    async def _target_member():
        if "m" not in _tm_state:
            try:
                with timed("tg.get_chat_member(target)"):
                    _tm_state["m"] = await context.bot.get_chat_member(chat_id=chat.id, user_id=target_id)
            except TelegramError as exc:
                _tm_state["m"] = exc
        m = _tm_state["m"]
        if isinstance(m, TelegramError):
            raise m
        return m

    # Verify Telegram's current state before attempting duplicate moderation actions.
    if matched_cmd in ban_cmds:
        try:
            current = await _target_member()
            if current.status in (ChatMemberStatus.BANNED, "kicked"):
                banned_user = getattr(current, "user", None)
                banned_name = escape((getattr(banned_user, "full_name", None) or target_name or "غير معروف").strip())
                banned_username = getattr(banned_user, "username", None)
                username_line = f"\n<b>اليوزر:</b> @{escape(banned_username)}" if banned_username else "\n<b>اليوزر:</b> لا يوجد"
                reason_line = f"\n<b>سبب الحظر:</b> {escape(reason)}" if reason else "\n<b>سبب الحظر:</b> غير متوفر"
                already_banned_card = (
                    "<b>العضو محظور بالفعل</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━\n"
                    f"<b>الاسم:</b> {banned_name}"
                    f"{username_line}\n"
                    f"<b>الآيدي:</b> <code>{target_id}</code>\n"
                    "<b>الحالة:</b> محظور بالفعل"
                    f"{reason_line}"
                )
                await message.reply_text(already_banned_card, parse_mode=ParseMode.HTML)
                return True
        except TelegramError:
            pass
    if matched_cmd in mute_cmds:
        try:
            current = await _target_member()
            if current.status == ChatMemberStatus.RESTRICTED and hasattr(current, "can_send_messages") and not current.can_send_messages:
                await message.reply_text("العضو مكتوم بالفعل.")
                return True
        except TelegramError:
            pass

    is_punishment = (matched_cmd in ban_cmds) or (matched_cmd in mute_cmds)
    if is_punishment:
        if await is_primary_owner(target_id, context):
            await message.reply_text(
                '<tg-emoji emoji-id="5890787894049643800">⛔</tg-emoji> <b>عفواً! لا يمكنك حظر أو كتم مطور ومالك البوت الأساسي.</b>',
                parse_mode=ParseMode.HTML,
            )
            return True

        try:
            target_chat_member = await _target_member()
            if target_chat_member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
                await message.reply_text(
                    '⚠️ <b>لا يمكنك تطبيق هذه العقوبة: هذا العضو مشرف/أدمن في المجموعة!</b>',
                    parse_mode=ParseMode.HTML,
                )
                return True
        except Exception:
            pass

    delete_task = None
    if message.reply_to_message:
        should_delete_target = (matched_cmd in ban_cmds) or ("احذف" in matched_cmd)
        if should_delete_target:
            # Runs concurrently with the ban/mute call; awaited before the handler returns.
            delete_task = spawn(_safe_delete(message.reply_to_message))

    if matched_cmd in lift_all_cmds:
        if matched_cmd == "تنزيل الكل":
            if await is_primary_owner(target_id, context):
                await message.reply_text("⚠️ لا يمكن إزالة رتبة المالك الأساسي للبوت.")
                return True
            # Remove every bot-managed role for this chat.
            bot_admins_map = context.application.bot_data.setdefault("bot_admins", db_load("bot_admins"))
            chat_admins = bot_admins_map.setdefault(str(chat.id), {})
            chat_admins.pop(str(target_id), None)
            bot_admins_map[str(chat.id)] = chat_admins
            db_save("bot_admins", bot_admins_map)
            distinguished_map = context.application.bot_data.setdefault("distinguished_users", db_load("distinguished_users"))
            chat_dist = distinguished_map.setdefault(str(chat.id), {})
            chat_dist.pop(str(target_id), None)
            distinguished_map[str(chat.id)] = chat_dist
            db_save("distinguished_users", distinguished_map)
            # If they are a Telegram group administrator, attempt to demote them too.
            try:
                current_member = await _target_member()
                if current_member.status == ChatMemberStatus.ADMINISTRATOR:
                    await context.bot.promote_chat_member(
                        chat_id=chat.id, user_id=target_id,
                        can_change_info=False, can_post_messages=False, can_edit_messages=False,
                        can_delete_messages=False, can_invite_users=False, can_restrict_members=False,
                        can_pin_messages=False, can_promote_members=False, can_manage_chat=False,
                        can_manage_video_chats=False,
                    )
                    invalidate_admin_cache(chat.id)
            except TelegramError:
                pass
        full_permissions = ChatPermissions(
            can_send_messages=True,
            can_send_photos=True,
            can_send_videos=True,
            can_send_other_messages=True,
            can_send_polls=True,
            can_add_web_page_previews=True,
        )
        # القيود الحالية قبل رفعها (تُحسب فقط إذا كانت الكليشة تستخدم {restrictions}).
        prior_restrictions: list[str] = []
        tpl_key_lift = TPL_BY_CMD.get(matched_cmd)
        if tpl_key_lift and "{restrictions}" in get_template(tpl_key_lift)[0]:
            try:
                pm = await _target_member()
                if pm.status in (ChatMemberStatus.BANNED, "kicked"):
                    prior_restrictions.append("حظر")
                elif pm.status == ChatMemberStatus.RESTRICTED:
                    prior_restrictions.append("كتم" if getattr(pm, "can_send_messages", None) is False else "تقييد")
            except TelegramError:
                pass
            if not prior_restrictions and str(target_id) in db_load("restricted_users").get(str(chat.id), {}):
                prior_restrictions.append("كتم")

        try:
            await context.bot.unban_chat_member(chat_id=chat.id, user_id=target_id, only_if_banned=True)
        except Exception:
            pass

        try:
            await context.bot.restrict_chat_member(chat_id=chat.id, user_id=target_id, permissions=full_permissions)
        except Exception:
            pass

        restr = db_load("restricted_users")
        if str(chat.id) in restr and str(target_id) in restr[str(chat.id)]:
            del restr[str(chat.id)][str(target_id)]
            db_save("restricted_users", restr)

        if tpl_key_lift:
            restrictions_txt = "\n".join(f"{i}) {r}" for i, r in enumerate(prior_restrictions, 1)) or "لا توجد قيود مفروضة"
            await reply_with_template(
                message, tpl_key_lift,
                _punish_values(target_id, target_name, restrictions=restrictions_txt),
                mention_uid=target_id,
            )
            return True

        card_lift = (
            f'<tg-emoji emoji-id="5891235511246264255">✔️</tg-emoji> <b>{lift_all_cmds[matched_cmd]}</b>\n'
            f'━━━━━━━━━━━━━━━━━━━━\n\n'
            f'<tg-emoji emoji-id="5445092669522996408">👤</tg-emoji> <b>العضو:</b> {target_name}\n'
            f'<tg-emoji emoji-id="5935966809655088705">🪪</tg-emoji> <b>الآيدي:</b> <code>{target_id}</code>\n'
            f'⚡ <b>الحالة:</b> {"تمت إزالة الرتب التي يديرها البوت ومحاولة تنزيل إشراف المجموعة، مع فك الحظر والكتم والقيود." if matched_cmd == "تنزيل الكل" else "تم فك الحظر، وإلغاء الكتم، وإعادة كافة الصلاحيات."}'
        )
        await message.reply_text(card_lift, parse_mode=ParseMode.HTML)
        return True

    if matched_cmd in unban_cmds:
        try:
            current = await _target_member()
            if current.status not in (ChatMemberStatus.BANNED, "kicked"):
                await message.reply_text("العضو غير محظور بالفعل.")
                return True
        except TelegramError:
            pass
        try:
            await context.bot.unban_chat_member(chat_id=chat.id, user_id=target_id, only_if_banned=True)
            if matched_cmd in TPL_BY_CMD:
                await reply_with_template(message, TPL_BY_CMD[matched_cmd], _punish_values(target_id, target_name), mention_uid=target_id)
            else:
                await message.reply_text(f"{unban_cmds[matched_cmd]}\n👤 <b>العضو:</b> {target_name}", parse_mode=ParseMode.HTML)
        except TelegramError:
            await message.reply_text("تعذر فك الحظر: تأكد من صلاحيات البوت.")
        return True

    if matched_cmd in unmute_cmds:
        try:
            current = await _target_member()
            if current.status != ChatMemberStatus.RESTRICTED or getattr(current, "can_send_messages", True):
                await message.reply_text("العضو غير مكتوم بالفعل.")
                return True
        except TelegramError:
            pass
        full_permissions = ChatPermissions(
            can_send_messages=True,
            can_send_photos=True,
            can_send_videos=True,
            can_send_other_messages=True,
            can_send_polls=True,
            can_add_web_page_previews=True,
        )
        try:
            await context.bot.restrict_chat_member(chat_id=chat.id, user_id=target_id, permissions=full_permissions)
            restr = db_load("restricted_users")
            if str(chat.id) in restr and str(target_id) in restr[str(chat.id)]:
                del restr[str(chat.id)][str(target_id)]
                db_save("restricted_users", restr)

            if matched_cmd in TPL_BY_CMD:
                await reply_with_template(message, TPL_BY_CMD[matched_cmd], _punish_values(target_id, target_name), mention_uid=target_id)
            else:
                await message.reply_text(f"{unmute_cmds[matched_cmd]}\n👤 <b>العضو:</b> {target_name}", parse_mode=ParseMode.HTML)
        except TelegramError:
            await message.reply_text("تعذر فك الكتم: تأكد من صلاحيات البوت.")
        return True

    until_date = None
    time_info = "دائم"
    if duration:
        now_utc = datetime.now(timezone.utc)
        until_utc = now_utc + duration
        until_date = until_utc
        cairo_time = until_utc + timedelta(hours=3)
        time_info = cairo_time.strftime("حتى الساعة %I:%M %p (%Y-%m-%d)")

    # عقوبة أقل من دقيقة: تيليجرام يتجاهل until_date الأقل من 30 ثانية، لذلك نجدول فك العقوبة فوراً.
    display_until = until_date
    short_lift_secs = None
    if duration and duration.total_seconds() < 60:
        short_lift_secs = duration.total_seconds()
        if short_lift_secs < 35:
            until_date = None

    try:
        if matched_cmd in ban_cmds:
            import logging
            from telethon.tl.functions.channels import EditBannedRequest
            from telethon.tl.types import ChatBannedRights

            log = logging.getLogger("telegram_escrow_bot")
            banned_successfully = False
            bot_api_error = None
            telethon_error = None

            try:
                await context.bot.ban_chat_member(
                    chat_id=chat.id,
                    user_id=int(target_id),
                    until_date=until_date,
                )
                banned_successfully = True
                log.info(
                    "BAN_SUCCESS via=BotAPI chat_id=%s target_id=%s",
                    chat.id, target_id
                )

            except Exception as exc:
                bot_api_error = exc
                log.exception(
                    "BAN_BOT_API_FAILED chat_id=%s target_id=%s error_type=%s error=%s",
                    chat.id, target_id, type(exc).__name__, str(exc)
                )

            if not banned_successfully:
                global telethon_client

                if not telethon_client or not telethon_client.is_connected():
                    log.error(
                        "BAN_TELETHON_UNAVAILABLE chat_id=%s target_id=%s",
                        chat.id, target_id
                    )
                else:
                    try:
                        target_entity = None

                        username_hint = next(
                            (
                                token.lstrip("@")
                                for token in all_tokens
                                if token.startswith("@") and len(token) > 1
                            ),
                            None,
                        )

                        if username_hint:
                            try:
                                target_entity = await telethon_client.get_entity(
                                    username_hint
                                )
                                log.info(
                                    "BAN_ENTITY_RESOLVED_BY_USERNAME username=%s user_id=%s",
                                    username_hint,
                                    getattr(target_entity, "id", None),
                                )
                            except Exception as exc:
                                log.warning(
                                    "BAN_USERNAME_RESOLVE_FAILED username=%s error_type=%s error=%s",
                                    username_hint, type(exc).__name__, str(exc)
                                )

                        if target_entity is None:
                            try:
                                target_entity = await telethon_client.get_entity(
                                    int(target_id)
                                )
                                log.info(
                                    "BAN_ENTITY_RESOLVED_BY_ID target_id=%s",
                                    target_id
                                )
                            except Exception as exc:
                                log.warning(
                                    "BAN_ID_RESOLVE_FAILED target_id=%s error_type=%s error=%s",
                                    target_id, type(exc).__name__, str(exc)
                                )
                                raise

                        input_entity = await telethon_client.get_input_entity(
                            target_entity
                        )

                        rights = ChatBannedRights(
                            until_date=until_date,
                            view_messages=True,
                        )

                        with timed("telethon.EditBannedRequest"):
                            await telethon_client(
                                EditBannedRequest(
                                    channel=chat.id,
                                    participant=input_entity,
                                    banned_rights=rights,
                                )
                            )

                        banned_successfully = True
                        log.info(
                            "BAN_SUCCESS via=Telethon chat_id=%s target_id=%s",
                            chat.id, target_id
                        )

                    except Exception as exc:
                        telethon_error = exc
                        log.exception(
                            "BAN_TELETHON_FAILED chat_id=%s target_id=%s "
                            "bot_api_error=%s telethon_error_type=%s telethon_error=%s",
                            chat.id,
                            target_id,
                            repr(bot_api_error),
                            type(exc).__name__,
                            str(exc),
                        )

            if not banned_successfully:
                log.error(
                    "BAN_FINAL_FAILURE chat_id=%s target_id=%s "
                    "bot_api_error=%s telethon_error=%s",
                    chat.id,
                    target_id,
                    repr(bot_api_error),
                    repr(telethon_error),
                )
                await message.reply_text(
                    '<tg-emoji emoji-id="5447381715293074599">⚠️</tg-emoji> <b>تعذر حظر هذا الحساب:</b> الحساب غير متواجد بالشات ولا يمتلك يوزر متاح لحله (تنص حماية تيليجرام على توفر يوزر لحظر الحسابات الخارجية المجهولة).',
                    parse_mode=ParseMode.HTML
                )
                await _finish_delete(delete_task)
                return True

            resp = (
                f"{ban_cmds[matched_cmd]}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f'<tg-emoji emoji-id="5445092669522996408">👤</tg-emoji> <b>العضو:</b> {target_name}\n'
                f'<tg-emoji emoji-id="5935966809655088705">🪪</tg-emoji> <b>الآيدي:</b> <code>{target_id}</code>\n'
                f'⏳ <b>مدة الحظر:</b> {time_info}\n'
                f'📝 <b>السبب:</b> {escape(reason)}'
            )
            if short_lift_secs is not None:
                spawn(_auto_lift_later(context.bot, chat.id, int(target_id), "ban", short_lift_secs))
            if matched_cmd in TPL_BY_CMD:
                await reply_with_template(
                    message, TPL_BY_CMD[matched_cmd],
                    _punish_values(target_id, target_name, duration, display_until), mention_uid=target_id)
            else:
                await message.reply_text(resp, parse_mode=ParseMode.HTML)

        elif matched_cmd in mute_cmds:
            no_send = ChatPermissions(
                can_send_messages=False,
                can_send_photos=False,
                can_send_videos=False,
                can_send_other_messages=False,
                can_send_polls=False,
                can_add_web_page_previews=False,
            )
            await context.bot.restrict_chat_member(chat_id=chat.id, user_id=target_id, permissions=no_send, until_date=until_date)

            restr = db_load("restricted_users")
            chat_restr = restr.setdefault(str(chat.id), {})
            chat_restr[str(target_id)] = {
                "name": target_name,
                "reason": reason,
                "until": time_info,
            }
            db_save("restricted_users", restr)

            resp = (
                f"{mute_cmds[matched_cmd]}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f'<tg-emoji emoji-id="5445092669522996408">👤</tg-emoji> <b>العضو:</b> {target_name}\n'
                f'<tg-emoji emoji-id="5935966809655088705">🪪</tg-emoji> <b>الآيدي:</b> <code>{target_id}</code>\n'
                f'⏳ <b>مكتوم:</b> {time_info}\n'
                f'📝 <b>السبب:</b> {escape(reason)}'
            )
            if short_lift_secs is not None:
                spawn(_auto_lift_later(context.bot, chat.id, int(target_id), "mute", short_lift_secs))
            if matched_cmd in TPL_BY_CMD:
                await reply_with_template(
                    message, TPL_BY_CMD[matched_cmd],
                    _punish_values(target_id, target_name, duration, display_until), mention_uid=target_id)
            else:
                await message.reply_text(resp, parse_mode=ParseMode.HTML)

    except TelegramError:
        await message.reply_text("تعذر تنفيذ الإجراء: تأكد من رفع البوت مشرفاً بصلاحيات كاملة.")

    await _finish_delete(delete_task)
    return True


# -------------------------------------------------------------
# Handlers: Precise Purging Engine (مسح دقيق بالعدد الفعلي عبر Telethon)
# -------------------------------------------------------------

async def handle_purge_messages(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.effective_message
    chat = update.effective_chat
    if not message or not chat:
        return False

    text = (message.text or message.caption or "").strip()
    if not (text.startswith("مسح") or text.startswith("احذف") or text == "مسح"):
        return False

    if any(k in text for k in ["نصاب", "المقيدين", "رد", "مشرف", "مالك", "كارت", "ادمن", "مدير", "مميز"]):
        return False

    if not await check_bot_permission(update, context, "can_purge"):
        return False

    # 1. الحذف بالرد على رسالة معينة
    if message.reply_to_message:
        await asyncio.gather(
            _safe_delete(message.reply_to_message),
            _safe_delete(message),
            return_exceptions=True,
        )
        return True

    # 2. الحذف بعدد محدد
    count_match = re.search(r"\b(\d+)\b", text)
    if count_match:
        count = int(count_match.group(1))
        count = min(max(count, 1), 100)

        global telethon_client, _TELETHON_HISTORY_OK
        purged = False

        if _TELETHON_HISTORY_OK and telethon_client and telethon_client.is_connected():
            try:
                with timed("telethon.get_messages"):
                    messages_to_del = await telethon_client.get_messages(chat.id, limit=count + 1)
                del_ids = [m.id for m in messages_to_del]
                if del_ids:
                    with timed("telethon.delete_messages"):
                        await telethon_client.delete_messages(chat.id, del_ids)
                    purged = True
            except Exception as e:
                if "BotMethodInvalid" in type(e).__name__ or "BOT_METHOD_INVALID" in str(e):
                    # Bots can't read history over MTProto: stop paying a wasted round-trip on every purge.
                    _TELETHON_HISTORY_OK = False
                    logger.info("Telethon history unavailable for bot accounts; purge will use Bot API batches.")
                else:
                    logger.warning(f"Telethon purge fallback: {e}")

        if not purged:
            current_msg_id = message.message_id
            candidate_ids = list(range(current_msg_id, max(1, current_msg_id - count - 15), -1))
            await bulk_delete_messages(context.bot, chat.id, candidate_ids[:count + 1])

        sent = await context.bot.send_message(
            chat_id=chat.id,
            text=f'<tg-emoji emoji-id="5445005936953424165">🗑</tg-emoji> تم مسح <b>{count}</b> من الرسائل بنجاح.',
            parse_mode=ParseMode.HTML,
        )

        async def _auto_delete_confirm(msg_to_del):
            await asyncio.sleep(4)
            try:
                await msg_to_del.delete()
            except Exception:
                pass

        spawn(_auto_delete_confirm(msg_to_del=sent))
        return True

    return False


# -------------------------------------------------------------
# Handlers: Custom Replies & Colored Buttons
# -------------------------------------------------------------

async def handle_custom_response_system(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.effective_message
    user = update.effective_user
    chat = update.effective_chat
    if not message or not user or not chat:
        return False

    custom_replies_map = context.application.bot_data.setdefault("custom_replies", db_load("custom_replies"))
    chat_replies = custom_replies_map.setdefault(str(chat.id), {})

    if chat.id == PRIMARY_CHAT_ID:
        for def_k, def_v in DEFAULT_DYNAMIC_REPLIES.items():
            if def_k not in chat_replies:
                chat_replies[def_k] = def_v.copy()

    pending_states = context.application.bot_data.setdefault("pending_reply_states", {})

    # استقبال المحتوى بعد أمر send المنفصل مع الحفاظ على رسالة الاستهداف.
    pending_send = context.application.bot_data.setdefault("pending_send_states", {})
    if user.id in pending_send:
        send_state = pending_send[user.id]
        if send_state.get("chat_id") == chat.id:
            if (message.text or "").strip().lower() in {"الغاء", "إلغاء"}:
                pending_send.pop(user.id, None)
                await message.reply_text("تم إلغاء أمر send.")
                return True
            try:
                await _send_message_content_as_bot(message, context, chat.id,
                    reply_to_message_id=send_state["target_message_id"], payload_text=None)
                pending_send.pop(user.id, None)
                try:
                    await message.delete()
                except TelegramError:
                    pass
            except Exception as exc:
                logger.warning("send follow-up content failed: %s", exc, exc_info=True)
                await message.reply_text("تعذر إرسال المحتوى باستخدام send.")
            return True

    if user.id in pending_states:
        state_data = pending_states[user.id]
        if state_data.get("chat_id") != chat.id:
            return False

        step = state_data.get("step")
        raw_text_in = (message.text or message.caption or "").strip()
        if raw_text_in in ["الغاء", "إلغاء"]:
            del pending_states[user.id]
            await message.reply_text("تم إلغاء عملية إضافة الرد بنجاح.")
            return True

        mode = state_data.get("mode", "normal")

        if step == "awaiting_delete_trigger":
            target_word = raw_text_in.strip().lower()
            mapped_target = DEFAULT_ALIASES.get(target_word, target_word)
            mapping = context.application.bot_data.setdefault("custom_replies", db_load("custom_replies"))
            replies = mapping.setdefault(str(state_data.get("chat_id", chat.id)), {})
            if mapped_target in replies:
                del replies[mapped_target]
                db_save("custom_replies", mapping)
                del pending_states[user.id]
                await message.reply_text(f"✅ تم مسح الرد «{escape(target_word)}» بنجاح.", parse_mode=ParseMode.HTML)
            else:
                del pending_states[user.id]
                await message.reply_text("⚠️ لا يوجد رد مسجل بهذه الكلمة في هذه المجموعة.")
            return True

        if step == "awaiting_trigger":
            # Accept one or many trigger words, one per line, for every reply mode.
            trigger_words = list(dict.fromkeys(
                line.strip().lower() for line in raw_text_in.splitlines() if line.strip()
            ))
            if not trigger_words:
                await message.reply_text("أرسل كلمة مفتاحية واحدة على الأقل، وكل كلمة في سطر منفصل.")
                return True
            state_data["triggers"] = trigger_words
            # Keep the legacy field for older callbacks and data paths.
            state_data["trigger"] = trigger_words[0]
            state_data["step"] = "awaiting_content"
            shown_words = "\n".join(f"• <code>{escape(word)}</code>" for word in trigger_words)
            await message.reply_text(
                f'<tg-emoji emoji-id="5891235511246264255">✔️</tg-emoji> <b>تم تحديد الكلمات المفتاحية ({len(trigger_words)}):</b>\n{shown_words}\n\n'
                f'أرسل الآن محتوى الرد (نص، صورة، فيديو، ملصق، فويس، أو ملف صوتي):\n'
                f'<i>(كل الكلمات ستستدعي نفس الرد. أو اكتب إلغاء للتراجع)</i>',
                parse_mode=ParseMode.HTML,
            )
            return True

        elif step == "awaiting_content":
            reply_obj: dict[str, Any] = {"type": "text", "added_by": user.mention_html()}
            if message.text:
                reply_obj["type"] = "text"
                reply_obj["raw_text"] = message.text
                reply_obj["html"] = message.text_html
            elif message.photo:
                reply_obj["type"] = "photo"
                reply_obj["file_id"] = message.photo[-1].file_id
                reply_obj["caption"] = message.caption_html or message.caption or ""
            elif message.sticker:
                reply_obj["type"] = "sticker"
                reply_obj["file_id"] = message.sticker.file_id
            elif message.video:
                reply_obj["type"] = "video"
                reply_obj["file_id"] = message.video.file_id
                reply_obj["caption"] = message.caption_html or message.caption or ""
            elif message.animation:
                reply_obj["type"] = "animation"
                reply_obj["file_id"] = message.animation.file_id
                reply_obj["caption"] = message.caption_html or message.caption or ""
            elif message.voice:
                reply_obj["type"] = "voice"
                reply_obj["file_id"] = message.voice.file_id
                reply_obj["caption"] = message.caption_html or message.caption or ""
            elif message.audio:
                reply_obj["type"] = "audio"
                reply_obj["file_id"] = message.audio.file_id
                reply_obj["caption"] = message.caption_html or message.caption or ""
            elif message.document:
                reply_obj["type"] = "document"
                reply_obj["file_id"] = message.document.file_id
                reply_obj["caption"] = message.caption_html or message.caption or ""
            elif message.video_note:
                reply_obj["type"] = "video_note"
                reply_obj["file_id"] = message.video_note.file_id
            else:
                await message.reply_text("نوع المحتوى غير مدعوم. أرسل نصًا أو صورة أو فيديو أو ملصقًا أو فويس/صوتًا.")
                return True

            state_data["reply_obj"] = reply_obj

            if mode == "with_button":
                state_data["step"] = "awaiting_button_name" if state_data.get("multi_button") else "awaiting_button_info"
                prompt = ('🔘 <b>أرسل اسم الزر فقط:</b>' if state_data.get("multi_button") else
                    '🔘 <b>أرسل الآن نص الزر والرابط مفصولين بمسافة:</b>\nمثال: <code>دخول الشات https://t.me/example</code>')
                await message.reply_text(prompt + '\n\n<i>(أو اكتب إلغاء للتراجع)</i>', parse_mode=ParseMode.HTML)
                return True
            else:
                trigger_words = state_data.get("triggers") or [state_data["trigger"]]
                for trigger_word in trigger_words:
                    chat_replies[trigger_word] = reply_obj.copy()
                custom_replies_map[str(chat.id)] = chat_replies
                db_save("custom_replies", custom_replies_map)
                del pending_states[user.id]

                shown_words = "\n".join(f"• <code>{escape(word)}</code>" for word in trigger_words)
                await message.reply_text(
                    f'<tg-emoji emoji-id="5812288471837975057">💯</tg-emoji> <b>تم حفظ الرد العادي بنجاح!</b>\n\n'
                    f'<tg-emoji emoji-id="5891235511246264255">✔️</tg-emoji> <b>الكلمات المفتاحية ({len(trigger_words)}):</b>\n{shown_words}',
                    parse_mode=ParseMode.HTML,
                )
                return True

        elif step == "awaiting_button_name":
            if not raw_text_in:
                await message.reply_text("أرسل اسم الزر فقط.")
                return True
            button_label, button_icon_id = extract_button_custom_emoji(message, raw_text_in)
            if not button_label and not button_icon_id:
                await message.reply_text("اسم الزر غير صالح. أرسل نصًا أو إيموجي مميزًا.")
                return True
            state_data["current_button_text"] = button_label
            state_data["current_button_icon_custom_emoji_id"] = button_icon_id
            state_data["step"] = "awaiting_button_url"
            await message.reply_text("أرسل رابط الزر فقط (يبدأ بـ https:// أو http:// أو tg://).")
            return True

        elif step == "awaiting_button_url":
            btn_url = raw_text_in.strip()
            if not (btn_url.startswith("http://") or btn_url.startswith("https://") or btn_url.startswith("tg://")):
                await message.reply_text("الرابط غير صحيح. أرسله ويبدأ بـ https:// أو http:// أو tg://")
                return True
            state_data["btn_text"] = state_data.pop("current_button_text")
            state_data["btn_icon_custom_emoji_id"] = state_data.pop("current_button_icon_custom_emoji_id", None)
            state_data["btn_url"] = btn_url
            state_data["step"] = "awaiting_color_selection"
            color_kb = InlineKeyboardMarkup([[ 
                InlineKeyboardButton("🔴 أحمر", callback_data=f"btncol:danger:{user.id}:{chat.id}", style="danger"),
                InlineKeyboardButton("🟢 أخضر", callback_data=f"btncol:success:{user.id}:{chat.id}", style="success"),
                InlineKeyboardButton("🔵 أزرق", callback_data=f"btncol:primary:{user.id}:{chat.id}", style="primary"),
            ]])
            await message.reply_text("🎨 اختر لون الزر:", reply_markup=color_kb)
            return True

        elif step == "awaiting_button_info":
            parts = raw_text_in.split()
            if len(parts) < 2:
                await message.reply_text("يرجى كتابة نص الزر ثم الرابط: <code>دخول الرابط https://t.me/...</code>", parse_mode=ParseMode.HTML)
                return True

            btn_url = parts[-1]
            btn_text = " ".join(parts[:-1])
            label_start = len((message.text or "")[:len(message.text or "") - len((message.text or "").lstrip())])
            btn_text, btn_icon_id = extract_button_custom_emoji(message, btn_text, label_start)

            if not (btn_url.startswith("http://") or btn_url.startswith("https://") or btn_url.startswith("tg://")):
                await message.reply_text("يرجى التأكد من أن الرابط يبدأ بـ https:// أو tg://")
                return True

            state_data["btn_text"] = btn_text
            state_data["btn_icon_custom_emoji_id"] = btn_icon_id
            state_data["btn_url"] = btn_url
            state_data["step"] = "awaiting_color_selection"

            color_kb = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("🔴 أحمر", callback_data=f"btncol:danger:{user.id}:{chat.id}", style="danger"),
                    InlineKeyboardButton("🟢 أخضر", callback_data=f"btncol:success:{user.id}:{chat.id}", style="success"),
                    InlineKeyboardButton("🔵 أزرق", callback_data=f"btncol:primary:{user.id}:{chat.id}", style="primary"),
                ]
            ])

            await message.reply_text(
                '🎨 <b>اختر لون الزر التفاعلي المطلوب:</b>',
                parse_mode=ParseMode.HTML,
                reply_markup=color_kb
            )
            return True

    text = (message.text or message.caption or "").strip()
    if not text:
        return False

    if text in ["اضف رد", "أضف رد"]:
        if not await is_management_admin(update, context):
            return True

        pending_states[user.id] = {"step": "awaiting_reply_mode", "chat_id": chat.id}
        mode_kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("رد بزر", callback_data=f"replymode:button:{user.id}:{chat.id}", style="primary"),
            InlineKeyboardButton("بدون زر", callback_data=f"replymode:plain:{user.id}:{chat.id}", style="success"),
        ]])
        await message.reply_text(
            '<tg-emoji emoji-id="5890978075201509010">⭐</tg-emoji> <b>إضافة رد تلقائي</b>\n\n'
            'تحب الرد يكون بزر تفاعلي ولا رد عادي بدون زر؟',
            parse_mode=ParseMode.HTML,
            reply_markup=mode_kb,
        )
        return True

    if text in ["اضف رد بزر", "أضف رد بزر", "اضف زر", "أضف زر"]:
        if not await is_management_admin(update, context):
            return True

        pending_states[user.id] = {"step": "awaiting_trigger", "chat_id": chat.id, "mode": "with_button", "multi_button": True, "buttons": []}
        await message.reply_text(
            '🔘 <b>إضافة رد تفاعلي مزود بزرار ملون</b> 🔘\n\n'
            'أرسل <b>الكلمات المفتاحية</b>، كل كلمة في سطر منفصل.\n'
            'يمكنك إرسال كلمة واحدة أو عدة كلمات، وكلها ستستدعي نفس الرد.\n'
            '<i>(أو اكتب إلغاء للتراجع)</i>',
            parse_mode=ParseMode.HTML,
        )
        return True

    if text in ("مسح رد", "حذف رد"):
        if not await is_management_admin(update, context):
            return True
        pending_states[user.id] = {"step": "awaiting_delete_trigger", "chat_id": chat.id}
        await message.reply_text("أرسل كلمة الرد الذي تريد مسحه.")
        return True

    if text.startswith("مسح رد ") or text.startswith("حذف رد "):
        if not await is_management_admin(update, context):
            return True

        cmd_part = "مسح رد" if text.startswith("مسح رد") else "حذف رد"
        target_word = text[len(cmd_part):].strip().lower()

        if not target_word:
            await message.reply_text("يرجى كتابة الكلمة: <code>مسح رد مشرفين</code>", parse_mode=ParseMode.HTML)
            return True

        mapped_target = DEFAULT_ALIASES.get(target_word, target_word)
        if mapped_target in chat_replies:
            del chat_replies[mapped_target]
            custom_replies_map[str(chat.id)] = chat_replies
            db_save("custom_replies", custom_replies_map)
            await message.reply_text(f'✅ تم مسح الرد المخصص للكلمة « <b>{target_word}</b> » بنجاح.', parse_mode=ParseMode.HTML)
        else:
            await message.reply_text("⚠️ هذه الكلمة غير مسجلة في قائمة الردود لهذا الشات.")
        return True

    if text in ["الردود", "قائمة الردود"]:
        if not chat_replies:
            await message.reply_text("لا توجد ردود مخصصة مسجلة حالياً لهذه المجموعة.")
            return True

        lines_rep = [
            '<tg-emoji emoji-id="5890978075201509010">⭐</tg-emoji> <b>قائمة الردود التلقائية للشات:</b>\n'
            '━━━━━━━━━━━━━━━━━━━━'
        ]
        for idx, (trig, rdata) in enumerate(chat_replies.items(), 1):
            t_type = rdata.get("type", "text")
            has_b = " + زرار" if rdata.get("button") else ""
            lines_rep.append(f"<b>{idx}.</b> « <code>{trig}</code> » ➔ [{t_type}{has_b}]")
        lines_rep.append("━━━━━━━━━━━━━━━━━━━━\nلتعديل أو إضافة: <code>أضف رد</code> أو <code>أضف رد بزر</code>")
        await message.reply_text("\n".join(lines_rep), parse_mode=ParseMode.HTML)
        return True

    clean_lookup = text.lower()
    mapped_word = DEFAULT_ALIASES.get(clean_lookup, clean_lookup)

    if mapped_word in chat_replies:
        rdata = chat_replies[mapped_word]
        r_type = rdata.get("type", "text")

        kb = None
        if rdata.get("buttons") or rdata.get("button"):
            buttons = rdata.get("buttons") or [rdata["button"]]
            rows = []
            for b in buttons:
                button_kwargs = {"url": b["url"], "style": b.get("style", "success")}
                if b.get("icon_custom_emoji_id"):
                    button_kwargs["icon_custom_emoji_id"] = b["icon_custom_emoji_id"]
                btn = InlineKeyboardButton(b["text"], **button_kwargs)
                pos = b.get("position", "side")
                if pos == "side" and rows:
                    rows[-1].append(btn)
                elif pos == "above" and rows:
                    rows.insert(0, [btn])
                else:
                    rows.append([btn])
            kb = InlineKeyboardMarkup(rows)
        elif mapped_word == "ممنوع" and chat.id == PRIMARY_CHAT_ID:
            kb = InlineKeyboardMarkup(
                [[InlineKeyboardButton("✔️ قراءة رسالة القوانين ▶️", url=RULES_LINK, style="danger")]]
            )

        try:
            # عند استخدام رد محفوظ بالرد على رسالة عضو، اجعل الرد على رسالة العضو نفسها.
            reply_target_id = message.reply_to_message.message_id if message.reply_to_message else message.message_id
            send_kwargs = {"chat_id": chat.id, "reply_to_message_id": reply_target_id}
            if r_type == "text":
                html_resp = rdata.get("html", rdata.get("text", ""))
                try:
                    await context.bot.send_message(text=html_resp, parse_mode=ParseMode.HTML, reply_markup=kb, **send_kwargs)
                except Exception:
                    raw_fallback = rdata.get("raw_text", html_resp)
                    await context.bot.send_message(text=raw_fallback, reply_markup=kb, **send_kwargs)
            elif r_type == "photo":
                await context.bot.send_photo(photo=rdata["file_id"], caption=rdata.get("caption", ""), parse_mode=ParseMode.HTML, reply_markup=kb, **send_kwargs)
            elif r_type == "sticker":
                await context.bot.send_sticker(sticker=rdata["file_id"], **send_kwargs)
            elif r_type == "video":
                await context.bot.send_video(video=rdata["file_id"], caption=rdata.get("caption", ""), parse_mode=ParseMode.HTML, reply_markup=kb, **send_kwargs)
            elif r_type == "animation":
                await context.bot.send_animation(animation=rdata["file_id"], caption=rdata.get("caption", ""), parse_mode=ParseMode.HTML, reply_markup=kb, **send_kwargs)
            elif r_type == "voice":
                await context.bot.send_voice(voice=rdata["file_id"], caption=rdata.get("caption", ""), parse_mode=ParseMode.HTML, reply_markup=kb, **send_kwargs)
            elif r_type == "audio":
                await context.bot.send_audio(audio=rdata["file_id"], caption=rdata.get("caption", ""), parse_mode=ParseMode.HTML, reply_markup=kb, **send_kwargs)
            elif r_type == "document":
                await context.bot.send_document(document=rdata["file_id"], caption=rdata.get("caption", ""), parse_mode=ParseMode.HTML, reply_markup=kb, **send_kwargs)
            elif r_type == "video_note":
                await context.bot.send_video_note(video_note=rdata["file_id"], **send_kwargs)
            return True
        except Exception as e:
            logger.error(f"Error sending custom reply for {mapped_word}: {e}")

    return False


async def handle_button_color_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data or not query.data.startswith("btncol:"):
        return

    parts = query.data.split(":")
    chosen_style = parts[1]
    user_id = int(parts[2])
    chat_id = int(parts[3])

    if query.from_user.id != user_id:
        await query.answer("هذا الإجراء لمن قام بطلب إضافة الرد فقط.", show_alert=True)
        return

    pending_states = context.application.bot_data.get("pending_reply_states", {})
    if user_id not in pending_states:
        await query.answer("انتهت جلسة الإضافة.", show_alert=True)
        return

    state_data = pending_states.get(user_id)
    if not state_data or state_data.get("step") != "awaiting_color_selection":
        await query.answer("انتهت جلسة الإضافة أو لم تعد جاهزة.", show_alert=True)
        return
    trigger_word = state_data["trigger"]
    reply_obj = state_data["reply_obj"]
    btn_text = state_data["btn_text"]
    btn_url = state_data["btn_url"]
    btn_icon_id = state_data.get("btn_icon_custom_emoji_id")

    button = {"text": btn_text, "url": btn_url, "style": chosen_style}
    if btn_icon_id:
        button["icon_custom_emoji_id"] = btn_icon_id
    if state_data.get("multi_button"):
        button["position"] = state_data.pop("button_position", "side")
        state_data.setdefault("buttons", []).append(button)
        state_data["step"] = "awaiting_add_another"
        kb = InlineKeyboardMarkup([[ 
            InlineKeyboardButton("لا، حفظ", callback_data=f"btnmore:no:{user_id}:{chat_id}", style="success"),
            InlineKeyboardButton("نعم", callback_data=f"btnmore:yes:{user_id}:{chat_id}", style="primary"),
        ]])
        color_name = {"danger":"الأحمر", "success":"الأخضر", "primary":"الأزرق"}.get(chosen_style, "الأخضر")
        await query.answer("تم تسجيل الزر")
        await query.edit_message_text(f"✅ تم إعداد الزر {color_name}: {escape(btn_text)}\nهل تريد إضافة زر آخر؟", reply_markup=kb, parse_mode=ParseMode.HTML)
        return

    reply_obj["button"] = button
    custom_replies_map = context.application.bot_data.setdefault("custom_replies", db_load("custom_replies"))
    chat_replies = custom_replies_map.setdefault(str(chat_id), {})
    trigger_words = state_data.get("triggers") or [trigger_word]
    for keyword in trigger_words:
        chat_replies[keyword] = reply_obj.copy()
    custom_replies_map[str(chat_id)] = chat_replies
    db_save("custom_replies", custom_replies_map)
    await query.answer("تم اعتماد الرد والزر بنجاح! ✅")
    color_name = {"danger":"الأحمر", "success":"الأخضر", "primary":"الأزرق"}.get(chosen_style, "الأخضر")
    shown_words = "\n".join(f"• <code>{escape(word)}</code>" for word in (state_data.get("triggers") or [trigger_word]))
    await query.edit_message_text(f'💯 <b>تم حفظ الرد التفاعلي بالزر {color_name} بنجاح!</b>\n\n🔹 <b>الكلمات المفتاحية:</b>\n{shown_words}\n🔹 نص الزر: <b>{escape(btn_text)}</b>', parse_mode=ParseMode.HTML)


async def handle_reply_mode_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data or not query.data.startswith("replymode:"):
        return
    _, selected_mode, uid_s, cid_s = query.data.split(":", 3)
    uid, cid = int(uid_s), int(cid_s)
    if query.from_user.id != uid:
        await query.answer("هذا الاختيار لمن بدأ الإضافة فقط.", show_alert=True)
        return
    states = context.application.bot_data.setdefault("pending_reply_states", {})
    state = states.get(uid)
    if not state or state.get("chat_id") != cid or state.get("step") != "awaiting_reply_mode":
        await query.answer("انتهت جلسة إضافة الرد.", show_alert=True)
        return
    if selected_mode == "button":
        state.update({"step": "awaiting_trigger", "mode": "with_button", "multi_button": True, "buttons": []})
        prompt = "🔘 <b>إضافة رد بالأزرار</b>\n\nأرسل الكلمات المفتاحية، كل كلمة في سطر منفصل. جميعها ستستدعي نفس الرد:"
    else:
        state.update({"step": "awaiting_trigger", "mode": "normal"})
        prompt = "📝 <b>إضافة رد عادي بدون أزرار</b>\n\nأرسل الكلمات المفتاحية، كل كلمة في سطر منفصل. جميعها ستستدعي نفس الرد:"
    await query.answer()
    await query.edit_message_text(prompt + "\n<i>أو اكتب إلغاء للتراجع</i>", parse_mode=ParseMode.HTML)


async def handle_button_more_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data or not query.data.startswith("btnmore:"):
        return
    _, action, uid_s, cid_s = query.data.split(":", 3)
    uid, cid = int(uid_s), int(cid_s)
    if query.from_user.id != uid:
        await query.answer("هذا الإجراء لمن بدأ الإضافة فقط.", show_alert=True)
        return
    states = context.application.bot_data.get("pending_reply_states", {})
    st = states.get(uid)
    if not st or st.get("step") != "awaiting_add_another":
        await query.answer("انتهت جلسة الإضافة.", show_alert=True)
        return
    if action == "no":
        reply_obj = st["reply_obj"]
        reply_obj["buttons"] = st.get("buttons", [])
        # Compatibility: keep first button in legacy field for existing integrations.
        if reply_obj["buttons"]:
            reply_obj["button"] = reply_obj["buttons"][0]
        mapping = context.application.bot_data.setdefault("custom_replies", db_load("custom_replies"))
        trigger_words = st.get("triggers") or [st["trigger"]]
        for keyword in trigger_words:
            mapping.setdefault(str(cid), {})[keyword] = reply_obj.copy()
        db_save("custom_replies", mapping)
        states.pop(uid, None)
        await query.answer("تم الحفظ")
        await query.edit_message_text("✅ تم حفظ الرد وجميع أزراره بنجاح.")
        return
    st["step"] = "awaiting_button_position"
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("بجانبه", callback_data=f"btnpos:side:{uid}:{cid}"), InlineKeyboardButton("فوقه", callback_data=f"btnpos:above:{uid}:{cid}"), InlineKeyboardButton("تحته", callback_data=f"btnpos:below:{uid}:{cid}")]])
    await query.answer()
    await query.edit_message_text("أين تريد وضع الزر الجديد بالنسبة للزر الحالي؟", reply_markup=kb)


async def handle_button_position_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data or not query.data.startswith("btnpos:"):
        return
    _, position, uid_s, cid_s = query.data.split(":", 3)
    uid, cid = int(uid_s), int(cid_s)
    if query.from_user.id != uid:
        await query.answer("هذا الإجراء لمن بدأ الإضافة فقط.", show_alert=True)
        return
    st = context.application.bot_data.get("pending_reply_states", {}).get(uid)
    if not st or st.get("step") != "awaiting_button_position":
        await query.answer("انتهت جلسة الإضافة.", show_alert=True)
        return
    st["button_position"] = position
    st["step"] = "awaiting_button_name"
    await query.answer()
    await query.edit_message_text("أرسل اسم الزر الجديد فقط.")


# -------------------------------------------------------------
# Handlers: Comprehensive Lookup (مفتوح للجميع وبدون سرد صلاحيات)
# -------------------------------------------------------------

async def handle_comprehensive_lookup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.effective_message
    chat = update.effective_chat
    if not message or not chat:
        return False

    raw_text = (message.text or message.caption or "").strip()
    if not raw_text:
        return False

    lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]
    if not lines:
        return False

    # كشف رتب المجموعة الحالية فقط، دون خلط بيانات البوت أو كروت مجموعات أخرى.
    if raw_text in ("كاشف الجروب", "كاشف المجموعة", "كشف الجروب", "كشف المجموعة"):
        if not await is_real_admin(update, context):
            return True
        rows = ["⇜ <b>كشف المجموعة:</b>", "", "⇜ <b>المالكين الأساسيين:</b>"]
        try:
            admins = await context.bot.get_chat_administrators(chat.id)
            owners = [m.user for m in admins if m.status == ChatMemberStatus.OWNER]
            moderators = [m.user for m in admins if m.status == ChatMemberStatus.ADMINISTRATOR]
            if owners:
                for idx, u in enumerate(owners, 1):
                    username = f"@{escape(u.username)}" if u.username else "بدون يوزر"
                    rows.append(f"{idx} ➣ {username} ࿓ ( {u.id} )")
            else:
                rows.append("لا يوجد مالك أساسي متاح للعرض.")
            rows.extend(["", "⇜ <b>المشرفين:</b>"])
            if moderators:
                for idx, u in enumerate(moderators, 1):
                    username = f"@{escape(u.username)}" if u.username else "بدون يوزر"
                    rows.append(f"{idx} ➣ {username} ࿓ ( {u.id} )")
            else:
                rows.append("لا يوجد مشرفون في هذه المجموعة.")
        except TelegramError:
            rows.append("تعذر جلب رتب المجموعة من تيليجرام.")
        await message.reply_text("\n".join(rows), parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        return True

    # لا يُنفّذ كشف الحساب إلا بثلاث صيغ واضحة فقط:
    # 1) "كشف" وحدها كـ reply على رسالة العضو.
    # 2) "كشف <numeric ID>".
    # 3) "كشف @username".
    # هذا يمنع التقاط كلمة كشف داخل الكلام العادي أو التعارض مع كشف المجموعة.
    command_text = apply_alias(" ".join(raw_text.split()), {"kashf"})
    target_id = None
    target_username = None
    target_name = None

    if command_text == "كشف":
        if not message.reply_to_message or not message.reply_to_message.from_user:
            return False
        target_user = message.reply_to_message.from_user
        target_id = target_user.id
        target_username = target_user.username
        target_name = f'<a href="tg://user?id={target_user.id}">{escape(target_user.full_name)}</a>'
    else:
        id_match = re.fullmatch(r"كشف\s+(\d{6,12})", command_text)
        username_match = re.fullmatch(r"كشف\s+@([A-Za-z0-9_]{5,32})", command_text)
        if id_match:
            target_id = int(id_match.group(1))
        elif username_match:
            target_username = username_match.group(1)
            resolved_id, resolved_uname, resolved_name = await resolve_username_to_user(target_username, context)
            if resolved_id:
                target_id = resolved_id
                target_username = resolved_uname
                target_name = f'<a href="tg://user?id={resolved_id}">{escape(resolved_name)}</a>'
            else:
                await message.reply_text("⚠️ لم أتمكن من العثور على الحساب بهذا اليوزر.")
                return True
        else:
            return False

    if not target_id:
        await message.reply_text("⚠️ لم أتمكن من تحديد الحساب المطلوب للكشف.")
        return True

    group_rank = await get_accurate_chat_status(chat.id, target_id, context)

    bot_admins_map = context.application.bot_data.get("bot_admins", {})
    chat_admins = bot_admins_map.get(str(chat.id), {})
    distinguished_map = context.application.bot_data.get("distinguished_users", {})
    chat_dist = distinguished_map.get(str(chat.id), {})
    uid_str = str(target_id)

    if await is_primary_owner(target_id, context):
        bot_rank = '<tg-emoji emoji-id="5875344888165308204">👑</tg-emoji> مطور ومالك أساسي'
    elif uid_str in chat_admins:
        admin_rec = chat_admins[uid_str]
        role_label = admin_rec.get("role", "أدمن")
        icon = "" if role_label == "مدير" else '<tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji>'
        bot_rank = f'{icon} {role_label} بالبوت'
    elif uid_str in chat_dist:
        bot_rank = '<tg-emoji emoji-id="5776113577211468831">⭐</tg-emoji> عضو مميز'
    else:
        bot_rank = "عضو"

    is_super = ("مالك" in group_rank or "مشرف" in group_rank) or ("مطور" in bot_rank)
    if is_super:
        merchant_status = '<tg-emoji emoji-id="5875344888165308204">👑</tg-emoji> مشرف / مالك معتمد (الضمان معاه مباشرة)'
        matched_merchant = None
    else:
        merchants_map = context.application.bot_data.get("merchants", {})
        chat_merchants = merchants_map.get(str(chat.id), {})
        matched_merchant = None
        for mk, mv in chat_merchants.items():
            if mv.get("user_id") and str(mv["user_id"]) == uid_str:
                matched_merchant = mv
                break
            m_uname = (mv.get("username") or "").lower()
            if target_username and m_uname == target_username.lower():
                matched_merchant = mv
                break

        if matched_merchant:
            merchant_status = f'<tg-emoji emoji-id="5872935716454930968">🤑</tg-emoji> تاجر معتمد في مبلغ {matched_merchant.get("limit", "500")} جنيه'
        else:
            merchant_status = "غير معتمد كتاجر"

    blacklist = context.application.bot_data.get("blacklist", {})
    is_blacklisted = False
    for bk, bv in blacklist.items():
        if bv.get("user_id") and str(bv["user_id"]) == uid_str:
            is_blacklisted = True
            break
        b_uname = (bv.get("username") or "").lower()
        if target_username and b_uname == target_username.lower():
            is_blacklisted = True
            break

    if not target_name:
        resolved_name, resolved_uname = await resolve_user_id_info(target_id, context)
        target_name = resolved_name
        target_username = target_username or resolved_uname

    u_display = f"@{target_username}" if target_username else "بدون يوزر"
    plain_rank = re.sub(r"<[^>]+>", "", bot_rank).strip()
    if is_super:
        role_txt = "مالك / مشرف معتمد"
    elif matched_merchant:
        role_txt = "تاجر معتمد"
    else:
        role_txt = "لا يوجد"
    reports_txt = "مسجل في القائمة السوداء ⚠️" if is_blacklisted else "لا يوجد"
    link_txt = f"https://t.me/{target_username}" if target_username else f"tg://user?id={target_id}"

    kb = None
    if matched_merchant and target_username:
        kb = InlineKeyboardMarkup(
            [[InlineKeyboardButton("تواصل مع التاجر المعتمد ➜", url=f"https://t.me/{target_username.lstrip('@')}", style="success")]]
        )

    await reply_with_template(
        message, "cmd_kashf",
        {
            "name": _plain_name(target_name, target_id), "id": str(target_id), "username": u_display,
            "rank": plain_rank, "membership": group_rank, "role": role_txt,
            "reports": reports_txt, "link": link_txt,
        },
        mention_uid=target_id, reply_markup=kb,
    )
    return True


# -------------------------------------------------------------
# Handlers: Global Blacklist & Merchants
# -------------------------------------------------------------

async def handle_merchant_management(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.effective_message
    user = update.effective_user
    chat = update.effective_chat
    if not message or not user or not chat:
        return False

    raw_text = (message.text or message.caption or "").strip()
    if not raw_text:
        return False

    merchants_map = context.application.bot_data.setdefault("merchants", db_load("merchants"))
    chat_merchants = merchants_map.setdefault(str(chat.id), {})

    if raw_text.startswith("رفع تاجر"):
        if not await is_real_admin(update, context):
            return True

        content = raw_text[len("رفع تاجر"):].strip()
        tokens = content.split()

        amount_limit = "500"
        for t in list(tokens):
            clean_num = t.replace("ج", "").replace("EGP", "").rstrip(",;.:")
            if clean_num.isdigit():
                amount_limit = clean_num
                tokens.remove(t)
                break

        target_id = None
        target_username = None
        target_name = None

        if message.reply_to_message and message.reply_to_message.from_user:
            t_user = message.reply_to_message.from_user
            target_id = t_user.id
            target_username = t_user.username if t_user.username else None
            target_name = t_user.full_name

        if not target_id and tokens:
            target_id, target_username, target_name, _, _ = await resolve_target_and_details(update, context, tokens)

        if not target_id and not target_username:
            for t in tokens:
                clean_t = t.strip().rstrip(",;.:")
                if clean_t.startswith("@"):
                    target_username = clean_t.lstrip("@")
                    break

        if not target_id and not target_username:
            await message.reply_text(
                "⚠️ <b>يرجى تحديد التاجر بالرد على رسالته أو كتابة يوزره مع سقف الضمان:</b>\n"
                "• بالرد: <code>رفع تاجر 1500</code>\n"
                "• باليوزر: <code>رفع تاجر @username 1500</code>",
                parse_mode=ParseMode.HTML,
            )
            return True

        clean_uname = target_username.lstrip("@").rstrip(",;.:") if target_username else ""
        uid_key = str(target_id) if target_id else f"user_{clean_uname.lower()}"

        chat_merchants[uid_key] = {
            "user_id": str(target_id) if target_id else None,
            "username": clean_uname,
            "name": target_name or (f"@{clean_uname}" if clean_uname else uid_key),
            "limit": amount_limit,
            "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "promoted_by": update.effective_user.mention_html() if update.effective_user else "الإدارة",
        }
        merchants_map[str(chat.id)] = chat_merchants
        db_save("merchants", merchants_map)

        u_display = f"@{clean_uname}" if clean_uname else (target_name or uid_key)
        resp = (
            '<tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji> <b>ترقية تاجر معتمد</b> <tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji>\n'
            '━━━━━━━━━━━━━━━━━━━━\n\n'
            f'<tg-emoji emoji-id="5445092669522996408">👤</tg-emoji> <b>التاجر:</b> {u_display}\n'
            f'<tg-emoji emoji-id="5891235511246264255">✔️</tg-emoji> <b>سقف الضمان المعتمد:</b> <b>{amount_limit} جنيه</b>\n'
            f'📅 <b>تاريخ الاعتماد:</b> {datetime.now(timezone.utc).strftime("%Y-%m-%d")}\n\n'
            f'<i>تم تسجيل التاجر وتفعيل كارت الضمان التلقائي له بنجاح لهذا الشات <tg-emoji emoji-id="5872935716454930968">🤑</tg-emoji></i>'
        )
        await message.reply_text(resp, parse_mode=ParseMode.HTML)
        return True

    if raw_text.startswith("تنزيل تاجر"):
        if not await is_real_admin(update, context):
            return True

        content = raw_text[len("تنزيل تاجر"):].strip()
        tokens = content.split()

        target_id = None
        target_username = None

        if message.reply_to_message and message.reply_to_message.from_user:
            target_id = message.reply_to_message.from_user.id
            if message.reply_to_message.from_user.username:
                target_username = message.reply_to_message.from_user.username

        if not target_id and tokens:
            target_id, target_username, _, _, _ = await resolve_target_and_details(update, context, tokens)

        if not target_username:
            for t in tokens:
                clean_t = t.strip().rstrip(",;.:")
                if clean_t.startswith("@"):
                    target_username = clean_t.lstrip("@")
                    break

        found_uid = None
        for uid_k, mdata in list(chat_merchants.items()):
            if target_id and str(target_id) == str(mdata.get("user_id")):
                found_uid = uid_k
                break
            if target_username and target_username.lower() == str(mdata.get("username", "")).lower():
                found_uid = uid_k
                break
            if target_username and uid_k == f"user_{target_username.lower()}":
                found_uid = uid_k
                break

        if found_uid:
            del chat_merchants[found_uid]
            merchants_map[str(chat.id)] = chat_merchants
            db_save("merchants", merchants_map)
            await message.reply_text("✅ تم تنزيل التاجر وإلغاء صفة الاعتماد والضمان عنه بنجاح في هذا الشات.")
        else:
            await message.reply_text("⚠️ هذا الشخص ليس مسجلاً ضمن التجار المعتمدين في هذا الشات.")
        return True

    if raw_text in ["التجار", "قائمة التجار", "قائمه التجار"]:
        if not chat_merchants:
            await message.reply_text("لا يوجد تجار معتمدون مسجلون حالياً في هذا الشات.")
            return True

        lines_resp = [
            '<tg-emoji emoji-id="5875344888165308204">👑</tg-emoji> <b>قائمة التجار المعتمدين داخل الشات:</b>\n'
            '━━━━━━━━━━━━━━━━━━━━'
        ]
        for idx, (uid_k, mdata) in enumerate(chat_merchants.items(), 1):
            u_name = f"@{mdata['username']}" if mdata.get("username") else f"<code>{uid_k}</code>"
            lines_resp.append(
                f"<b>{idx}.</b> {u_name}\n"
                f'   <tg-emoji emoji-id="5891235511246264255">✔️</tg-emoji> حد الضمان: <b>{mdata.get("limit", "500")} ج</b> | 📅 {mdata.get("date", "")}'
            )
        lines_resp.append("━━━━━━━━━━━━━━━━━━━━\n⚠️ اتأكد دائماً من الرابط المباشر للتاجر قبل التحويل.")
        await message.reply_text("\n".join(lines_resp), parse_mode=ParseMode.HTML)
        return True

    return False


async def handle_merchant_lookup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.effective_message
    chat = update.effective_chat
    if not message or not chat:
        return False

    text = (message.text or message.caption or "").strip()
    if not text:
        return False

    # A username repeated as a reply to a prior message should not re-emit the
    # same merchant verification card. Users can still request it by sending
    # the username as a standalone message.
    if message.reply_to_message:
        return False

    merchants_map = context.application.bot_data.setdefault("merchants", db_load("merchants"))
    chat_merchants = merchants_map.get(str(chat.id), {})

    clean_text = text.lstrip("@").lower().rstrip(",;.:")
    matched_merchant = None
    for uid_k, mdata in chat_merchants.items():
        if mdata.get("username") and mdata["username"].lower() == clean_text:
            matched_merchant = mdata
            break
        if uid_k == f"user_{clean_text}":
            matched_merchant = mdata
            break

    if matched_merchant:
        u_disp = f"@{matched_merchant['username']}"
        link_direct = f"https://t.me/{matched_merchant['username'].lstrip('@')}" if matched_merchant.get("username") else None
        limit_val = matched_merchant.get("limit", "500")

        full_card = (
            '<tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji> <b>تـاجـر مـعـتـمـد ومـضـمـون</b> <tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji>\n'
            '━━━━━━━━━━━━━━━━━━\n\n'
            f'<tg-emoji emoji-id="5875344888165308204">👑</tg-emoji> <b>الـيـوزر:</b> {u_disp}\n\n'
            f'─── ⋆⋅☆⋅⋆ ───\n\n'
            f'<tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji> <b>مـضـمـون بـحـد أقـصـى:</b> <b>{limit_val} جنيه</b>\n\n'
            '━━━━━━━━━━━━━━━━━━\n'
            '<tg-emoji emoji-id="5253768790067406491">‼️</tg-emoji> <b>اتأكد من فتح المحادثة عبر زر التواصل منعاً للمنتحلين.</b>\n\n'
            f'<tg-emoji emoji-id="5890787894049643800">⛔</tg-emoji> <b>إخلاء مسؤولية:</b> إدارة الشات تضمن هذا التاجر في حدود مبلغ ({limit_val} ج) الموضح أعلاه فقط، وغير مسؤولين عن أي مبلغ يتجاوز هذا المبلغ نهائياً.'
        )
        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("تواصل مع التاجر المعتمد ➜", url=link_direct, style="success")]]
        ) if link_direct else None
        await message.reply_text(full_card, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        return True

    return False


async def handle_blacklist_commands(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.effective_message
    chat = update.effective_chat
    if not message:
        return False

    raw_text = (message.text or message.caption or "").strip()
    if not raw_text:
        return False

    lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]
    if not lines:
        return False

    raw_tokens = raw_text.split()
    text = raw_text
    blacklist: dict[str, dict[str, Any]] = context.application.bot_data["blacklist"]

    if text in ["النصابين", "قائمة النصابين", "قائمه النصابين"]:
        if not blacklist:
            await message.reply_text("🛡️ القائمة السوداء نظيفة تماماً ولا يوجد أي نصاب مسجل حالياً.")
            return True

        lines_resp = [
            '<tg-emoji emoji-id="5890814398292826361">🚨</tg-emoji> <b>قائمة النصابين المعتمدة (احذر التعامل معهم):</b>\n'
            '━━━━━━━━━━━━━━━━━━━━'
        ]
        count = 1
        for record_id, data in blacklist.items():
            u_name = f"@{data['username']}" if data.get("username") else "بدون يوزر"
            b_uid = data.get("user_id")
            uid_str = f"<code>{b_uid}</code>" if b_uid else "غير مسجل"
            raw_n = data.get("raw_name") or data.get("name", "نصاب")
            p_link = f'<a href="tg://user?id={b_uid}">{escape(raw_n)}</a>' if b_uid else escape(raw_n)
            reason_str = escape(data.get("reason", "لم يُحدد"))
            lines_resp.append(
                f"<b>{count}.</b> <tg-emoji emoji-id=\"5445092669522996408\">👤</tg-emoji> {p_link}\n"
                f"   🔹 <b>اليوزر:</b> {u_name}\n"
                f'   <tg-emoji emoji-id="5935966809655088705">🪪</tg-emoji> <b>الآيدي:</b> {uid_str}\n'
                f"   📝 <b>السبب:</b> {reason_str}\n"
                f"   📅 <b>التاريخ:</b> {data.get('date', 'غير محدد')}\n"
            )
            count += 1

        lines_resp.append("⚠️ <b>تنبيه:</b> يُمنع تحويل أي مبالغ أو تسليم حسابات لهذه المعرفات نهائياً.")
        await message.reply_text("\n".join(lines_resp), parse_mode=ParseMode.HTML)
        return True

    delete_keywords = ["حذف نصاب", "مسح نصاب", "فك نصاب", "شيل نصاب"]
    matched_del_cmd = None
    for line in lines:
        for dkw in delete_keywords:
            if line.startswith(dkw):
                matched_del_cmd = dkw
                break
        if matched_del_cmd:
            break

    if matched_del_cmd:
        if not await check_bot_permission(update, context, "can_blacklist"):
            return True

        clean_tokens = text.replace(matched_del_cmd, " ").split()
        target_id, target_username, _, _, _ = await resolve_target_and_details(update, context, clean_tokens)

        found_key = None
        for key, data in list(blacklist.items()):
            if target_id and str(target_id) == str(data.get("user_id")):
                found_key = key
                break
            db_uname = (data.get("username") or "").lower()
            if target_username and target_username.lower() == db_uname:
                found_key = key
                break
            if clean_tokens and clean_tokens[0].lstrip("@").lower().rstrip(",;.:") == db_uname:
                found_key = key
                break
            if clean_tokens and clean_tokens[0].rstrip(",;.:") == str(data.get("user_id")):
                found_key = key
                break

        if found_key:
            del blacklist[found_key]
            db_save("blacklist", blacklist)
            await message.reply_text("✅ تم حذف الحساب من القائمة السوداء العامة بنجاح.")
        else:
            await message.reply_text("هذا الحساب غير مدرج في القائمة السوداء.")
        return True

    is_blacklist_add = False
    for line in lines:
        w_list = line.split()
        if w_list and w_list[0] == "نصاب":
            is_blacklist_add = True
            break

    if is_blacklist_add:
        # أمر «نصاب» يعمل فقط مع يوزر أو آيدي مكتوب صراحةً، ولا يعمل بالرد.
        # إذا كُتبت الكلمة وحدها أو كُتبت كـ reply نتجاهلها تماماً.
        if message.reply_to_message:
            return True

        clean_tokens = [t for t in raw_tokens if t != "نصاب"]
        explicit_target = next((t for t in clean_tokens if t.startswith("@") or t.isdigit()), None)
        if not explicit_target:
            return True

        if not await check_bot_permission(update, context, "can_blacklist"):
            return True

        target_id, target_username, target_name, _, reason = await resolve_target_and_details(update, context, clean_tokens)

        if not target_id and not target_username:
            return True

        if target_id and await is_primary_owner(target_id, context):
            await message.reply_text("⛔ لا يمكن إضافة مطور ومالك البوت إلى القائمة السوداء.")
            return True

        raw_display_name = "نصاب"
        if message.reply_to_message and message.reply_to_message.from_user:
            raw_display_name = message.reply_to_message.from_user.full_name
        elif target_name:
            raw_display_name = re.sub(r"<[^>]+>", "", target_name)

        primary_key = str(target_id) if target_id else f"@{target_username}"
        blacklist[primary_key] = {
            "name": target_name or "نصاب",
            "raw_name": raw_display_name,
            "user_id": str(target_id) if target_id else None,
            "username": target_username,
            "reason": reason,
            "added_by": update.effective_user.mention_html() if update.effective_user else "مشرف",
            "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        }

        banned_status = "وحظره من الجروب نهائياً 🚫"
        if target_id and chat and chat.type != "private":
            try:
                await context.bot.ban_chat_member(chat_id=chat.id, user_id=target_id)
            except Exception:
                pass

        db_save("blacklist", blacklist)
        u_display = f"@{target_username}" if target_username else "بدون يوزر"
        id_display = f"<code>{target_id}</code>" if target_id else "غير مسجل"
        clickable_name = f'<a href="tg://user?id={target_id}">{escape(raw_display_name)}</a>' if target_id else escape(raw_display_name)

        resp = (
            "🚫 <b>تم تسجيل النصاب بالقائمة السوداء العامة وحظره بنجاح:</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            f'<tg-emoji emoji-id="5445092669522996408">👤</tg-emoji> {clickable_name}\n'
            f'   🔹 اليوزر: {u_display} | <tg-emoji emoji-id="5935966809655088705">🪪</tg-emoji> الآيدي: {id_display}\n'
            f"   📝 السبب: {escape(reason)}\n"
            f"   ⚡ الحالة: تم إدراجه بالقائمة السوداء {banned_status}"
        )
        await message.reply_text(resp, parse_mode=ParseMode.HTML)
        return True

    is_check_cmd = False
    for line in lines:
        w_list = line.split()
        if w_list and w_list[0] == "فحص":
            is_check_cmd = True
            break

    if is_check_cmd:
        clean_tokens = [t for t in raw_tokens if t != "فحص"]
        target_id, target_username, _, _, _ = await resolve_target_and_details(update, context, clean_tokens)

        matched_data = None
        for key, data in blacklist.items():
            if target_id and str(target_id) == str(data.get("user_id")):
                matched_data = data
                break
            db_uname = (data.get("username") or "").lower()
            if target_username and target_username.lower() == db_uname:
                matched_data = data
                break

        if matched_data:
            u_name = f"@{matched_data.get('username')}" if matched_data.get('username') else "بدون يوزر"
            b_uid = matched_data.get("user_id")
            uid_str = f"<code>{b_uid}</code>" if b_uid else "غير مسجل"
            raw_n = matched_data.get("raw_name") or matched_data.get("name", "نصاب")
            clickable_name = f'<a href="tg://user?id={b_uid}">{escape(raw_n)}</a>' if b_uid else escape(raw_n)

            await message.reply_text(
                '<tg-emoji emoji-id="5890787894049643800">⛔</tg-emoji> <b>تحذير عاجل: هذا الحساب مسجل بالقائمة السوداء العامة!</b> <tg-emoji emoji-id="5890787894049643800">⛔</tg-emoji>\n'
                '━━━━━━━━━━━━━━━━━━━━\n\n'
                f'<tg-emoji emoji-id="5445092669522996408">👤</tg-emoji> <b>الحساب:</b> {clickable_name}\n'
                f'<tg-emoji emoji-id="5935966809655088705">🪪</tg-emoji> <b>الآيدي:</b> {uid_str}\n'
                f'🔹 <b>اليوزر:</b> {u_name}\n'
                f'📝 <b>سبب الإدراج:</b> {escape(matched_data.get("reason", "نصب"))}\n'
                f'📅 <b>تاريخ التسجيل:</b> {matched_data.get("date", "غير محدد")}\n\n'
                '━━━━━━━━━━━━━━━━━━━━\n'
                '🚫 <b>ممنوع التعامل أو إرسال أي أموال له نهائياً!</b>',
                parse_mode=ParseMode.HTML,
            )
        else:
            await message.reply_text("✅ الحساب غير مدرج بالقائمة السوداء، ولكن تعامل دائماً عبر المشرفين والوسطاء المعتمدين فقط.")
        return True

    return False


async def check_for_blacklisted_mentions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.effective_message
    if not message:
        return False

    raw_text = message.text or message.caption or ""
    if not raw_text:
        return False

    blacklist: dict[str, dict[str, Any]] = context.application.bot_data.get("blacklist", {})
    if not blacklist:
        return False

    text_lower = raw_text.lower()
    found_usernames = set(re.findall(r"@([a-zA-Z0-9_]{4,32})", text_lower))
    found_numbers = set(re.findall(r"\b(\d{6,13})\b", raw_text))
    sender_id = str(message.from_user.id) if message.from_user else None

    matched_data = None
    for key, data in blacklist.items():
        b_uid = str(data.get("user_id")) if data.get("user_id") else None
        b_uname = (data.get("username") or "").lstrip("@").lower().strip()

        if b_uid and sender_id and b_uid == sender_id:
            matched_data = data
            break

        if b_uname and b_uname in found_usernames:
            matched_data = data
            break

        if b_uid and b_uid in found_numbers:
            matched_data = data
            break

        if b_uname and f"@{b_uname}" in text_lower:
            matched_data = data
            break

    if matched_data:
        u_display = f"@{matched_data.get('username')}" if matched_data.get("username") else "بدون يوزر"
        b_uid = matched_data.get("user_id")
        id_display = f"<code>{b_uid}</code>" if b_uid else "غير مسجل"
        raw_n = matched_data.get("raw_name") or matched_data.get("name", "نصاب")
        clickable_name = f'<a href="tg://user?id={b_uid}">{escape(raw_n)}</a>' if b_uid else escape(raw_n)
        reason_txt = escape(matched_data.get("reason", "نصب واحتيال"))

        warn_msg = (
            '<tg-emoji emoji-id="5890787894049643800">⛔</tg-emoji> <b>تـحـذيـر أمـنـي عـاجـل: حـسـاب نـصـاب!</b> <tg-emoji emoji-id="5890787894049643800">⛔</tg-emoji>\n'
            '━━━━━━━━━━━━━━━━━━━━\n\n'
            '<tg-emoji emoji-id="5890814398292826361">🚨</tg-emoji> <b>انتبه! المعرف المذكور مسجل في القائمة السوداء العامة:</b>\n\n'
            f'<tg-emoji emoji-id="5445092669522996408">👤</tg-emoji> <b>الحساب:</b> {clickable_name}\n'
            f'🔹 <b>اليوزر:</b> {u_display}\n'
            f'<tg-emoji emoji-id="5935966809655088705">🪪</tg-emoji> <b>الآيدي:</b> {id_display}\n'
            f'📝 <b>السبب:</b> {reason_txt}\n'
            f'📅 <b>تاريخ التسجيل:</b> {matched_data.get("date", "سابقاً")}\n\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
            '🚫 <b>ممنوع التعامل معه أو إرسال أي أموال له نهائياً تحت أي ظرف.</b>'
        )
        await message.reply_text(warn_msg, parse_mode=ParseMode.HTML)
        return True

    return False


async def handle_restricted_list_and_unmute(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.effective_message
    chat = update.effective_chat
    if not message or not chat:
        return False

    text = (message.text or message.caption or "").strip()
    if text in ["المقيدين", "قائمة المقيدين", "المكتومين"]:
        if not await check_bot_permission(update, context, "can_mute"):
            return False
        restricted_map = db_load("restricted_users")
        chat_restr = restricted_map.get(str(chat.id), {})
        if not chat_restr:
            await message.reply_text("✅ لا يوجد أعضاء مقيدين مسجلين في النظام حالياً.")
            return True

        lines = ["🤐 <b>قائمة الأعضاء المقيدين داخل الشات:</b>\n━━━━━━━━━━━━━━━━━━━━"]
        for idx, (uid, uinfo) in enumerate(chat_restr.items(), 1):
            lines.append(f"<b>{idx}.</b> {uinfo.get('name', uid)} — <code>{uid}</code>\n   📝 السبب: {uinfo.get('reason', 'تقييد إداري')}")
        lines.append("━━━━━━━━━━━━━━━━━━━━\nلإلغاء كتم الجميع اكتب: <code>مسح المقيدين</code>")
        await message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)
        return True

    if text in ["مسح المقيدين", "فك المقيدين", "الغاء تقييد الكل"]:
        if not await check_bot_permission(update, context, "can_mute"):
            return True

        restricted_map = db_load("restricted_users")
        chat_restr = restricted_map.get(str(chat.id), {})
        if not chat_restr:
            await message.reply_text("لا يوجد أعضاء مقيدين في السجل لإلغاء تقييدهم.")
            return True

        full_perms = ChatPermissions(
            can_send_messages=True,
            can_send_photos=True,
            can_send_videos=True,
            can_send_other_messages=True,
            can_send_polls=True,
            can_add_web_page_previews=True,
        )

        unmuted_count = 0
        for uid_str in list(chat_restr.keys()):
            try:
                await context.bot.restrict_chat_member(chat_id=chat.id, user_id=int(uid_str), permissions=full_perms)
                unmuted_count += 1
            except Exception:
                pass

        restricted_map[str(chat.id)] = {}
        db_save("restricted_users", restricted_map)
        await message.reply_text(f"✅ تم فك الكتم والتقييد بنجاح عن <b>{unmuted_count}</b> عضواً.", parse_mode=ParseMode.HTML)
        return True

    return False


# -------------------------------------------------------------
# Handlers: Transactions System
# -------------------------------------------------------------

async def handle_transaction_commands(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.effective_message
    user = update.effective_user
    chat = update.effective_chat
    if not message or not user or not chat:
        return False

    text = (message.text or message.caption or "").strip()
    transactions_map = context.application.bot_data.setdefault("transactions", db_load("transactions"))
    chat_txns = transactions_map.setdefault(str(chat.id), {})

    if text.startswith("تسجيل"):
        if not await is_real_admin(update, context):
            return True

        target_mention = None
        target_user_id = None
        if message.reply_to_message and message.reply_to_message.from_user:
            t_user = message.reply_to_message.from_user
            target_user_id = t_user.id
            target_mention = f'<a href="tg://user?id={t_user.id}">{escape(t_user.full_name)}</a>'
            if t_user.username:
                target_mention = f"@{t_user.username}"

        content = text[len("تسجيل"):].strip()
        phone = None
        account_name = None
        amount = None
        product = None

        if "-" in content:
            parts = [p.strip() for p in content.split("-")]
            if len(parts) >= 4:
                phone = parts[0]
                account_name = parts[1]
                amount = parts[2]
                product = "-".join(parts[3:])
                prod_tokens = product.split()
                for pt in list(prod_tokens):
                    clean_pt = pt.strip().rstrip(",;.:")
                    if clean_pt.startswith("@"):
                        target_mention = clean_pt
                        uid, _, _ = await resolve_username_to_user(clean_pt, context)
                        if uid:
                            target_user_id = uid
                        prod_tokens.remove(pt)
                product = " ".join(prod_tokens)
        else:
            tokens = content.split()
            phone_match = re.search(r"\b(01[0125]\d{8})\b", content)
            if phone_match:
                phone = phone_match.group(1)
                tokens.remove(phone)

            for t in list(tokens):
                clean_t = t.replace("ج", "").replace("EGP", "").rstrip(",;.:")
                if clean_t.isdigit():
                    amount = clean_t
                    tokens.remove(t)
                    break

            for t in list(tokens):
                clean_t = t.strip().rstrip(",;.:")
                if clean_t.startswith("@"):
                    target_mention = clean_t
                    uid, _, _ = await resolve_username_to_user(clean_t, context)
                    if uid:
                        target_user_id = uid
                    tokens.remove(t)

            if len(tokens) >= 2:
                account_name = tokens[0] + " " + tokens[1]
                product = " ".join(tokens[2:]) or "غير محدد"
            elif len(tokens) == 1:
                account_name = tokens[0]
                product = "غير محدد"

        if not phone or not account_name or not amount:
            await message.reply_text(
                "⚠️ <b>يرجى كتابة البيانات بالفاصل (-) لضبط الاسم المزدوج:</b>\n"
                "<code>تسجيل [رقم الكاش] - [الاسم بالكامل] - [المبلغ] - [المنتج]</code>\n\n"
                "<b>مثال:</b>\n"
                "<code>تسجيل 01042923585 - محمد فهمي - 10 - حسابات جوجل @username</code>",
                parse_mode=ParseMode.HTML,
            )
            return True

        tx_id = f"#TX-{random.randint(1000, 9999)}"
        chat_txns[phone] = {
            "tx_id": tx_id,
            "phone": phone,
            "name": account_name,
            "amount": amount,
            "product": product,
            "mention": target_mention or escape(account_name),
            "target_user_id": target_user_id,
            "status": "pending",
            "date": datetime.now(timezone.utc).strftime("%Y-%m-%d %I:%M %p"),
            "admin": update.effective_user.mention_html() if update.effective_user else "الأدمن",
        }
        transactions_map[str(chat.id)] = chat_txns
        db_save("transactions", transactions_map)

        wallet_icon = get_wallet_emoji(phone)
        await message.reply_text(
            f'<tg-emoji emoji-id="5775979900649347911">💵</tg-emoji> <b>تم تسجيل المعاملة بنجاح في النظام</b> <tg-emoji emoji-id="5775979900649347911">💵</tg-emoji>\n'
            f'━━━━━━━━━━━━━━━━━━━━\n\n'
            f'<tg-emoji emoji-id="5886413422614026772">🧾</tg-emoji> <b>رقم المعاملة:</b> <code>{tx_id}</code>\n'
            f'{wallet_icon}<b>رقم المحفظة:</b> <code>{phone}</code>\n'
            f'<tg-emoji emoji-id="5445092669522996408">👤</tg-emoji> <b>اسم صاحب الكاش:</b> {escape(account_name)}\n'
            f'💰 <b>المبلغ:</b> {escape(amount)} ج\n'
            f'📦 <b>المنتج / السلعة:</b> {escape(product)}\n'
            f'⏳ <b>الحالة:</b> قيد الانتظار للتحويل',
            parse_mode=ParseMode.HTML,
        )
        return True

    if text.startswith("تم التحويل") or text.startswith("تم تحويل"):
        if not await is_real_admin(update, context):
            return True

        photo_to_send = None
        if message.photo:
            photo_to_send = message.photo[-1].file_id
        elif message.reply_to_message and message.reply_to_message.photo:
            photo_to_send = message.reply_to_message.photo[-1].file_id

        parts = text.split()
        phone = None
        for p in parts:
            clean_p = p.strip().rstrip(",;.:")
            if re.match(r"^01[0125]\d{8}$", clean_p):
                phone = clean_p
                break

        if not phone:
            await message.reply_text("يرجى كتابة رقم الكاش: <code>تم التحويل 010xxxxxxxx</code>", parse_mode=ParseMode.HTML)
            return True

        if phone not in chat_txns:
            await message.reply_text("⚠️ هذا الرقم غير مسجل في معاملات هذا الشات.", parse_mode=ParseMode.HTML)
            return True

        txn = chat_txns[phone]
        txn["status"] = "transferred"
        txn["completed_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %I:%M %p")

        person_mention = txn.get("mention", escape(txn["name"]))
        tx_id_str = txn.get("tx_id", "غير محدد")
        wallet_icon = get_wallet_emoji(txn["phone"])

        caption = (
            '<tg-emoji emoji-id="5812288471837975057">💯</tg-emoji> <b>تم تحويل الأموال بنجاح من إدارة الشات 💸✔️</b>\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
            f'<tg-emoji emoji-id="5886413422614026772">🧾</tg-emoji> <b>رقم المعاملة:</b> <code>{tx_id_str}</code>\n'
            f'<tg-emoji emoji-id="5445092669522996408">👤</tg-emoji> <b>المستلم:</b> {person_mention}\n'
            f'{wallet_icon}<b>رقم المحفظة:</b> <code>{txn["phone"]}</code>\n'
            f'<tg-emoji emoji-id="5445092669522996408">👤</tg-emoji> <b>الكاش باسم:</b> {escape(txn["name"])}\n'
            f'💰 <b>المبلغ المحول:</b> {escape(txn["amount"])} ج\n'
            f'📦 <b>العملية:</b> {escape(txn["product"])}\n'
            f'📅 <b>الوقت:</b> {txn["completed_at"]}\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
            '⚡ <i>معاملة موثقة وآمنة 100% عبر إدارة الشات المعتمدة.</i>\n\n'
            '👇 <b>يرجى من المستلم مراجعة حسابه وتأكيد الاستلام:</b>'
        )

        reception_keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("تم استلام الفلوس", callback_data=f"tx_received:{phone}:{chat.id}", style="success"),
                    InlineKeyboardButton("لم أستلم بعد", callback_data=f"tx_not_received:{phone}:{chat.id}", style="danger"),
                ]
            ]
        )

        try:
            await message.delete()
        except TelegramError:
            pass

        if photo_to_send:
            sent_msg = await context.bot.send_photo(
                chat_id=chat.id,
                photo=photo_to_send,
                caption=caption,
                parse_mode=ParseMode.HTML,
                reply_markup=reception_keyboard,
            )
        else:
            sent_msg = await context.bot.send_message(
                chat_id=chat.id,
                text=caption,
                parse_mode=ParseMode.HTML,
                reply_markup=reception_keyboard,
            )

        if sent_msg:
            txn["payout_message_id"] = sent_msg.message_id
            txn["payout_chat_id"] = chat.id
            txn["has_photo"] = bool(photo_to_send)

        transactions_map[str(chat.id)] = chat_txns
        db_save("transactions", transactions_map)
        return True

    if text.startswith("حذف معاملة") or text.startswith("حذف معامله"):
        if not await is_real_admin(update, context):
            return True

        parts = text.split()
        phone = None
        for p in parts:
            clean_p = p.strip().rstrip(",;.:")
            if re.match(r"^01[0125]\d{8}$", clean_p):
                phone = clean_p
                break

        if not phone:
            await message.reply_text("يرجى كتابة رقم الكاش: <code>حذف معاملة 010xxxxxxxx</code>", parse_mode=ParseMode.HTML)
            return True

        if phone in chat_txns:
            tx_id_str = chat_txns[phone].get("tx_id", "غير محدد")
            del chat_txns[phone]
            transactions_map[str(chat.id)] = chat_txns
            db_save("transactions", transactions_map)
            await message.reply_text(f"✅ تم حذف المعاملة (<code>{tx_id_str}</code>) الخاصة بالرقم <code>{phone}</code> نهائياً من هذا الشات.", parse_mode=ParseMode.HTML)
        else:
            await message.reply_text("⚠️ هذا الرقم غير موجود في معاملات هذا الشات.", parse_mode=ParseMode.HTML)
        return True

    if text in ["المعاملات", "المستحقات", "قائمة التحويلات"]:
        if not await is_real_admin(update, context):
            return False

        pending = [v for v in chat_txns.values() if v.get("status") in ["pending", "confirmed"]]
        if not pending:
            await message.reply_text("✨ لا توجد أي مستحقات معلقة حالياً في هذا الشات.")
            return True

        lines_resp = ["📋 <b>قائمة المستحقات المعلقة للتحويل:</b>\n━━━━━━━━━━━━━━━━━━━━"]
        for idx, t in enumerate(pending, 1):
            st = "مؤكدة" if t.get("status") == "confirmed" else "قيد الانتظار"
            lines_resp.append(
                f"<b>{idx}.</b> <code>{t.get('tx_id', '#TX')}</code> | 📱 <code>{t['phone']}</code> ({st})\n"
                f"   👤 {escape(t['name'])} | 💰 {escape(t['amount'])} ج\n"
                f"   📦 {escape(t['product'])}\n"
            )
        await message.reply_text("\n".join(lines_resp), parse_mode=ParseMode.HTML)
        return True

    return False


async def check_for_tx_code_lookup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.effective_message
    chat = update.effective_chat
    if not message or not chat:
        return False

    raw_text = (message.text or message.caption or "").strip()
    if not raw_text:
        return False

    text = raw_text.upper()
    match = re.search(r"#?TX-\d{4}", text)
    if not match:
        return False

    searched_code = match.group(0)
    if not searched_code.startswith("#"):
        searched_code = f"#{searched_code}"

    transactions_map = context.application.bot_data.setdefault("transactions", db_load("transactions"))
    chat_txns = transactions_map.get(str(chat.id), {})

    found_txn = None
    for txn in chat_txns.values():
        if txn.get("tx_id", "").upper() == searched_code:
            found_txn = txn
            break

    if not found_txn:
        if text.startswith("فحص") or text.startswith("معاملة") or text == searched_code:
            await message.reply_text(f"⚠️ لم يتم العثور على أي معاملة بالكود <code>{searched_code}</code> في هذا الشات.", parse_mode=ParseMode.HTML)
            return True
        return False

    status_map = {
        "pending": "⏳ قيد الانتظار للتحويل",
        "confirmed": "✅ مؤكدة وجاهزة للتحويل",
        "transferred": "💸 تم التحويل وبانتظار الاستلام",
        "completed": "🌟 مكتملة ومستلمة رسمياً",
    }
    st_text = status_map.get(found_txn.get("status", "pending"), "غير معروفة")

    person_mention = found_txn.get("mention", escape(found_txn["name"]))
    rev = found_txn.get("review")
    rev_str = f'\n<tg-emoji emoji-id="5956391732789646927">💬</tg-emoji> <b>رأي المستلم:</b> « <i>{escape(rev)}</i> »' if rev else ""
    wallet_icon = get_wallet_emoji(found_txn["phone"])

    info = (
        f'<tg-emoji emoji-id="5886413422614026772">🧾</tg-emoji> <b>سجل تفاصيل المعاملة ({searched_code})</b>\n'
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 <b>الحالة:</b> {st_text}\n"
        f'<tg-emoji emoji-id="5445092669522996408">👤</tg-emoji> <b>المستلم:</b> {person_mention}\n'
        f'{wallet_icon}<b>رقم المحفظة:</b> <code>{found_txn["phone"]}</code>\n'
        f'<tg-emoji emoji-id="5445092669522996408">👤</tg-emoji> <b>الاسم:</b> {escape(found_txn["name"])}\n'
        f'💰 <b>المبلغ:</b> {escape(found_txn["amount"])} ج\n'
        f'📦 <b>السلعة:</b> {escape(found_txn["product"])}\n'
        f'👮 <b>المشرف المسجل:</b> {found_txn.get("admin", "مشرف")}\n'
        f'📅 <b>تاريخ التسجيل:</b> {found_txn.get("date", "غير محدد")}'
        f"{rev_str}\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )
    await message.reply_text(info, parse_mode=ParseMode.HTML)
    return True


async def check_for_cash_number_mention(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.effective_message
    chat = update.effective_chat
    if not message or not chat:
        return False

    text = (message.text or message.caption or "").strip()
    if not text:
        return False

    transactions_map = context.application.bot_data.setdefault("transactions", db_load("transactions"))
    chat_txns = transactions_map.get(str(chat.id), {})

    found_numbers = re.findall(r"\b(01[0125]\d{8})\b", text)
    if not found_numbers:
        return False

    for phone in found_numbers:
        if phone in chat_txns:
            txn = chat_txns[phone]
            if txn.get("status") in ["pending", "confirmed"]:
                keyboard = InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton("تأكيد التحويل", callback_data=f"tx_confirm:{phone}:{chat.id}", style="success"),
                            InlineKeyboardButton("إلغاء المعاملة", callback_data=f"tx_cancel:{phone}:{chat.id}", style="danger"),
                        ]
                    ]
                )
                wallet_icon = get_wallet_emoji(txn["phone"])
                resp = (
                    '<tg-emoji emoji-id="5775979900649347911">💵</tg-emoji> <b>بيانات المستحقات المالية المعتمدة</b> <tg-emoji emoji-id="5775979900649347911">💵</tg-emoji>\n'
                    '━━━━━━━━━━━━━━━━━━━━\n'
                    f'<tg-emoji emoji-id="5886413422614026772">🧾</tg-emoji> <b>رقم المعاملة:</b> <code>{txn.get("tx_id", "#TX")}</code>\n'
                    f'{wallet_icon}<b>رقم المحفظة:</b> <code>{txn["phone"]}</code>\n'
                    f'<tg-emoji emoji-id="5445092669522996408">👤</tg-emoji> <b>الكاش مسجل باسم:</b> {escape(txn["name"])}\n'
                    f'💰 <b>المبلغ المطلوب:</b> {escape(txn["amount"])} ج\n'
                    f'📦 <b>المنتج / الشيء:</b> {escape(txn["product"])}\n'
                    '━━━━━━━━━━━━━━━━━━━━\n'
                    'يرجى مراجعة البيانات والضغط على الإجراء المناسب:'
                )
                await message.reply_text(resp, parse_mode=ParseMode.HTML, reply_markup=keyboard)
                return True
    return False


async def handle_transaction_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return

    parts = query.data.split(":")
    action = parts[0]
    phone = parts[1]
    c_id_str = parts[2] if len(parts) > 2 else str(query.message.chat_id)

    transactions_map = context.application.bot_data.setdefault("transactions", db_load("transactions"))
    chat_txns = transactions_map.get(c_id_str, {})

    if phone not in chat_txns:
        await query.answer("عذراً، هذه المعاملة غير مسجلة أو تم حذفها مسبقاً.", show_alert=True)
        return

    txn = chat_txns[phone]
    user = query.from_user

    admin_status = await is_real_admin(update, context)
    is_target_user = False

    if txn.get("target_user_id") and user.id == txn["target_user_id"]:
        is_target_user = True
    elif user.username and txn.get("mention") and f"@{user.username.lower()}" == txn["mention"].lower():
        is_target_user = True

    tx_id_str = txn.get("tx_id", "#TX")
    wallet_icon = get_wallet_emoji(txn["phone"])

    if action == "tx_confirm":
        if not (admin_status or is_target_user):
            await query.answer("هذا الإجراء مخصص لصاحب المعاملة أو المشرفين فقط.", show_alert=True)
            return

        txn["status"] = "confirmed"
        transactions_map[c_id_str] = chat_txns
        db_save("transactions", transactions_map)
        await query.answer("تم تأكيد التحويل بنجاح ✅")

        confirmed_text = (
            '<tg-emoji emoji-id="5891235511246264255">✔️</tg-emoji> <b>بيانات المستحقات المالية (تم تأكيد التحويل بنجاح)</b>\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
            f'<tg-emoji emoji-id="5886413422614026772">🧾</tg-emoji> <b>رقم المعاملة:</b> <code>{tx_id_str}</code>\n'
            f'{wallet_icon}<b>رقم المحفظة:</b> <code>{txn["phone"]}</code>\n'
            f'<tg-emoji emoji-id="5445092669522996408">👤</tg-emoji> <b>الكاش مسجل باسم:</b> {escape(txn["name"])}\n'
            f'💰 <b>المبلغ المطلوب:</b> {escape(txn["amount"])} ج\n'
            f'📦 <b>المنتج / الشيء:</b> {escape(txn["product"])}\n'
            f'✔ <b>تم التأكيد بواسطة:</b> {user.mention_html()}\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
            '⏳ <b>سيتم إرسال إشعار الدفع حالاً من إدارة الشات المعتمدة.</b>'
        )
        try:
            await query.edit_message_text(text=confirmed_text, parse_mode=ParseMode.HTML, reply_markup=None)
        except Exception:
            pass

    elif action == "tx_cancel":
        if not (admin_status or is_target_user):
            await query.answer("هذا الإجراء مخصص لصاحب المعاملة أو المشرفين فقط.", show_alert=True)
            return

        del chat_txns[phone]
        transactions_map[c_id_str] = chat_txns
        db_save("transactions", transactions_map)
        await query.answer("تم إلغاء المعاملة ❌")

        cancelled_text = (
            f'<tg-emoji emoji-id="5251457350337849697">❌</tg-emoji> <b>تم إلغاء المعاملة وحذفها من النظام بواسطة:</b> {user.mention_html()}\n'
            f'🧾 <b>رقم المعاملة:</b> <code>{tx_id_str}</code>\n'
            f'📱 <b>الرقم:</b> <code>{phone}</code>'
        )
        try:
            await query.edit_message_text(text=cancelled_text, parse_mode=ParseMode.HTML, reply_markup=None)
        except Exception:
            pass

    elif action == "tx_received":
        if not (admin_status or is_target_user):
            await query.answer("هذا التأكيد مخصص للمستلم الفعلي للمعاملة أو المشرف فقط.", show_alert=True)
            return

        context.application.bot_data.setdefault("pending_reviews", {})[user.id] = f"{phone}:{c_id_str}"
        await query.answer("عاش! تم تأكيد وصول الفلوس ✅", show_alert=False)

        await query.message.reply_text(
            f'<tg-emoji emoji-id="5850481872244578228">🌸</tg-emoji> أهلاً بك {user.mention_html()}، ألف مبروك وصول المبلغ!\n'
            '✍️ <b>يرجى كتابة رأيك وتقييمك الآن في رسالة هنا بالشات</b> ليتم توثيقه وحفظه في كليشة المعاملة رسمياً.',
            parse_mode=ParseMode.HTML,
        )

    elif action == "tx_not_received":
        if not (admin_status or is_target_user):
            await query.answer("هذا الإجراء مخصص للمستلم الفعلي للمعاملة.", show_alert=True)
            return

        await query.answer("تم إرسال إشعار تنبيه للإدارة لمتابعة التحويل.", show_alert=True)
        await query.message.reply_text(
            f'<tg-emoji emoji-id="5890814398292826361">🚨</tg-emoji> <b>تنبيه أمني للإدارة:</b>\n'
            f'أفاد المستلم {user.mention_html()} بأنه <b>لم يستلم المبلغ حتى الآن</b> على الرقم: <code>{phone}</code>!\n'
            f'يرجى مراجعة إيصال التحويل والشبكة فوراً 🛡️',
            parse_mode=ParseMode.HTML,
        )


async def handle_pending_review_submission(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.effective_message
    user = update.effective_user
    if not message or not user:
        return False

    raw_text = (message.text or message.caption or "").strip()
    if not raw_text:
        return False

    pending_reviews: dict[int, str] = context.application.bot_data.setdefault("pending_reviews", {})
    if user.id not in pending_reviews:
        return False

    raw_p_data = pending_reviews.pop(user.id)
    p_parts = raw_p_data.split(":")
    phone = p_parts[0]
    c_id_str = p_parts[1] if len(p_parts) > 1 else str(update.effective_chat.id)

    transactions_map = context.application.bot_data.setdefault("transactions", db_load("transactions"))
    chat_txns = transactions_map.get(c_id_str, {})

    if phone not in chat_txns:
        return False

    review_comment = raw_text
    txn = chat_txns[phone]
    txn["status"] = "completed"
    txn["review"] = review_comment
    txn["reviewed_by"] = user.mention_html()
    transactions_map[c_id_str] = chat_txns
    db_save("transactions", transactions_map)

    person_mention = txn.get("mention", escape(txn["name"]))
    tx_id_str = txn.get("tx_id", "#TX")
    wallet_icon = get_wallet_emoji(txn["phone"])

    updated_caption = (
        '<tg-emoji emoji-id="5812288471837975057">💯</tg-emoji> <b>تم تحويل واستلام الأموال بنجاح 💸✔️</b>\n'
        '━━━━━━━━━━━━━━━━━━━━\n'
        f'<tg-emoji emoji-id="5886413422614026772">🧾</tg-emoji> <b>رقم المعاملة:</b> <code>{tx_id_str}</code>\n'
        f'<tg-emoji emoji-id="5445092669522996408">👤</tg-emoji> <b>المستلم:</b> {person_mention}\n'
        f'{wallet_icon}<b>رقم المحفظة:</b> <code>{txn["phone"]}</code>\n'
        f'<tg-emoji emoji-id="5445092669522996408">👤</tg-emoji> <b>الكاش باسم:</b> {escape(txn["name"])}\n'
        f'💰 <b>المبلغ المحول:</b> {escape(txn["amount"])} ج\n'
        f'📦 <b>العملية:</b> {escape(txn["product"])}\n'
        f'📅 <b>الوقت:</b> {txn.get("completed_at", "مكتملة")}\n'
        '━━━━━━━━━━━━━━━━━━━━\n'
        f'<tg-emoji emoji-id="5956391732789646927">💬</tg-emoji> <b>رأي وتقييم المستلم:</b>\n'
        f'« <i>{escape(review_comment)}</i> »\n'
        '━━━━━━━━━━━━━━━━━━━━\n'
        '⚡ <i>معاملة موثقة وآمنة 100% عبر إدارة الشات المعتمدة.</i>'
    )

    msg_id = txn.get("payout_message_id")
    chat_id = txn.get("payout_chat_id")
    if msg_id and chat_id:
        try:
            if txn.get("has_photo"):
                await context.bot.edit_message_caption(
                    chat_id=chat_id,
                    message_id=msg_id,
                    caption=updated_caption,
                    parse_mode=ParseMode.HTML,
                    reply_markup=None,
                )
            else:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=msg_id,
                    text=updated_caption,
                    parse_mode=ParseMode.HTML,
                    reply_markup=None,
                )
        except Exception:
            pass

    await message.reply_text(
        f'<tg-emoji emoji-id="5767416500365303020">❤️</tg-emoji> شكراً لك {user.mention_html()}، تم توثيق تقييمك بنجاح داخل كليشة المعاملة!',
        parse_mode=ParseMode.HTML,
    )
    return True


# -------------------------------------------------------------
# Handlers: Automatic Keywords & Verification Cards
# -------------------------------------------------------------

async def handle_auto_replies(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.effective_message
    chat = update.effective_chat
    if not message or not chat:
        return False

    raw_text = (message.text or message.caption or "").strip()
    if not raw_text:
        return False

    normalized = re.sub(r"[^\w\s]", "", raw_text).strip()

    if normalized == "المالك" and chat.type in ("group", "supergroup"):
        # Global per-chat owner lookup: identify the actual Telegram chat creator.
        try:
            admins = await context.bot.get_chat_administrators(chat.id)
            owner = next((m.user for m in admins if m.status == ChatMemberStatus.OWNER), None)
        except Exception as exc:
            logger.warning("Unable to resolve chat owner for %s: %s", chat.id, exc)
            owner = None

        if owner:
            owner_name = escape((owner.full_name or "مالك الشات").strip())
            if owner.username:
                owner_identity = f"@{escape(owner.username)}"
            else:
                owner_identity = f'<a href="tg://user?id={owner.id}">{owner_name}</a>'
            owner_text = (
                '<tg-emoji emoji-id="5891000366081775168">👑</tg-emoji> '
                '<b>صاحب المقام والقرار</b> '
                '<tg-emoji emoji-id="5891000366081775168">👑</tg-emoji>\n'
                '━━━━━━━━━━━━━━━━━━━━\n\n'
                '<tg-emoji emoji-id="5890891742063893790">💫</tg-emoji> '
                f'<b>مالك الشات هو:</b> {owner_identity}\n\n'
                '<i>صاحب الصلاحية الأعلى في هذه المجموعة.</i> '
                '<tg-emoji emoji-id="5891182846357281226">🥇</tg-emoji>'
            )
        else:
            owner_text = '⚠️ لم أتمكن من تحديد مالك الشات حالياً.'
        await message.reply_text(owner_text, parse_mode=ParseMode.HTML)
        return True

    # Shared cheeky replies for direct calls of the bot name or "رعد".
    # No emojis; choose a different reply when possible.
    if normalized in ["بوت", "يا بوت", "بوتك", "رعد", "يا رعد", "رعدد"]:
        call_replies = [
            "وش تبي؟",
            "مالك يا حبيبي؟",
            "مش فاضي لك.",
            "تفضل، معاك عمك رعد.",
            "حاضر يا سيدي، تؤمر بإيه؟",
            "عمك رعد بنفسه، عايز إيه؟",
            "نعم يا روح أمك؟",
            "خير يا نجم، وراك إيه؟",
            "هو أنا ناقصك؟",
            "عايز إيه يا ورع؟",
            "نعم، خلصنا، قول عايز إيه.",
            "يا ساتر، إنت تاني؟",
            "هو كل شوية تنادي عليّا ليه؟",
            "قول يا عم، عندي شغل.",
            "أيوه يا حبيبي، انطق.",
            "نعم يا فندم، طلباتك أوامر.",
            "عمك رعد موجود، بس مش لأي حد.",
            "يا عم ارحمني، أنا لسه صاحي.",
            "خير؟ حد قالك إني فاضي؟",
            "عايز إيه يا نجم النجوم؟",
            "سمّ، بس بسرعة.",
            "إيه يا عم، هتخليني أندم إني رديت؟",
            "أيوه يا سيدي، الدنيا مش سايعة.",
            "ناديت؟ طب اتفضل قول.",
            "عمك رعد يا ورع، اتكلم.",
            "مين مزعّل سيادتك؟",
            "يا عم قول اللي عندك بدل الرغي.",
            "أيوه، أنا هنا، مش محتاج إعلان.",
            "يا حبيبي ادخل في الموضوع.",
            "مش كل ما تزهق تنادي رعد.",
            "عايز إيه يا زعيم، خلصني.",
            "يا عم أنا بوت، مش خدمة عملاء.",
            "حاضر، بس متتعودش على الدلع.",
            "رعد معاك، والنكد عليك.",
            "اتفضل يا باشا، عندك كام ثانية.",
            "عمك رعد يا زج، خير؟",
            "يا عم سيبني في حالي شوية.",
            "أيوه يا نجم، هتقول ولا أرجع أنام؟",
            "مين نده على أسطورة الشات؟",
            "اتكلم يا حبيبي، وقتي مش ببلاش.",
        ]
        previous_reply = context.chat_data.get("last_ra3d_call_reply")
        choices = [reply for reply in call_replies if reply != previous_reply] or call_replies
        selected_reply = random.choice(choices)
        context.chat_data["last_ra3d_call_reply"] = selected_reply
        await message.reply_text(selected_reply, reply_to_message_id=message.message_id)
        return True

    if normalized in ["هلا", "هلا والله", "هلاو", "هلاا", "هلا رعد"]:
        hello_replies = [
            "هلا وغلا 🌸",
            "يا مرحبا، حيّاك الله 🤍",
            "هلا بك، وش علومك؟ 😎",
            "مرحبتين، تفضل! ✨",
            "هلا والله بالنور كله 🌟",
            "يا هلا، وش عندك؟ 👀",
            "حيّ الله من جانا 🤍",
            "هلا هلا، نورت الشات 🔥",
        ]
        await message.reply_text(random.choice(hello_replies), reply_to_message_id=message.message_id)
        return True

    salam_patterns = [
        r"^السلام عليكم",
        r"^سلام عليكم",
        r"^سلام عليكو",
        r"^سلامو عليكم",
        r"^وعليكم السلام",
    ]
    for pattern in salam_patterns:
        if re.search(pattern, normalized):
            replies = [
                'وعليكم السلام ورحمة الله وبركاته، نورت يا غالي <tg-emoji emoji-id="5850481872244578228">🌸</tg-emoji>',
                "وعليكم السلام ورحمة الله وبركاته، مرحب بيك في الشات 🤍",
                "أهلاً وسهلاً، وعليكم السلام ورحمة الله وبركاته ✨",
            ]
            await message.reply_text(random.choice(replies), parse_mode=ParseMode.HTML)
            return True

    fun_bank = {
        ("فين الادمن", "فين المشرف", "فين الاداره", "فين المشرفين"): [
            "الأدمن موجود، اطلب وساطتك مع المشرف المعتمد مباشرة 🛡️",
            "الأدمن بيشرب شاي، اكتب يوزره وهيظهرلك كليشته فوراً ☕",
            "جميع المشرفين تحت أمرك، بس اتأكد من يوزر المشرف قبل التحويل ✔",
        ],
        ("صباح الخير", "صباح النور", "صباح الورد"): [
            "صباح الفل والياسمين يا غالي ☀️",
            "صباحك رزق وبركة وتجارة ناجحة إن شاء الله ☕",
        ],
        ("مساء الخير", "مساء النور", "مساء الورد"): [
            "مساء الخير والأنوار يا محترم 🌙",
            'مساء الورد، يومك سعيد إن شاء الله <tg-emoji emoji-id="5850481872244578228">🌸</tg-emoji>',
        ],
        ("منور", "منورين", "الشات منور"): [
            "بنورك يا طيب وبنور الشباب المحترمين 🤍",
        ],
        ("شكرا", "تسلم", "الف شكر", "الله يباركلك"): [
            "العفو يا غالي في الخدمة دايماً 🤍",
            'حبيبي تسلم، ربنا يبارك فيك ويحفظك <tg-emoji emoji-id="5850481872244578228">🌸</tg-emoji>',
        ],
    }

    for triggers, responses in fun_bank.items():
        if any(trig == normalized for trig in triggers):
            await message.reply_text(random.choice(responses), parse_mode=ParseMode.HTML)
            return True

    return False


async def handle_custom_keyword_triggers(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.effective_message
    chat = update.effective_chat
    if not message or not chat:
        return False

    raw_text = (message.text or message.caption or "").strip()
    if not raw_text:
        return False

    if chat.id == PRIMARY_CHAT_ID and raw_text in ["قوانين", "القوانين"]:
        rules_text = (
            '📜 <b>قـوانـيـن وتـعـلـيـمـات الـشـات</b> 📜\n'
            '━━━━━━━━━━━━━━━━━━\n\n'
            '⚠️ <b>دي قوانين الشات الأساسية، ارجع اقرأها كويس جداً عشان ميتعملكش كتم أو حظر.</b>\n\n'
            '━━━━━━━━━━━━━━━━━━'
        )
        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("✔️ قراءة رسالة القوانين ▶️", url=RULES_LINK, style="danger")]]
        )
        await message.reply_text(rules_text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        return True

    return False


async def handle_escrow_group_card(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.effective_message
    chat = update.effective_chat
    if not message or not chat or chat.id != PRIMARY_CHAT_ID:
        return False

    text = (message.text or message.caption or "").strip()
    if text not in ["وساطة", "وساطه", "وسيط"]:
        return False

    caption = (
        '<tg-emoji emoji-id="5456371000239212004">🤝</tg-emoji> <b>جروب الوساطة والضمان المعتمد</b> <tg-emoji emoji-id="5456371000239212004">🤝</tg-emoji>\n'
        '━━━━━━━━━━━━━━━━━━━━\n\n'
        'الوساطة والتعامل المالي يتم داخل الروم المخصص لضمان حق الطرفين بأمان تام <tg-emoji emoji-id="5891235511246264255">✔️</tg-emoji>\n\n'
        '🔹 <b>شروط ونسب الوساطة:</b>\n'
        '• المعاملات <b>أعلى من 500 جنيه:</b> نسبة 10%\n'
        '• المعاملات <b>أقل من 500 جنيه:</b> بدون أي نسبة (مجاناً 100%)\n\n'
        '━━━━━━━━━━━━━━━━━━━━\n'
        '<tg-emoji emoji-id="5253768790067406491">‼️</tg-emoji> <b>غير مسئولين عن أي شخص يدعي أنه وسيط في الخاص نهائياً.</b>'
    )

    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("دخول جروب الوساطة ➜", url=ESCROW_LINK, style="danger")]]
    )

    image_path = BASE_DIR / ESCROW_IMAGE
    if image_path.is_file():
        try:
            with open(image_path, "rb") as media:
                await message.reply_photo(
                    photo=media,
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                    reply_markup=keyboard,
                )
            return True
        except Exception:
            pass

    await message.reply_text(caption, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    return True


async def send_verification_card(update: Update, context: ContextTypes.DEFAULT_TYPE, card_key: str) -> None:
    message = update.message
    if not message:
        return
    card = CARD_DETAILS[card_key]
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton(f"{card['button']} ➜", url=card.get("button_link", card["link"]), style="danger")]]
    )
    media_path = _candidate_image(card_key)
    if media_path is None:
        return

    try:
        with open(media_path, "rb") as media:
            await message.reply_photo(photo=media, caption=card["caption"], parse_mode=ParseMode.HTML, reply_markup=keyboard)
    except Exception:
        pass


async def handle_id_card_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.effective_message
    if not message:
        return False

    raw = (message.text or message.caption or "").strip().lower()
    if raw not in ["ايدي", "آيدي", "id"]:
        return False

    if message.reply_to_message and message.reply_to_message.from_user:
        target_user = message.reply_to_message.from_user
    else:
        target_user = message.from_user

    if not target_user:
        return False

    t_id = target_user.id
    t_uname = f"@{target_user.username}" if target_user.username else "لا يوجد"
    t_name_link = f'<a href="tg://user?id={t_id}">{escape(target_user.full_name)}</a>'

    id_caption = (
        '<tg-emoji emoji-id="5890795783904565270">⚡</tg-emoji> <b>BRQ | برق</b> <tg-emoji emoji-id="5890795783904565270">⚡</tg-emoji>\n'
        '━━━━━━━━━━━━━━━━━━━━\n\n'
        f'<tg-emoji emoji-id="5445092669522996408">👤</tg-emoji> <b>الحساب:</b> {t_name_link}\n'
        f'🔗 <b>اليوزر:</b> {t_uname}\n'
        f'<tg-emoji emoji-id="5935966809655088705">🪪</tg-emoji> <b>الأيدي:</b> <code>{t_id}</code>'
    )

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    f"📋 {t_id}",
                    copy_text={"text": str(t_id)},
                )
            ]
        ]
    )

    await message.reply_text(
        id_caption,
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
    )
    return True


async def handle_rank_commands(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.effective_message
    chat = update.effective_chat
    if not message or not chat:
        return False

    raw_text = (message.text or message.caption or "").strip()
    if not raw_text:
        return False

    lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]
    if not lines:
        return False

    raw_tokens = raw_text.split()

    if lines and lines[0] == "رتبتي" and len(lines) == 1:
        user = message.from_user
        if not user:
            return True

        uid_str = str(user.id)
        grp_rank = await get_accurate_chat_status(chat.id, user.id, context)

        bot_admins_map = context.application.bot_data.get("bot_admins", {})
        chat_admins = bot_admins_map.get(str(chat.id), {})
        distinguished_map = context.application.bot_data.get("distinguished_users", {})
        chat_dist = distinguished_map.get(str(chat.id), {})

        if await is_primary_owner(user.id, context):
            bot_rank = '<tg-emoji emoji-id="5875344888165308204">👑</tg-emoji> مطور ومالك أساسي'
        elif uid_str in chat_admins:
            role_label = chat_admins[uid_str].get("role", "أدمن")
            icon = "" if role_label == "مدير" else '<tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji>'
            bot_rank = f'{icon} {role_label} بالبوت'
        elif uid_str in chat_dist:
            bot_rank = '<tg-emoji emoji-id="5776113577211468831">⭐</tg-emoji> عضو مميز'
        elif "مشرف" in grp_rank or "مالك" in grp_rank:
            bot_rank = '<tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji> مشرف معتمد'
        else:
            merchants_map = context.application.bot_data.get("merchants", {})
            chat_merchants = merchants_map.get(str(chat.id), {})
            matched = None
            for mv in chat_merchants.values():
                m_uname = (mv.get("username") or "").lower()
                if str(mv.get("user_id")) == uid_str or (user.username and m_uname == user.username.lower()):
                    matched = mv
                    break
            if matched:
                bot_rank = f'<tg-emoji emoji-id="5872935716454930968">🤑</tg-emoji> تاجر معتمد في مبلغ {matched.get("limit", "500")} جنيه'
            else:
                bot_rank = "عضو عادي"

        user_link = f'<a href="tg://user?id={user.id}">{escape(user.full_name)}</a>'
        card = (
            '<tg-emoji emoji-id="5445092669522996408">👤</tg-emoji> <b>اسـتـعـلام عـن الـرتـبـة</b> <tg-emoji emoji-id="5445092669522996408">👤</tg-emoji>\n'
            '━━━━━━━━━━━━━━━━━━━━\n\n'
            f'<tg-emoji emoji-id="5445092669522996408">👤</tg-emoji> <b>حسابك:</b> {user_link}\n'
            f'<tg-emoji emoji-id="5935966809655088705">🪪</tg-emoji> <b>الآيدي:</b> <code>{user.id}</code>\n\n'
            f'<tg-emoji emoji-id="5258486128742244085">👥</tg-emoji> <b>رتبتك في المجموعة:</b> <b>{grp_rank}</b>\n'
            f'<tg-emoji emoji-id="5983534401386976295">🤖</tg-emoji> <b>رتبتك في البوت:</b> <b>{bot_rank}</b>'
        )
        await message.reply_text(card, parse_mode=ParseMode.HTML)
        return True

    is_his_rank = False
    for line in lines:
        w_list = line.split()
        if w_list and w_list[0] == "رتبته":
            is_his_rank = True
            break

    if is_his_rank:
        clean_tokens = [t for t in raw_tokens if t != "رتبته"]
        target_id, target_username, target_name, _, _ = await resolve_target_and_details(update, context, clean_tokens)

        if not target_id:
            await message.reply_text("⚠️ يرجى تحديد العضو بالرد أو باليوزر أو بالآيدي لمعرفة رتبته.")
            return True

        uid_str = str(target_id)
        grp_rank = await get_accurate_chat_status(chat.id, target_id, context)

        bot_admins_map = context.application.bot_data.get("bot_admins", {})
        chat_admins = bot_admins_map.get(str(chat.id), {})
        distinguished_map = context.application.bot_data.get("distinguished_users", {})
        chat_dist = distinguished_map.get(str(chat.id), {})

        if await is_primary_owner(target_id, context):
            bot_rank = '<tg-emoji emoji-id="5875344888165308204">👑</tg-emoji> مطور ومالك أساسي'
        elif uid_str in chat_admins:
            role_label = chat_admins[uid_str].get("role", "أدمن")
            icon = "" if role_label == "مدير" else '<tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji>'
            bot_rank = f'{icon} {role_label} بالبوت'
        elif uid_str in chat_dist:
            bot_rank = '<tg-emoji emoji-id="5776113577211468831">⭐</tg-emoji> عضو مميز'
        elif "مشرف" in grp_rank or "مالك" in grp_rank:
            bot_rank = '<tg-emoji emoji-id="5891225499677496831">🛡️</tg-emoji> مشرف معتمد'
        else:
            merchants_map = context.application.bot_data.get("merchants", {})
            chat_merchants = merchants_map.get(str(chat.id), {})
            matched = None
            for mv in chat_merchants.values():
                m_uname = (mv.get("username") or "").lower()
                if str(mv.get("user_id")) == uid_str or (target_username and m_uname == target_username.lower()):
                    matched = mv
                    break
            if matched:
                bot_rank = f'<tg-emoji emoji-id="5872935716454930968">🤑</tg-emoji> تاجر معتمد في مبلغ {matched.get("limit", "500")} جنيه'
            else:
                bot_rank = "عضو عادي"

        u_disp = f"@{target_username}" if target_username else "بدون يوزر"
        card = (
            '<tg-emoji emoji-id="5445092669522996408">👤</tg-emoji> <b>بـيـانـات رتـبـة الـعـضـو</b> <tg-emoji emoji-id="5445092669522996408">👤</tg-emoji>\n'
            '━━━━━━━━━━━━━━━━━━━━\n\n'
            f'<tg-emoji emoji-id="5445092669522996408">👤</tg-emoji> <b>العضو:</b> {target_name}\n'
            f'<tg-emoji emoji-id="5935966809655088705">🪪</tg-emoji> <b>الآيدي:</b> <code>{target_id}</code>\n'
            f'🔹 <b>اليوزر:</b> {u_disp}\n\n'
            f'<tg-emoji emoji-id="5258486128742244085">👥</tg-emoji> <b>رتبته في المجموعة:</b> <b>{grp_rank}</b>\n'
            f'<tg-emoji emoji-id="5983534401386976295">🤖</tg-emoji> <b>رتبته في البوت:</b> <b>{bot_rank}</b>'
        )
        await message.reply_text(card, parse_mode=ParseMode.HTML)
        return True

    return False


# -------------------------------------------------------------
# Handlers: Roulette System (عربي + إيموجي مميز + أمر تم)
# -------------------------------------------------------------

async def handle_roulette_messages(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.effective_message
    chat = update.effective_chat
    if not message or not chat or chat.type == "private":
        return False

    raw_text = (message.text or message.caption or "").strip()
    if not raw_text:
        return False

    # 1. أمر إنهاء السحب (تم / انهاء السحب)
    if raw_text in ["تم", "انهاء السحب", "إنهاء السحب"]:
        state = context.application.bot_data["roulette_state"]
        chat_key = str(chat.id)
        roulette = state.get(chat_key)

        if not roulette or not roulette.get("active"):
            return False

        if not await is_management_admin(update, context):
            return True

        participants = list(roulette.get("participants", {}).values())
        winner_count = min(int(roulette.get("winner_count", 1)), len(participants))
        winners = random.sample(participants, winner_count) if winner_count else []
        roulette["active"] = False
        db_save("roulette_state", state)

        if winners:
            winner_lines = [
                f'• <tg-emoji emoji-id="5891182846357281226">🥇</tg-emoji> <b>الفائز:</b> {escape(w["name"])} (@{escape(w["username"]) if w.get("username") else "بدون يوزر"}) — <code>{w["id"]}</code>'
                for w in winners
            ]
            text = (
                f'<tg-emoji emoji-id="6001546218621114827">🎁</tg-emoji> <b>انتهى السحب بنجاح! مبروك للفائزين</b> <tg-emoji emoji-id="6001546218621114827">🎁</tg-emoji>\n'
                f'━━━━━━━━━━━━━━━━━━━━\n\n'
                f'<tg-emoji emoji-id="5776113577211468831">⭐</tg-emoji> <b>الجائزة:</b> <b>{escape(roulette["prize"])}</b>\n\n'
                f'🎉 <b>قائمة الفائزين بالقرعة:</b>\n'
                + "\n".join(winner_lines) + "\n\n"
                f'━━━━━━━━━━━━━━━━━━━━\n'
                f'⚡ <i>تواصل مع مشرفي المجموعة لاستلام جائزتك فوراً.</i>'
            )
        else:
            text = (
                f'<tg-emoji emoji-id="5251457350337849697">❌</tg-emoji> <b>انتهى السحب!</b>\n'
                f'لم يسجل أي مشارك في السحب.'
            )

        await message.reply_text(text, parse_mode=ParseMode.HTML)

        if roulette.get("message_id"):
            try:
                await context.bot.edit_message_reply_markup(chat_id=chat.id, message_id=roulette["message_id"], reply_markup=None)
            except Exception:
                pass
        return True

    # 2. أمر بدء السحب (روليت [العدد] [الجائزة])
    if raw_text.startswith("روليت") or raw_text.startswith("/roulette"):
        if not await is_management_admin(update, context):
            return True

        state = context.application.bot_data["roulette_state"]
        chat_key = str(chat.id)
        if state.get(chat_key, {}).get("active"):
            await message.reply_text("⚠️ يوجد سحب نشط بالفعل في هذه المجموعة، اكتب (تم) لإنهائه أولاً.")
            return True

        cmd_prefix = "/roulette" if raw_text.startswith("/roulette") else "روليت"
        content = raw_text[len(cmd_prefix):].strip()

        winner_count = 1
        prize = "سحب مميز"

        match_count = re.search(r"^[\[\(]?(\d+)[\]\)]?\b", content)
        if match_count:
            winner_count = max(1, int(match_count.group(1)))
            prize = content[match_count.end():].strip() or prize
        elif content:
            prize = content

        state[chat_key] = {
            "active": True,
            "prize": prize,
            "winner_count": winner_count,
            "participants": {},
            "message_id": None,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        roulette_card = (
            f'<tg-emoji emoji-id="6001546218621114827">🎁</tg-emoji> <b>سـحـب رولـيـت جـديـد</b> <tg-emoji emoji-id="6001546218621114827">🎁</tg-emoji>\n'
            f'━━━━━━━━━━━━━━━━━━━━\n\n'
            f'<tg-emoji emoji-id="5776113577211468831">⭐</tg-emoji> <b>الجائزة:</b> <b>{escape(prize)}</b>\n'
            f'<tg-emoji emoji-id="5891182846357281226">🥇</tg-emoji> <b>عدد الفائزين:</b> <b>{winner_count}</b>\n\n'
            f'👇 <i>اضغط على الزر الأخضر بالأسفل للدخول في السحب فوراً:</i>\n'
            f'━━━━━━━━━━━━━━━━━━━━\n'
            f'⏳ <b>لإنهاء السحب واختيار الفائزين:</b> اكتب « <code>تم</code> » بالشات.'
        )

        sent = await message.reply_text(
            roulette_card,
            parse_mode=ParseMode.HTML,
            reply_markup=_roulette_keyboard(chat.id, 0),
        )
        state[chat_key]["message_id"] = sent.message_id
        db_save("roulette_state", state)
        return True

    return False


async def roulette_join(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data or not query.message:
        return
    try:
        chat_id = int(query.data.split(":", 1)[1])
    except Exception:
        return

    state = context.application.bot_data["roulette_state"]
    chat_key = str(chat_id)
    roulette = state.get(chat_key)
    if not roulette or not roulette.get("active"):
        await query.answer("هذا السحب انتهى.", show_alert=True)
        return

    user = query.from_user
    if str(user.id) in roulette.setdefault("participants", {}):
        await query.answer("أنت مسجل بالفعل في هذا السحب! 🎰", show_alert=True)
        return

    roulette["participants"][str(user.id)] = {
        "id": user.id,
        "username": user.username or "",
        "name": user.full_name,
        "joined_at": datetime.now(timezone.utc).isoformat(),
    }
    db_save("roulette_state", state)
    await query.answer("تم تسجيلك في السحب بنجاح! فالك الفوز 🎰✨")
    try:
        await query.edit_message_reply_markup(reply_markup=_roulette_keyboard(chat_id, len(roulette["participants"])))
    except Exception:
        pass


async def notify_mentioned_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """DM users explicitly mentioned in a group message, with a direct message link."""
    message = update.effective_message
    chat = update.effective_chat
    sender = update.effective_user
    if not message or not chat or not sender or chat.type == "private" or sender.is_bot:
        return
    entities = list(message.entities or []) + list(message.caption_entities or [])
    targets = {}
    for ent in entities:
        if ent.type == MessageEntityType.TEXT_MENTION and ent.user:
            targets[ent.user.id] = ent.user
        elif ent.type == MessageEntityType.MENTION:
            source = message.text or message.caption or ""
            token = source[ent.offset:ent.offset + ent.length].lstrip("@").lower()
            user_map = context.application.bot_data.get("user_map", db_load("user_map"))
            uid = user_map.get("@" + token)
            if uid and str(uid).isdigit():
                try:
                    udata = await context.bot.get_chat_member(chat_id=chat.id, user_id=int(uid))
                    targets[udata.user.id] = udata.user
                except TelegramError:
                    pass
    if not targets:
        return
    if chat.username:
        message_url = f"https://t.me/{chat.username}/{message.message_id}"
    elif str(chat.id).startswith("-100"):
        message_url = f"https://t.me/c/{str(chat.id)[4:]}/{message.message_id}"
    else:
        return
    group_name = escape(chat.title or "المجموعة")
    sender_name = escape(sender.full_name)
    sender_ref = f'<a href="tg://user?id={sender.id}">{sender_name}</a>'
    text = f"<b>تمت الإشارة إليك</b>\n• من: {sender_ref}\n• في مجموعة: {group_name}"
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("👉 اذهب للرسالة", url=message_url, style="success")]])
    for uid, target in targets.items():
        if uid == sender.id:
            continue
        try:
            await context.bot.send_message(chat_id=uid, text=text, parse_mode=ParseMode.HTML, reply_markup=keyboard, disable_web_page_preview=True)
        except TelegramError:
            # The user may not have started the bot or may have blocked it.
            continue



# -------------------------------------------------------------
# Per-chat content locks + three-strike warning/mute
# -------------------------------------------------------------
LOCK_COMMANDS = {
    "قفل الروابط": ("links", True), "فتح الروابط": ("links", False),
    "قفل التوجيه": ("forward", True), "فتح التوجيه": ("forward", False),
    "قفل الملصقات": ("stickers", True), "فتح الملصقات": ("stickers", False),
    "قفل الصوت": ("audio", True), "فتح الصوت": ("audio", False),
    "قفل الفويس": ("voice", True), "فتح الفويس": ("voice", False),
    "قفل الصور": ("photos", True), "فتح الصور": ("photos", False),
    "قفل السب": ("profanity", True), "فتح السب": ("profanity", False),
    "قفل كل حاجة": ("all", True), "فتح كل حاجة": ("all", False),
}
LOCK_LABELS = {"links":"الروابط", "forward":"التوجيه", "stickers":"الملصقات", "audio":"الصوت", "voice":"الفويس", "photos":"الصور", "profanity":"السب والشتائم", "all":"كل أنواع الوسائط المحظورة"}

def _content_locks() -> dict[str, Any]:
    return db_load("content_locks")

def _warning_store() -> dict[str, Any]:
    return db_load("content_lock_warnings")

async def handle_content_lock_commands(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message, chat, user = update.effective_message, update.effective_chat, update.effective_user
    if not message or not chat or not user or chat.type not in ("group", "supergroup"):
        return False
    text = (message.text or "").strip()
    if text not in LOCK_COMMANDS:
        return False
    if not await is_management_admin(update, context):
        return True  # silently ignore unauthorized use
    key, enabled = LOCK_COMMANDS[text]
    data = _content_locks()
    cdata = data.setdefault(str(chat.id), {})
    if key == "all":
        for item in ("links", "forward", "stickers", "audio", "voice", "photos"):
            cdata[item] = enabled
    elif key == "profanity":
        db_save("profanity_filter_enabled", {"enabled": enabled, "by": user.id, "at": datetime.now(timezone.utc).isoformat()})
    else:
        cdata[key] = enabled
    db_save("content_locks", data)
    state = "تم قفل" if enabled else "تم فتح"
    admin_username = f" (@{escape(user.username)})" if user.username else ""
    await message.reply_text(f"{state} {LOCK_LABELS[key]} بواسطة {user.mention_html()}{admin_username}.", parse_mode=ParseMode.HTML)
    return True

# قائمة أولية قابلة للتعديل من لوحة المالك. يمكن إضافة الكلمات الخاصة من الخاص أو لكل جروب.
DEFAULT_PROFANITY_WORDS = [
    "كس", "كسمك", "كس امك", "كسم امك", "كسم ابوك", "شرموط", "شرموطة", "متناك", "متناكة",
    "منيوك", "منيوكة", "خول", "خولات", "عرص", "معرص", "قحبة", "قحاب", "زب", "زبي", "طيز",
    "نيك", "انيك", "ينك", "يلعن", "يلعن دينك", "يلعن ابو", "ابن الكلب", "بنت الكلب",
    "كلب", "كلبة", "حمار", "يا حيوان", "قذر", "وسخ", "حقير", "يا غبي", "يا اهبل",
    "يا زفت", "يا عرص", "يا خول", "يا شرموط", "يا منيوك", "يا ابن الحرام",
]

def _normalize_arabic(value: str, compact: bool = False) -> str:
    value = (value or "").lower()
    value = re.sub(r"[\u064b-\u065f\u0670\u0640]", "", value)
    value = value.translate(str.maketrans({"أ":"ا", "إ":"ا", "آ":"ا", "ى":"ي", "ؤ":"و", "ئ":"ي", "ة":"ه"}))
    if compact:
        value = re.sub(r"[^\w]", "", value, flags=re.UNICODE)
    else:
        value = re.sub(r"[^\w\s]", " ", value, flags=re.UNICODE)
        value = re.sub(r"\s+", " ", value).strip()
    return value

def _global_profanity_words() -> list[str]:
    data = db_load("profanity_words")
    words = data.get("words", []) if isinstance(data, dict) else []
    merged = list(DEFAULT_PROFANITY_WORDS)
    for word in words:
        if isinstance(word, str) and word.strip() and word.strip() not in merged:
            merged.append(word.strip())
    return merged

_WORDSET_CACHE: dict[tuple, list] = {}


def _compiled_words(words) -> list:
    key = tuple(words)
    compiled = _WORDSET_CACHE.get(key)
    if compiled is None:
        compiled = []
        for w in words:
            norm = _normalize_arabic(w)
            if not norm:
                continue
            compiled.append((re.compile(r"(?<!\w)" + re.escape(norm) + r"(?!\w)"), _normalize_arabic(w, compact=True)))
        if len(_WORDSET_CACHE) > 64:
            _WORDSET_CACHE.clear()
        _WORDSET_CACHE[key] = compiled
    return compiled


def _words_hit(compiled, text_norm: str, compact_text: str, min_compact: int) -> bool:
    for pattern, compact in compiled:
        if pattern.search(text_norm) or (len(compact) >= min_compact and compact in compact_text):
            return True
    return False


async def _enforce_manual_chat_lock(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message, chat, user = update.effective_message, update.effective_chat, update.effective_user
    if not message or not chat or not user or chat.type not in ("group", "supergroup") or user.is_bot:
        return False
    locked = context.application.bot_data.setdefault("manual_chat_locks", db_load("manual_chat_locks"))
    lock_state = locked.get(str(chat.id))
    if not lock_state:
        return False
    # لا نغيّر صلاحيات الإرسال في إعدادات تيليجرام؛ نسمح للمشرفين (في البوت أو تيليجرام)
    # ونحذف رسالة أي عضو آخر بصمت فور وصولها.
    if await is_management_admin(update, context):
        return False
    try:
        await message.delete()
    except TelegramError as err:
        logger.warning("Could not delete message during manual chat lock in %s: %s", chat.id, err)
    return True

async def handle_profanity_and_word_commands(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message, chat, user = update.effective_message, update.effective_chat, update.effective_user
    if not message or not chat or not user:
        return False
    raw = (message.text or message.caption or "").strip()
    if chat.type in ("group", "supergroup"):
        match = re.match(r"^(منع الكلمة|سماح الكلمة)\s+(.+)$", raw, re.S)
        if match:
            if not await is_management_admin(update, context):
                return True
            command, word = match.group(1), match.group(2).strip()
            if len(word) > 80 or not word:
                await message.reply_text("الكلمة أو العبارة غير صالحة.")
                return True
            data = db_load("chat_forbidden_words")
            chat_words = data.setdefault(str(chat.id), [])
            normalized = _normalize_arabic(word)
            if command == "منع الكلمة":
                if not any(_normalize_arabic(w) == normalized for w in chat_words):
                    chat_words.append(word)
                await message.reply_text(f"تم منع الكلمة «{escape(word)}» في هذه المجموعة فقط.", parse_mode=ParseMode.HTML)
            else:
                data[str(chat.id)] = [w for w in chat_words if _normalize_arabic(w) != normalized]
                await message.reply_text(f"تم السماح بالكلمة «{escape(word)}» في هذه المجموعة.", parse_mode=ParseMode.HTML)
            db_save("chat_forbidden_words", data)
            return True

        if raw in ("قفل السب", "فتح السب"):
            return False  # handled by handle_content_lock_commands

        profanity_enabled = db_load("profanity_filter_enabled").get("enabled", False)
        per_chat_words = db_load("chat_forbidden_words").get(str(chat.id), [])
        if not raw or not (profanity_enabled or per_chat_words):
            return False  # early exit: nothing to filter, no normalisation, no permission lookups
        text_norm = _normalize_arabic(raw)
        compact_text = _normalize_arabic(raw, compact=True)
        if profanity_enabled:
            if _words_hit(_compiled_words(_global_profanity_words()), text_norm, compact_text, 4):
                if not await is_management_admin(update, context):
                    try:
                        await message.delete()
                    except TelegramError:
                        logger.warning("Could not delete profanity message in %s", chat.id)
                    return True
        if per_chat_words:
            if _words_hit(_compiled_words(per_chat_words), text_norm, compact_text, 3):
                if not await is_management_admin(update, context):
                    try:
                        await message.delete()
                    except TelegramError:
                        logger.warning("Could not delete custom forbidden word in %s", chat.id)
                    return True
        return False

    # لوحة التحكم تظهر لصاحب البوت فقط في الخاص.
    if chat.type == "private" and raw == "لوحة تحكم الأدمن":
        if not await is_bot_owner(user.id, context):
            await message.reply_text("هذه اللوحة متاحة لمالك البوت فقط.")
            return True
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ إضافة كلمات (دفعة واحدة)", callback_data="profanity:add")],
            [InlineKeyboardButton("📋 عرض قائمة الكلمات", callback_data="profanity:list")],
            [InlineKeyboardButton("🔎 بحث عن كلمة", callback_data="profanity:search")],
            [make_button("⚙️ أوامر البوت", callback_data="botcmds_open", style="primary")],
        ])
        await message.reply_text("👑 لوحة تحكم مالك البوت\\nإدارة الكلمات المحظورة العامة لقفل السب.\\n\\nتقدري تضيفي كلمات كتير مرة واحدة، كل كلمة أو عبارة في سطر مستقل.", reply_markup=keyboard)
        return True
    if chat.type == "private" and await is_bot_owner(user.id, context):
        pending = context.application.bot_data.setdefault("pending_profanity_word", set())
        pending_search = context.application.bot_data.setdefault("pending_profanity_search", set())
        if user.id in pending_search and raw:
            pending_search.discard(user.id)
            words = _global_profanity_words()
            term = _normalize_arabic(raw)
            matches = [w for w in words if term in _normalize_arabic(w)] if term else []
            result = "\\n".join(f"• {escape(w)}" for w in matches) if matches else "ملقتش كلمة مطابقة."
            await message.reply_text("🔎 نتائج البحث:\\n" + result, parse_mode=ParseMode.HTML)
            return True
        # أوامر الحذف تعمل مباشرة من الخاص، من غير الحاجة لفتح وضع الإضافة.
        if raw.startswith("حذف "):
            target = raw[4:].strip()
            if not target:
                await message.reply_text("اكتبي بالشكل ده: حذف الكلمة\\nمثال: حذف يا غبي")
                return True
            data = db_load("profanity_words")
            existing = data.get("words", []) if isinstance(data, dict) else []
            before = len(existing)
            data["words"] = [w for w in existing if _normalize_arabic(w) != _normalize_arabic(target)]
            db_save("profanity_words", data)
            pending.discard(user.id)
            await message.reply_text(
                f"تم حذف «{escape(target)}» من الكلمات المضافة إن كانت موجودة.\\nملاحظة: الكلمات الافتراضية المدمجة في البوت لا تُحذف من هذه اللوحة.",
                parse_mode=ParseMode.HTML,
            )
            return True
        if user.id in pending and raw:
            if raw.strip().lower() in ("إلغاء", "الغاء", "إلغاء الإضافة"):
                pending.discard(user.id)
                await message.reply_text("تم إلغاء الإضافة.")
                return True
            data = db_load("profanity_words")
            words = data.setdefault("words", [])
            added, duplicates, invalid = [], [], []
            existing_norms = {_normalize_arabic(w) for w in _global_profanity_words()}
            for line in raw.splitlines():
                word = line.strip().lstrip("•- ").strip()
                if not word:
                    continue
                if len(word) > 80:
                    invalid.append(word[:80])
                    continue
                normalized_word = _normalize_arabic(word)
                if normalized_word in existing_norms:
                    duplicates.append(word)
                    continue
                words.append(word)
                added.append(word)
                existing_norms.add(normalized_word)
            db_save("profanity_words", data)
            pending.discard(user.id)
            summary = [f"✅ تمت إضافة {len(added)} كلمة/عبارة."]
            if added:
                summary.append("المضاف:\\n" + "\\n".join(f"• {escape(w)}" for w in added[:50]))
            if duplicates:
                summary.append("⚠️ موجودة بالفعل (تم تخطيها):\\n" + "\\n".join(f"• {escape(w)}" for w in duplicates[:30]))
            if invalid:
                summary.append(f"⚠️ تم تخطي {len(invalid)} سطرًا لأن طوله أكبر من 80 حرفًا.")
            summary.append("للحذف اكتبي: حذف الكلمة\\nوللبحث استخدمي زر «بحث عن كلمة» في اللوحة.")
            await message.reply_text("\\n\\n".join(summary), parse_mode=ParseMode.HTML)
            return True
    return False


async def handle_profanity_panel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    if not await is_bot_owner(query.from_user.id, context):
        await query.answer("لوحة المالك فقط", show_alert=True)
        return
    if query.data == "profanity:open":
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ إضافة كلمات (دفعة واحدة)", callback_data="profanity:add")],
            [InlineKeyboardButton("📋 عرض قائمة الكلمات", callback_data="profanity:list")],
            [InlineKeyboardButton("🔎 بحث عن كلمة", callback_data="profanity:search")],
            [make_button("⚙️ أوامر البوت", callback_data="botcmds_open", style="primary")],
        ])
        await query.answer()
        await query.message.reply_text("👑 لوحة تحكم مالك البوت\\nاختاري العملية المطلوبة.", reply_markup=keyboard)
    elif query.data == "profanity:add":
        context.application.bot_data.setdefault("pending_profanity_word", set()).add(query.from_user.id)
        await query.answer()
        await query.message.reply_text(
            "ابعتي الكلمات أو العبارات في رسالة واحدة، كل كلمة/عبارة في سطر مستقل.\\n"
            "مثال:\\nكلمة أولى\\nكلمة ثانية\\nعبارة من كلمتين\\n\\n"
            "هتظهرلك نتيجة بعدد الكلمات اللي اتضافت والمكررة. للإلغاء اكتبي: إلغاء"
        )
    elif query.data == "profanity:search":
        context.application.bot_data.setdefault("pending_profanity_search", set()).add(query.from_user.id)
        await query.answer()
        await query.message.reply_text("ابعتي كلمة أو جزءًا منها للبحث في قائمة السب العامة.")
    elif query.data == "profanity:list":
        words = _global_profanity_words()
        display = "\\n".join(f"{i}. {escape(w)}" for i, w in enumerate(words, 1)) or "لا توجد كلمات."
        await query.answer()
        await query.message.reply_text(
            "📋 قائمة الكلمات العامة الحالية (" + str(len(words)) + "):\\n" + display +
            "\\n\\n🗑️ لحذف كلمة مضافة اكتبي في الخاص مباشرة:\\nحذف الكلمة\\nمثال: حذف يا غبي",
            parse_mode=ParseMode.HTML,
        )

async def _warn_for_locked_content(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message, chat, user = update.effective_message, update.effective_chat, update.effective_user
    if not message or not chat or not user or chat.type not in ("group", "supergroup"):
        return False
    if user.is_bot:
        return False
    settings = _content_locks().get(str(chat.id), {})
    if not any(settings.get(k) for k in ("links", "forward", "stickers", "audio", "voice", "photos")):
        return False
    text = message.text or message.caption or ""
    entities = list(message.entities or []) + list(message.caption_entities or [])
    has_link = any(getattr(e, "type", None) in (MessageEntityType.URL, MessageEntityType.TEXT_LINK) for e in entities) or bool(re.search(r"(?:https?://|www\.|t\.me/|telegram\.me/)[^\s]+", text, re.I))
    forwarded = bool(getattr(message, "forward_origin", None) or getattr(message, "forward_date", None) or getattr(message, "forward_from", None) or getattr(message, "forward_from_chat", None))
    kind = None
    if settings.get("links") and has_link: kind = "links"
    elif settings.get("forward") and forwarded: kind = "forward"
    elif settings.get("stickers") and message.sticker: kind = "stickers"
    elif settings.get("audio") and message.audio: kind = "audio"
    elif settings.get("voice") and (message.voice or message.video_note): kind = "voice"
    elif settings.get("photos") and message.photo: kind = "photos"
    if not kind:
        return False
    if await is_management_admin(update, context):
        return False
    try:
        await message.delete()
    except TelegramError:
        logger.warning("Could not delete restricted content in chat %s", chat.id)
        return True
    warnings = _warning_store()
    chat_warnings = warnings.setdefault(str(chat.id), {})
    count = int(chat_warnings.get(str(user.id), 0)) + 1
    chat_warnings[str(user.id)] = count
    db_save("content_lock_warnings", warnings)
    if count >= 3:
        try:
            await context.bot.restrict_chat_member(chat.id, user.id, permissions=ChatPermissions(can_send_messages=False), until_date=datetime.now(timezone.utc) + timedelta(hours=1))
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("إلغاء الكتم", callback_data=f"lockunmute:{chat.id}:{user.id}", style="success")]])
            await context.bot.send_message(chat.id, f"تم كتم {user.mention_html()} لمدة ساعة بعد 3 تحذيرات بسبب مخالفة قفل {LOCK_LABELS[kind]}.", parse_mode=ParseMode.HTML, reply_markup=keyboard)
            chat_warnings[str(user.id)] = 0
            db_save("content_lock_warnings", warnings)
        except TelegramError:
            await context.bot.send_message(chat.id, f"{user.mention_html()} — تحذير 3 من 3: ممنوع إرسال {LOCK_LABELS[kind]}، وتعذر تنفيذ الكتم. تأكدي أن البوت مشرف ولديه صلاحية تقييد الأعضاء.", parse_mode=ParseMode.HTML)
    else:
        await context.bot.send_message(chat.id, f"{user.mention_html()} — ممنوع إرسال {LOCK_LABELS[kind]}، تحذير {count} من 3.", parse_mode=ParseMode.HTML)
    return True

async def handle_lock_unmute_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    try:
        _, chat_raw, user_raw = query.data.split(":", 2)
        chat_id, target_id = int(chat_raw), int(user_raw)
    except (ValueError, TypeError):
        await query.answer("طلب غير صالح", show_alert=True); return
    actor = query.from_user
    try:
        member = await context.bot.get_chat_member(chat_id, actor.id)
        if member.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER) and actor.id not in context.application.bot_data.get("admin_user_ids", set()):
            await query.answer("فك الكتم للمشرفين فقط", show_alert=True); return
        await context.bot.restrict_chat_member(chat_id, target_id, permissions=ChatPermissions(can_send_messages=True, can_send_audios=True, can_send_documents=True, can_send_photos=True, can_send_videos=True, can_send_video_notes=True, can_send_voice_notes=True, can_send_polls=True, can_add_web_page_previews=True, can_send_other_messages=True))
        warnings = _warning_store(); warnings.setdefault(str(chat_id), {})[str(target_id)] = 0; db_save("content_lock_warnings", warnings)
        await query.answer("تم إلغاء الكتم")
        try: await query.edit_message_reply_markup(reply_markup=None)
        except TelegramError: pass
    except TelegramError:
        await query.answer("تعذر فك الكتم؛ تأكدي من صلاحيات البوت", show_alert=True)


# -------------------------------------------------------------
# Owner panel: editable punishment templates + custom command aliases
# (كليشات العقوبات القابلة للتعديل + أضف أمر / حذف أمر + المدد)
# -------------------------------------------------------------

OWNER_ID = PRIMARY_OWNER_ID  # آيدي مالك البوت المعتمد لإظهار لوحة "أوامر البوت"

_RLM = "‏"
TPL_DEFAULTS: dict[str, str] = {
    "cmd_ban": _RLM + "「 {name} 」\n⇜ حظرته\n⇜ المدة: {time}\n⇜ حتى: {until}\n༄",
    "cmd_mute": _RLM + "「 {name} 」\n⇜ كتمته\n⇜ المدة: {time}\n⇜ حتى: {until}\n༄",
    "cmd_kick": _RLM + "「 {name} 」\n⇜ طردته\n⇜ المدة: {time}\n⇜ حتى: {until}\n༄",
    "cmd_tayeed": _RLM + "「 {name} 」\n⇜ قيّدته\n⇜ المدة: {time}\n⇜ حتى: {until}\n༄",
    "cmd_unban": _RLM + "「 {name} 」\n⇜ ألغيت الحظر عنه\n༄",
    "cmd_unmute": _RLM + "「 {name} 」\n⇜ ألغيت الكتم عنه\n༄",
    "cmd_unrestrict": _RLM + "「 {name} 」\nرفعت القيود التالية عنه:\n{restrictions}\n༄",
    "cmd_kashf": (
        _RLM + "⇜ الاسم ↢ {name}\n⇜ الايدي ↢ {id}\n⇜ اليوزر ↢ {username}\n"
        "⇜ الرتبه ↢ {rank}\n⇜ بالمجموعة ↢ {membership}\n༄\n"
        "⇜ الصفة المعتمدة ↢ {role}\n⇜ بلاغات النصب ↢ {reports}\n⇜ الرابط ↢ {link}"
    ),
}
TPL_LABELS: dict[str, str] = {
    "cmd_ban": "الحظر", "cmd_mute": "الكتم", "cmd_kick": "الطرد", "cmd_tayeed": "التقييد",
    "cmd_unban": "إلغاء الحظر", "cmd_unmute": "إلغاء الكتم", "cmd_unrestrict": "رفع القيود", "cmd_kashf": "الكشف",
}
_PUNISH_VARS = "{name} {time} {until}  (وأيضاً {duration} {end_time})"
TPL_VARS: dict[str, str] = {
    "cmd_ban": _PUNISH_VARS, "cmd_mute": _PUNISH_VARS, "cmd_kick": _PUNISH_VARS, "cmd_tayeed": _PUNISH_VARS,
    "cmd_unban": "{name}", "cmd_unmute": "{name}", "cmd_unrestrict": "{name} {restrictions}",
    "cmd_kashf": "{name} {id} {username} {rank} {membership} {role} {reports} {link}",
}
# الأوامر الأساسية التي تستخدم الكليشات القابلة للتعديل (الأوامر الظريفة تحتفظ بردودها القديمة).
TPL_BY_CMD: dict[str, str] = {
    "حظر": "cmd_ban", "طرد": "cmd_kick", "كتم": "cmd_mute", "تقييد": "cmd_tayeed",
    "الغاء الحظر": "cmd_unban", "فك الحظر": "cmd_unban",
    "الغاء الكتم": "cmd_unmute", "فك الكتم": "cmd_unmute",
    "رفع القيود": "cmd_unrestrict", "فك القيود": "cmd_unrestrict",
}


def tpl_render(text, entities, values, spans=None):
    """Replace {vars} while keeping every entity (custom emoji, links...) aligned (UTF-16 offsets)."""
    u16 = lambda t: len(t.encode("utf-16-le")) // 2
    reps, out, pos, cum = [], "", 0, 0
    for m in re.finditer(r"\{(\w+)\}", text):
        if m.group(1) not in values:
            continue
        repl = str(values[m.group(1)])
        old_s = u16(text[:m.start()])
        n_old, n_new = u16(m.group(0)), u16(repl)
        reps.append((old_s, old_s + n_old, n_new))
        if spans is not None and m.group(1) not in spans:
            spans[m.group(1)] = (old_s + cum, n_new)
        cum += n_new - n_old
        out += text[pos:m.start()] + repl
        pos = m.end()
    out += text[pos:]
    new = []
    for ent in entities or []:
        item = dict(ent)
        off = int(ent["offset"])
        length = int(ent["length"])
        end = off + length
        shift = sum(n - (e - s) for s, e, n in reps if e <= off)
        grow = sum(n - (e - s) for s, e, n in reps if s >= off and e <= end)  # متغير داخل الكيان
        item["offset"] = off + shift
        item["length"] = length + grow
        new.append(item)
    return out, new


def fmt_duration_ar(duration) -> str:
    if not duration:
        return "دائم"
    secs = int(duration.total_seconds())
    if secs <= 0:
        return "دائم"
    parts = []
    for unit_secs, label in ((86400, "يوم"), (3600, "ساعة"), (60, "دقيقة"), (1, "ثانية")):
        n, secs = divmod(secs, unit_secs)
        if n:
            parts.append(f"{n} {label}")
    return " و ".join(parts) or "دائم"


def fmt_until_cairo(dt) -> str:
    if not dt:
        return "دائم"
    local = dt.astimezone(CAIRO_TZ)
    return f"{local:%Y-%m-%d} {local:%I:%M} {'ص' if local.hour < 12 else 'م'}"


def get_template(key: str) -> tuple[str, list]:
    saved = db_load("cmd_templates").get(key)
    if isinstance(saved, dict) and isinstance(saved.get("text"), str) and saved["text"].strip():
        return saved["text"], saved.get("entities") or []
    return TPL_DEFAULTS[key], []


def render_template(key: str, values: dict, mention_uid: int | None = None):
    text, ents = get_template(key)
    spans: dict = {}
    out, new = tpl_render(text, ents, values, spans)
    if mention_uid and "name" in spans and spans["name"][1] > 0:
        off, ln = spans["name"]
        new.append({"type": "text_link", "offset": off, "length": ln, "url": f"tg://user?id={mention_uid}"})
    objs = []
    for d in new:
        if int(d.get("length", 0)) <= 0:
            continue
        try:
            objs.append(MessageEntity.de_json(d, None))
        except Exception:
            pass
    return out, objs


async def reply_with_template(message, key: str, values: dict, mention_uid: int | None = None, reply_markup=None):
    text, ents = render_template(key, values, mention_uid)
    try:
        return await message.reply_text(text, entities=ents or None, reply_markup=reply_markup,
                                        disable_web_page_preview=True)
    except TelegramError:
        # كيان غير صالح؟ نرسل النص بدون تنسيق بدل أن تضيع الرسالة.
        return await message.reply_text(text, reply_markup=reply_markup, disable_web_page_preview=True)


def _plain_name(html_name, uid) -> str:
    name = html_unescape(re.sub(r"<[^>]+>", "", html_name or "")).strip()
    return name or str(uid)


def _full_permissions() -> ChatPermissions:
    return ChatPermissions(
        can_send_messages=True, can_send_photos=True, can_send_videos=True,
        can_send_other_messages=True, can_send_polls=True, can_add_web_page_previews=True,
    )


async def _auto_lift_later(bot, chat_id: int, uid: int, kind: str, delay: float) -> None:
    """Instant scheduling for sub-minute punishments (Telegram ignores until_date < 30s)."""
    await asyncio.sleep(max(0.0, delay))
    try:
        if kind == "ban":
            await bot.unban_chat_member(chat_id=chat_id, user_id=uid, only_if_banned=True)
        else:
            await bot.restrict_chat_member(chat_id=chat_id, user_id=uid, permissions=_full_permissions())
            restr = db_load("restricted_users")
            if str(chat_id) in restr and str(uid) in restr[str(chat_id)]:
                del restr[str(chat_id)][str(uid)]
                db_save("restricted_users", restr)
    except Exception as exc:
        logger.warning("auto-lift failed chat=%s uid=%s kind=%s: %s", chat_id, uid, kind, exc)


# ---- مدد بالعربي: يومين / ساعتين / 3 ساعات / 10 دقائق ... ----
_AR_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_AR_UNITS: dict[str, tuple[int, int | None]] = {
    "ثانيه": (1, 1), "ثانيتين": (1, 2), "ثواني": (1, None), "ثوان": (1, None),
    "دقيقه": (60, 1), "دقيقتين": (60, 2), "دقائق": (60, None), "دقايق": (60, None),
    "ساعه": (3600, 1), "ساعتين": (3600, 2), "ساعات": (3600, None),
    "يوم": (86400, 1), "يومين": (86400, 2), "ايام": (86400, None),
    "اسبوع": (604800, 1), "اسبوعين": (604800, 2), "اسابيع": (604800, None),
    "شهر": (2592000, 1), "شهرين": (2592000, 2), "شهور": (2592000, None), "اشهر": (2592000, None),
}


def extract_duration_tokens(tokens: list[str]):
    """Pull the first Arabic duration (e.g. 'يومين' or '3 ساعات') out of tokens. English forms
    like 30m / 1h / 2d are still handled by parse_duration downstream."""
    out: list[str] = []
    duration = None
    for tok in tokens:
        if duration is None:
            unit = _AR_UNITS.get(_normalize_arabic(tok))
            if unit:
                secs, fixed = unit
                count = fixed
                popped = False
                prev = out[-1].translate(_AR_DIGITS).rstrip(",;.:") if out else ""
                if prev.isdigit() and len(prev) <= 4 and int(prev) > 0:
                    count = int(prev)
                    popped = True
                if count:
                    if popped:
                        out.pop()
                    duration = timedelta(seconds=secs * count)
                    continue
        out.append(tok)
    return duration, out


# ---- أوامر مضافة (Aliases) ----
ALIAS_CANON: dict[str, str] = {
    "ban": "حظر", "mute": "كتم", "kick": "طرد", "tayeed": "تقييد", "unrestrict": "رفع القيود",
    "unban": "فك الحظر", "unmute": "فك الكتم", "kashf": "كشف", "pin": "تثبيت",
    "lock_chat": "قفل الشات", "unlock_chat": "فتح الشات",
}
ALIAS_LABELS: dict[str, str] = {
    "ban": "حظر", "mute": "كتم", "kick": "طرد", "tayeed": "تقييد", "unrestrict": "رفع القيود",
    "unban": "إلغاء الحظر", "unmute": "إلغاء الكتم", "kashf": "كشف", "pin": "تثبيت",
    "lock_chat": "قفل الشات", "unlock_chat": "فتح الشات",
}
MOD_ALIAS_ACTIONS = {"ban", "mute", "kick", "tayeed", "unrestrict", "unban", "unmute"}
_BUILTIN_WORDS = {"كشف", "تثبيت", "ثبت", "قفل الشات", "فتح الشات", "قفل الجروب", "فتح الجروب"}


def apply_alias(text: str, actions) -> str:
    """cmd_dispatch: rewrite a leading custom word to the real command word (hot path: one dict lookup)."""
    if not text:
        return text
    aliases = db_load("cmd_aliases")
    if not aliases:
        return text
    low = text.lower()
    best = None
    for word, action in aliases.items():
        if action in actions and (low == word or low.startswith(word + " ")):
            if best is None or len(word) > len(best[0]):
                best = (word, action)
    if best is None:
        return text
    return ALIAS_CANON[best[1]] + text[len(best[0]):]


# ---- واجهة اللوحة ----
def make_button(text, callback_data=None, url=None, style=None, icon_custom_emoji_id=None):
    kw = {}
    if callback_data is not None:
        kw["callback_data"] = callback_data
    if url is not None:
        kw["url"] = url
    if style:                      # primary / success / danger (بدون style = شفاف)
        kw["style"] = style
    if icon_custom_emoji_id:
        kw["icon_custom_emoji_id"] = icon_custom_emoji_id
    return InlineKeyboardButton(text, **kw)


def two_column_rows(buttons: list) -> list:
    return [buttons[i:i + 2] for i in range(0, len(buttons), 2)]


def build_botcmds_markup() -> InlineKeyboardMarkup:
    rows = two_column_rows([
        make_button(f"✏️ كليشة {label}", callback_data=f"tpl_edit:{key}", style="primary")
        for key, label in TPL_LABELS.items()
    ])
    rows.append([
        make_button("➕ إضافة أمر", callback_data="alias:add", style="success"),
        make_button("🗑 حذف أمر مضاف", callback_data="alias:del", style="danger"),
    ])
    rows.append([make_button("🔙 رجوع", callback_data="profanity:open")])
    return InlineKeyboardMarkup(rows)


async def _panel_show(query, text: str, markup=None) -> None:
    try:
        await query.edit_message_text(text, reply_markup=markup)
    except TelegramError as exc:
        if "not modified" in str(exc).lower():
            return
        try:
            await query.message.reply_text(text, reply_markup=markup)
        except TelegramError:
            pass


def _clear_panel_states(context, uid: int) -> None:
    bd = context.application.bot_data
    bd.setdefault("pending_tpl_edit", {}).pop(uid, None)
    bd.setdefault("pending_alias_add", {}).pop(uid, None)


async def handle_botcmds_open_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    if query.from_user.id != OWNER_ID:
        await query.answer("لوحة المالك فقط", show_alert=True)
        return
    await query.answer()
    _clear_panel_states(context, query.from_user.id)
    await _panel_show(
        query,
        "⚙️ أوامر البوت\nاختر كليشة لتعديلها، أو أضف/احذف أمراً مخصصاً.",
        build_botcmds_markup(),
    )


async def handle_tpl_edit_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    if query.from_user.id != OWNER_ID:
        await query.answer("لوحة المالك فقط", show_alert=True)
        return
    if not query.message or query.message.chat.type != "private":
        await query.answer("التعديل من الخاص فقط", show_alert=True)
        return
    key = query.data.split(":", 1)[1]
    if key not in TPL_DEFAULTS:
        await query.answer("كليشة غير معروفة", show_alert=True)
        return
    _clear_panel_states(context, query.from_user.id)
    context.application.bot_data.setdefault("pending_tpl_edit", {})[query.from_user.id] = key
    await query.answer()
    await query.message.reply_text(
        f"✏️ تعديل كليشة ({TPL_LABELS[key]})\n\n"
        f"المتغيرات المتاحة:\n{TPL_VARS[key]}\n\n"
        "أرسل النص الجديد (يدعم الإيموجي المميز والتنسيق).\n"
        "• اكتب «افتراضي» للرجوع للنص الأصلي.\n"
        "• اكتب «إلغاء» للتراجع.\n\n"
        "النص الحالي في الرسالة التالية 👇"
    )
    text, ents = get_template(key)
    try:
        objs = [MessageEntity.de_json(d, None) for d in ents]
        await context.bot.send_message(chat_id=query.message.chat_id, text=text, entities=objs or None)
    except Exception:
        await context.bot.send_message(chat_id=query.message.chat_id, text=text)


async def handle_alias_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    uid = query.from_user.id
    if uid != OWNER_ID:
        await query.answer("لوحة المالك فقط", show_alert=True)
        return
    parts = query.data.split(":", 2)
    action = parts[1] if len(parts) > 1 else ""
    arg = parts[2] if len(parts) > 2 else ""
    bd = context.application.bot_data
    aliases = db_load("cmd_aliases")

    if action == "add":
        _clear_panel_states(context, uid)
        bd.setdefault("pending_alias_add", {})[uid] = {"step": "word"}
        await query.answer()
        await query.message.reply_text(
            "أرسل الكلمة الجديدة التي تريد إضافتها كأمر (مثال: بان)\n(أو اكتب «إلغاء» للتراجع)"
        )
        return

    if action == "act":
        st = bd.get("pending_alias_add", {}).get(uid)
        if not st or st.get("step") != "action" or arg not in ALIAS_CANON:
            await query.answer("انتهت الجلسة، ابدأ من «إضافة أمر».", show_alert=True)
            return
        word = st["word"]
        replaced = word in aliases
        aliases[word] = arg
        db_save("cmd_aliases", aliases)
        bd["pending_alias_add"].pop(uid, None)
        await query.answer("تم الحفظ ✅")
        await _panel_show(
            query,
            f"✅ تم {'تحديث' if replaced else 'ربط'} الأمر «{word}» ليعمل كأمر «{ALIAS_LABELS[arg]}».\n"
            "يعمل بالرد أو بالآيدي أو باليوزر، ومع المدة (مثال: " + word + " 30m).",
            InlineKeyboardMarkup([[make_button("🔙 أوامر البوت", callback_data="botcmds_open")]]),
        )
        return

    if action in ("del", "rm"):
        if action == "rm":
            if arg in aliases:
                del aliases[arg]
                db_save("cmd_aliases", aliases)
                await query.answer("تم الحذف 🗑")
            else:
                await query.answer("الأمر غير موجود", show_alert=True)
        else:
            await query.answer()
        if not aliases:
            await _panel_show(
                query, "لا توجد أوامر مضافة حالياً.",
                InlineKeyboardMarkup([[make_button("🔙 أوامر البوت", callback_data="botcmds_open")]]),
            )
            return
        btns = [
            make_button(f"🗑 {w} ← {ALIAS_LABELS.get(a, a)}", callback_data=f"alias:rm:{w}", style="danger")
            for w, a in sorted(aliases.items())
        ]
        rows = two_column_rows(btns)
        rows.append([make_button("🔙 أوامر البوت", callback_data="botcmds_open")])
        await _panel_show(query, "🗑 اختر الأمر المضاف الذي تريد حذفه:", InlineKeyboardMarkup(rows))
        return

    await query.answer()


async def handle_owner_panel_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Private-chat input of the owner while editing a template or adding an alias."""
    message, user, chat = update.effective_message, update.effective_user, update.effective_chat
    if not message or not user or not chat or chat.type != "private" or user.id != OWNER_ID:
        return False
    bd = context.application.bot_data
    tpl_pending = bd.get("pending_tpl_edit")
    alias_pending = bd.get("pending_alias_add")
    if not tpl_pending and not alias_pending:
        return False  # fast path: nothing pending
    raw = (message.text or "").strip()
    cancel_words = ("إلغاء", "الغاء")

    if tpl_pending and user.id in tpl_pending:
        key = tpl_pending[user.id]
        if not message.text:
            await message.reply_text("أرسل نصاً (يمكن أن يحتوي على إيموجي مميز)، أو اكتب «إلغاء».")
            return True
        if raw in cancel_words:
            tpl_pending.pop(user.id, None)
            await message.reply_text("تم إلغاء التعديل.")
            return True
        store = db_load("cmd_templates")
        if raw == "افتراضي":
            store.pop(key, None)
            db_save("cmd_templates", store)
            tpl_pending.pop(user.id, None)
            await message.reply_text(f"✅ تمت إعادة كليشة ({TPL_LABELS[key]}) للنص الافتراضي.")
            return True
        if len(message.text) > 3500:
            await message.reply_text("النص طويل جداً (الحد 3500 حرف).")
            return True
        ents = [e.to_dict() for e in (message.entities or [])]
        # معاينة بقيم تجريبية: لا نحفظ إلا إذا قبلها تيليجرام.
        sample = {
            "name": "الاسم", "id": "123456789", "username": "@username", "rank": "عضو عادي",
            "membership": "عضو", "role": "لا يوجد", "reports": "لا يوجد", "link": "https://t.me/username",
            "time": "1 ساعة", "duration": "1 ساعة", "until": "2026-01-01 10:00 ص",
            "end_time": "2026-01-01 10:00 ص", "restrictions": "1) كتم",
        }
        spans: dict = {}
        prev_text, prev_ents = tpl_render(message.text, ents, sample, spans)
        try:
            objs = [MessageEntity.de_json(d, None) for d in prev_ents if int(d.get("length", 0)) > 0]
            await message.reply_text("👀 معاينة:")
            await context.bot.send_message(chat_id=chat.id, text=prev_text, entities=objs or None,
                                           disable_web_page_preview=True)
        except TelegramError as exc:
            await message.reply_text(f"تعذر قبول هذا التنسيق من تيليجرام ({exc}). أرسل نصاً آخر أو «إلغاء».")
            return True
        store[key] = {"text": message.text, "entities": ents}
        db_save("cmd_templates", store)
        tpl_pending.pop(user.id, None)
        await message.reply_text(f"✅ تم حفظ كليشة ({TPL_LABELS[key]}).")
        return True

    if alias_pending and user.id in alias_pending:
        st = alias_pending[user.id]
        if st.get("step") != "word":
            if raw in cancel_words:
                alias_pending.pop(user.id, None)
                await message.reply_text("تم الإلغاء.")
                return True
            await message.reply_text("اختر الإجراء من الأزرار أعلاه، أو اكتب «إلغاء».")
            return True
        if not raw:
            await message.reply_text("أرسل الكلمة كنص، أو اكتب «إلغاء».")
            return True
        if raw in cancel_words:
            alias_pending.pop(user.id, None)
            await message.reply_text("تم الإلغاء.")
            return True
        word = raw.lower()
        if "\n" in raw or len(word) > 20 or len(word.encode("utf-8")) > 40:
            await message.reply_text("الكلمة لازم تكون سطر واحد وقصيرة (حتى 20 حرف). أرسل كلمة أخرى.")
            return True
        if word in _BUILTIN_WORDS or word in _MOD_TABLES[6]:
            await message.reply_text("هذه الكلمة أمر أساسي في البوت بالفعل. أرسل كلمة أخرى.")
            return True
        st.update({"step": "action", "word": word})
        buttons = [make_button(label, callback_data=f"alias:act:{key}", style="primary")
                   for key, label in ALIAS_LABELS.items()]
        await message.reply_text(
            f"اختر الإجراء الذي سيُنفَّذ عند كتابة «{word}»:",
            reply_markup=InlineKeyboardMarkup(two_column_rows(buttons)),
        )
        return True

    return False


def _punish_values(target_id, target_name, duration=None, until_dt=None, **extra) -> dict:
    t, u = fmt_duration_ar(duration), fmt_until_cairo(until_dt)
    values = {"name": _plain_name(target_name, target_id), "id": str(target_id),
              "time": t, "duration": t, "until": u, "end_time": u}
    values.update(extra)
    return values


# -------------------------------------------------------------
# Main Message Router (أولوية مطلقة لكافة الأوامر)
# -------------------------------------------------------------

_USER_LOCKS: "weakref.WeakValueDictionary[tuple[int, int], asyncio.Lock]" = weakref.WeakValueDictionary()


def _user_lock(key: tuple[int, int]) -> asyncio.Lock:
    lock = _USER_LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _USER_LOCKS[key] = lock
    return lock


async def _h(fn, update, context):
    """Run one handler and record how long it took."""
    t0 = time.perf_counter()
    try:
        return await fn(update, context)
    finally:
        perf.record("handler." + fn.__name__, (time.perf_counter() - t0) * 1000.0)


async def _notify_mentions_bg(update, context) -> None:
    try:
        await notify_mentioned_users(update, context)
    except Exception as exc:
        logger.warning("notify_mentioned_users failed: %s", exc)


async def handle_exact_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # This router must run ONLY for a newly sent message. Telegram can deliver
    # reactions, reaction-count updates, edited messages, and other updates
    # through broad filters; none of those should re-run a command/card handler.
    if (
        getattr(update, "message_reaction", None) is not None
        or getattr(update, "message_reaction_count", None) is not None
        or getattr(update, "edited_message", None) is not None
        or getattr(update, "edited_channel_post", None) is not None
        or getattr(update, "channel_post", None) is not None
        or getattr(update, "business_message", None) is not None
        or getattr(update, "edited_business_message", None) is not None
    ):
        return

    # Only process an actual newly sent chat message, not effective_message
    # which may refer to a message attached to a non-message update.
    message = getattr(update, "message", None)
    chat = message.chat if message else None
    if not message or not chat:
        return

    # Never route messages authored by bots (including this bot) as user input.
    if message.from_user and message.from_user.is_bot:
        return

    # Updates are processed concurrently; messages of the same user in the same chat are
    # still handled strictly in order (interactive sessions rely on that), so no command can
    # run twice or race with itself.
    try:
        lag_ms = max(0.0, (time.time() - message.date.timestamp()) * 1000.0)
        perf.record("update.arrival_lag(1s resolution)", lag_ms)
    except Exception:
        pass
    uid = message.from_user.id if message.from_user else 0
    lock = _user_lock((chat.id, uid))
    t_wait = time.perf_counter()
    async with lock:
        perf.record("message.lock_wait", (time.perf_counter() - t_wait) * 1000.0)
        t_total = time.perf_counter()
        try:
            await _route_message(update, context, message, chat)
        finally:
            perf.record("message.total", (time.perf_counter() - t_total) * 1000.0)


async def _route_message(update: Update, context: ContextTypes.DEFAULT_TYPE, message, chat) -> None:
    try:
        if await _h(_enforce_manual_chat_lock, update, context):
            return
        if await _h(handle_content_lock_commands, update, context):
            return
        if await _h(handle_owner_panel_input, update, context):
            return
        if await _h(handle_profanity_and_word_commands, update, context):
            return
        if await _h(_warn_for_locked_content, update, context):
            return

        with timed("handler.track_user_map"):
            await track_user_map(update, context)
        if message.entities or message.caption_entities:
            spawn(_notify_mentions_bg(update, context))  # DMs must never delay the reply

        # رقم التحديث MG2 متاح في أي محادثة تصل إليها رسائل البوت.
        if (message.text or "").strip() == "التحديث":
            await message.reply_text("تحديث البوت الحالي: MG1")
            return

        # تقرير الأداء (لمالك البوت فقط)
        if (message.text or "").strip() == "تقرير الأداء":
            if update.effective_user and await is_bot_owner(update.effective_user.id, context):
                await message.reply_text(
                    "<pre>" + escape(perf.report(20)) + "</pre>", parse_mode=ParseMode.HTML)
            return

        # أمر send يجب أن يسبق الردود التلقائية حتى لا تلتقطه معالجات أخرى.
        if await _h(handle_send_as_bot, update, context):
            return

        # 1. أوامر كشف الحسابات واستعلام الرتب والآيدي
        if await _h(handle_id_card_command, update, context):
            return
        if await _h(handle_comprehensive_lookup, update, context):
            return
        if await _h(handle_rank_commands, update, context):
            # Allow a group-specific custom reply for the exact trigger "رتبتي"
            # to be sent after the built-in rank card, instead of being blocked by
            # the rank handler's early return.
            if (message.text or "").strip() == "رتبتي":
                await _h(handle_custom_response_system, update, context)
            return

        # 2. أمر الأوامر وقائمة الدليل التفاعلي
        if await _h(handle_commands_menu_trigger, update, context):
            return

        # 3. نظام الروليت الفوري (روليت / تم)
        if await _h(handle_roulette_messages, update, context):
            return

        # 4. إدارة مواقيت الصلاة وقفل وفتح الشات اليدوي
        if await _h(handle_prayer_and_lock_management, update, context):
            return

        # 5. جلسات الإدخال التفاعلية المباشرة (أمر انشر، إضافة رد، إضافة كارت)
        if await _h(handle_broadcast_command, update, context):
            return
        if await _h(handle_custom_response_system, update, context):
            return
        if await _h(handle_dynamic_admin_cards, update, context):
            return
        if await _h(handle_pending_review_submission, update, context):
            return

        # 6. رفع وتنزيل المشرفين (في المجموعة رسمياً أو بالبوت فقط)
        if await _h(handle_bot_internal_roles, update, context):
            return
        if await _h(handle_real_group_admin_commands, update, context):
            return

        text_check = (message.text or message.caption or "").strip()

        # 7. أمر التثبيت بالرد
        if await _h(handle_pin_message, update, context):
            return

        # 8. أوامر الإشراف والعقوبات والمسح الدقيق
        if await _h(handle_moderation, update, context):
            return
        if await _h(handle_purge_messages, update, context):
            return

        # 9. كشف النصابين والتجار والمعاملات
        if await _h(handle_blacklist_commands, update, context):
            return
        if await _h(handle_merchant_lookup, update, context):
            return
        if await _h(handle_merchant_management, update, context):
            return
        if await _h(check_for_tx_code_lookup, update, context):
            return

        # 10. الردود التلقائية العادية
        if await _h(handle_auto_replies, update, context):
            return

        # 11. كروت الوساطة والقوانين والمشرفين الأساسيين (محصورة في شاتك الأساسي فقط)
        if chat.id == PRIMARY_CHAT_ID:
            if await _h(handle_custom_keyword_triggers, update, context):
                return

            card_key = exact_trigger(message.text or message.caption)
            if card_key is not None:
                await send_verification_card(update, context, card_key)
                return

        # 12. فحص النصابين في الشات العام
        is_blacklist_cmd = any(k in text_check for k in ["نصاب", "حذف نصاب", "مسح نصاب", "فك نصاب", "شيل نصاب", "فحص"])
        if not is_blacklist_cmd:
            if await _h(check_for_blacklisted_mentions, update, context):
                return

        # 13. الإدارة والمعاملات المتبقية
        if await _h(handle_restricted_list_and_unmute, update, context):
            return
        if await _h(handle_transaction_commands, update, context):
            return
        if await _h(check_for_cash_number_mention, update, context):
            return

    except Exception as e:
        logger.error(f"Error handling message: {e}", exc_info=True)


async def global_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Exception while handling an update:", exc_info=context.error)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text(
        build_full_help_guide(),
        parse_mode=ParseMode.HTML,
    )


async def on_startup(application: Application) -> None:
    global telethon_client
    try:
        telethon_client = TelegramClient("bot_session", API_ID, API_HASH)
        await telethon_client.start(bot_token=BOT_TOKEN)
        logger.info("Telethon MTProto Client connected successfully for Username resolving & Purging.")
    except Exception as e:
        logger.warning(f"Telethon MTProto failed to start: {e}")

    # تشغيل مراقبة الصلاة في الخلفية فور تشغيل البوت
    spawn(prayer_times_background_loop(application))
    spawn(perf_log_loop())


async def perf_log_loop() -> None:
    """Every 10 minutes log the slowest operations so real bottlenecks are visible in the logs."""
    while True:
        await asyncio.sleep(600)
        try:
            logger.info("PERF REPORT\n%s", perf.report(15))
        except Exception:
            pass


async def on_shutdown(application: Application) -> None:
    flush_db()


async def handle_chat_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Any admin/member status change invalidates that chat's cached admin list."""
    change = update.chat_member
    if change:
        invalidate_admin_cache(change.chat.id)



def _utf16_offset_to_index(value: str, offset: int) -> int:
    """Convert Telegram UTF-16 entity offsets to Python string indices."""
    units = 0
    for index, char in enumerate(value):
        if units >= offset:
            return index
        units += 2 if ord(char) > 0xFFFF else 1
    return len(value)


def extract_button_custom_emoji(message: Message, label: str, label_start: int = 0) -> tuple[str, str | None]:
    """Remove the fallback glyph for the first custom emoji in a button label.

    Telegram represents custom emoji in message text as a fallback character plus
    a CUSTOM_EMOJI entity. The button API accepts the emoji ID separately, so the
    fallback glyph must not remain in the button label. label_start is a Python
    character offset into message.text where the label begins.
    """
    source = message.text or message.caption or ""
    entities = message.entities or message.caption_entities or []
    label_end = label_start + len(label)
    for entity in entities:
        if entity.type != MessageEntityType.CUSTOM_EMOJI:
            continue
        start = _utf16_offset_to_index(source, entity.offset)
        end = _utf16_offset_to_index(source, entity.offset + entity.length)
        if start < label_start or end > label_end:
            continue
        # Remove exactly the fallback character covered by the custom-emoji entity.
        cleaned = label[:start-label_start] + label[end-label_start:]
        cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
        return cleaned, entity.custom_emoji_id
    return label.strip(), None


async def handle_bot_admin_promotion(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Announce the bot's promotion to admin in a group or supergroup."""
    change = update.my_chat_member
    if not change:
        return

    chat = change.chat
    old_status = change.old_chat_member.status
    new_status = change.new_chat_member.status
    admin_statuses = {ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER}

    if chat.type not in ("group", "supergroup"):
        return
    # Send when becoming an admin, including after a demotion and re-promotion.
    if new_status not in admin_statuses or old_status in admin_statuses:
        return

    # Use HTML tg-emoji tags instead of manually supplied entity offsets. This
    # avoids invalid UTF-16 offsets causing Telegram to reject the whole message.
    announcement = (
        '<tg-emoji emoji-id="5782774972703315705">⛈</tg-emoji> 𝐑𝐀𝐀𝐃 | ࢪَعْـد '
        '<tg-emoji emoji-id="5782774972703315705">⛈</tg-emoji>\n\n'
        '<tg-emoji emoji-id="5801031104857117851">👑</tg-emoji> '
        'تَـمَّ اعْـتِـمَـادُ رَعْـد لِـقِـيَـادَةِ الإِدَارَة '
        '<tg-emoji emoji-id="5801031104857117851">👑</tg-emoji>\n\n'
        'هَيْبَةٌ فِي الحُضُور، حَزْمٌ فِي القَرَار، وَقُوَّةٌ فِي الأَدَاء '
        '<tg-emoji emoji-id="5801165752081847408">🌟</tg-emoji>\n\n'
        '<tg-emoji emoji-id="5899911688781566954">⚡</tg-emoji> '
        'إِدَارَةٌ ذَكِيَّة • حِمَايَةٌ مُتَقَدِّمَة • سَيْطَرَةٌ مُحْكَمَة\n\n'
        '<tg-emoji emoji-id="5807594609454356181">🤩</tg-emoji> '
        '<b>ࢪَعْـد — حَيْثُ تَبْدَأُ الهَيْبَةُ.</b>'
    )
    promotion_keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("الدخول للبوت", url="https://t.me/BRQ99_bot", style="success")
    ]])
    try:
        await context.bot.send_message(
            chat_id=chat.id,
            text=announcement,
            parse_mode=ParseMode.HTML,
            reply_markup=promotion_keyboard,
        )
    except TelegramError:
        logger.exception("Failed to send bot-admin promotion announcement in chat %s", chat.id)


def build_application(token: str) -> Application:
    request = TimedRequest(connection_pool_size=int(os.getenv("TG_POOL_SIZE", "64")), pool_timeout=5.0)
    application = (
        Application.builder()
        .token(token)
        .request(request)
        .concurrent_updates(int(os.getenv("CONCURRENT_UPDATES", "128")))
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .build()
    )
    application.add_handler(ChatMemberHandler(handle_bot_admin_promotion, ChatMemberHandler.MY_CHAT_MEMBER))
    application.add_handler(ChatMemberHandler(handle_chat_member_update, ChatMemberHandler.CHAT_MEMBER), group=1)

    prefetch_db([
        "primary_owners", "roulette_state", "blacklist", "user_map", "transactions", "merchants",
        "bot_admins", "distinguished_users", "custom_replies", "dynamic_admins", "prayer_chats",
        "manual_chat_locks", "content_locks", "content_lock_warnings", "profanity_filter_enabled",
        "profanity_words", "chat_forbidden_words", "restricted_users", "cmd_templates", "cmd_aliases",
    ])

    application.bot_data["admin_user_ids"] = _parse_admin_ids()
    application.bot_data["primary_owners"] = db_load("primary_owners")
    application.bot_data["roulette_state"] = db_load("roulette_state")
    application.bot_data["blacklist"] = db_load("blacklist")
    application.bot_data["user_map"] = db_load("user_map")
    application.bot_data["transactions"] = db_load("transactions")
    application.bot_data["merchants"] = db_load("merchants")
    application.bot_data["bot_admins"] = db_load("bot_admins")
    application.bot_data["distinguished_users"] = db_load("distinguished_users")
    application.bot_data["custom_replies"] = db_load("custom_replies")
    application.bot_data["dynamic_admins"] = db_load("dynamic_admins")
    application.bot_data["prayer_chats"] = db_load("prayer_chats")
    application.bot_data["manual_chat_locks"] = db_load("manual_chat_locks")
    application.bot_data["pending_broadcast_states"] = {}
    application.bot_data["pending_send_states"] = {}
    application.bot_data["pending_group_perms"] = {}
    application.bot_data["pending_bot_perms"] = {}

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("commands", help_command))
    application.add_handler(CallbackQueryHandler(handle_start_help_callback, pattern=r"^(show_help_callback|lang_select_info)$"))
    application.add_handler(CallbackQueryHandler(handle_button_color_callback, pattern=r"^btncol:(danger|success|primary):-?\d+:-?\d+$"))
    application.add_handler(CallbackQueryHandler(handle_reply_mode_callback, pattern=r"^replymode:(button|plain):-?\d+:-?\d+$"))
    application.add_handler(CallbackQueryHandler(handle_button_more_callback, pattern=r"^btnmore:(yes|no):-?\d+:-?\d+$"))
    application.add_handler(CallbackQueryHandler(handle_button_position_callback, pattern=r"^btnpos:(side|above|below):-?\d+:-?\d+$"))
    application.add_handler(CallbackQueryHandler(handle_group_admin_perm_callback, pattern=r"^gperm:"))
    application.add_handler(CallbackQueryHandler(handle_bot_role_perm_callback, pattern=r"^bperm:"))
    application.add_handler(CallbackQueryHandler(handle_lock_unmute_callback, pattern=r"^lockunmute:-?\d+:-?\d+$"))
    application.add_handler(CallbackQueryHandler(handle_profanity_panel_callback, pattern=r"^profanity:(open|add|list)$"))
    application.add_handler(CallbackQueryHandler(roulette_join, pattern=r"^roulette_join:-?\d+$"))
    application.add_handler(CallbackQueryHandler(handle_botcmds_open_callback, pattern=r"^botcmds_open$"))
    application.add_handler(CallbackQueryHandler(handle_tpl_edit_callback, pattern=r"^tpl_edit:cmd_\w+$"))
    application.add_handler(CallbackQueryHandler(handle_alias_callback, pattern=r"^alias:"))
    application.add_handler(CallbackQueryHandler(handle_transaction_callback, pattern=r"^tx_(confirm|cancel|received|not_received):01[0125]\d{8}(:-?\d+)?$"))
    application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_exact_message))
    application.add_error_handler(global_error_handler)
    return application


def main() -> None:
    token = BOT_TOKEN
    if not token:
        raise RuntimeError("TELEGRAM_TOKEN is missing.")
    start_keep_alive(port=8080)
    application = build_application(token)
    logger.info("Starting Telegram polling and keep-alive server with Redis persistence")
    application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
