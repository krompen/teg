"""
Платформа личных ботов обратной связи (зеркал) на aiogram >= 3.4.
Обязательная подписка, рефералка (15 Stars), кастомизация зеркал, антифрод/антифлуд.
"""

import asyncio
import contextlib
import html
import logging
import os
import re
import sqlite3
import time
from collections import deque

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command, StateFilter
from aiogram.filters.command import CommandObject
from aiogram.types import (
    Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton, ReplyParameters,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup, any_state
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import (
    TelegramAPIError, TelegramUnauthorizedError, TelegramForbiddenError,
    TelegramRetryAfter, TelegramBadRequest,
)

# ==================== КОНФИГУРАЦИЯ ====================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
_admin_env = os.getenv("ADMIN_ID", "").strip()
if not BOT_TOKEN or not _admin_env.isdigit():
    raise SystemExit("❌ Установите BOT_TOKEN и ADMIN_ID (число) в переменных окружения.")

ADMIN_ID = int(_admin_env)
DB_FILE = os.getenv("DB_FILE", "bot_database.sqlite")
PROMO_LIMIT = int(os.getenv("PROMO_LIMIT", "10"))
PROMO_DAYS = int(os.getenv("PROMO_DAYS", "30"))
REQUIRED_CHANNEL = os.getenv("REQUIRED_CHANNEL", "@swithub")
SUPPORT_ACCOUNT = os.getenv("SUPPORT_ACCOUNT", "@sb_teh_robot")
REF_STEP = 5            # активных рефералов за одну награду
REF_REWARD = 15         # Stars за одну награду
REF_MAX_REWARDS = int(os.getenv("REF_MAX_REWARDS", "1"))  # сколько раз можно получить награду

FLOOD_WINDOW_SECONDS = 8
FLOOD_MAX_MESSAGES = 6
FLOOD_AUTOBAN_STRIKES = 3
FLOOD_MUTE_SECONDS = 900
SUB_CACHE_TTL = 300
BROADCAST_COOLDOWN = 3600
BROADCAST_COOLDOWN_PREMIUM = 600
RAID_WINDOW_SECONDS = 60
RAID_REGISTRATIONS_THRESHOLD = 8
RAID_COOLDOWN_SECONDS = 300
TOKEN_RE = re.compile(r"^\d{6,12}:[A-Za-z0-9_-]{30,50}$")
SPAM_RE = re.compile(r"(https?://|t\.me/|www\.|@\w{4,})", re.I)

MAIN_BOT_USERNAME = ""
running_child_bots: dict[int, asyncio.Task] = {}
child_invalid: set[int] = set()

_flood_log: dict = {}
_flood_strikes: dict = {}
_muted_until: dict = {}
_sub_cache: dict = {}
_global_bans: set[int] = set()
_registration_log: list[float] = []
_raid_mode_until = 0.0
_sub_alert_ts = 0.0
_bg_tasks: set = set()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def esc(value) -> str:
    return html.escape(str(value), quote=False) if value is not None else ""


def spawn(coro):
    """create_task с удержанием ссылки (иначе задачу может собрать GC)."""
    t = asyncio.create_task(coro)
    _bg_tasks.add(t)
    t.add_done_callback(_bg_tasks.discard)
    return t


# ==================== БАЗА ДАННЫХ ====================
def _connect():
    conn = sqlite3.connect(DB_FILE, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _exec(sql, params=(), fetch=None):
    conn = _connect()
    try:
        cur = conn.execute(sql, params)
        if fetch == "one":
            res = cur.fetchone()
        elif fetch == "all":
            res = cur.fetchall()
        else:
            res = cur.rowcount
        conn.commit()
        return res
    finally:
        conn.close()


async def dbx(sql, params=(), fetch=None):
    return await asyncio.to_thread(_exec, sql, params, fetch)


async def db_tx(fn, write=True):
    """Атомарная транзакция: fn(conn) выполняется в потоке, соединение всегда закрывается."""
    def _run():
        conn = _connect()
        try:
            if write:
                conn.execute("BEGIN IMMEDIATE")
            res = fn(conn)
            if write:
                conn.commit()
            return res
        except Exception:
            if write:
                conn.rollback()
            raise
        finally:
            conn.close()
    return await asyncio.to_thread(_run)


def _cols(conn, table):
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _init_db_sync():
    conn = _connect()
    try:
        conn.executescript('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY, username TEXT, full_name TEXT,
            is_banned INTEGER DEFAULT 0, premium INTEGER DEFAULT 0,
            premium_expires INTEGER DEFAULT 0, registered_at INTEGER,
            referrer_id INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS child_bots (
            owner_id INTEGER PRIMARY KEY, bot_token TEXT UNIQUE,
            bio TEXT DEFAULT 'Информация отсутствует.', working_hours TEXT DEFAULT 'Не указаны',
            social_links TEXT DEFAULT '-', custom_start TEXT DEFAULT 'Привет! Напиши свой вопрос, и я отвечу.',
            spam_filter INTEGER DEFAULT 0, msg_count INTEGER DEFAULT 0, last_broadcast INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS clients (
            owner_id INTEGER, client_id INTEGER, msgs INTEGER DEFAULT 0,
            PRIMARY KEY (owner_id, client_id));
        CREATE TABLE IF NOT EXISTS msg_map (
            owner_id INTEGER, owner_msg_id INTEGER, client_id INTEGER, created_at INTEGER,
            PRIMARY KEY (owner_id, owner_msg_id));
        CREATE TABLE IF NOT EXISTS global_bans (user_id INTEGER PRIMARY KEY, banned_at INTEGER);
        CREATE TABLE IF NOT EXISTS mirror_bans (
            owner_id INTEGER, client_id INTEGER, banned_at INTEGER,
            PRIMARY KEY (owner_id, client_id));
        CREATE TABLE IF NOT EXISTS promo_claims (user_id INTEGER PRIMARY KEY, claimed_at INTEGER);
        ''')
        # --- миграции ---
        cb = _cols(conn, "child_bots")
        if "social_links" not in cb:
            conn.execute("ALTER TABLE child_bots ADD COLUMN social_links TEXT DEFAULT '-'")
        if "custom_start" not in cb:
            conn.execute("ALTER TABLE child_bots ADD COLUMN custom_start TEXT DEFAULT 'Привет! Напиши свой вопрос, и я отвечу.'")
        if "referrer_id" not in _cols(conn, "users"):
            conn.execute("ALTER TABLE users ADD COLUMN referrer_id INTEGER DEFAULT 0")
        if "msgs" not in _cols(conn, "clients"):
            conn.execute("ALTER TABLE clients ADD COLUMN msgs INTEGER DEFAULT 0")

        # ref_withdrawals: раньше user_id был PRIMARY KEY (одна заявка навсегда)
        wcols = _cols(conn, "ref_withdrawals")
        if wcols and "id" not in wcols:
            conn.execute("ALTER TABLE ref_withdrawals RENAME TO ref_withdrawals_old")
            wcols = []
        if not wcols:
            conn.execute("""CREATE TABLE ref_withdrawals (
                id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER,
                status TEXT DEFAULT 'pending', created_at INTEGER)""")
            old = conn.execute("SELECT 1 FROM sqlite_master WHERE name='ref_withdrawals_old'").fetchone()
            if old:
                conn.execute("INSERT INTO ref_withdrawals (user_id, status, created_at) "
                             "SELECT user_id, status, created_at FROM ref_withdrawals_old")
                conn.execute("DROP TABLE ref_withdrawals_old")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_users_ref ON users(referrer_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_wd_user ON ref_withdrawals(user_id)")
        conn.commit()
    finally:
        conn.close()


async def init_db():
    await asyncio.to_thread(_init_db_sync)


async def load_global_bans():
    rows = await dbx("SELECT user_id FROM global_bans", fetch="all")
    _global_bans.update(r["user_id"] for r in rows)


async def db_add_user(user_id, username, full_name, referrer_id=0) -> bool:
    def fn(conn):
        if conn.execute("SELECT 1 FROM users WHERE id=?", (user_id,)).fetchone():
            conn.execute("UPDATE users SET username=?, full_name=? WHERE id=?", (username, full_name, user_id))
            return False
        ref = 0
        # реферер должен существовать и не быть самим пользователем
        if referrer_id and referrer_id != user_id and \
                conn.execute("SELECT 1 FROM users WHERE id=?", (referrer_id,)).fetchone():
            ref = referrer_id
        conn.execute(
            "INSERT INTO users (id, username, full_name, registered_at, referrer_id) VALUES (?,?,?,?,?)",
            (user_id, username, full_name, int(time.time()), ref))
        return True
    return await db_tx(fn)


async def db_get_user(user_id):
    return await dbx("SELECT * FROM users WHERE id=?", (user_id,), "one")


async def is_user_premium(user_row) -> bool:
    if not user_row or user_row["premium"] != 1:
        return False
    if user_row["premium_expires"] == 0 or user_row["premium_expires"] > int(time.time()):
        return True
    await dbx("UPDATE users SET premium=0, premium_expires=0 WHERE id=?", (user_row["id"],))
    return False


async def db_claim_promo(uid) -> str:
    def fn(conn):
        if conn.execute("SELECT 1 FROM promo_claims WHERE user_id=?", (uid,)).fetchone():
            return "already"
        if conn.execute("SELECT COUNT(*) FROM promo_claims").fetchone()[0] >= PROMO_LIMIT:
            return "empty"
        now = int(time.time())
        conn.execute("INSERT INTO promo_claims (user_id, claimed_at) VALUES (?,?)", (uid, now))
        conn.execute("UPDATE users SET premium=1, premium_expires=? WHERE id=?", (now + PROMO_DAYS * 86400, uid))
        return "ok"
    return await db_tx(fn)


async def promo_slots_left() -> int:
    row = await dbx("SELECT COUNT(*) c FROM promo_claims", fetch="one")
    return max(0, PROMO_LIMIT - row["c"])


# ---- child bots ----
async def db_get_child_bot(owner_id):
    return await dbx("SELECT * FROM child_bots WHERE owner_id=?", (owner_id,), "one")


async def db_all_child_bots():
    return await dbx("SELECT owner_id, bot_token FROM child_bots", fetch="all")


async def db_add_child_bot(owner_id, token) -> bool:
    try:
        await dbx("INSERT INTO child_bots (owner_id, bot_token) VALUES (?,?)", (owner_id, token))
        return True
    except sqlite3.IntegrityError:
        return False


async def db_delete_child_bot(owner_id):
    def fn(conn):
        for t in ("child_bots", "clients", "mirror_bans", "msg_map"):
            conn.execute(f"DELETE FROM {t} WHERE owner_id=?", (owner_id,))
    await db_tx(fn)


async def db_update_child_bot(owner_id, field, value):
    if field not in {"bio", "working_hours", "social_links", "custom_start", "spam_filter"}:
        return
    await dbx(f"UPDATE child_bots SET {field}=? WHERE owner_id=?", (value, owner_id))


async def db_touch_client(owner_id, client_id, count_msg=False):
    def fn(conn):
        conn.execute("INSERT OR IGNORE INTO clients (owner_id, client_id) VALUES (?,?)", (owner_id, client_id))
        if count_msg:
            conn.execute("UPDATE clients SET msgs=msgs+1 WHERE owner_id=? AND client_id=?", (owner_id, client_id))
            conn.execute("UPDATE child_bots SET msg_count=msg_count+1 WHERE owner_id=?", (owner_id,))
    await db_tx(fn)


async def db_map_msgs(owner_id, msg_ids, client_id):
    now = int(time.time())
    def fn(conn):
        conn.executemany("INSERT OR REPLACE INTO msg_map VALUES (?,?,?,?)",
                         [(owner_id, m, client_id, now) for m in msg_ids])
    await db_tx(fn)


async def db_msg_client(owner_id, msg_id):
    row = await dbx("SELECT client_id FROM msg_map WHERE owner_id=? AND owner_msg_id=?", (owner_id, msg_id), "one")
    return row["client_id"] if row else None


async def db_mirror_ban(owner_id, client_id, on=True):
    if on:
        await dbx("INSERT OR REPLACE INTO mirror_bans VALUES (?,?,?)", (owner_id, client_id, int(time.time())))
    else:
        await dbx("DELETE FROM mirror_bans WHERE owner_id=? AND client_id=?", (owner_id, client_id))


async def db_is_mirror_banned(owner_id, client_id) -> bool:
    return await dbx("SELECT 1 FROM mirror_bans WHERE owner_id=? AND client_id=?",
                     (owner_id, client_id), "one") is not None


async def db_global_ban(uid, on=True):
    if on:
        await dbx("INSERT OR REPLACE INTO global_bans VALUES (?,?)", (uid, int(time.time())))
        _global_bans.add(uid)
    else:
        await dbx("DELETE FROM global_bans WHERE user_id=?", (uid,))
        _global_bans.discard(uid)


async def db_try_start_broadcast(owner_id, cooldown) -> bool:
    now = int(time.time())
    n = await dbx("UPDATE child_bots SET last_broadcast=? WHERE owner_id=? AND last_broadcast<=?",
                  (now, owner_id, now - cooldown))
    return n > 0


async def db_broadcast_targets(owner_id):
    rows = await dbx(
        """SELECT client_id FROM clients WHERE owner_id=?
           AND client_id NOT IN (SELECT client_id FROM mirror_bans WHERE owner_id=?)
           AND client_id NOT IN (SELECT user_id FROM global_bans)""", (owner_id, owner_id), "all")
    return [r["client_id"] for r in rows]


# ---- рефералка (антифрод: считаются только зеркала с реальными сообщениями от ЧУЖИХ клиентов) ----
ACTIVE_SQL = """
SELECT COUNT(*) FROM users u JOIN child_bots cb ON cb.owner_id = u.id
WHERE u.referrer_id=? AND cb.msg_count > 0
  AND u.id NOT IN (SELECT user_id FROM global_bans)
  AND EXISTS (SELECT 1 FROM clients c WHERE c.owner_id = u.id AND c.msgs > 0
              AND c.client_id NOT IN (u.id, u.referrer_id))
"""


def _ref_numbers(conn, uid):
    total = conn.execute("SELECT COUNT(*) FROM users WHERE referrer_id=?", (uid,)).fetchone()[0]
    active = conn.execute(ACTIVE_SQL, (uid,)).fetchone()[0]
    used = conn.execute("SELECT COUNT(*) FROM ref_withdrawals WHERE user_id=? AND status!='rejected'", (uid,)).fetchone()[0]
    pending = conn.execute("SELECT COUNT(*) FROM ref_withdrawals WHERE user_id=? AND status='pending'", (uid,)).fetchone()[0]
    available = max(0, min(active // REF_STEP, REF_MAX_REWARDS) - used)
    return total, active, used, available, pending


async def db_referral_stats(uid):
    return await db_tx(lambda conn: _ref_numbers(conn, uid), write=False)


async def db_create_withdrawal(uid):
    """Возвращает id заявки или None (сервер сам проверяет право на выплату)."""
    def fn(conn):
        if _ref_numbers(conn, uid)[3] <= 0:
            return None
        cur = conn.execute("INSERT INTO ref_withdrawals (user_id, status, created_at) VALUES (?, 'pending', ?)",
                           (uid, int(time.time())))
        return cur.lastrowid
    return await db_tx(fn)


# ==================== БОТ ====================
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
is_admin = F.from_user.id == ADMIN_ID


async def notify_admin(text, **kw):
    with contextlib.suppress(TelegramAPIError):
        await bot.send_message(ADMIN_ID, text, **kw)


# ==================== АНТИФЛУД / РЕЙД ====================
def flood_hit(key) -> str:
    """'ok' | 'muted' (молча игнорируем) | 'mute' (только что замьючен) | 'ban' (страйки исчерпаны)."""
    now = time.time()
    if _muted_until.get(key, 0) > now:
        return "muted"
    log = _flood_log.setdefault(key, deque())
    log.append(now)
    while log and now - log[0] > FLOOD_WINDOW_SECONDS:
        log.popleft()
    if len(log) > FLOOD_MAX_MESSAGES:
        _flood_strikes[key] = _flood_strikes.get(key, 0) + 1
        _muted_until[key] = now + FLOOD_MUTE_SECONDS
        log.clear()
        return "ban" if _flood_strikes[key] >= FLOOD_AUTOBAN_STRIKES else "mute"
    return "ok"


async def note_registration():
    global _raid_mode_until
    now = time.time()
    _registration_log.append(now)
    _registration_log[:] = [t for t in _registration_log if now - t <= RAID_WINDOW_SECONDS]
    if len(_registration_log) >= RAID_REGISTRATIONS_THRESHOLD and now > _raid_mode_until:
        _raid_mode_until = now + RAID_COOLDOWN_SECONDS
        await notify_admin(f"🚨 <b>Подозрение на рейд:</b> {len(_registration_log)} регистраций за "
                           f"{RAID_WINDOW_SECONDS} с. Подключение ботов приостановлено на {RAID_COOLDOWN_SECONDS // 60} мин.")


def raid_active() -> bool:
    return time.time() < _raid_mode_until


async def maintenance_loop():
    while True:
        await asyncio.sleep(600)
        try:
            now = time.time()
            for k in [k for k, v in _flood_log.items() if not v or now - v[-1] > FLOOD_WINDOW_SECONDS]:
                _flood_log.pop(k, None)
            for k in [k for k, t in _muted_until.items() if t <= now]:
                _muted_until.pop(k, None)
                _flood_strikes.pop(k, None) if _flood_strikes.get(k, 0) < FLOOD_AUTOBAN_STRIKES else None
            for k in [k for k, t in _sub_cache.items() if now - t > SUB_CACHE_TTL]:
                _sub_cache.pop(k, None)
            await dbx("DELETE FROM msg_map WHERE created_at<?", (int(now) - 30 * 86400,))
        except Exception:
            logger.exception("maintenance error")


# ==================== ПОДПИСКА ====================
async def is_subscribed_to_channel(user_id: int):
    try:
        member = await bot.get_chat_member(chat_id=REQUIRED_CHANNEL, user_id=user_id)
    except TelegramAPIError as e:
        logger.warning("get_chat_member failed: %s", e)
        return None
    if member.status in ("creator", "administrator", "member"):
        return True
    if member.status == "restricted":
        return bool(getattr(member, "is_member", False))
    return False


async def ensure_subscribed(user_id: int):
    ts = _sub_cache.get(user_id)
    if ts and time.time() - ts < SUB_CACHE_TTL:
        return True
    res = await is_subscribed_to_channel(user_id)
    if res:
        _sub_cache[user_id] = time.time()
    return res


# ==================== МЕНЮ ГЛАВНОГО БОТА ====================
def get_main_kb(user_id: int):
    kb = [
        [KeyboardButton(text="👤 Мой профиль"), KeyboardButton(text="🤖 Мой бот")],
        [KeyboardButton(text="🎁 Рефералка (Stars)"), KeyboardButton(text="💎 Premium")],
        [KeyboardButton(text="🎧 Поддержка")],
    ]
    if user_id == ADMIN_ID:
        kb.append([KeyboardButton(text="⚙️ Админ-панель")])
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


class CreateBotFSM(StatesGroup):
    waiting_for_token = State()


class ChildFSM(StatesGroup):
    waiting_value = State()
    waiting_broadcast = State()


# ==================== ХЕНДЛЕРЫ ГЛАВНОГО БОТА ====================
@dp.message(CommandStart(), StateFilter(any_state))
async def cmd_start(message: Message, state: FSMContext, command: CommandObject):
    await state.clear()
    m = re.fullmatch(r"ref_(\d{1,15})", command.args or "")
    referrer_id = int(m.group(1)) if m else 0
    u = message.from_user
    is_new = await db_add_user(u.id, u.username, u.full_name, referrer_id)
    if is_new:
        await note_registration()
    await message.answer(
        f"👋 <b>Привет, {esc(u.full_name)}!</b>\n\n"
        f"Здесь ты можешь создать своего <b>личного бота</b> для общения с аудиторией или клиентами. "
        f"Сообщения будут приходить прямо туда, а твой личный аккаунт останется в секрете.\n\n"
        f"<i>Создай бота в 2 клика, настрой оформление и начинай работу!</i>",
        reply_markup=get_main_kb(u.id))


@dp.callback_query(F.data == "check_sub")
async def check_sub_cb(call: CallbackQuery):
    _sub_cache.pop(call.from_user.id, None)
    if await ensure_subscribed(call.from_user.id) is False:
        return await call.answer("Вы ещё не подписаны на канал.", show_alert=True)
    with contextlib.suppress(TelegramAPIError):
        await call.message.edit_text("✅ Подписка подтверждена! Нажмите /start")
    await call.answer()


@dp.message(F.text == "👤 Мой профиль", StateFilter(any_state))
async def cmd_profile(message: Message, state: FSMContext):
    await state.clear()
    row = await db_get_user(message.from_user.id)
    prem = await is_user_premium(row)
    child = await db_get_child_bot(message.from_user.id)
    until = ""
    if prem and row["premium_expires"]:
        until = f" (до {time.strftime('%d.%m.%Y', time.localtime(row['premium_expires']))})"
    await message.answer(
        f"👤 <b>Профиль</b>\n\nID: <code>{message.from_user.id}</code>\n"
        f"Premium: {'💎 активен' + until if prem else 'нет'}\n"
        f"Бот: {child_status(message.from_user.id) if child else 'не подключён'}")


@dp.message(F.text == "🎧 Поддержка", StateFilter(any_state))
async def cmd_support(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        f"🛠 <b>Служба поддержки</b>\n\nЕсли у вас возникли вопросы, технические проблемы или предложения, "
        f"напишите нам напрямую: {esc(SUPPORT_ACCOUNT)}")


# ---------- Premium ----------
@dp.message(F.text == "💎 Premium", StateFilter(any_state))
async def cmd_premium(message: Message, state: FSMContext):
    await state.clear()
    row = await db_get_user(message.from_user.id)
    prem = await is_user_premium(row)
    slots = await promo_slots_left()
    text = ("💎 <b>Premium</b>\n\n• Рассылки чаще (раз в 10 минут вместо часа)\n"
            "• Без рекламной кнопки «Создать такого же бота» у клиентов\n\n")
    kb = None
    if prem:
        text += "✅ У вас уже активен Premium."
    else:
        text += f"Для покупки напишите в поддержку: {esc(SUPPORT_ACCOUNT)}"
        claimed = await dbx("SELECT 1 FROM promo_claims WHERE user_id=?", (message.from_user.id,), "one")
        if slots > 0 and not claimed:
            text += f"\n\n🎁 Промо: осталось <b>{slots}</b> мест — {PROMO_DAYS} дней бесплатно."
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎁 Забрать промо", callback_data="claim_promo")]])
    await message.answer(text, reply_markup=kb)


@dp.callback_query(F.data == "claim_promo")
async def cb_claim_promo(call: CallbackQuery):
    res = await db_claim_promo(call.from_user.id)
    msg = {"ok": f"✅ Premium на {PROMO_DAYS} дней активирован!",
           "already": "Вы уже получали промо.",
           "empty": "Промо-места закончились."}[res]
    await call.answer(msg, show_alert=True)
    if res == "ok":
        with contextlib.suppress(TelegramAPIError):
            await call.message.edit_reply_markup(reply_markup=None)


# ---------- Рефералка ----------
@dp.message(F.text == "🎁 Рефералка (Stars)", StateFilter(any_state))
async def cmd_referral(message: Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    total, active, used, available, pending = await db_referral_stats(uid)
    link = f"https://t.me/{MAIN_BOT_USERNAME}?start=ref_{uid}"
    text = (
        f"🎁 <b>Заработай {REF_REWARD} Telegram Stars!</b>\n\n"
        f"Приглашай друзей создать своего бота. За <b>{REF_STEP} активных пользователей</b> "
        f"ты получишь {REF_REWARD} Stars.\n\n"
        f"🛡 <i>Антифрод: активным считается тот, кто создал зеркало и получил в него сообщение "
        f"от стороннего клиента (не от владельца и не от пригласившего).</i>\n\n"
        f"<b>Твоя статистика:</b>\n👥 Всего переходов: <b>{total}</b>\n"
        f"🔥 Активных ботов: <b>{active}</b> (нужно {REF_STEP})\n\n"
        f"🔗 <b>Твоя ссылка:</b>\n<code>{link}</code>")
    kb = None
    if available > 0:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(
            text=f"💸 Вывести {REF_REWARD} Stars", callback_data="req_stars")]])
    if pending:
        text += "\n\n⏳ <i>Ваша заявка на вывод обрабатывается администрацией.</i>"
    await message.answer(text, reply_markup=kb)


@dp.callback_query(F.data == "req_stars")
async def process_stars_request(call: CallbackQuery):
    wid = await db_create_withdrawal(call.from_user.id)   # права проверяются на сервере
    if wid is None:
        return await call.answer("Вывод сейчас недоступен.", show_alert=True)
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Выплачено", callback_data=f"wd:ok:{wid}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"wd:no:{wid}")]])
    uname = f"@{esc(call.from_user.username)}" if call.from_user.username else "без username"
    await notify_admin(f"🔔 <b>Заявка #{wid} на вывод {REF_REWARD} Stars</b>\n"
                       f"От: <code>{call.from_user.id}</code> ({uname})", reply_markup=kb)
    with contextlib.suppress(TelegramAPIError):
        await call.message.edit_text(call.message.html_text + "\n\n✅ <b>Заявка отправлена!</b> Ожидайте начисления.",
                                     reply_markup=None)
    await call.answer()


@dp.callback_query(F.data.startswith("wd:"), is_admin)
async def admin_withdrawal(call: CallbackQuery):
    try:
        _, act, wid = call.data.split(":")
        wid = int(wid)
    except ValueError:
        return await call.answer()
    status = "paid" if act == "ok" else "rejected"
    row = await dbx("SELECT user_id FROM ref_withdrawals WHERE id=? AND status='pending'", (wid,), "one")
    if not row:
        return await call.answer("Уже обработана.", show_alert=True)
    await dbx("UPDATE ref_withdrawals SET status=? WHERE id=?", (status, wid))
    with contextlib.suppress(TelegramAPIError):
        await bot.send_message(row["user_id"], f"💸 Ваша заявка на {REF_REWARD} Stars "
                               + ("выплачена ✅" if status == "paid" else "отклонена ❌"))
        await call.message.edit_reply_markup(reply_markup=None)
    await call.answer("Готово")


# ---------- Мой бот ----------
def child_status(owner_id: int) -> str:
    t = running_child_bots.get(owner_id)
    if owner_id in child_invalid:
        return "🔴 Токен недействителен"
    return "🟢 В сети" if t and not t.done() else "🔴 Остановлен"


@dp.message(F.text == "🤖 Мой бот", StateFilter(any_state))
async def manage_bot_cmd(message: Message, state: FSMContext):
    await state.clear()
    child = await db_get_child_bot(message.from_user.id)
    if not child:
        await message.answer(
            "🤖 <b>Подключение бота</b>\n\n1. Зайди в официального @BotFather\n"
            "2. Нажми <code>/newbot</code> и придумай название\n"
            "3. Скопируй <b>Токен</b> (длинный набор букв и цифр)\n"
            "4. Отправь его прямо сюда в чат 👇")
        await state.set_state(CreateBotFSM.waiting_for_token)
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Перезапустить", callback_data="restart_bot")],
        [InlineKeyboardButton(text="🗑 Отключить бота", callback_data="delete_bot_ask")]])
    await message.answer(
        f"🤖 <b>Твой бот подключён!</b> ({child_status(message.from_user.id)})\n\n"
        f"Вся кастомизация (тексты, ссылки, антиспам) настраивается <b>внутри твоего бота</b>. "
        f"Просто перейди в него и нажми «⚙️ Настройки».", reply_markup=kb)


@dp.callback_query(F.data == "restart_bot")
async def cb_restart_bot(call: CallbackQuery):
    child = await db_get_child_bot(call.from_user.id)
    if not child:
        return await call.answer("Бот не найден.", show_alert=True)
    await stop_child(call.from_user.id)
    start_child(call.from_user.id, child["bot_token"])
    await call.answer("Перезапускаю…")


@dp.callback_query(F.data == "delete_bot_ask")
async def cb_delete_ask(call: CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Да, удалить", callback_data="delete_bot_yes"),
        InlineKeyboardButton(text="↩️ Отмена", callback_data="delete_bot_no")]])
    await call.message.edit_text("⚠️ Отключить бота? Клиенты и настройки будут удалены безвозвратно.", reply_markup=kb)
    await call.answer()


@dp.callback_query(F.data == "delete_bot_no")
async def cb_delete_no(call: CallbackQuery):
    await call.message.edit_text("Отменено.")
    await call.answer()


@dp.callback_query(F.data == "delete_bot_yes")
async def cb_delete_yes(call: CallbackQuery):
    await stop_child(call.from_user.id)
    await db_delete_child_bot(call.from_user.id)
    child_invalid.discard(call.from_user.id)
    await call.message.edit_text("🗑 Бот отключён и удалён.")
    await call.answer()


# ---------- Админ ----------
@dp.message(F.text == "⚙️ Админ-панель", is_admin, StateFilter(any_state))
@dp.message(Command("stats"), is_admin)
async def admin_panel(message: Message, state: FSMContext):
    await state.clear()
    users = (await dbx("SELECT COUNT(*) c FROM users", fetch="one"))["c"]
    bots = (await dbx("SELECT COUNT(*) c FROM child_bots", fetch="one"))["c"]
    pend = (await dbx("SELECT COUNT(*) c FROM ref_withdrawals WHERE status='pending'", fetch="one"))["c"]
    alive = sum(1 for t in running_child_bots.values() if not t.done())
    await message.answer(
        f"⚙️ <b>Админ-панель</b>\n\nПользователей: {users}\nЗеркал: {bots} (онлайн: {alive})\n"
        f"Глобальных банов: {len(_global_bans)}\nЗаявок на вывод: {pend}\n\n"
        f"<code>/ban ID</code> · <code>/unban ID</code>\n<code>/premium ID ДНИ</code> · <code>/unpremium ID</code>\n"
        f"<code>/withdrawals</code>")


def _parse_id(command: CommandObject, idx=0):
    parts = (command.args or "").split()
    return int(parts[idx]) if len(parts) > idx and parts[idx].lstrip("-").isdigit() else None


@dp.message(Command("ban"), is_admin)
async def admin_ban(message: Message, command: CommandObject):
    uid = _parse_id(command)
    if not uid or uid == ADMIN_ID:
        return await message.answer("Использование: /ban ID")
    await db_global_ban(uid)
    await stop_child(uid)
    await message.answer(f"🚫 {uid} забанен, его зеркало остановлено.")


@dp.message(Command("unban"), is_admin)
async def admin_unban(message: Message, command: CommandObject):
    uid = _parse_id(command)
    if not uid:
        return await message.answer("Использование: /unban ID")
    await db_global_ban(uid, on=False)
    child = await db_get_child_bot(uid)
    if child:
        start_child(uid, child["bot_token"])
    await message.answer(f"✅ {uid} разбанен.")


@dp.message(Command("premium"), is_admin)
async def admin_premium(message: Message, command: CommandObject):
    uid, days = _parse_id(command, 0), _parse_id(command, 1)
    if not uid or not days or days <= 0 or not await db_get_user(uid):
        return await message.answer("Использование: /premium ID ДНИ (пользователь должен быть в базе)")
    await dbx("UPDATE users SET premium=1, premium_expires=? WHERE id=?", (int(time.time()) + days * 86400, uid))
    await message.answer(f"💎 Premium выдан {uid} на {days} дн.")


@dp.message(Command("unpremium"), is_admin)
async def admin_unpremium(message: Message, command: CommandObject):
    uid = _parse_id(command)
    if not uid:
        return await message.answer("Использование: /unpremium ID")
    await dbx("UPDATE users SET premium=0, premium_expires=0 WHERE id=?", (uid,))
    await message.answer("Готово.")


@dp.message(Command("withdrawals"), is_admin)
async def admin_withdrawals(message: Message):
    rows = await dbx("SELECT id, user_id FROM ref_withdrawals WHERE status='pending' ORDER BY id LIMIT 10", fetch="all")
    if not rows:
        return await message.answer("Нет активных заявок.")
    for r in rows:
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Выплачено", callback_data=f"wd:ok:{r['id']}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"wd:no:{r['id']}")]])
        await message.answer(f"Заявка #{r['id']} от <code>{r['user_id']}</code>", reply_markup=kb)


# ---------- Токен нового бота (регистрируется ПОСЛЕ всех кнопок меню) ----------
@dp.message(CreateBotFSM.waiting_for_token)
async def process_new_bot_token(message: Message, state: FSMContext):
    token = (message.text or "").strip()
    if not TOKEN_RE.match(token):
        return await message.answer("❌ Токен выглядит неверно. Скопируй его из @BotFather и отправь снова.")
    with contextlib.suppress(TelegramAPIError):
        await message.delete()   # токен — секрет, не оставляем в чате
    if raid_active():
        return await message.answer("⏳ Подключение ботов временно приостановлено. Попробуйте через несколько минут.")
    if token == BOT_TOKEN:
        return await message.answer("❌ Это токен главного бота. Создайте нового у @BotFather.")
    if message.from_user.id in _global_bans:
        return
    if await db_get_child_bot(message.from_user.id):
        await state.clear()
        return await message.answer("У вас уже есть подключённый бот.")

    status = await message.answer("⏳ Проверяем...")
    test_bot = Bot(token=token)
    try:
        me = await test_bot.get_me()
    except TelegramUnauthorizedError:
        return await status.edit_text("❌ Токен не работает. Возможно, он скопирован с ошибкой или отозван.")
    except TelegramAPIError:
        return await status.edit_text("⚠️ Telegram сейчас недоступен, попробуйте чуть позже.")
    finally:
        await test_bot.session.close()

    if not await db_add_child_bot(message.from_user.id, token):
        return await status.edit_text("❌ Этот токен уже используется.")
    start_child(message.from_user.id, token)
    await state.clear()
    await status.edit_text(f"✅ <b>Бот @{esc(me.username)} запущен!</b>\nПереходи в него и настраивай дизайн под себя.")


@dp.message(F.chat.type == "private", StateFilter(None))
async def fallback(message: Message):
    await message.answer("Используйте меню ниже 👇", reply_markup=get_main_kb(message.from_user.id))


# ==================== ДВИЖОК ЗЕРКАЛ ====================
FIELD_MAP = {
    "set_start_msg": ("custom_start", 800, "Отправьте текст, который клиенты увидят при запуске бота:"),
    "set_bio": ("bio", 800, "Отправьте текст раздела «Обо мне»:"),
    "set_links": ("social_links", 300, "Отправьте ваши ссылки (соцсети, портфолио, сайт):"),
    "set_hours": ("working_hours", 100, "Отправьте график работы (например: Пн–Пт 10:00–19:00):"),
}


def render_settings(d):
    text = (
        f"⚙️ <b>Оформление и Настройки</b>\n\n"
        f"💬 <b>Приветствие (/start):</b>\n{esc(d['custom_start'])}\n\n"
        f"📝 <b>Обо мне:</b>\n{esc(d['bio'])}\n\n"
        f"🕒 <b>График:</b> {esc(d['working_hours'])}\n🔗 <b>Ссылки:</b> {esc(d['social_links'])}")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎨 Изменить приветствие", callback_data="set_start_msg")],
        [InlineKeyboardButton(text="📝 Изменить Bio", callback_data="set_bio"),
         InlineKeyboardButton(text="🔗 Ссылки", callback_data="set_links")],
        [InlineKeyboardButton(text="🕒 График", callback_data="set_hours"),
         InlineKeyboardButton(text=f"🛡 Антиспам: {'вкл' if d['spam_filter'] else 'выкл'}", callback_data="toggle_spam")]])
    return text, kb


def build_child_dispatcher(owner_id: int) -> Dispatcher:
    cdp = Dispatcher(storage=MemoryStorage())
    cdp.message.filter(F.chat.type == "private")
    is_owner = F.from_user.id == owner_id
    not_owner = F.from_user.id != owner_id

    def child_kb(owner: bool, premium: bool = False):
        if owner:
            return ReplyKeyboardMarkup(keyboard=[
                [KeyboardButton(text="⚙️ Настройки"), KeyboardButton(text="📊 Статистика")],
                [KeyboardButton(text="📢 Рассылка"), KeyboardButton(text="🚫 Бан-лист")]], resize_keyboard=True)
        rows = [[KeyboardButton(text="✉️ Написать"), KeyboardButton(text="ℹ️ Обо мне")]]
        if not premium:
            rows.append([KeyboardButton(text="🚀 Создать такого же бота")])
        return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

    # ---- gate: бан / флуд ----
    async def gate(handler, event, data):
        user = event.from_user
        if not user or user.id == owner_id:
            return await handler(event, data)
        if user.id in _global_bans or await db_is_mirror_banned(owner_id, user.id):
            if isinstance(event, CallbackQuery):
                await event.answer()
            return
        verdict = flood_hit((owner_id, user.id))
        if verdict != "ok":
            if verdict == "mute" and isinstance(event, Message):
                await event.answer(f"⏳ Слишком много сообщений. Подождите {FLOOD_MUTE_SECONDS // 60} мин.")
            elif verdict == "ban":
                await db_mirror_ban(owner_id, user.id)
            if isinstance(event, CallbackQuery):
                await event.answer()
            return
        return await handler(event, data)

    cdp.message.outer_middleware(gate)
    cdp.callback_query.outer_middleware(gate)

    async def owner_premium():
        return await is_user_premium(await db_get_user(owner_id))

    @cdp.message(CommandStart(), StateFilter(any_state))
    async def child_start(message: Message, state: FSMContext):
        await state.clear()
        owner = message.from_user.id == owner_id
        if owner:
            text = "👋 <b>Привет, Владелец!</b>\nСообщения от клиентов будут приходить сюда. Управляй ботом через меню ниже."
        else:
            await db_touch_client(owner_id, message.from_user.id)
            d = await db_get_child_bot(owner_id)
            text = esc(d["custom_start"]) if d else "Привет!"
        await message.answer(text, reply_markup=child_kb(owner, await owner_premium() if not owner else False))

    @cdp.message(Command("cancel"), StateFilter(any_state))
    async def child_cancel(message: Message, state: FSMContext):
        await state.clear()
        await message.answer("Отменено.")

    # ---- меню владельца ----
    @cdp.message(F.text == "⚙️ Настройки", is_owner, StateFilter(any_state))
    async def ch_settings(message: Message, state: FSMContext):
        await state.clear()
        text, kb = render_settings(await db_get_child_bot(owner_id))
        await message.answer(text, reply_markup=kb)

    @cdp.message(F.text == "📊 Статистика", is_owner, StateFilter(any_state))
    async def ch_stats(message: Message, state: FSMContext):
        await state.clear()
        clients = (await dbx("SELECT COUNT(*) c FROM clients WHERE owner_id=?", (owner_id,), "one"))["c"]
        banned = (await dbx("SELECT COUNT(*) c FROM mirror_bans WHERE owner_id=?", (owner_id,), "one"))["c"]
        d = await db_get_child_bot(owner_id)
        await message.answer(f"📊 <b>Статистика</b>\n\nКлиентов: {clients}\nСообщений получено: {d['msg_count']}\nВ бане: {banned}")

    @cdp.message(F.text == "🚫 Бан-лист", is_owner, StateFilter(any_state))
    async def ch_banlist(message: Message, state: FSMContext):
        await state.clear()
        rows = await dbx("SELECT client_id FROM mirror_bans WHERE owner_id=? ORDER BY banned_at DESC LIMIT 20", (owner_id,), "all")
        if not rows:
            return await message.answer("Бан-лист пуст.")
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(
            text=f"♻️ Разбанить {r['client_id']}", callback_data=f"munban:{r['client_id']}")] for r in rows])
        await message.answer("🚫 <b>Заблокированные</b> (последние 20):", reply_markup=kb)

    @cdp.message(F.text == "📢 Рассылка", is_owner, StateFilter(any_state))
    async def ch_broadcast(message: Message, state: FSMContext):
        await state.clear()
        d = await db_get_child_bot(owner_id)
        cd = BROADCAST_COOLDOWN_PREMIUM if await owner_premium() else BROADCAST_COOLDOWN
        left = d["last_broadcast"] + cd - int(time.time())
        if left > 0:
            return await message.answer(f"⏳ Следующая рассылка будет доступна через {left // 60 + 1} мин.")
        await state.set_state(ChildFSM.waiting_broadcast)
        await message.answer("Отправьте сообщение для рассылки (текст, фото и т.д.).\n\n/cancel — отмена")

    # ---- меню клиента ----
    @cdp.message(F.text == "✉️ Написать", not_owner, StateFilter(any_state))
    async def cl_write(message: Message):
        await message.answer("Просто отправьте сообщение сюда — оно будет передано владельцу. ✍️")

    @cdp.message(F.text == "ℹ️ Обо мне", StateFilter(any_state))
    async def ch_info(message: Message):
        d = await db_get_child_bot(owner_id)
        await message.answer(
            f"ℹ️ <b>Информация:</b>\n\n📝 {esc(d['bio'])}\n\n🕒 График: {esc(d['working_hours'])}\n"
            f"🔗 Контакты/Портфолио:\n{esc(d['social_links'])}")

    @cdp.message(F.text == "🚀 Создать такого же бота", not_owner, StateFilter(any_state))
    async def cl_clone(message: Message):
        await message.answer(f"🚀 Создайте своего бота: https://t.me/{MAIN_BOT_USERNAME}?start=ref_{owner_id}")

    # ---- callbacks (только владелец) ----
    @cdp.callback_query(F.data.in_(set(FIELD_MAP)), is_owner)
    async def ch_set_callbacks(call: CallbackQuery, state: FSMContext):
        field, limit, prompt = FIELD_MAP[call.data]
        await state.set_state(ChildFSM.waiting_value)
        await state.update_data(field=field, limit=limit)
        await call.message.answer(prompt + "\n\n/cancel — отмена")
        await call.answer()

    @cdp.callback_query(F.data == "toggle_spam", is_owner)
    async def ch_toggle_spam(call: CallbackQuery):
        d = await db_get_child_bot(owner_id)
        await db_update_child_bot(owner_id, "spam_filter", 0 if d["spam_filter"] else 1)
        text, kb = render_settings(await db_get_child_bot(owner_id))
        with contextlib.suppress(TelegramBadRequest):
            await call.message.edit_text(text, reply_markup=kb)
        await call.answer("Антиспам: ссылки от клиентов блокируются" if not d["spam_filter"] else "Антиспам выключен")

    @cdp.callback_query(F.data.startswith("mban:"), is_owner)
    async def ch_ban(call: CallbackQuery):
        cid = int(call.data.split(":")[1])
        await db_mirror_ban(owner_id, cid)
        with contextlib.suppress(TelegramAPIError):
            await call.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="♻️ Разбанить", callback_data=f"munban:{cid}")]]))
        await call.answer("Заблокирован")

    @cdp.callback_query(F.data.startswith("munban:"), is_owner)
    async def ch_unban(call: CallbackQuery):
        await db_mirror_ban(owner_id, int(call.data.split(":")[1]), on=False)
        with contextlib.suppress(TelegramAPIError):
            await call.message.edit_reply_markup(reply_markup=None)
        await call.answer("Разбанен")

    @cdp.callback_query(F.data == "bc_cancel", is_owner)
    async def ch_bc_cancel(call: CallbackQuery, state: FSMContext):
        await state.clear()
        await call.message.edit_text("Рассылка отменена.")
        await call.answer()

    @cdp.callback_query(F.data == "bc_go", is_owner)
    async def ch_bc_go(call: CallbackQuery, state: FSMContext):
        data = await state.get_data()
        await state.clear()
        if "msg_id" not in data:
            return await call.answer("Сессия истекла, начните заново.", show_alert=True)
        cd = BROADCAST_COOLDOWN_PREMIUM if await owner_premium() else BROADCAST_COOLDOWN
        if not await db_try_start_broadcast(owner_id, cd):
            return await call.answer("Рассылка сейчас недоступна (лимит по времени).", show_alert=True)
        await call.message.edit_text("📤 Рассылка запущена, пришлю отчёт по завершении.")
        spawn(run_broadcast(call.bot, owner_id, data["chat_id"], data["msg_id"]))
        await call.answer()

    # ---- FSM владельца ----
    @cdp.message(ChildFSM.waiting_value, is_owner, F.text)
    async def ch_save_value(message: Message, state: FSMContext):
        if message.text.startswith("/"):
            return await message.answer("Отправьте обычный текст или /cancel.")
        data = await state.get_data()
        await db_update_child_bot(owner_id, data["field"], message.text.strip()[:data["limit"]])
        await state.clear()
        await message.answer("✅ Сохранено!")

    @cdp.message(ChildFSM.waiting_broadcast, is_owner)
    async def ch_bc_msg(message: Message, state: FSMContext):
        n = len(await db_broadcast_targets(owner_id))
        await state.update_data(chat_id=message.chat.id, msg_id=message.message_id)
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Отправить", callback_data="bc_go"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="bc_cancel")]])
        await message.reply(f"Отправить это сообщение {n} получателям?", reply_markup=kb)

    # ---- ответ владельца клиенту (reply на сообщение) ----
    @cdp.message(is_owner, StateFilter(None))
    async def owner_msg(message: Message):
        if message.text and message.text.startswith("/"):
            return
        rep = message.reply_to_message
        if not rep:
            return await message.answer("ℹ️ Чтобы ответить клиенту, сделайте reply на его сообщение.")
        client_id = await db_msg_client(owner_id, rep.message_id)
        if not client_id:
            return await message.answer("❌ Не нашёл получателя (сообщение слишком старое).")
        try:
            await message.copy_to(client_id)
            await message.answer("✅ Доставлено")
        except TelegramForbiddenError:
            await message.answer("⚠️ Клиент заблокировал бота.")
        except TelegramAPIError as e:
            await message.answer(f"⚠️ Не удалось доставить: {esc(e.message)}")

    # ---- сообщение клиента владельцу ----
    @cdp.message(not_owner)
    async def client_msg(message: Message):
        d = await db_get_child_bot(owner_id)
        if not d:
            return
        txt = message.text or message.caption or ""
        if d["spam_filter"] and SPAM_RE.search(txt):
            return await message.answer("🛡 Сообщения со ссылками и упоминаниями не принимаются.")
        user = message.from_user
        await db_touch_client(owner_id, user.id, count_msg=True)
        header = f"✉️ <b>{esc(user.full_name)}</b>" + (f" (@{esc(user.username)})" if user.username else "") \
                 + f"\nID: <code>{user.id}</code>"
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚫 Заблокировать", callback_data=f"mban:{user.id}")]])
        try:
            h = await message.bot.send_message(owner_id, header, reply_markup=kb)
            c = await message.bot.copy_message(
                chat_id=owner_id, from_chat_id=message.chat.id, message_id=message.message_id,
                reply_parameters=ReplyParameters(message_id=h.message_id))
        except TelegramAPIError:
            logger.warning("delivery to owner %s failed", owner_id)
            return await message.answer("⚠️ Не удалось доставить сообщение. Попробуйте позже.")
        await db_map_msgs(owner_id, [h.message_id, c.message_id], user.id)
        await message.answer("✅ Сообщение отправлено.")

    return cdp


async def run_broadcast(child_bot: Bot, owner_id: int, from_chat: int, msg_id: int):
    ok = fail = 0
    for cid in await db_broadcast_targets(owner_id):
        for attempt in range(2):
            try:
                await child_bot.copy_message(cid, from_chat, msg_id)
                ok += 1
                break
            except TelegramRetryAfter as e:
                await asyncio.sleep(e.retry_after + 1)
            except TelegramAPIError:
                fail += 1
                break
        else:
            fail += 1
        await asyncio.sleep(0.05)   # ~20 сообщений/сек — в пределах лимитов Telegram
    with contextlib.suppress(TelegramAPIError):
        await child_bot.send_message(owner_id, f"📢 Рассылка завершена.\n✅ Доставлено: {ok}\n❌ Не доставлено: {fail}")


async def _run_child(owner_id: int, token: str):
    child_bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    backoff = 5
    try:
        while True:
            try:
                await child_bot.get_me()      # раннее обнаружение мёртвого токена
                child_invalid.discard(owner_id)
                cdp = build_child_dispatcher(owner_id)
                await cdp.start_polling(child_bot, handle_signals=False, close_bot_session=False,
                                        allowed_updates=["message", "callback_query"])
                return
            except asyncio.CancelledError:
                raise
            except TelegramUnauthorizedError:
                child_invalid.add(owner_id)
                logger.warning("child %s: token invalid", owner_id)
                with contextlib.suppress(TelegramAPIError):
                    await bot.send_message(owner_id, "⚠️ Токен вашего бота недействителен (отозван?). "
                                                     "Отключите бота и подключите заново.")
                return
            except Exception:
                logger.exception("child %s crashed, restart in %ss", owner_id, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 300)
    finally:
        with contextlib.suppress(Exception):
            await child_bot.session.close()


def start_child(owner_id: int, token: str):
    old = running_child_bots.get(owner_id)
    if old and not old.done():
        return
    child_invalid.discard(owner_id)
    running_child_bots[owner_id] = asyncio.create_task(_run_child(owner_id, token), name=f"child-{owner_id}")


async def stop_child(owner_id: int):
    t = running_child_bots.pop(owner_id, None)
    if t and not t.done():
        t.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await t


# ==================== MIDDLEWARE ГЛАВНОГО БОТА ====================
async def main_gate_middleware(handler, event, data):
    global _sub_alert_ts
    user = event.from_user
    if not user:
        return await handler(event, data)
    if isinstance(event, Message) and event.chat.type != "private":
        return
    if user.id == ADMIN_ID:
        return await handler(event, data)
    is_cb = isinstance(event, CallbackQuery)
    if user.id in _global_bans:
        if is_cb:
            await event.answer()
        return

    verdict = flood_hit(user.id)
    if verdict != "ok":
        if verdict == "mute" and not is_cb:
            await event.answer(f"⏳ Слишком много сообщений. Пауза {FLOOD_MUTE_SECONDS // 60} мин.")
        elif verdict == "ban":
            await db_global_ban(user.id)
            await stop_child(user.id)
            await notify_admin(f"🚫 Автобан за флуд: <code>{user.id}</code>")
        if is_cb:
            await event.answer()
        return

    if not (is_cb and event.data == "check_sub"):
        sub = await ensure_subscribed(user.id)
        if sub is None and time.time() - _sub_alert_ts > 3600:
            _sub_alert_ts = time.time()
            await notify_admin(f"⚠️ Не удалось проверить подписку на {esc(REQUIRED_CHANNEL)}. "
                               f"Убедитесь, что бот — администратор канала. Проверка временно пропускается.")
        if sub is False:
            if is_cb:
                await event.answer("Нужна подписка!", show_alert=True)
            else:
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔗 Подписаться", url=f"https://t.me/{REQUIRED_CHANNEL.lstrip('@')}")],
                    [InlineKeyboardButton(text="✅ Проверить", callback_data="check_sub")]])
                await event.answer(f"🔒 <b>Для использования платформы подписка на {esc(REQUIRED_CHANNEL)} обязательна.</b>",
                                   reply_markup=kb)
            return
    return await handler(event, data)


# ==================== ЗАПУСК ====================
async def main():
    global MAIN_BOT_USERNAME
    await init_db()
    await load_global_bans()
    me = await bot.get_me()
    MAIN_BOT_USERNAME = me.username
    dp.message.outer_middleware(main_gate_middleware)
    dp.callback_query.outer_middleware(main_gate_middleware)

    for row in await db_all_child_bots():
        if row["owner_id"] not in _global_bans:
            start_child(row["owner_id"], row["bot_token"])
            await asyncio.sleep(0.1)
    spawn(maintenance_loop())
    logger.info("Запущен @%s, зеркал: %s", me.username, len(running_child_bots))
    try:
        await dp.start_polling(bot, allowed_updates=["message", "callback_query"])
    finally:
        for oid in list(running_child_bots):
            await stop_child(oid)
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
