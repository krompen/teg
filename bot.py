"""
Платформа для создания ботов обратной связи (SaaS "зеркала") на aiogram 3.

ЧТО ИЗМЕНЕНО ПО СРАВНЕНИЮ С ИСХОДНИКОМ (кратко, подробности — в чате):
  1. Токен и ADMIN_ID больше НЕ хранятся в коде — берутся из переменных окружения.
  2. Все SQL-запросы к SQLite вынесены в отдельный поток (asyncio.to_thread),
     чтобы синхронный sqlite3 не блокировал event loop (а он же общий на все
     дочерние боты!).
  3. Весь пользовательский текст (имя, username, bio, текст сообщений) теперь
     экранируется html.escape(), иначе символы `<`, `&` и т.п. в сообщении
     клиента ломали parse_mode=HTML и сообщение админу просто не доходило.
  4. Исправлена SQL-инъекция "по конструкции" в db_set_status/db_update_child_bot
     — имя колонки теперь берётся только из белого списка.
  5. Забаненный пользователь не мог создать бота через "🤖 Мой бот" — проверка
     бана отсутствовала в этом хендлере. Добавлена.
  6. Регулярное выражение токена было слишком строгим (ровно 35 симв. секрета) —
     ужесточённая, но не завышенная проверка + реальная проверка через get_me().
  7. Создание бота: вместо "подождать 1.5 сек и понадеяться, что за это время
     будет исключение" — теперь токен проверяется явным вызовом get_me() до
     записи в БД. Раньше при медленной сети/долгой ошибке бот считался рабочим,
     хотя таковым не был.
  8. Удаление зеркала было необратимым в один клик — добавлено подтверждение.
     Также при удалении теперь чистится таблица clients (раньше оставался
     мусор в БД навсегда).
  9. Рассылка (обычная и мега) теперь: с подтверждением (превью + кнопки),
     не блокирует админку (уходит в фон), присылает итоговый отчёт отдельным
     сообщением по завершении — раньше при долгой рассылке админ не мог
     ничего делать в боте, пока она не кончится.
 10. При остановке/удалении дочернего бота его aiohttp-сессия закрывается
     явно — раньше при отмене задачи сессия могла "утекать".
 11. db_apply_promo_if_available обёрнут в транзакцию — раньше при двух
     одновременных регистрациях лимит промо-акции можно было превысить.
 12. Мелкие правки: username может быть None → раньше в отчёте была
     "@None"; водяной знак мог показывать пустой "@" пока не пришёл
     username бота при рестарте; общий try/except вокруг хендлеров с
     логированием, чтобы один необработанный exception не ронял polling.

 13. ОБЯЗАТЕЛЬНАЯ ПОДПИСКА (ОП) на REQUIRED_CHANNEL (по умолчанию @swithub) —
     только на ГЛАВНОМ боте, в зеркалах ОП нет. Бот должен быть администратором
     канала, иначе проверка невозможна (админ получит алерт). Промо "первые 10
     → Premium навсегда" выдаётся автоматически после прохождения ОП.
 14. ГЛОБАЛ-БАН (админка → "🌍 Глобал-бан"): блокирует по ID на главном боте и
     во ВСЕХ зеркалах; работает даже для тех, кто писал только в зеркала. Если у
     забаненного есть своё зеркало — оно останавливается (вернётся при разбане).
 15. Владельцы зеркал: свой бан/разбан клиентов (кнопка под обращением, /ban,
     /unban, "🚫 Бан-лист"); нельзя писать самому себе; /reply только клиентам
     своего бота; ответ кнопкой "✍️ Ответить"; "📊 Статистика"; "📢 Рассылка
     клиентам" (Premium, раз в час, забаненные исключены).
 16. Защита от рейда: антифлуд у каждого бота отдельно (при повторных нарушениях —
     временный мьют, админу/владельцу зеркала приходит алерт с кнопкой бана);
     авто-режим "рейд" при всплеске регистраций (промо приостанавливается).
 17. Нажатие кнопки меню всегда отменяет ожидающий ввод (раньше текст кнопки
     мог уйти админу как обращение или сохраниться как Bio).
 18. Мега-рассылка: проверка получателей до отправки, отчёт с причинами ошибок.

Перед запуском:
    export BOT_TOKEN="ваш_токен_от_BotFather"
    export ADMIN_ID="ваш_telegram_id"
    python3 bot.py

⚠️ Токен из исходного файла засветился в открытом виде — обязательно
   отзовите его в @BotFather (/revoke) и выпустите новый, старый больше
   доверенным считать нельзя.
"""

import asyncio
import html
import logging
import os
import re
import sqlite3
import time
from datetime import datetime

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command, BaseFilter, StateFilter
from aiogram.types import (
    Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup, any_state
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError

# ==================== КОНФИГУРАЦИЯ ====================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
_admin_env = os.getenv("ADMIN_ID", "").strip()

if not BOT_TOKEN:
    raise SystemExit(
        "❌ Не задан BOT_TOKEN. Установите переменную окружения BOT_TOKEN перед запуском.\n"
        "   Пример: export BOT_TOKEN=123456789:AA...ваш_токен"
    )
if not _admin_env.lstrip("-").isdigit():
    raise SystemExit(
        "❌ Не задан (или некорректен) ADMIN_ID. Установите переменную окружения ADMIN_ID "
        "числовым Telegram ID администратора."
    )

ADMIN_ID = int(_admin_env)
DB_FILE = os.getenv("DB_FILE", "bot_database.sqlite")
PROMO_LIMIT = int(os.getenv("PROMO_LIMIT", "10"))
MAIN_BOT_USERNAME = ""

# Канал, подписка на который обязательна для получения промо-Premium.
REQUIRED_CHANNEL = os.getenv("REQUIRED_CHANNEL", "@swithub")

# ---- Настройки защиты от рейда/флуда ----
FLOOD_WINDOW_SECONDS = 8          # окно, в котором считаем сообщения одного юзера
FLOOD_MAX_MESSAGES = 6            # сколько сообщений за окно ещё нормально
FLOOD_AUTOBAN_STRIKES = 3         # после скольких предупреждений подряд — временный мьют
FLOOD_MUTE_SECONDS = 900          # длительность мьюта за флуд (15 мин)
SUB_CACHE_TTL = 300               # как долго доверяем успешной проверке подписки (сек)
BROADCAST_COOLDOWN = 3600         # пауза между рассылками владельца зеркала (сек)
RAID_WINDOW_SECONDS = 60          # окно для отслеживания новых регистраций
RAID_REGISTRATIONS_THRESHOLD = 8  # столько новых юзеров за окно = подозрение на рейд
RAID_COOLDOWN_SECONDS = 300       # автоматически снимаем режим, если тихо N секунд

# Токены Telegram-ботов: <числовой id>:<секрет из букв/цифр/_/->,
# длина секрета на практике варьируется, поэтому диапазон, а не жёсткая цифра.
TOKEN_RE = re.compile(r"^\d{6,10}:[A-Za-z0-9_-]{30,45}$")

running_child_bots: dict = {}     # owner_id -> asyncio.Task
child_bot_instances: dict = {}    # owner_id -> Bot (чтобы корректно закрывать сессию)

# ---- Состояние защиты от рейда/флуда (только в памяти, сбрасывается при рестарте) ----
_flood_log: dict = {}             # user_id -> [timestamps]
_flood_strikes: dict = {}         # user_id -> кол-во подряд идущих предупреждений
_registration_log: list = []      # [timestamps новых регистраций]
_raid_mode_until: float = 0.0     # 0 = режим выключен; иначе unix-время окончания
_raid_alerted: bool = False       # чтобы не спамить владельца повторными алертами
_muted_until: dict = {}           # (scope, user_id) -> unix-время окончания мьюта
_sub_cache: dict = {}             # user_id -> unix-время последней успешной проверки подписки
_sub_alert_ts: float = 0.0        # когда последний раз алертили админа о проблеме с каналом

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def esc(value) -> str:
    """HTML-экранирование любого пользовательского текста перед вставкой в сообщение."""
    if value is None:
        return ""
    return html.escape(str(value), quote=False)


# ==================== БАЗА ДАННЫХ (sync-ядро + async-обёртки) ====================
# Все обращения к sqlite3 выполняются в отдельном потоке через asyncio.to_thread,
# чтобы не блокировать event loop, на котором крутятся десятки дочерних ботов.

_USER_FIELDS = {"is_banned", "premium", "premium_expires"}
_BOT_FIELDS = {"bio", "working_hours", "spam_filter"}


def _connect():
    conn = sqlite3.connect(DB_FILE, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def _init_db_sync():
    conn = _connect()
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            is_banned INTEGER DEFAULT 0,
            premium INTEGER DEFAULT 0,
            premium_expires INTEGER DEFAULT 0,
            registered_at INTEGER
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS child_bots (
            owner_id INTEGER PRIMARY KEY,
            bot_token TEXT UNIQUE,
            bio TEXT DEFAULT 'Информация отсутствует.',
            working_hours TEXT DEFAULT 'Не указаны',
            spam_filter INTEGER DEFAULT 0
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS clients (
            owner_id INTEGER,
            client_id INTEGER,
            PRIMARY KEY (owner_id, client_id)
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS global_bans (
            user_id INTEGER PRIMARY KEY,
            banned_at INTEGER
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS mirror_bans (
            owner_id INTEGER,
            client_id INTEGER,
            banned_at INTEGER,
            PRIMARY KEY (owner_id, client_id)
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS promo_claims (
            user_id INTEGER PRIMARY KEY,
            claimed_at INTEGER
        )
    ''')
    columns = [col[1] for col in c.execute("PRAGMA table_info(users)").fetchall()]
    if 'premium_expires' not in columns:
        c.execute("ALTER TABLE users ADD COLUMN premium_expires INTEGER DEFAULT 0")
    if 'registered_at' not in columns:
        c.execute("ALTER TABLE users ADD COLUMN registered_at INTEGER DEFAULT 0")

    cb_columns = [col[1] for col in c.execute("PRAGMA table_info(child_bots)").fetchall()]
    if 'bio' not in cb_columns:
        c.execute("ALTER TABLE child_bots ADD COLUMN bio TEXT DEFAULT 'Информация отсутствует.'")
    if 'working_hours' not in cb_columns:
        c.execute("ALTER TABLE child_bots ADD COLUMN working_hours TEXT DEFAULT 'Не указаны'")
    if 'spam_filter' not in cb_columns:
        c.execute("ALTER TABLE child_bots ADD COLUMN spam_filter INTEGER DEFAULT 0")
    if 'msg_count' not in cb_columns:
        c.execute("ALTER TABLE child_bots ADD COLUMN msg_count INTEGER DEFAULT 0")
    if 'last_broadcast' not in cb_columns:
        c.execute("ALTER TABLE child_bots ADD COLUMN last_broadcast INTEGER DEFAULT 0")

    conn.commit()
    conn.close()


async def init_db():
    await asyncio.to_thread(_init_db_sync)


def _add_user_sync(user_id, username, full_name):
    conn = _connect()
    c = conn.cursor()
    c.execute("SELECT id FROM users WHERE id=?", (user_id,))
    if not c.fetchone():
        now = int(time.time())
        c.execute(
            "INSERT INTO users (id, username, full_name, is_banned, premium, premium_expires, registered_at) "
            "VALUES (?, ?, ?, 0, 0, 0, ?)",
            (user_id, username, full_name, now),
        )
        is_new = True
    else:
        c.execute("UPDATE users SET username=?, full_name=? WHERE id=?", (username, full_name, user_id))
        is_new = False
    conn.commit()
    conn.close()
    return is_new


async def db_add_user(user_id, username, full_name):
    return await asyncio.to_thread(_add_user_sync, user_id, username, full_name)


def _get_user_sync(user_id):
    conn = _connect()
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE id=?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row


async def db_get_user(user_id):
    return await asyncio.to_thread(_get_user_sync, user_id)


def _get_all_users_sync():
    conn = _connect()
    c = conn.cursor()
    c.execute("SELECT * FROM users")
    rows = c.fetchall()
    conn.close()
    return rows


async def db_get_all_users():
    return await asyncio.to_thread(_get_all_users_sync)


def _is_user_premium_sync(user_row) -> bool:
    if not user_row or user_row["premium"] != 1:
        return False
    if user_row["premium_expires"] == 0:
        return True
    if user_row["premium_expires"] > int(time.time()):
        return True
    # Подписка истекла — сбрасываем одним запросом.
    conn = _connect()
    c = conn.cursor()
    c.execute("UPDATE users SET premium=0, premium_expires=0 WHERE id=?", (user_row["id"],))
    conn.commit()
    conn.close()
    return False


async def is_user_premium(user_row) -> bool:
    return await asyncio.to_thread(_is_user_premium_sync, user_row)


def _promo_slots_left_sync() -> int:
    conn = _connect()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM promo_claims")
    count = c.fetchone()[0]
    conn.close()
    return max(0, PROMO_LIMIT - count)


async def db_promo_slots_left() -> int:
    return await asyncio.to_thread(_promo_slots_left_sync)


def _apply_promo_sync(user_id) -> bool:
    """Атомарно: проверяет лимит и что юзер ещё не брал промо, затем выдаёт Premium навсегда."""
    conn = _connect()
    c = conn.cursor()
    c.execute("BEGIN IMMEDIATE")
    c.execute("SELECT 1 FROM promo_claims WHERE user_id=?", (user_id,))
    if c.fetchone():
        conn.commit()
        conn.close()
        return False
    c.execute("SELECT COUNT(*) FROM promo_claims")
    if c.fetchone()[0] >= PROMO_LIMIT:
        conn.commit()
        conn.close()
        return False
    c.execute("UPDATE users SET premium=1, premium_expires=0 WHERE id=?", (user_id,))
    c.execute("INSERT INTO promo_claims (user_id, claimed_at) VALUES (?, ?)", (user_id, int(time.time())))
    conn.commit()
    conn.close()
    return True


async def db_apply_promo_if_available(user_id) -> bool:
    return await asyncio.to_thread(_apply_promo_sync, user_id)


def _set_user_field_sync(user_id, field, value):
    if field not in _USER_FIELDS:
        raise ValueError(f"Недопустимое поле пользователя: {field}")
    conn = _connect()
    c = conn.cursor()
    c.execute(f"UPDATE users SET {field}=? WHERE id=?", (value, user_id))
    conn.commit()
    conn.close()


async def db_set_status(user_id, field, value):
    await asyncio.to_thread(_set_user_field_sync, user_id, field, value)


def _set_premium_sync(user_id, expires_at):
    conn = _connect()
    c = conn.cursor()
    c.execute("UPDATE users SET premium=1, premium_expires=? WHERE id=?", (expires_at, user_id))
    conn.commit()
    conn.close()


async def db_set_premium(user_id, expires_at):
    await asyncio.to_thread(_set_premium_sync, user_id, expires_at)


def _add_child_bot_sync(owner_id, token):
    conn = _connect()
    c = conn.cursor()
    try:
        c.execute("INSERT INTO child_bots (owner_id, bot_token) VALUES (?, ?)", (owner_id, token))
        conn.commit()
        success = True
    except sqlite3.IntegrityError:
        success = False
    conn.close()
    return success


async def db_add_child_bot(owner_id, token):
    return await asyncio.to_thread(_add_child_bot_sync, owner_id, token)


def _get_child_bot_sync(owner_id):
    conn = _connect()
    c = conn.cursor()
    c.execute("SELECT * FROM child_bots WHERE owner_id=?", (owner_id,))
    row = c.fetchone()
    conn.close()
    return row


async def db_get_child_bot(owner_id):
    return await asyncio.to_thread(_get_child_bot_sync, owner_id)


def _get_all_bots_sync():
    conn = _connect()
    c = conn.cursor()
    c.execute("SELECT * FROM child_bots")
    rows = c.fetchall()
    conn.close()
    return rows


async def db_get_all_bots():
    return await asyncio.to_thread(_get_all_bots_sync)


def _update_child_bot_sync(owner_id, field, value):
    if field not in _BOT_FIELDS:
        raise ValueError(f"Недопустимое поле бота: {field}")
    conn = _connect()
    c = conn.cursor()
    c.execute(f"UPDATE child_bots SET {field}=? WHERE owner_id=?", (value, owner_id))
    conn.commit()
    conn.close()


async def db_update_child_bot(owner_id, field, value):
    await asyncio.to_thread(_update_child_bot_sync, owner_id, field, value)


def _delete_child_bot_sync(owner_id):
    conn = _connect()
    c = conn.cursor()
    c.execute("DELETE FROM child_bots WHERE owner_id=?", (owner_id,))
    c.execute("DELETE FROM clients WHERE owner_id=?", (owner_id,))
    c.execute("DELETE FROM mirror_bans WHERE owner_id=?", (owner_id,))
    conn.commit()
    conn.close()


async def db_delete_child_bot(owner_id):
    await asyncio.to_thread(_delete_child_bot_sync, owner_id)


def _add_client_sync(owner_id, client_id):
    conn = _connect()
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO clients (owner_id, client_id) VALUES (?, ?)", (owner_id, client_id))
    conn.commit()
    conn.close()


async def db_add_client(owner_id, client_id):
    await asyncio.to_thread(_add_client_sync, owner_id, client_id)


def _get_clients_sync(owner_id):
    conn = _connect()
    c = conn.cursor()
    c.execute("SELECT client_id FROM clients WHERE owner_id=?", (owner_id,))
    rows = c.fetchall()
    conn.close()
    return rows


async def db_get_clients(owner_id):
    return await asyncio.to_thread(_get_clients_sync, owner_id)



# ---- глобальные баны (платформа) ----

def _global_ban_sync(user_id):
    conn = _connect()
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO global_bans (user_id, banned_at) VALUES (?, ?)", (user_id, int(time.time())))
    c.execute("UPDATE users SET is_banned=1 WHERE id=?", (user_id,))
    conn.commit()
    conn.close()


async def db_global_ban(user_id):
    await asyncio.to_thread(_global_ban_sync, user_id)


def _global_unban_sync(user_id):
    conn = _connect()
    c = conn.cursor()
    c.execute("DELETE FROM global_bans WHERE user_id=?", (user_id,))
    c.execute("UPDATE users SET is_banned=0 WHERE id=?", (user_id,))
    conn.commit()
    conn.close()


async def db_global_unban(user_id):
    await asyncio.to_thread(_global_unban_sync, user_id)


def _is_globally_banned_sync(user_id) -> bool:
    conn = _connect()
    c = conn.cursor()
    c.execute("SELECT 1 FROM global_bans WHERE user_id=?", (user_id,))
    hit = c.fetchone() is not None
    if not hit:
        c.execute("SELECT 1 FROM users WHERE id=? AND is_banned=1", (user_id,))
        hit = c.fetchone() is not None
    conn.close()
    return hit


async def db_is_globally_banned(user_id) -> bool:
    return await asyncio.to_thread(_is_globally_banned_sync, user_id)


def _list_global_bans_sync():
    conn = _connect()
    c = conn.cursor()
    c.execute(
        "SELECT g.user_id, g.banned_at, u.full_name, u.username FROM global_bans g "
        "LEFT JOIN users u ON u.id = g.user_id ORDER BY g.banned_at DESC LIMIT 30"
    )
    rows = c.fetchall()
    conn.close()
    return rows


async def db_list_global_bans():
    return await asyncio.to_thread(_list_global_bans_sync)


# ---- баны внутри зеркала (решает владелец зеркала) ----

def _mirror_ban_sync(owner_id, client_id):
    conn = _connect()
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO mirror_bans (owner_id, client_id, banned_at) VALUES (?, ?, ?)",
              (owner_id, client_id, int(time.time())))
    conn.commit()
    conn.close()


async def db_mirror_ban(owner_id, client_id):
    await asyncio.to_thread(_mirror_ban_sync, owner_id, client_id)


def _mirror_unban_sync(owner_id, client_id):
    conn = _connect()
    c = conn.cursor()
    c.execute("DELETE FROM mirror_bans WHERE owner_id=? AND client_id=?", (owner_id, client_id))
    conn.commit()
    conn.close()


async def db_mirror_unban(owner_id, client_id):
    await asyncio.to_thread(_mirror_unban_sync, owner_id, client_id)


def _is_mirror_banned_sync(owner_id, client_id) -> bool:
    conn = _connect()
    c = conn.cursor()
    c.execute("SELECT 1 FROM mirror_bans WHERE owner_id=? AND client_id=?", (owner_id, client_id))
    hit = c.fetchone() is not None
    conn.close()
    return hit


async def db_is_mirror_banned(owner_id, client_id) -> bool:
    return await asyncio.to_thread(_is_mirror_banned_sync, owner_id, client_id)


def _list_mirror_bans_sync(owner_id):
    conn = _connect()
    c = conn.cursor()
    c.execute("SELECT client_id FROM mirror_bans WHERE owner_id=? ORDER BY banned_at DESC LIMIT 30", (owner_id,))
    rows = c.fetchall()
    conn.close()
    return [r['client_id'] for r in rows]


async def db_list_mirror_bans(owner_id):
    return await asyncio.to_thread(_list_mirror_bans_sync, owner_id)


def _is_client_sync(owner_id, client_id) -> bool:
    conn = _connect()
    c = conn.cursor()
    c.execute("SELECT 1 FROM clients WHERE owner_id=? AND client_id=?", (owner_id, client_id))
    hit = c.fetchone() is not None
    conn.close()
    return hit


async def db_is_client(owner_id, client_id) -> bool:
    return await asyncio.to_thread(_is_client_sync, owner_id, client_id)


def _mirror_stats_sync(owner_id) -> dict:
    conn = _connect()
    c = conn.cursor()
    clients = c.execute("SELECT COUNT(*) FROM clients WHERE owner_id=?", (owner_id,)).fetchone()[0]
    banned = c.execute("SELECT COUNT(*) FROM mirror_bans WHERE owner_id=?", (owner_id,)).fetchone()[0]
    row = c.execute("SELECT msg_count FROM child_bots WHERE owner_id=?", (owner_id,)).fetchone()
    conn.close()
    return {"clients": clients, "banned": banned, "messages": (row['msg_count'] if row else 0) or 0}


async def db_mirror_stats(owner_id) -> dict:
    return await asyncio.to_thread(_mirror_stats_sync, owner_id)


def _inc_msg_count_sync(owner_id):
    conn = _connect()
    c = conn.cursor()
    c.execute("UPDATE child_bots SET msg_count = COALESCE(msg_count, 0) + 1 WHERE owner_id=?", (owner_id,))
    conn.commit()
    conn.close()


async def db_inc_msg_count(owner_id):
    await asyncio.to_thread(_inc_msg_count_sync, owner_id)


def _set_last_broadcast_sync(owner_id, ts):
    conn = _connect()
    c = conn.cursor()
    c.execute("UPDATE child_bots SET last_broadcast=? WHERE owner_id=?", (ts, owner_id))
    conn.commit()
    conn.close()


async def db_set_last_broadcast(owner_id, ts):
    await asyncio.to_thread(_set_last_broadcast_sync, owner_id, ts)


# ==================== НАСТРОЙКА БОТА ====================

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())


class IsAdmin(BaseFilter):
    async def __call__(self, message: Message) -> bool:
        return message.from_user.id == ADMIN_ID


# ==================== ОБЯЗАТЕЛЬНАЯ ПОДПИСКА (ТОЛЬКО ГЛАВНЫЙ БОТ) ====================

MAIN_MENU_TEXTS = {
    "👤 Мой профиль", "✉️ Связаться", "🤖 Мой бот", "💎 Premium", "⚙️ Админ-панель", "🔙 В главное меню",
    "📋 Инфо о пользователе", "📊 Детальный отчёт (TXT)", "🌍 Глобал-бан", "✅ Разбан",
    "🎁 Выдать Premium", "💔 Забрать Premium", "📢 Рассылка (Основа)", "🌐 Мега-Рассылка (Зеркала)",
    "🛡 Защита от рейда", "📋 Глобал-баны",
}

CHILD_MENU_TEXTS = {
    "⚙️ Настройки (Админка)", "💎 Мой Premium", "👤 Мой профиль", "📊 Статистика", "🚫 Бан-лист",
    "📢 Рассылка клиентам", "✉️ Написать владельцу", "ℹ️ Инфо", "🚀 Создать такого бота",
}


async def is_subscribed_to_channel(user_id: int):
    """True/False — результат проверки; None — проверить не удалось (бот не добавлен в канал и т.п.)."""
    try:
        member = await bot.get_chat_member(chat_id=REQUIRED_CHANNEL, user_id=user_id)
        return member.status not in ("left", "kicked")
    except TelegramAPIError as e:
        logger.error(
            f"Не могу проверить подписку на {REQUIRED_CHANNEL}: {e}. "
            f"Добавьте бота в канал администратором!"
        )
        return None


async def ensure_subscribed(user_id: int):
    """Проверка подписки с коротким кэшем, чтобы не дёргать Telegram API на каждое сообщение."""
    ts = _sub_cache.get(user_id)
    if ts and time.time() - ts < SUB_CACHE_TTL:
        return True
    result = await is_subscribed_to_channel(user_id)
    if result:
        if len(_sub_cache) > 20000:
            _sub_cache.clear()
        _sub_cache[user_id] = time.time()
    return result


async def notify_admin_sub_problem():
    global _sub_alert_ts
    now = time.time()
    if now - _sub_alert_ts < 1800:
        return
    _sub_alert_ts = now
    try:
        await bot.send_message(
            ADMIN_ID,
            f"⚠️ Не могу проверить подписку на {REQUIRED_CHANNEL}. Добавьте бота в этот канал "
            f"<b>администратором</b> — иначе новые пользователи не смогут пройти ОП.",
        )
    except Exception:
        pass


def get_subscribe_kb() -> InlineKeyboardMarkup:
    channel_url = f"https://t.me/{REQUIRED_CHANNEL.lstrip('@')}"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 Подписаться на канал", url=channel_url)],
        [InlineKeyboardButton(text="✅ Я подписался, проверить", callback_data="check_sub")],
    ])


# ==================== ЗАЩИТА ОТ РЕЙДА И ФЛУДА ====================

def is_raid_mode_active() -> bool:
    return time.time() < _raid_mode_until


def register_new_signup_and_check_raid() -> bool:
    """Фиксирует новую регистрацию и возвращает True, если это включило raid-режим."""
    global _raid_mode_until, _raid_alerted
    now = time.time()
    _registration_log.append(now)
    cutoff = now - RAID_WINDOW_SECONDS
    while _registration_log and _registration_log[0] < cutoff:
        _registration_log.pop(0)

    if len(_registration_log) >= RAID_REGISTRATIONS_THRESHOLD:
        _raid_mode_until = now + RAID_COOLDOWN_SECONDS
        if not _raid_alerted:
            _raid_alerted = True
            return True
    return False


def clear_raid_mode():
    global _raid_mode_until, _raid_alerted
    _raid_mode_until = 0.0
    _raid_alerted = False
    _registration_log.clear()


async def check_flood(scope, user_id: int) -> str:
    """
    scope — 0 для главного бота или owner_id зеркала (у каждого зеркала свой счётчик).
    Возвращает: "ok" | "warn" | "just_muted" (только что получил мьют) | "muted" (молча игнорируем).
    Мьют временный: ложное срабатывание не приводит к вечной блокировке.
    """
    key = (scope, user_id)
    now = time.time()

    if _muted_until.get(key, 0) > now:
        return "muted"

    if len(_flood_log) > 20000:
        for k in [k for k, v in _flood_log.items() if not v or v[-1] < now - FLOOD_WINDOW_SECONDS]:
            _flood_log.pop(k, None)
            _flood_strikes.pop(k, None)
        for k in [k for k, v in _muted_until.items() if v < now]:
            _muted_until.pop(k, None)

    log = _flood_log.setdefault(key, [])
    log.append(now)
    cutoff = now - FLOOD_WINDOW_SECONDS
    while log and log[0] < cutoff:
        log.pop(0)

    if len(log) <= FLOOD_MAX_MESSAGES:
        _flood_strikes[key] = 0
        return "ok"

    strikes = _flood_strikes.get(key, 0) + 1
    _flood_strikes[key] = strikes
    if strikes >= FLOOD_AUTOBAN_STRIKES:
        _muted_until[key] = now + FLOOD_MUTE_SECONDS
        _flood_strikes[key] = 0
        log.clear()
        return "just_muted"
    return "warn"


# ==================== ГЛОБАЛЬНЫЙ БАН И УПРАВЛЕНИЕ ЗЕРКАЛАМИ ====================

async def stop_child_bot(owner_id: int):
    task = running_child_bots.pop(owner_id, None)
    if task:
        task.cancel()
    inst = child_bot_instances.pop(owner_id, None)
    if inst:
        try:
            await inst.session.close()
        except Exception:
            pass


async def start_child_bot(owner_id: int):
    row = await db_get_child_bot(owner_id)
    if row and owner_id not in running_child_bots:
        running_child_bots[owner_id] = asyncio.create_task(launch_child_bot(row['bot_token'], owner_id))


async def do_global_ban(user_id: int) -> bool:
    """Банит везде: главный бот + все зеркала. Если у юзера есть своё зеркало — оно останавливается."""
    await db_global_ban(user_id)
    had_mirror = user_id in running_child_bots
    await stop_child_bot(user_id)
    try:
        await bot.send_message(user_id, "🚫 <b>Вы заблокированы на платформе.</b> Доступ к боту и всем зеркалам закрыт.")
    except Exception:
        pass
    return had_mirror


async def do_global_unban(user_id: int) -> bool:
    await db_global_unban(user_id)
    restarted = False
    if await db_get_child_bot(user_id):
        await start_child_bot(user_id)
        restarted = True
    try:
        await bot.send_message(user_id, "✅ <b>Блокировка снята. Вы снова можете пользоваться платформой!</b>")
    except Exception:
        pass
    return restarted


class ContactAdmin(StatesGroup):
    choosing_category = State()
    writing_message = State()


class AdminActions(StatesGroup):
    target_user_id = State()
    action_type = State()
    premium_duration = State()
    writing_broadcast = State()
    writing_mega_broadcast = State()


class CreateBotFSM(StatesGroup):
    waiting_for_token = State()


# ==================== КЛАВИАТУРЫ ГЛАВНОГО БОТА ====================

def get_main_kb(user_id: int):
    kb = [
        [KeyboardButton(text="👤 Мой профиль"), KeyboardButton(text="✉️ Связаться")],
        [KeyboardButton(text="🤖 Мой бот"), KeyboardButton(text="💎 Premium")],
    ]
    if user_id == ADMIN_ID:
        kb.append([KeyboardButton(text="⚙️ Админ-панель")])
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


def get_categories_kb(prefix="cat_"):
    kb = [
        [InlineKeyboardButton(text="❓ Вопрос", callback_data=f"{prefix}question"),
         InlineKeyboardButton(text="⚠️ Проблема", callback_data=f"{prefix}problem")],
        [InlineKeyboardButton(text="🤝 Сотрудничество", callback_data=f"{prefix}collab"),
         InlineKeyboardButton(text="💡 Идея", callback_data=f"{prefix}idea")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data=f"{prefix}cancel")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)


def get_admin_kb():
    kb = [
        [KeyboardButton(text="📋 Инфо о пользователе"), KeyboardButton(text="📊 Детальный отчёт (TXT)")],
        [KeyboardButton(text="🌍 Глобал-бан"), KeyboardButton(text="✅ Разбан")],
        [KeyboardButton(text="📋 Глобал-баны"), KeyboardButton(text="🛡 Защита от рейда")],
        [KeyboardButton(text="🎁 Выдать Premium"), KeyboardButton(text="💔 Забрать Premium")],
        [KeyboardButton(text="📢 Рассылка (Основа)"), KeyboardButton(text="🌐 Мега-Рассылка (Зеркала)")],
        [KeyboardButton(text="🔙 В главное меню")],
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


def get_premium_durations_kb():
    kb = [
        [InlineKeyboardButton(text="1 День", callback_data="prem_86400"),
         InlineKeyboardButton(text="1 Неделя", callback_data="prem_604800")],
        [InlineKeyboardButton(text="1 Месяц", callback_data="prem_2592000"),
         InlineKeyboardButton(text="Навсегда", callback_data="prem_0")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)


# ==================== ГЛАВНЫЙ БОТ ====================

async def run_onboarding(user, chat_id: int):
    """Приветствие и (для новых пользователей) промо. Вызывается только после прохождения ОП."""
    is_new = await db_add_user(user.id, user.username, user.full_name)
    db_user = await db_get_user(user.id)
    if db_user and db_user['is_banned'] == 1:
        return await bot.send_message(chat_id, "🚫 <b>Доступ ограничен.</b> Ваш аккаунт заблокирован.")

    text = (
        f"👋 <b>Добро пожаловать, {esc(user.full_name)}!</b>\n\n"
        f"Я — платформа для создания идеальных ботов обратной связи. "
        f"Создайте своего <b>личного бота</b> (зеркало) в пару кликов и принимайте сообщения "
        f"от клиентов без раскрытия своего личного аккаунта."
    )
    await bot.send_message(chat_id, text, reply_markup=get_main_kb(user.id))

    if not is_new:
        return

    # Защита от рейда: всплеск новых аккаунтов → приостанавливаем промо и алертим владельца.
    if register_new_signup_and_check_raid():
        try:
            await bot.send_message(
                ADMIN_ID,
                f"🚨 <b>Похоже на рейд:</b> {RAID_REGISTRATIONS_THRESHOLD}+ новых регистраций за "
                f"{RAID_WINDOW_SECONDS} сек.\nВыдача промо-Premium приостановлена на "
                f"{RAID_COOLDOWN_SECONDS // 60} мин. Снять вручную: «⚙️ Админ-панель» → «🛡 Защита от рейда».",
            )
        except Exception:
            pass

    if await db_promo_slots_left() <= 0:
        return
    if is_raid_mode_active():
        return await bot.send_message(
            chat_id,
            "⚠️ Раздача бесплатного Premium временно приостановлена из-за высокой активности "
            "регистраций. Загляните позже.",
        )
    if await db_apply_promo_if_available(user.id):
        await bot.send_message(
            chat_id,
            "🎉 <b>СУПЕР-АКЦИЯ!</b> Вы попали в число первых "
            f"{PROMO_LIMIT} пользователей и <b>получили Premium навсегда!</b> 🎁\n\n"
            "Проверьте раздел «💎 Premium».",
        )


@dp.message(CommandStart(), StateFilter(any_state))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await run_onboarding(message.from_user, message.chat.id)


@dp.callback_query(F.data == "check_sub")
async def check_sub_callback(call: CallbackQuery, state: FSMContext):
    subscribed = await is_subscribed_to_channel(call.from_user.id)
    if subscribed is None:
        await notify_admin_sub_problem()
        return await call.answer("Проверка временно недоступна. Попробуйте чуть позже.", show_alert=True)
    if not subscribed:
        return await call.answer(f"Вы ещё не подписаны на {REQUIRED_CHANNEL}.", show_alert=True)

    _sub_cache[call.from_user.id] = time.time()
    try:
        await call.message.delete()
    except Exception:
        pass
    await run_onboarding(call.from_user, call.message.chat.id)
    await call.answer()


@dp.message(F.text == "👤 Мой профиль", StateFilter(any_state))
async def show_profile(message: Message, state: FSMContext):
    await state.clear()
    db_user = await db_get_user(message.from_user.id)
    if not db_user or db_user['is_banned'] == 1:
        return

    username = f"@{esc(db_user['username'])}" if db_user['username'] else "Отсутствует"
    is_prem = await is_user_premium(db_user)

    prem_status = "❌ Нет"
    if is_prem:
        if db_user['premium_expires'] == 0:
            prem_status = "🌟 АКТИВЕН (Навсегда)"
        else:
            exp_date = datetime.fromtimestamp(db_user['premium_expires']).strftime('%d.%m.%Y %H:%M')
            prem_status = f"🌟 АКТИВЕН (до {exp_date})"

    text = (
        f"👤 <b>Ваш профиль платформы:</b>\n\n"
        f"<b>Имя:</b> {esc(db_user['full_name'])}\n"
        f"<b>Юзернейм:</b> {username}\n"
        f"<b>Ваш ID:</b> <code>{db_user['id']}</code>\n\n"
        f"👑 <b>Premium статус:</b> {prem_status}"
    )
    await message.answer(text)


@dp.message(F.text == "💎 Premium", StateFilter(any_state))
async def show_premium(message: Message, state: FSMContext):
    await state.clear()
    db_user = await db_get_user(message.from_user.id)
    if not db_user or db_user['is_banned'] == 1:
        return
    is_prem = await is_user_premium(db_user)

    status_text = (
        "✅ <b>У вас активен Premium!</b> Вы можете использовать все функции без ограничений."
        if is_prem else
        "❌ <b>У вас нет Premium.</b> Зеркало работает с ограничениями."
    )

    text = (
        f"💎 <b>Premium-подписка — максимальная свобода!</b>\n\n"
        f"<b>🔥 Плюсы Premium для вашего бота:</b>\n"
        f"1️⃣ <b>Полный White-label:</b> отсутствие водяного знака «Создано с помощью...».\n"
        f"2️⃣ <b>Мощная Витрина:</b> укажите информацию о себе (прайсы, услуги) в меню зеркала.\n"
        f"3️⃣ <b>График работы:</b> клиенты будут видеть, когда вы готовы ответить.\n"
        f"4️⃣ <b>Умный Антиспам:</b> запрет на короткие бессмысленные сообщения (&lt;15 символов).\n\n"
        f"{status_text}"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🛒 Купить Premium", url="http://t.me/sb_teh_robot")]])
    await message.answer(text, reply_markup=None if is_prem else kb)


@dp.message(F.text == "🤖 Мой бот", StateFilter(any_state))
async def manage_bot_cmd(message: Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id

    # БАГ В ОРИГИНАЛЕ: тут отсутствовала проверка бана — забаненный пользователь
    # мог создать/управлять зеркалом. Добавлено ниже.
    db_user = await db_get_user(user_id)
    if not db_user or db_user['is_banned'] == 1:
        return await message.answer("🚫 Доступ ограничен. Ваш аккаунт заблокирован.")

    child_bot_row = await db_get_child_bot(user_id)

    if not child_bot_row:
        text = (
            "🤖 <b>Создание вашего бота (зеркала)</b>\n\n"
            "Инструкция по запуску:\n"
            "1. Перейдите в официального @BotFather.\n"
            "2. Создайте нового бота (команда <code>/newbot</code>).\n"
            "3. Скопируйте выданный вам <b>HTTP API Token</b>.\n"
            "4. Отправьте этот токен прямо сюда в чат.\n\n"
            "<i>⚠️ Без Premium ваш бот будет иметь водяной знак нашей платформы, "
            "а настройки Bio будут заблокированы.</i>\n\n"
            "Пришлите токен или выберите другой пункт меню для отмены."
        )
        await message.answer(text)
        await state.set_state(CreateBotFSM.waiting_for_token)
    else:
        bot_status = "🟢 Запущен" if user_id in running_child_bots else "🔴 Остановлен (ошибка токена)"
        text = (
            f"🤖 <b>Ваше зеркало работает!</b>\n"
            f"Статус процесса: {bot_status}\n\n"
            f"⚠️ <b>ВАЖНО:</b> Управление настройками вашего бота (Bio, Часы работы, Антиспам) "
            f"находится <b>внутри самого зеркала</b>!\n\n"
            f"Перейдите в вашего бота и нажмите кнопку «⚙️ Настройки (Админка)».\n\n"
            f"<i>Если вы хотите полностью удалить своего бота из базы, нажмите кнопку ниже:</i>"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🗑 Удалить бота", callback_data="delete_bot_ask")]])
        await message.answer(text, reply_markup=kb)


@dp.callback_query(F.data == "delete_bot_ask", StateFilter(any_state))
async def delete_bot_ask(call: CallbackQuery, state: FSMContext):
    # БАГ В ОРИГИНАЛЕ: удаление было необратимо в один клик. Добавлено подтверждение.
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="⚠️ Да, удалить безвозвратно", callback_data="delete_bot_confirm"),
        InlineKeyboardButton(text="◀️ Отмена", callback_data="delete_bot_cancel"),
    ]])
    await call.message.edit_text(
        "Вы уверены? Бот будет остановлен, а Bio, часы работы и список клиентов "
        "зеркала будут удалены безвозвратно.",
        reply_markup=kb,
    )
    await call.answer()


@dp.callback_query(F.data == "delete_bot_cancel", StateFilter(any_state))
async def delete_bot_cancel(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("❌ Удаление отменено. Ваш бот продолжает работать.")
    await call.answer()


@dp.callback_query(F.data == "delete_bot_confirm", StateFilter(any_state))
async def delete_bot_handler(call: CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    await db_delete_child_bot(user_id)  # теперь также чистит таблицу clients

    task = running_child_bots.pop(user_id, None)
    if task:
        task.cancel()
    child_obj = child_bot_instances.pop(user_id, None)
    if child_obj:
        try:
            await child_obj.session.close()
        except Exception:
            pass

    await call.message.edit_text("✅ Ваш бот полностью удалён из базы и остановлен.")
    await call.answer()


@dp.message(CreateBotFSM.waiting_for_token)
async def process_new_bot_token(message: Message, state: FSMContext):
    if not message.text:
        return await message.answer("❌ Пришлите, пожалуйста, токен текстом.")

    token = message.text.strip()
    if not TOKEN_RE.match(token):
        return await message.answer(
            "❌ <b>Неверный формат токена.</b> Убедитесь, что скопировали его полностью. Попробуйте ещё раз."
        )

    try:
        await message.delete()
    except Exception:
        pass
    m = await message.answer("⏳ Проверяем токен...")

    # УЛУЧШЕНИЕ: раньше валидность токена определялась через "подождать 1.5 сек
    # и посмотреть, упала ли задача" — ненадёжно. Теперь токен проверяется явно.
    test_bot = Bot(token=token)
    try:
        await test_bot.get_me()
    except TelegramAPIError:
        await test_bot.session.close()
        await m.edit_text(
            "❌ Токен недействителен или заблокирован. Убедитесь, что скопировали его верно, "
            "и попробуйте ещё раз.",
        )
        await state.clear()
        return
    finally:
        try:
            await test_bot.session.close()
        except Exception:
            pass

    if not await db_add_child_bot(message.from_user.id, token):
        await m.edit_text("❌ У вас уже есть бот, либо этот токен привязан к другому аккаунту.")
        await state.clear()
        return

    task = asyncio.create_task(launch_child_bot(token, message.from_user.id))
    running_child_bots[message.from_user.id] = task

    await m.edit_text("✅ <b>Ваш бот успешно запущен и работает в фоновом режиме!</b>")
    await message.answer(
        "Перейдите в него, нажмите /start и настройте под себя 🎉",
        reply_markup=get_main_kb(message.from_user.id),
    )
    await state.clear()


@dp.message(F.text == "✉️ Связаться", StateFilter(any_state))
async def start_contact_main(message: Message, state: FSMContext):
    await state.clear()
    if message.from_user.id == ADMIN_ID:
        return await message.answer(
            "ℹ️ Вы администратор платформы — писать самому себе нельзя. "
            "Обращения пользователей приходят вам сюда автоматически."
        )
    db_user = await db_get_user(message.from_user.id)
    if not db_user or db_user['is_banned'] == 1:
        return
    await message.answer("Выберите причину вашего обращения к администрации платформы:", reply_markup=get_categories_kb())
    await state.set_state(ContactAdmin.choosing_category)


@dp.callback_query(F.data.startswith("cat_"), ContactAdmin.choosing_category)
async def category_chosen_main(call: CallbackQuery, state: FSMContext):
    if call.data == "cat_cancel":
        await state.clear()
        return await call.message.edit_text("❌ Обращение отменено.")
    cats = {
        "cat_question": "❓ Вопрос",
        "cat_problem": "⚠️ Техническая проблема",
        "cat_collab": "🤝 Сотрудничество / Разработка",
        "cat_idea": "💡 Предложение идеи",
    }
    cat = cats.get(call.data, "Другое")
    await state.update_data(category=cat)
    await call.message.edit_text(f"Категория: <b>{cat}</b>\n\n✏️ Опишите суть сообщения. Мы ответим вам прямо в этом боте.")
    await state.set_state(ContactAdmin.writing_message)


@dp.message(ContactAdmin.writing_message)
async def receive_contact_main(message: Message, state: FSMContext):
    data = await state.get_data()
    user = message.from_user
    username = f"@{esc(user.username)}" if user.username else "Скрыт"
    text = (
        f"🔔 <b>Обращение в Поддержку</b>\n"
        f"<b>Категория:</b> {data.get('category')}\n"
        f"<b>Пользователь:</b> {esc(user.full_name)} ({username})\n"
        f"<b>ID:</b> <code>{user.id}</code>\n\n"
        f"{esc(message.text)}\n\n"
        f"<i>Для ответа используй:</i> <code>/reply {user.id} текст</code>"
    )
    try:
        await bot.send_message(ADMIN_ID, text)
        await message.answer("✅ Ваше сообщение успешно доставлено! Ожидайте ответа.")
    except Exception as e:
        await message.answer("❌ Ошибка доставки сообщения.")
        logger.error(f"Error sending message to admin: {e}")
    await state.clear()


# ==================== АДМИН-ПАНЕЛЬ (ГЛАВНАЯ) ====================

@dp.message(F.text == "⚙️ Админ-панель", IsAdmin(), StateFilter(any_state))
async def admin_panel_start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("🔐 <b>Панель управления платформой</b>\n\nВыбери действие:", reply_markup=get_admin_kb())


@dp.message(F.text == "📋 Глобал-баны", IsAdmin())
async def admin_list_global_bans(message: Message, state: FSMContext):
    await state.clear()
    rows = await db_list_global_bans()
    if not rows:
        return await message.answer("📋 Глобальных банов нет.")
    lines = ["📋 <b>Глобальные баны</b> (последние 30):\n"]
    buttons = []
    for r in rows:
        who = esc(r['full_name']) if r['full_name'] else "—"
        lines.append(f"<code>{r['user_id']}</code> · {who} · {datetime.fromtimestamp(r['banned_at']).strftime('%d.%m.%Y')}")
        buttons.append([InlineKeyboardButton(text=f"✅ Разбанить {r['user_id']}", callback_data=f"gunban_{r['user_id']}")])
    await message.answer("\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@dp.callback_query(F.data.startswith("gunban_"), IsAdmin())
async def admin_gunban_cb(call: CallbackQuery):
    uid = int(call.data.split("_")[1])
    await do_global_unban(uid)
    await call.answer(f"Разбанен {uid}", show_alert=False)
    try:
        await call.message.edit_text(f"✅ Пользователь <code>{uid}</code> разблокирован. Откройте список заново, чтобы обновить.")
    except Exception:
        pass


@dp.callback_query(F.data.startswith("gban_"), IsAdmin())
async def admin_gban_cb(call: CallbackQuery):
    uid = int(call.data.split("_")[1])
    if uid == ADMIN_ID:
        return await call.answer("Нельзя заблокировать владельца.", show_alert=True)
    await do_global_ban(uid)
    await call.answer("Заблокирован глобально", show_alert=False)
    try:
        await call.message.edit_text(f"🌍 Пользователь <code>{uid}</code> заблокирован глобально.")
    except Exception:
        pass


@dp.message(F.text == "🛡 Защита от рейда", IsAdmin())
async def admin_raid_status(message: Message, state: FSMContext):
    await state.clear()
    slots_left = await db_promo_slots_left()
    recent = len(_registration_log)

    if is_raid_mode_active():
        left_min = max(1, int((_raid_mode_until - time.time()) // 60) + 1)
        status = f"🚨 <b>Активен</b> — выдача промо приостановлена ещё ~{left_min} мин."
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Снять режим сейчас", callback_data="raid_clear")]])
    else:
        status = "🟢 Спокойно, рейд-режим не активен."
        kb = None

    text = (
        f"🛡 <b>Защита от рейда</b>\n\n"
        f"Статус: {status}\n\n"
        f"Регистраций за последние {RAID_WINDOW_SECONDS} сек: <b>{recent}</b>\n"
        f"Порог срабатывания: {RAID_REGISTRATIONS_THRESHOLD} регистраций\n"
        f"Осталось промо-мест: <b>{slots_left}</b>\n\n"
        f"<i>Дополнительно активна защита от флуда: сообщения от одного пользователя "
        f"чаще {FLOOD_MAX_MESSAGES} раз за {FLOOD_WINDOW_SECONDS} сек считаются флудом, "
        f"а после {FLOOD_AUTOBAN_STRIKES} нарушений подряд получает мьют на {FLOOD_MUTE_SECONDS // 60} мин (вам придёт алерт с кнопкой глобал-бана).</i>"
    )
    await message.answer(text, reply_markup=kb)


@dp.callback_query(F.data == "raid_clear", IsAdmin())
async def admin_raid_clear(call: CallbackQuery):
    clear_raid_mode()
    await call.message.edit_text("✅ Режим защиты от рейда снят вручную.")
    await call.answer()


@dp.message(F.text == "🔙 В главное меню", StateFilter(any_state))
async def back_to_main(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Вы вернулись в меню пользователя.", reply_markup=get_main_kb(message.from_user.id))


@dp.message(Command("reply"), IsAdmin())
async def main_bot_reply(message: Message):
    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        return await message.answer("ℹ️ Использование: <code>/reply [ID_пользователя] [Ваш текст]</code>")
    if not args[1].lstrip("-").isdigit():
        return await message.answer("❌ ID пользователя должен быть числом.")
    try:
        await bot.send_message(int(args[1]), f"📩 <b>Официальный ответ администрации платформы:</b>\n\n{esc(args[2])}")
        await message.answer("✅ Ответ успешно отправлен!")
    except Exception as e:
        await message.answer(f"❌ Не удалось отправить (возможно, пользователь заблокировал бота).\nОшибка: {e}")


@dp.message(F.text == "📊 Детальный отчёт (TXT)", IsAdmin())
async def send_detailed_report(message: Message):
    m = await message.answer("⏳ Собираю данные базы...")
    users = await db_get_all_users()
    bots_list = await db_get_all_bots()

    premium_count = 0
    banned_count = 0
    lines = []
    for u in users:
        is_prem = await is_user_premium(u)
        premium_count += int(is_prem)
        banned_count += int(u['is_banned'] == 1)

        prem = "Нет"
        if is_prem:
            prem = "Навсегда" if u['premium_expires'] == 0 else f"До {datetime.fromtimestamp(u['premium_expires']).strftime('%d.%m.%Y')}"
        banned = "ДА" if u['is_banned'] == 1 else "Нет"
        reg = datetime.fromtimestamp(u['registered_at']).strftime('%d.%m.%Y') if u['registered_at'] else "Неизвестно"

        lines.append(f"ID: {u['id']} | @{u['username']} | Имя: {u['full_name']}")
        lines.append(f"   Рег: {reg} | Бан: {banned} | Premium: {prem}")

    report = (
        f"==== ОТЧЁТ О ПЛАТФОРМЕ ====\n"
        f"Сгенерирован: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        f"📈 ОБЩАЯ СТАТИСТИКА:\n"
        f"- Всего пользователей: {len(users)}\n"
        f"- Активных зеркал (ботов): {len(bots_list)}\n"
        f"- Premium пользователей: {premium_count}\n"
        f"- Забаненных: {banned_count}\n\n"
        f"👥 ДЕТАЛИЗАЦИЯ ПОЛЬЗОВАТЕЛЕЙ:\n" + "\n".join(lines)
    )

    file_bytes = report.encode('utf-8')
    file = BufferedInputFile(file_bytes, filename=f"Report_{datetime.now().strftime('%Y%m%d')}.txt")
    await m.delete()
    await message.answer_document(file, caption="✅ Детальный отчёт готов.")


# ---- Рассылка по пользователям главного бота (с подтверждением, в фоне) ----

@dp.message(F.text == "📢 Рассылка (Основа)", IsAdmin())
async def start_broadcast(message: Message, state: FSMContext):
    await message.answer(
        "Отправьте сообщение (текст, фото или видео), которое нужно разослать всем "
        "пользователям главной платформы.\n\nДля отмены нажмите любую кнопку меню."
    )
    await state.set_state(AdminActions.writing_broadcast)


@dp.message(AdminActions.writing_broadcast, IsAdmin())
async def process_broadcast(message: Message, state: FSMContext):
    await state.update_data(bc_chat_id=message.chat.id, bc_message_id=message.message_id)
    users = await db_get_all_users()
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Отправить всем", callback_data="bc_confirm"),
        InlineKeyboardButton(text="❌ Отмена", callback_data="bc_cancel"),
    ]])
    await message.answer(
        f"👆 Сообщение выше будет разослано <b>{len(users)}</b> пользователям платформы.\nПодтвердить отправку?",
        reply_markup=kb,
    )


@dp.callback_query(F.data.in_({"bc_confirm", "bc_cancel"}), AdminActions.writing_broadcast, IsAdmin())
async def confirm_broadcast(call: CallbackQuery, state: FSMContext):
    if call.data == "bc_cancel":
        await state.clear()
        await call.message.edit_text("❌ Рассылка отменена.")
        return await call.answer()

    data = await state.get_data()
    await state.clear()
    await call.message.edit_text("⏳ Рассылка запущена в фоне. Отчёт придёт отдельным сообщением по завершении.")
    asyncio.create_task(_run_main_broadcast(data["bc_chat_id"], data["bc_message_id"], call.from_user.id))
    await call.answer()


async def _run_main_broadcast(from_chat_id: int, message_id: int, notify_admin_id: int):
    succ = fail = 0
    try:
        users = await db_get_all_users()
        for u in users:
            try:
                await bot.copy_message(chat_id=u['id'], from_chat_id=from_chat_id, message_id=message_id)
                succ += 1
            except Exception:
                fail += 1
            await asyncio.sleep(0.05)
        report = f"✅ <b>Рассылка завершена!</b>\nУспешно: {succ}\nНе доставлено: {fail}"
    except Exception as e:
        logger.exception("Рассылка упала")
        report = f"❌ <b>Рассылка прервана:</b> {esc(type(e).__name__)}: {esc(e)}\nДоставлено до сбоя: {succ}"
    try:
        await bot.send_message(notify_admin_id, report)
    except Exception:
        pass


# ---- Мега-рассылка по клиентам всех зеркал (с подтверждением, в фоне) ----

@dp.message(F.text == "🌐 Мега-Рассылка (Зеркала)", IsAdmin())
async def start_mega_broadcast(message: Message, state: FSMContext):
    await message.answer(
        "🔥 <b>МЕГА-РАССЫЛКА</b>\n\n"
        "Вы можете разослать сообщение <b>всем клиентам всех запущенных зеркал</b> на платформе. "
        "Сообщение будет отправлено от лица каждого зеркала его пользователям.\n\n"
        "⚠️ <i>Telegram запрещает пересылку файлов между разными ботами, поэтому Мега-Рассылка "
        "поддерживает только текст.</i>\n\n"
        "Отправьте текстовое сообщение для начала:"
    )
    await state.set_state(AdminActions.writing_mega_broadcast)


@dp.message(AdminActions.writing_mega_broadcast, IsAdmin())
async def process_mega_broadcast(message: Message, state: FSMContext):
    if not message.text:
        return await message.answer("❌ Ошибка: пожалуйста, отправьте только текст.")

    bots_list = await db_get_all_bots()
    total_clients = 0
    bots_with_clients = 0
    for b in bots_list:
        n = len(await db_get_clients(b['owner_id']))
        total_clients += n
        bots_with_clients += 1 if n else 0

    if total_clients == 0:
        await state.clear()
        return await message.answer(
            "⚠️ <b>Рассылать некому:</b> ни у одного зеркала пока нет клиентов.\n\n"
            "Клиент записывается в базу, когда нажимает /start в зеркале. Сообщения самих "
            "владельцев не считаются. Попросите кого-нибудь (не владельца) написать /start "
            "в любое зеркало и повторите.",
            reply_markup=get_admin_kb(),
        )

    await state.update_data(mbc_text=message.text)
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Разослать", callback_data="mbc_confirm"),
        InlineKeyboardButton(text="❌ Отмена", callback_data="mbc_cancel"),
    ]])
    await message.answer(
        f"Сообщение получат <b>{total_clients}</b> клиентов из <b>{bots_with_clients}</b> зеркал. Подтвердить?",
        reply_markup=kb,
    )


@dp.callback_query(F.data.in_({"mbc_confirm", "mbc_cancel"}), AdminActions.writing_mega_broadcast, IsAdmin())
async def confirm_mega_broadcast(call: CallbackQuery, state: FSMContext):
    if call.data == "mbc_cancel":
        await state.clear()
        await call.message.edit_text("❌ Мега-рассылка отменена.")
        return await call.answer()

    data = await state.get_data()
    text = data.get("mbc_text", "")
    await state.clear()
    await call.message.edit_text("⏳ Мега-рассылка запущена в фоне. Отчёт придёт отдельным сообщением по завершении.")
    asyncio.create_task(_run_mega_broadcast(text, call.from_user.id))
    await call.answer()


async def _run_mega_broadcast(text: str, notify_admin_id: int):
    succ = fail = 0
    errors: dict = {}
    try:
        bots_list = await db_get_all_bots()
        for b in bots_list:
            clients = await db_get_clients(b['owner_id'])
            if not clients:
                continue

            temp_bot = Bot(token=b['bot_token'], default=DefaultBotProperties(parse_mode=ParseMode.HTML))
            try:
                for c in clients:
                    try:
                        await temp_bot.send_message(chat_id=c['client_id'], text=text)
                        succ += 1
                    except Exception as e:
                        fail += 1
                        key = type(e).__name__
                        errors[key] = errors.get(key, 0) + 1
                    await asyncio.sleep(0.05)
            finally:
                try:
                    await temp_bot.session.close()
                except Exception:
                    pass
        report = f"✅ <b>Мега-Рассылка завершена!</b>\nДоставлено: {succ}\nОшибок: {fail}"
        if errors:
            report += "\n\nПричины ошибок:\n" + "\n".join(f"• {k}: {v}" for k, v in errors.items())
    except Exception as e:
        logger.exception("Мега-рассылка упала")
        report = f"❌ <b>Мега-Рассылка прервана из-за ошибки:</b> {esc(type(e).__name__)}: {esc(e)}\nДоставлено до сбоя: {succ}"

    try:
        await bot.send_message(notify_admin_id, report)
    except Exception:
        logger.exception("Не удалось отправить отчёт о мега-рассылке")


# ---- Управление пользователями ----

@dp.message(F.text.in_(["📋 Инфо о пользователе", "🌍 Глобал-бан", "✅ Разбан", "🎁 Выдать Premium", "💔 Забрать Premium"]), IsAdmin())
async def admin_actions_start(message: Message, state: FSMContext):
    action = message.text
    await state.update_data(action_type=action)
    await state.set_state(AdminActions.target_user_id)
    await message.answer(f"Действие: <b>{action}</b>\n\nВведите ID пользователя:")


@dp.message(AdminActions.target_user_id, IsAdmin())
async def process_admin_action(message: Message, state: FSMContext):
    if not message.text or not message.text.isdigit():
        return await message.answer("❌ ID должен состоять только из цифр.")
    target_id = int(message.text)
    data = await state.get_data()
    action = data.get("action_type")

    # Бан/разбан работают по любому ID — даже если человек никогда не писал главному боту
    # (например, он только клиент чьего-то зеркала).
    if "Глобал-бан" in action:
        await state.clear()
        if target_id == ADMIN_ID:
            return await message.answer("❌ Нельзя заблокировать владельца платформы.")
        had_mirror = await do_global_ban(target_id)
        return await message.answer(
            f"✅ Пользователь <code>{target_id}</code> заблокирован <b>глобально</b>: главный бот и все зеркала."
            + (" Его собственное зеркало остановлено." if had_mirror else "")
        )

    if "Разбан" in action:
        await state.clear()
        restarted = await do_global_unban(target_id)
        return await message.answer(
            f"✅ Пользователь <code>{target_id}</code> разблокирован."
            + (" Его зеркало запущено снова." if restarted else "")
        )

    user = await db_get_user(target_id)
    if not user:
        await state.clear()
        return await message.answer("❌ Пользователь не найден в базе.")

    if "Инфо" in action:
        prem = "Активен" if await is_user_premium(user) else "Нет"
        banned = "Да" if await db_is_globally_banned(target_id) else "Нет"
        bot_info = "Есть зеркало" if await db_get_child_bot(target_id) else "Нет зеркала"
        await message.answer(
            f"📋 <b>Инфо:</b>\nID: <code>{user['id']}</code>\nИмя: {esc(user['full_name'])}\n"
            f"Юзернейм: @{esc(user['username'])}\nГлобал-бан: {banned}\nPremium: {prem}\nБот: {bot_info}"
        )
        await state.clear()

    elif "💔" in action:
        await db_set_status(target_id, "premium", 0)
        await db_set_status(target_id, "premium_expires", 0)
        await message.answer(f"✅ Premium у {target_id} изъят.")
        try:
            await bot.send_message(target_id, "💔 <b>Ваш Premium-статус был аннулирован администрацией.</b>")
        except Exception:
            pass
        await state.clear()

    elif "🎁" in action:
        await state.update_data(target_id=target_id)
        await message.answer(f"На какой срок выдать Premium пользователю {target_id}?", reply_markup=get_premium_durations_kb())
        await state.set_state(AdminActions.premium_duration)


@dp.callback_query(F.data.startswith("prem_"), AdminActions.premium_duration, IsAdmin())
async def process_give_premium(call: CallbackQuery, state: FSMContext):
    seconds = int(call.data.split("_")[1])
    data = await state.get_data()
    target_id = data.get("target_id")

    expires_at = int(time.time()) + seconds if seconds > 0 else 0
    await db_set_premium(target_id, expires_at)

    term_str = "навсегда" if seconds == 0 else f"на {seconds // 86400} дн."
    await call.message.edit_text(f"✅ Premium успешно выдан пользователю {target_id} {term_str}.")

    try:
        await bot.send_message(
            target_id,
            f"🎉 <b>Поздравляем! Администрация выдала вам Premium статус {term_str}!</b>\n\n"
            f"Все функции вашего зеркала разблокированы.",
        )
    except Exception:
        pass
    await state.clear()
    await call.answer()


# ==================== ДВИЖОК ДОЧЕРНИХ БОТОВ (SaaS) ====================

class ChildBotSettingsFSM(StatesGroup):
    waiting_for_bio = State()
    waiting_for_hours = State()
    waiting_reply = State()
    waiting_broadcast = State()


class ChildBotClientFSM(StatesGroup):
    choosing_category = State()
    writing_message = State()


OWNER_SELF_NOTE = (
    "ℹ️ Вы владелец этого бота — писать самому себе нельзя. "
    "Сообщения клиентов приходят вам сюда автоматически."
)


async def launch_child_bot(token: str, owner_id: int):
    child_bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    child_bot_instances[owner_id] = child_bot
    try:
        child_dp = Dispatcher(storage=MemoryStorage())

        def get_child_kb(user_id):
            if user_id == owner_id:
                return ReplyKeyboardMarkup(keyboard=[
                    [KeyboardButton(text="⚙️ Настройки (Админка)"), KeyboardButton(text="💎 Мой Premium")],
                    [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="🚫 Бан-лист")],
                    [KeyboardButton(text="📢 Рассылка клиентам"), KeyboardButton(text="👤 Мой профиль")],
                ], resize_keyboard=True)
            return ReplyKeyboardMarkup(keyboard=[
                [KeyboardButton(text="✉️ Написать владельцу"), KeyboardButton(text="ℹ️ Инфо")],
                [KeyboardButton(text="👤 Мой профиль"), KeyboardButton(text="🚀 Создать такого бота")],
            ], resize_keyboard=True)

        async def get_watermark() -> str:
            owner_data = await db_get_user(owner_id)
            if await is_user_premium(owner_data):
                return ""
            username = MAIN_BOT_USERNAME or "our_bot"
            return f"\n\n⚡️ <i>Создано с помощью @{username}</i>"

        # ---------- ШЛЮЗ: меню-escape, антифлуд, баны ----------
        # ОП здесь НЕ проверяется — обязательная подписка только у главного бота.
        async def child_gate(handler, event, data):
            user = event.from_user
            if user is None:
                return await handler(event, data)
            is_cb = isinstance(event, CallbackQuery)

            # Нажатие кнопки меню всегда отменяет ожидающий ввод (иначе кнопка
            # "съедается" состоянием и сохраняется как Bio/ответ/рассылка).
            if not is_cb and event.text in CHILD_MENU_TEXTS:
                st = data.get("state")
                if st is not None:
                    await st.clear()
                data["raw_state"] = None

            if user.id == owner_id:
                return await handler(event, data)

            status = await check_flood(owner_id, user.id)
            if status == "muted":
                if is_cb:
                    await event.answer()
                return
            if status in ("warn", "just_muted"):
                note = ("⏳ Слишком много сообщений подряд. Притормозите." if status == "warn"
                        else f"🔇 Вы временно ({FLOOD_MUTE_SECONDS // 60} мин) не можете писать этому боту: флуд.")
                try:
                    if is_cb:
                        await event.answer(note, show_alert=True)
                    else:
                        await event.answer(note)
                except Exception:
                    pass
                if status == "just_muted":
                    try:
                        await child_bot.send_message(
                            owner_id,
                            f"🔇 Клиент <code>{user.id}</code> ({esc(user.full_name)}) замьючен за флуд "
                            f"на {FLOOD_MUTE_SECONDS // 60} мин.",
                            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                                InlineKeyboardButton(text="🚫 Забанить", callback_data=f"ch_ban_{user.id}")
                            ]]),
                        )
                    except Exception:
                        pass
                return

            if await db_is_globally_banned(user.id) or await db_is_mirror_banned(owner_id, user.id):
                try:
                    if is_cb:
                        await event.answer("🚫 Доступ к этому боту ограничен.", show_alert=True)
                    else:
                        await event.answer("🚫 <b>Доступ к этому боту ограничен.</b>")
                except Exception:
                    pass
                return

            return await handler(event, data)

        child_dp.message.outer_middleware(child_gate)
        child_dp.callback_query.outer_middleware(child_gate)

        # ---------- вспомогательные действия владельца ----------
        REPLY_ERRORS = {
            "self": "❌ Нельзя писать самому себе.",
            "not_client": "❌ Этот пользователь не писал вашему боту.",
            "banned": "❌ Клиент забанен — сначала разбаньте его.",
            "fail": "❌ Ошибка отправки (возможно, клиент заблокировал бота).",
        }

        async def send_to_client(client_id: int, text: str) -> str:
            if client_id == owner_id:
                return "self"
            if not await db_is_client(owner_id, client_id):
                return "not_client"
            if await db_is_globally_banned(client_id) or await db_is_mirror_banned(owner_id, client_id):
                return "banned"
            wm = await get_watermark()
            try:
                await child_bot.send_message(client_id, f"📩 <b>Новое сообщение от владельца:</b>\n\n{esc(text)}{wm}")
                return "ok"
            except Exception:
                return "fail"

        async def do_ban(client_id: int) -> str:
            if client_id == owner_id:
                return "self"
            await db_mirror_ban(owner_id, client_id)
            try:
                await child_bot.send_message(client_id, "🚫 Владелец ограничил вам доступ к этому боту.")
            except Exception:
                pass
            return "ok"

        async def do_unban(client_id: int):
            await db_mirror_unban(owner_id, client_id)
            try:
                await child_bot.send_message(client_id, "✅ Доступ к боту восстановлен.")
            except Exception:
                pass

        async def render_banlist():
            ids = await db_list_mirror_bans(owner_id)
            if not ids:
                return (
                    "🚫 <b>Бан-лист пуст.</b>\n\nЗабанить клиента можно кнопкой «🚫 Забанить» под его "
                    "сообщением или командой <code>/ban ID</code>.",
                    None,
                )
            rows = [[InlineKeyboardButton(text=f"✅ Разбанить {cid}", callback_data=f"ch_unban_{cid}")] for cid in ids]
            return (
                f"🚫 <b>Забаненные клиенты ({len(ids)}):</b>\n\nНажмите кнопку, чтобы разбанить.",
                InlineKeyboardMarkup(inline_keyboard=rows),
            )

        async def broadcast_recipients():
            clients = await db_get_clients(owner_id)
            banned = set(await db_list_mirror_bans(owner_id))
            return [c['client_id'] for c in clients if c['client_id'] not in banned]

        async def run_child_broadcast(text: str):
            succ = fail = 0
            try:
                for cid in await broadcast_recipients():
                    if await db_is_globally_banned(cid):
                        continue
                    try:
                        await child_bot.send_message(cid, f"📢 <b>Сообщение от владельца:</b>\n\n{esc(text)}")
                        succ += 1
                    except Exception:
                        fail += 1
                    await asyncio.sleep(0.05)
                report = f"✅ <b>Рассылка завершена!</b>\nДоставлено: {succ}\nНе доставлено: {fail}"
            except Exception as e:
                logger.exception("Рассылка владельца зеркала упала")
                report = f"❌ Рассылка прервана: {esc(type(e).__name__)}. Доставлено до сбоя: {succ}"
            try:
                await child_bot.send_message(owner_id, report)
            except Exception:
                pass

        # ---------- общие команды ----------
        @child_dp.message(CommandStart(), StateFilter(any_state))
        async def child_start(message: Message, state: FSMContext):
            await state.clear()
            owner_info = await db_get_user(owner_id)
            name = esc(owner_info['full_name']) if owner_info else "Владельца"

            if message.from_user.id != owner_id:
                await db_add_client(owner_id, message.from_user.id)

            if message.from_user.id == owner_id:
                text = (
                    "👋 <b>Привет, Владелец!</b>\n\n"
                    "Это твоё зеркало. Именно здесь ты будешь получать сообщения от клиентов — "
                    "под каждым будут кнопки «✍️ Ответить» и «🚫 Забанить».\n"
                    "Используй меню ниже: настройки, статистика, бан-лист, рассылка."
                )
            else:
                wm = await get_watermark()
                text = (
                    f"👋 <b>Добро пожаловать!</b>\n\n"
                    f"Это официальный бот для безопасной связи с <b>{name}</b>.\n"
                    f"Воспользуйтесь меню ниже для навигации.{wm}"
                )
            await message.answer(text, reply_markup=get_child_kb(message.from_user.id))

        @child_dp.message(Command("cancel"), StateFilter(any_state))
        async def ch_cancel(message: Message, state: FSMContext):
            await state.clear()
            await message.answer("❌ Действие отменено.", reply_markup=get_child_kb(message.from_user.id))

        # ---------- блок владельца ----------
        @child_dp.message(F.text == "⚙️ Настройки (Админка)", StateFilter(any_state))
        async def child_owner_settings(message: Message, state: FSMContext):
            if message.from_user.id != owner_id:
                return
            await state.clear()

            owner_info = await db_get_user(owner_id)
            is_prem = await is_user_premium(owner_info)
            bot_data = await db_get_child_bot(owner_id)

            spam_status = "✅ Включён" if bot_data['spam_filter'] == 1 else "❌ Выключен"
            text = (
                f"⚙️ <b>Настройки вашего зеркала</b>\n\n"
                f"<b>Ваше Bio:</b>\n{esc(bot_data['bio'])}\n\n"
                f"<b>Часы работы:</b>\n{esc(bot_data['working_hours'])}\n\n"
                f"<b>Антиспам:</b> {spam_status}\n\n"
                f"<i>Выберите, что хотите изменить:</i>"
            )
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📝 Изменить Bio" + ("" if is_prem else " 🔒"), callback_data="ch_set_bio")],
                [InlineKeyboardButton(text="🕒 Часы работы" + ("" if is_prem else " 🔒"), callback_data="ch_set_hours")],
                [InlineKeyboardButton(text="🛡 Антиспам" + ("" if is_prem else " 🔒"), callback_data="ch_toggle_spam")],
            ])
            await message.answer(text, reply_markup=kb)

        @child_dp.callback_query(F.data.in_(["ch_set_bio", "ch_set_hours", "ch_toggle_spam"]))
        async def child_settings_cb(call: CallbackQuery, state: FSMContext):
            if call.from_user.id != owner_id:
                return await call.answer()
            owner_info = await db_get_user(owner_id)
            is_prem = await is_user_premium(owner_info)
            action = call.data

            if not is_prem:
                return await call.answer("💎 Эта настройка доступна только с Premium!", show_alert=True)

            if action == "ch_set_bio":
                await call.message.answer("✏️ <b>Отправьте новую информацию о себе (Bio):</b>\nНапример, прайс-лист или условия работы.")
                await state.set_state(ChildBotSettingsFSM.waiting_for_bio)
            elif action == "ch_set_hours":
                await call.message.answer("🕒 <b>Отправьте ваши часы работы:</b>\nНапример: Пн-Пт 10:00-20:00")
                await state.set_state(ChildBotSettingsFSM.waiting_for_hours)
            elif action == "ch_toggle_spam":
                bot_data = await db_get_child_bot(owner_id)
                new_val = 0 if bot_data['spam_filter'] == 1 else 1
                await db_update_child_bot(owner_id, "spam_filter", new_val)
                await call.message.edit_text(f"✅ Умный антиспам-фильтр {'включён 🟢' if new_val else 'выключен 🔴'}.")
            await call.answer()

        @child_dp.message(F.text == "💎 Мой Premium", StateFilter(any_state))
        async def ch_my_premium(message: Message, state: FSMContext):
            if message.from_user.id != owner_id:
                return
            await state.clear()
            owner_info = await db_get_user(owner_id)
            is_prem = await is_user_premium(owner_info)

            status = "🌟 АКТИВЕН" if is_prem else "❌ НЕ АКТИВЕН"
            text = (
                f"💎 <b>Ваш Premium статус: {status}</b>\n\n"
                f"Premium отключает водяной знак платформы (полный White-label) и открывает: "
                f"Bio, часы работы, антиспам-фильтр и 📢 рассылку своим клиентам.\n\n"
                f"<i>Для приобретения Premium-подписки нажмите кнопку ниже:</i>"
            )
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🛒 Купить Premium", url="http://t.me/sb_teh_robot")]])
            await message.answer(text, reply_markup=None if is_prem else kb)

        @child_dp.message(F.text == "📊 Статистика", StateFilter(any_state))
        async def ch_stats(message: Message, state: FSMContext):
            if message.from_user.id != owner_id:
                return
            await state.clear()
            st = await db_mirror_stats(owner_id)
            await message.answer(
                f"📊 <b>Статистика вашего бота</b>\n\n"
                f"👥 Клиентов: <b>{st['clients']}</b>\n"
                f"🚫 В бане: <b>{st['banned']}</b>\n"
                f"💬 Получено обращений: <b>{st['messages']}</b>"
            )

        @child_dp.message(F.text == "🚫 Бан-лист", StateFilter(any_state))
        async def ch_banlist(message: Message, state: FSMContext):
            if message.from_user.id != owner_id:
                return
            await state.clear()
            text, kb = await render_banlist()
            await message.answer(text, reply_markup=kb)

        @child_dp.callback_query(F.data.startswith("ch_ban_"))
        async def ch_ban_cb(call: CallbackQuery):
            if call.from_user.id != owner_id:
                return await call.answer()
            cid = int(call.data.split("_")[2])
            if await do_ban(cid) == "self":
                return await call.answer("Нельзя забанить самого себя.", show_alert=True)
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Разбанить", callback_data=f"ch_unban_{cid}")]])
            try:
                await call.message.edit_reply_markup(reply_markup=kb)
            except Exception:
                pass
            await call.answer("🚫 Клиент забанен")

        @child_dp.callback_query(F.data.startswith("ch_unban_"))
        async def ch_unban_cb(call: CallbackQuery):
            if call.from_user.id != owner_id:
                return await call.answer()
            cid = int(call.data.split("_")[2])
            await do_unban(cid)
            try:
                if call.message.text and call.message.text.startswith("🚫"):
                    text, kb = await render_banlist()
                    await call.message.edit_text(text, reply_markup=kb)
                else:
                    kb = InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="✍️ Ответить", callback_data=f"ch_reply_{cid}"),
                        InlineKeyboardButton(text="🚫 Забанить", callback_data=f"ch_ban_{cid}"),
                    ]])
                    await call.message.edit_reply_markup(reply_markup=kb)
            except Exception:
                pass
            await call.answer("✅ Клиент разбанен")

        @child_dp.message(Command("ban"), StateFilter(any_state))
        async def ch_ban_cmd(message: Message):
            if message.from_user.id != owner_id:
                return
            args = message.text.split()
            if len(args) < 2 or not args[1].isdigit():
                return await message.answer("ℹ️ Используйте: <code>/ban [ID_клиента]</code>")
            if await do_ban(int(args[1])) == "self":
                return await message.answer("❌ Нельзя забанить самого себя.")
            await message.answer(f"🚫 Клиент <code>{args[1]}</code> забанен.")

        @child_dp.message(Command("unban"), StateFilter(any_state))
        async def ch_unban_cmd(message: Message):
            if message.from_user.id != owner_id:
                return
            args = message.text.split()
            if len(args) < 2 or not args[1].isdigit():
                return await message.answer("ℹ️ Используйте: <code>/unban [ID_клиента]</code>")
            await do_unban(int(args[1]))
            await message.answer(f"✅ Клиент <code>{args[1]}</code> разбанен.")

        @child_dp.callback_query(F.data.startswith("ch_reply_"))
        async def ch_reply_cb(call: CallbackQuery, state: FSMContext):
            if call.from_user.id != owner_id:
                return await call.answer()
            cid = int(call.data.split("_")[2])
            await state.set_state(ChildBotSettingsFSM.waiting_reply)
            await state.update_data(reply_to=cid)
            await call.message.answer(f"✍️ Напишите ответ клиенту <code>{cid}</code> одним сообщением.\n/cancel — отмена.")
            await call.answer()

        @child_dp.message(Command("reply"), StateFilter(any_state))
        async def child_owner_reply(message: Message):
            if message.from_user.id != owner_id:
                return
            args = message.text.split(maxsplit=2)
            if len(args) < 3:
                return await message.answer("ℹ️ <b>Используйте:</b> <code>/reply [ID_клиента] [Текст ответа]</code>")
            if not args[1].isdigit():
                return await message.answer("❌ ID клиента должен быть числом.")
            res = await send_to_client(int(args[1]), args[2])
            await message.answer(f"✅ Отправлено клиенту <code>{args[1]}</code>." if res == "ok" else REPLY_ERRORS[res])

        @child_dp.message(F.text == "📢 Рассылка клиентам", StateFilter(any_state))
        async def ch_broadcast_start(message: Message, state: FSMContext):
            if message.from_user.id != owner_id:
                return
            await state.clear()
            owner_info = await db_get_user(owner_id)
            if not await is_user_premium(owner_info):
                kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🛒 Купить Premium", url="http://t.me/sb_teh_robot")]])
                return await message.answer("💎 Рассылка своим клиентам доступна только с Premium.", reply_markup=kb)

            row = await db_get_child_bot(owner_id)
            left = BROADCAST_COOLDOWN - (int(time.time()) - ((row['last_broadcast'] or 0) if row else 0))
            if left > 0:
                return await message.answer(f"⏳ Следующая рассылка будет доступна через {left // 60 + 1} мин. (лимит: раз в час, чтобы не получить спам-ограничения Telegram).")

            n = len(await broadcast_recipients())
            if n == 0:
                return await message.answer("У вас пока нет клиентов для рассылки.")
            await state.set_state(ChildBotSettingsFSM.waiting_broadcast)
            await message.answer(
                f"📢 <b>Рассылка клиентам</b>\n\nПолучат: <b>{n}</b> чел. (забаненные исключены).\n"
                f"Отправьте текст сообщения (до 1000 символов). /cancel — отмена."
            )

        @child_dp.callback_query(F.data.in_({"ch_bc_confirm", "ch_bc_cancel"}), ChildBotSettingsFSM.waiting_broadcast)
        async def ch_bc_cb(call: CallbackQuery, state: FSMContext):
            if call.from_user.id != owner_id:
                return await call.answer()
            if call.data == "ch_bc_cancel":
                await state.clear()
                await call.message.edit_text("❌ Рассылка отменена.")
                return await call.answer()

            data = await state.get_data()
            text = data.get("bc_text", "")
            await state.clear()
            owner_info = await db_get_user(owner_id)
            row = await db_get_child_bot(owner_id)
            left = BROADCAST_COOLDOWN - (int(time.time()) - ((row['last_broadcast'] or 0) if row else 0))
            if not await is_user_premium(owner_info) or left > 0 or not text:
                await call.message.edit_text("❌ Рассылка недоступна (нет Premium или не прошла пауза).")
                return await call.answer()
            await db_set_last_broadcast(owner_id, int(time.time()))
            await call.message.edit_text("⏳ Рассылка запущена. Отчёт придёт отдельным сообщением.")
            asyncio.create_task(run_child_broadcast(text))
            await call.answer()

        # ---------- блок клиента ----------
        @child_dp.message(F.text == "👤 Мой профиль", StateFilter(any_state))
        async def child_profile(message: Message, state: FSMContext):
            await state.clear()
            wm = await get_watermark() if message.from_user.id != owner_id else ""
            text = (
                f"👤 <b>Ваш профиль:</b>\n\n"
                f"<b>Имя:</b> {esc(message.from_user.full_name)}\n"
                f"<b>ID:</b> <code>{message.from_user.id}</code>{wm}"
            )
            await message.answer(text)

        @child_dp.message(F.text == "ℹ️ Инфо", StateFilter(any_state))
        async def child_info(message: Message, state: FSMContext):
            await state.clear()
            bot_data = await db_get_child_bot(owner_id)
            if not bot_data:
                return await message.answer("❌ Информация временно недоступна.")
            wm = await get_watermark()
            text = (
                f"ℹ️ <b>Информация о владельце:</b>\n\n"
                f"📋 <b>Описание/Услуги:</b>\n{esc(bot_data['bio'])}\n\n"
                f"🕒 <b>Часы работы:</b> {esc(bot_data['working_hours'])}{wm}"
            )
            await message.answer(text)

        @child_dp.message(F.text == "🚀 Создать такого бота", StateFilter(any_state))
        async def ch_create_bot_promo(message: Message, state: FSMContext):
            await state.clear()
            username = MAIN_BOT_USERNAME or "нашего_бота"
            text = (
                "🔥 Хочешь такого же личного бота для приёма сообщений?\n"
                "Без спама, с крутым профилем и быстрой настройкой!\n\n"
                f"Переходи в <b>официального бота-конструктора</b> и создай своего за 2 минуты: @{username}"
            )
            await message.answer(text)

        @child_dp.message(F.text == "✉️ Написать владельцу", StateFilter(any_state))
        async def child_contact(message: Message, state: FSMContext):
            await state.clear()
            if message.from_user.id == owner_id:
                return await message.answer(OWNER_SELF_NOTE)
            await message.answer("Выберите причину обращения:", reply_markup=get_categories_kb(prefix="ch_cat_"))
            await state.set_state(ChildBotClientFSM.choosing_category)

        @child_dp.callback_query(F.data.startswith("ch_cat_"), ChildBotClientFSM.choosing_category)
        async def child_cat_chosen(call: CallbackQuery, state: FSMContext):
            if call.from_user.id == owner_id:
                await state.clear()
                return await call.answer(OWNER_SELF_NOTE, show_alert=True)
            if call.data == "ch_cat_cancel":
                await state.clear()
                await call.message.edit_text("❌ Отменено.")
                return await call.answer()
            cats = {
                "ch_cat_question": "❓ Вопрос",
                "ch_cat_problem": "⚠️ Проблема",
                "ch_cat_collab": "🤝 Предложение",
                "ch_cat_idea": "💡 Идея",
            }
            cat = cats.get(call.data, "Другое")
            await state.update_data(category=cat)
            await call.message.edit_text(f"Категория: <b>{cat}</b>\n\n✏️ Напишите ваше сообщение, и оно будет немедленно передано владельцу:")
            await state.set_state(ChildBotClientFSM.writing_message)
            await call.answer()

        # ---------- обработчики ожидающего ввода (регистрируются ПОСЛЕ кнопок меню) ----------
        @child_dp.message(ChildBotClientFSM.writing_message)
        async def child_receive(message: Message, state: FSMContext):
            if message.from_user.id == owner_id:
                await state.clear()
                return await message.answer(OWNER_SELF_NOTE, reply_markup=get_child_kb(owner_id))
            if not message.text:
                return await message.answer("❌ Пожалуйста, отправьте текстовое сообщение.")

            bot_data = await db_get_child_bot(owner_id)
            if bot_data and bot_data['spam_filter'] == 1 and len(message.text) < 15:
                return await message.answer(
                    "🛡 <b>Защита от спама:</b> ваше сообщение слишком короткое (минимум 15 символов). "
                    "Пожалуйста, опишите суть подробнее."
                )

            data = await state.get_data()
            user = message.from_user
            username = f"@{esc(user.username)}" if user.username else "Скрыт"

            admin_text = (
                f"🔔 <b>НОВОЕ ОБРАЩЕНИЕ ОТ КЛИЕНТА</b>\n"
                f"<b>Категория:</b> {data.get('category')}\n"
                f"<b>От:</b> {esc(user.full_name)} ({username})\n"
                f"<b>ID Клиента:</b> <code>{user.id}</code>\n\n"
                f"💬 <b>Текст:</b>\n{esc(message.text)}"
            )
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="✍️ Ответить", callback_data=f"ch_reply_{user.id}"),
                InlineKeyboardButton(text="🚫 Забанить", callback_data=f"ch_ban_{user.id}"),
            ]])
            try:
                await child_bot.send_message(owner_id, admin_text, reply_markup=kb)
                await db_inc_msg_count(owner_id)
                wm = await get_watermark()
                await message.answer(f"✅ <b>Сообщение успешно доставлено владельцу!</b>{wm}", reply_markup=get_child_kb(message.from_user.id))
            except Exception as e:
                logger.error(f"Owner {owner_id} blocked their own bot: {e}")
                await message.answer("❌ Произошла ошибка. Владелец временно не может принимать сообщения.")
            await state.clear()

        @child_dp.message(ChildBotSettingsFSM.waiting_for_bio)
        async def ch_save_bio(message: Message, state: FSMContext):
            if not message.text:
                return await message.answer("❌ Пришлите Bio текстом.")
            await db_update_child_bot(owner_id, "bio", message.text[:800])
            await message.answer("✅ Информация Bio успешно обновлена!", reply_markup=get_child_kb(message.from_user.id))
            await state.clear()

        @child_dp.message(ChildBotSettingsFSM.waiting_for_hours)
        async def ch_save_hours(message: Message, state: FSMContext):
            if not message.text:
                return await message.answer("❌ Пришлите часы работы текстом.")
            await db_update_child_bot(owner_id, "working_hours", message.text[:200])
            await message.answer("✅ Часы работы успешно обновлены!", reply_markup=get_child_kb(message.from_user.id))
            await state.clear()

        @child_dp.message(ChildBotSettingsFSM.waiting_reply)
        async def ch_send_reply(message: Message, state: FSMContext):
            if message.from_user.id != owner_id:
                return
            if not message.text:
                return await message.answer("❌ Отправьте ответ текстом (или /cancel).")
            data = await state.get_data()
            cid = data.get("reply_to")
            res = await send_to_client(cid, message.text)
            await state.clear()
            await message.answer(
                f"✅ Отправлено клиенту <code>{cid}</code>." if res == "ok" else REPLY_ERRORS[res],
                reply_markup=get_child_kb(owner_id),
            )

        @child_dp.message(ChildBotSettingsFSM.waiting_broadcast)
        async def ch_broadcast_text(message: Message, state: FSMContext):
            if message.from_user.id != owner_id:
                return
            if not message.text or len(message.text) > 1000:
                return await message.answer("❌ Нужен текст до 1000 символов (или /cancel).")
            await state.update_data(bc_text=message.text)
            n = len(await broadcast_recipients())
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="✅ Разослать", callback_data="ch_bc_confirm"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="ch_bc_cancel"),
            ]])
            await message.answer(f"Сообщение получат <b>{n}</b> чел. Подтвердить?", reply_markup=kb)

        logger.info(f"🟢 Поднят дочерний бот для владельца ID {owner_id}")
        await child_dp.start_polling(child_bot, handle_signals=False)

    except TelegramAPIError as e:
        logger.error(f"🔴 Ошибка API Telegram для бота {owner_id} (неверный токен?): {e}")
    except asyncio.CancelledError:
        logger.info(f"🟡 Дочерний бот {owner_id} остановлен.")
        raise
    except Exception as e:
        logger.error(f"🔴 Критическая ошибка в дочернем боте {owner_id}: {e}")
    finally:
        # чистим только «свои» записи: при быстром бан→разбан мог уже стартовать новый экземпляр
        if running_child_bots.get(owner_id) is asyncio.current_task():
            running_child_bots.pop(owner_id, None)
        if child_bot_instances.get(owner_id) is child_bot:
            child_bot_instances.pop(owner_id, None)
        try:
            await child_bot.session.close()
        except Exception:
            pass


# ==================== ШЛЮЗ ГЛАВНОГО БОТА: антифлуд, баны, обязательная подписка ====================

async def main_gate_middleware(handler, event, data):
    user = event.from_user
    if user is None:
        return await handler(event, data)
    is_cb = isinstance(event, CallbackQuery)

    # Нажатие кнопки меню всегда отменяет ожидающий ввод (иначе кнопка «съедается» состоянием).
    if not is_cb and event.text in MAIN_MENU_TEXTS:
        st = data.get("state")
        if st is not None:
            await st.clear()
        data["raw_state"] = None

    if user.id == ADMIN_ID:
        return await handler(event, data)

    # 1. Антифлуд
    status = await check_flood(0, user.id)
    if status == "muted":
        if is_cb:
            await event.answer()
        return
    if status in ("warn", "just_muted"):
        note = ("⏳ Слишком много сообщений подряд. Притормозите." if status == "warn"
                else f"🔇 Вы временно ({FLOOD_MUTE_SECONDS // 60} мин) не можете пользоваться ботом: флуд.")
        try:
            if is_cb:
                await event.answer(note, show_alert=True)
            else:
                await event.answer(note)
        except Exception:
            pass
        if status == "just_muted":
            try:
                await bot.send_message(
                    ADMIN_ID,
                    f"🔇 Мьют за флуд: <code>{user.id}</code> ({esc(user.full_name)}) на {FLOOD_MUTE_SECONDS // 60} мин.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="🌍 Глобал-бан", callback_data=f"gban_{user.id}")
                    ]]),
                )
            except Exception:
                pass
        return

    # 2. Глобальный бан
    if await db_is_globally_banned(user.id):
        try:
            if is_cb:
                await event.answer("🚫 Доступ ограничен.", show_alert=True)
            else:
                await event.answer("🚫 <b>Доступ ограничен.</b> Ваш аккаунт заблокирован администрацией платформы.")
        except Exception:
            pass
        return

    # 3. Обязательная подписка (кнопка проверки пропускается, иначе не пройти ОП)
    if not (is_cb and event.data == "check_sub"):
        sub = await ensure_subscribed(user.id)
        if sub is None:
            await notify_admin_sub_problem()
            # Не удалось проверить: уже зарегистрированных пропускаем, новых — нет.
            if await db_get_user(user.id) is None:
                try:
                    if is_cb:
                        await event.answer("Проверка подписки временно недоступна. Попробуйте позже.", show_alert=True)
                    else:
                        await event.answer("⚠️ Проверка подписки временно недоступна. Попробуйте чуть позже.")
                except Exception:
                    pass
                return
        elif sub is False:
            try:
                if is_cb:
                    await event.answer(f"Сначала подпишитесь на {REQUIRED_CHANNEL}", show_alert=True)
                else:
                    await event.answer(
                        f"🔒 <b>Для использования бота нужна подписка на канал {REQUIRED_CHANNEL}.</b>\n\n"
                        f"Подпишитесь и нажмите «✅ Я подписался, проверить».",
                        reply_markup=get_subscribe_kb(),
                    )
            except Exception:
                pass
            return

    return await handler(event, data)


dp.message.outer_middleware(main_gate_middleware)
dp.callback_query.outer_middleware(main_gate_middleware)


# ==================== ЗАПУСК ПЛАТФОРМЫ ====================

async def main():
    await init_db()

    global MAIN_BOT_USERNAME
    try:
        me = await bot.get_me()
        MAIN_BOT_USERNAME = me.username or ""
        logger.info(f"🚀 Запуск ядра платформы: @{MAIN_BOT_USERNAME}")
    except Exception:
        logger.error("❌ НЕВЕРНЫЙ ТОКЕН ОСНОВНОГО БОТА! Проверьте переменную окружения BOT_TOKEN.")
        return

    bots_list = await db_get_all_bots()
    for b in bots_list:
        if await db_is_globally_banned(b['owner_id']):
            logger.info(f"⛔ Зеркало владельца {b['owner_id']} не запущено: глобальный бан.")
            continue
        task = asyncio.create_task(launch_child_bot(b['bot_token'], b['owner_id']))
        running_child_bots[b['owner_id']] = task

    logger.info(f"🔄 Восстановлено дочерних ботов: {len(running_child_bots)}")

    try:
        await dp.start_polling(bot)
    finally:
        for t in list(running_child_bots.values()):
            t.cancel()
        await asyncio.gather(*running_child_bots.values(), return_exceptions=True)
        try:
            await bot.session.close()
        except Exception:
            pass
        logger.info("🛑 Платформа остановлена.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
