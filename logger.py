import discord
import asyncio
import io
import os
import json
import aiohttp
import aiosqlite
import logging
import logging.handlers
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

TOKENS: list[str] = [
    t.strip()
    for t in os.environ.get("DISCORD_TOKENS", os.environ.get("DISCORD_TOKEN", "")).split(",")
    if t.strip()
]
if not TOKENS:
    raise RuntimeError("Set DISCORD_TOKENS (or DISCORD_TOKEN) in your .env")

WATCHED_GUILDS: list[int] = [
    int(g) for g in os.environ.get("WATCHED_GUILDS", "").split(",") if g.strip()
]

LOG_CHANNEL_ID: int | None = (
    int(os.environ["LOG_CHANNEL_ID"]) if os.environ.get("LOG_CHANNEL_ID") else None
)

LOG_POSTER_TOKEN: str | None = os.environ.get("LOG_POSTER_TOKEN", "").strip() or None

MIRROR_MAP: dict[int, list[str]] = {}
for _pair in os.environ.get("MIRROR_CHANNELS", "").split(","):
    _pair = _pair.strip()
    if not _pair:
        continue
    _cid, _, _wurl = _pair.partition(":")
    _cid, _wurl = _cid.strip(), _wurl.strip()
    if _cid and _wurl:
        MIRROR_MAP.setdefault(int(_cid), []).append(_wurl)

MIRROR_SERVERS: list[tuple[int, int]] = []
for _pair in os.environ.get("MIRROR_SERVERS", "").split(","):
    _pair = _pair.strip()
    if not _pair:
        continue
    _src, _, _dst = _pair.partition(":")
    _src, _dst = _src.strip(), _dst.strip()
    if _src and _dst:
        MIRROR_SERVERS.append((int(_src), int(_dst)))

_server_mirror_src: set[int] = {s for s, _d in MIRROR_SERVERS}

BASE_LOG_DIR = Path("logs")
MEDIA_DIR = Path("media")
# ─────────────────────────────────────────────────────────────────────────────

BASE_LOG_DIR.mkdir(exist_ok=True)
MEDIA_DIR.mkdir(exist_ok=True)

_lib_handler = logging.handlers.RotatingFileHandler(
    BASE_LOG_DIR / "discord.log",
    encoding="utf-8",
    maxBytes=10 * 1024 * 1024,
    backupCount=3,
)
_lib_handler.setFormatter(
    logging.Formatter("[{asctime}] [{levelname:<8}] {name}: {message}",
                      "%Y-%m-%d %H:%M:%S", style="{")
)
logging.getLogger("discord").addHandler(_lib_handler)
logging.getLogger("discord").setLevel(logging.WARNING)

console = logging.getLogger("message_logger")
console.setLevel(logging.INFO)
_ch = logging.StreamHandler()
_ch.setFormatter(logging.Formatter("[{asctime}] {message}", "%H:%M:%S", style="{"))
console.addHandler(_ch)


def _log_path(guild_name: str | None, channel_name: str, date: str) -> Path:
    safe = lambda s: "".join(c if c.isalnum() or c in " _-" else "_" for c in s)
    guild_dir = BASE_LOG_DIR / (safe(guild_name) if guild_name else "DMs")
    guild_dir.mkdir(parents=True, exist_ok=True)
    return guild_dir / f"{safe(channel_name)}_{date}.log"


def _format_ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _guild_name(message: discord.Message) -> str | None:
    return message.guild.name if message.guild else None


def _channel_label(message: discord.Message) -> str:
    if message.guild:
        return f"#{message.channel.name}"
    return f"DM:{message.channel}"  # type: ignore[arg-type]


def _write(path: Path, text: str) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(text)


def _write_guild_name(guild: discord.Guild) -> None:
    guild_dir = MEDIA_DIR / str(guild.id)
    guild_dir.mkdir(parents=True, exist_ok=True)
    (guild_dir / "!guild_name.txt").write_text(guild.name, encoding="utf-8")


def _att_path(att: dict, fallback_dir: Path, msg_id: int) -> Path:
    """Resolve attachment local path; falls back for pre-migration records without local_path."""
    if "local_path" in att:
        return MEDIA_DIR / att["local_path"]
    return fallback_dir / f"{msg_id}_{att['filename']}"


@dataclass
class CachedMessage:
    id: int
    author: str
    author_id: int
    channel: str
    guild_name: str | None
    content: str
    created_at: datetime
    attachments: list[dict]  # [{filename, url, ?local_path}, ...]
    stickers: list[dict]     # [{id, name, format, ?local_path}, ...]


_guild_owner: dict[int, int] = {}
_guild_client: dict[int, "MessageLogger"] = {}
_log_poster: "MessageLogger | None" = None

_post_queue: asyncio.Queue[tuple[str, list]] = asyncio.Queue()
_download_sem = asyncio.Semaphore(5)

_ready_count   = 0
_total_clients = 0
_server_mirror_ready: asyncio.Event | None = None


async def _post_worker() -> None:
    """Single consumer for log-channel posts; retries with exponential backoff."""
    while True:
        text, files = await _post_queue.get()
        try:
            if _log_poster is not None:
                delay = 1.0
                for attempt in range(5):
                    try:
                        await _log_poster._send_chunked(text, files)
                        break
                    except Exception as exc:
                        console.warning(
                            "Log channel post failed (attempt %d/5): %s", attempt + 1, exc
                        )
                        if attempt < 4:
                            await asyncio.sleep(delay)
                            delay = min(delay * 2, 30.0)
        finally:
            _post_queue.task_done()


async def _save_attachment(session: aiohttp.ClientSession,
                            message: discord.Message,
                            attachment: discord.Attachment) -> str | None:
    guild_dir = MEDIA_DIR / (str(message.guild.id) if message.guild else "DMs")
    guild_dir.mkdir(parents=True, exist_ok=True)
    dest = guild_dir / f"{message.id}_{attachment.filename}"
    if dest.exists():
        return str(dest.relative_to(MEDIA_DIR))
    try:
        async with _download_sem:
            async with session.get(attachment.url) as resp:
                if resp.status == 200:
                    dest.write_bytes(await resp.read())
                    return str(dest.relative_to(MEDIA_DIR))
    except Exception as exc:
        console.warning("Failed to save attachment %s: %s", attachment.filename, exc)
    return None


async def _save_sticker(session: aiohttp.ClientSession,
                         message: discord.Message,
                         sticker: discord.StickerItem) -> str | None:
    if sticker.format == discord.StickerFormatType.lottie:
        return None
    guild_dir = MEDIA_DIR / (str(message.guild.id) if message.guild else "DMs") / "stickers"
    guild_dir.mkdir(parents=True, exist_ok=True)
    ext = "gif" if sticker.format == discord.StickerFormatType.gif else "png"
    dest = guild_dir / f"{sticker.id}.{ext}"
    if dest.exists():
        return str(dest.relative_to(MEDIA_DIR))
    try:
        async with _download_sem:
            async with session.get(sticker.url) as resp:
                if resp.status == 200:
                    dest.write_bytes(await resp.read())
                    return str(dest.relative_to(MEDIA_DIR))
    except Exception as exc:
        console.warning("Failed to save sticker %s: %s", sticker.name, exc)
    return None


# ── Server mirror helpers ─────────────────────────────────────────────────────

async def _get_or_create_category(
    dst_guild: discord.Guild,
    base_name: str,
    overflow: int,
    category_cache: dict[str, discord.CategoryChannel],
) -> discord.CategoryChannel | None:
    name = base_name if overflow == 0 else f"{base_name} ({overflow + 1})"
    if name in category_cache:
        return category_cache[name]
    existing = discord.utils.get(dst_guild.categories, name=name)
    if existing is not None:
        category_cache[name] = existing
        return existing
    try:
        cat = await dst_guild.create_category(name)
        if overflow > 0:
            prev_name = base_name if overflow == 1 else f"{base_name} ({overflow})"
            prev_cat = category_cache.get(prev_name)
            if prev_cat is not None:
                try:
                    await cat.edit(position=prev_cat.position + 1)
                except Exception as exc:
                    console.warning("Server mirror: could not reorder category '%s': %s", name, exc)
        category_cache[name] = cat
        console.info("Server mirror: created category '%s' in %s", name, dst_guild.name)
        return cat
    except Exception as exc:
        console.warning("Server mirror: could not create category '%s': %s", name, exc)
        return None


async def _ensure_server_mirror_channel(
    db: aiosqlite.Connection,
    dst_guild: discord.Guild,
    src_channel: discord.TextChannel,
    category_cache: dict[str, discord.CategoryChannel],
) -> None:
    async with db.execute(
        "SELECT webhook_url FROM server_mirror_channels WHERE source_channel_id = ?",
        (src_channel.id,),
    ) as cur:
        if await cur.fetchone():
            return

    readable = await _probe_readable(src_channel)
    if not readable:
        console.info("Server mirror: #%s is unreadable, placing in '%s'", src_channel.name, UNREADABLE_CATEGORY_NAME)

    base_cat_name = (
        src_channel.category.name if (readable and src_channel.category) else
        UNREADABLE_CATEGORY_NAME if not readable else None
    )

    dst_channel = discord.utils.get(dst_guild.text_channels, name=src_channel.name)
    if dst_channel is None:
        for overflow in range(20):
            dst_category: discord.CategoryChannel | None = (
                await _get_or_create_category(dst_guild, base_cat_name, overflow, category_cache)
                if base_cat_name else None
            )
            if base_cat_name and dst_category is None:
                return
            try:
                dst_channel = await dst_guild.create_text_channel(
                    src_channel.name,
                    category=dst_category,
                    topic=src_channel.topic or "",
                )
                console.info("Server mirror: created channel #%s in %s", src_channel.name, dst_guild.name)
                break
            except discord.HTTPException as exc:
                if base_cat_name and "Maximum number of channels in category" in exc.text:
                    continue
                console.warning("Server mirror: could not create channel #%s: %s", src_channel.name, exc)
                return
            except Exception as exc:
                console.warning("Server mirror: could not create channel #%s: %s", src_channel.name, exc)
                return
        else:
            console.warning("Server mirror: no category with room for #%s, giving up", src_channel.name)
            return

    try:
        webhooks = await dst_channel.webhooks()
        wh = discord.utils.get(webhooks, name="MessageMirror")
        if wh is None:
            wh = await dst_channel.create_webhook(name="MessageMirror")
            console.info("Server mirror: created webhook in #%s (%s)", dst_channel.name, dst_guild.name)
    except Exception as exc:
        console.warning("Server mirror: could not create webhook in #%s: %s", dst_channel.name, exc)
        return

    await db.execute(
        "INSERT OR REPLACE INTO server_mirror_channels (source_channel_id, dest_channel_id, webhook_url, unreadable) VALUES (?, ?, ?, ?)",
        (src_channel.id, dst_channel.id, wh.url, 0 if readable else 1),
    )
    await db.commit()


async def _ensure_server_mirror_voice_channel(
    db: aiosqlite.Connection,
    dst_guild: discord.Guild,
    src_channel: discord.VoiceChannel,
    category_cache: dict[str, discord.CategoryChannel],
) -> None:
    async with db.execute(
        "SELECT webhook_url FROM server_mirror_channels WHERE source_channel_id = ?",
        (src_channel.id,),
    ) as cur:
        if await cur.fetchone():
            return

    dst_channel = discord.utils.get(dst_guild.text_channels, name=src_channel.name)
    if dst_channel is None:
        base_cat_name = src_channel.category.name if src_channel.category else None
        for overflow in range(20):
            dst_category: discord.CategoryChannel | None = (
                await _get_or_create_category(dst_guild, base_cat_name, overflow, category_cache)
                if base_cat_name else None
            )
            if base_cat_name and dst_category is None:
                return
            try:
                dst_channel = await dst_guild.create_text_channel(src_channel.name, category=dst_category)
                console.info("Server mirror: created text channel #%s (from voice) in %s", src_channel.name, dst_guild.name)
                break
            except discord.HTTPException as exc:
                if base_cat_name and "Maximum number of channels in category" in exc.text:
                    continue
                console.warning("Server mirror: could not create voice channel #%s: %s", src_channel.name, exc)
                return
            except Exception as exc:
                console.warning("Server mirror: could not create voice channel #%s: %s", src_channel.name, exc)
                return
        else:
            console.warning("Server mirror: no category with room for #%s (voice), giving up", src_channel.name)
            return

    try:
        webhooks = await dst_channel.webhooks()
        wh = discord.utils.get(webhooks, name="MessageMirror")
        if wh is None:
            wh = await dst_channel.create_webhook(name="MessageMirror")
            console.info("Server mirror: created webhook in voice #%s (%s)", dst_channel.name, dst_guild.name)
    except Exception as exc:
        console.warning("Server mirror: could not create webhook in voice #%s: %s", dst_channel.name, exc)
        return

    await db.execute(
        "INSERT OR REPLACE INTO server_mirror_channels (source_channel_id, dest_channel_id, webhook_url) VALUES (?, ?, ?)",
        (src_channel.id, dst_channel.id, wh.url),
    )
    await db.commit()


async def _ensure_server_mirror_forum(
    db: aiosqlite.Connection,
    dst_guild: discord.Guild,
    src_forum: discord.ForumChannel,
    category_cache: dict[str, discord.CategoryChannel],
) -> None:
    async with db.execute(
        "SELECT webhook_url FROM server_mirror_forums WHERE source_forum_id = ?",
        (src_forum.id,),
    ) as cur:
        if await cur.fetchone():
            return

    dst_forum = discord.utils.get(dst_guild.forums, name=src_forum.name)
    if dst_forum is None:
        base_cat_name = src_forum.category.name if src_forum.category else None
        for overflow in range(20):
            dst_category: discord.CategoryChannel | None = (
                await _get_or_create_category(dst_guild, base_cat_name, overflow, category_cache)
                if base_cat_name else None
            )
            if base_cat_name and dst_category is None:
                return
            try:
                dst_forum = await dst_guild.create_forum(
                    src_forum.name,
                    category=dst_category,
                    topic=src_forum.topic or "",
                )
                console.info("Server mirror: created forum #%s in %s", src_forum.name, dst_guild.name)
                break
            except discord.HTTPException as exc:
                if base_cat_name and "Maximum number of channels in category" in exc.text:
                    continue
                console.warning("Server mirror: could not create forum #%s: %s", src_forum.name, exc)
                return
            except Exception as exc:
                console.warning("Server mirror: could not create forum #%s: %s", src_forum.name, exc)
                return
        else:
            console.warning("Server mirror: no category with room for forum #%s, giving up", src_forum.name)
            return

    try:
        webhooks = await dst_forum.webhooks()
        wh = discord.utils.get(webhooks, name="MessageMirror")
        if wh is None:
            wh = await dst_forum.create_webhook(name="MessageMirror")
            console.info("Server mirror: created webhook in forum #%s (%s)", dst_forum.name, dst_guild.name)
    except Exception as exc:
        console.warning("Server mirror: could not create webhook in forum #%s: %s", dst_forum.name, exc)
        return

    await db.execute(
        "INSERT OR REPLACE INTO server_mirror_forums (source_forum_id, dest_forum_id, webhook_url) VALUES (?, ?, ?)",
        (src_forum.id, dst_forum.id, wh.url),
    )
    await db.commit()


async def _clear_dest_guild(db: aiosqlite.Connection, dst_guild: discord.Guild) -> None:
    """Delete every channel and category in the destination guild, then wipe their DB entries.
    Skips the log channel so it is never accidentally removed."""
    # Snapshot IDs before deletion for DB cleanup
    channel_ids = [ch.id for ch in dst_guild.channels if not isinstance(ch, discord.CategoryChannel)]

    for channel in list(dst_guild.channels):
        if isinstance(channel, discord.CategoryChannel):
            continue
        if LOG_CHANNEL_ID and channel.id == LOG_CHANNEL_ID:
            continue
        try:
            await channel.delete()
        except Exception as exc:
            console.warning("Server mirror: could not delete #%s during dest clear: %s", channel.name, exc)

    for category in list(dst_guild.categories):
        try:
            await category.delete()
        except Exception as exc:
            console.warning("Server mirror: could not delete category '%s' during dest clear: %s", category.name, exc)

    if channel_ids:
        placeholders = ",".join("?" * len(channel_ids))
        await db.execute(
            f"DELETE FROM server_mirror_channels WHERE dest_channel_id IN ({placeholders})",
            channel_ids,
        )
        await db.execute(
            f"DELETE FROM server_mirror_forums WHERE dest_forum_id IN ({placeholders})",
            channel_ids,
        )
        await db.commit()

    console.info("Server mirror: cleared dest guild '%s'", dst_guild.name)


async def _probe_readable(channel: discord.TextChannel) -> bool:
    try:
        async for _ in channel.history(limit=1):
            break
        return True
    except discord.Forbidden:
        return False
    except Exception:
        return True  # treat transient errors as readable


async def _setup_server_mirrors(db: aiosqlite.Connection) -> None:
    if not MIRROR_SERVERS or _server_mirror_ready is None:
        return
    await _server_mirror_ready.wait()
    for src_guild_id, dst_guild_id in MIRROR_SERVERS:
        src_client = _guild_client.get(src_guild_id)
        dst_client = _guild_client.get(dst_guild_id)
        if src_client is None:
            console.warning("Server mirror: no client found in source guild %s — skipping", src_guild_id)
            continue
        if dst_client is None:
            console.warning("Server mirror: no client found in dest guild %s — skipping", dst_guild_id)
            continue
        src_guild = src_client.get_guild(src_guild_id)
        dst_guild = dst_client.get_guild(dst_guild_id)
        if src_guild is None or dst_guild is None:
            console.warning("Server mirror: guild object unavailable for %s→%s", src_guild_id, dst_guild_id)
            continue
        console.info("Server mirror: setting up %s → %s (%d text, %d voice, %d forum channels)",
                     src_guild.name, dst_guild.name, len(src_guild.text_channels),
                     len(src_guild.voice_channels), len(src_guild.forums))
        await _clear_dest_guild(db, dst_guild)
        category_cache: dict[str, discord.CategoryChannel] = {}
        for channel in src_guild.text_channels:
            await _ensure_server_mirror_channel(db, dst_guild, channel, category_cache)
        for channel in src_guild.voice_channels:
            await _ensure_server_mirror_voice_channel(db, dst_guild, channel, category_cache)
        for channel in src_guild.forums:
            await _ensure_server_mirror_forum(db, dst_guild, channel, category_cache)


ARCHIVE_CATEGORY_NAME = "📁 Archived"
UNREADABLE_CATEGORY_NAME = "🔒 Unreadable"

async def _archive_sync_worker(db: aiosqlite.Connection) -> None:
    """Every 30 minutes:
    - Archive dest channels whose source has disappeared.
    - Unarchive dest channels whose source has reappeared.
    - Re-check channels marked unreadable: move to proper category if now readable,
      or ensure they stay in the unreadable category if still forbidden.
    """
    if not MIRROR_SERVERS or _server_mirror_ready is None:
        return
    await _server_mirror_ready.wait()
    while True:
        await asyncio.sleep(1800)
        for src_guild_id, dst_guild_id in MIRROR_SERVERS:
            src_client = _guild_client.get(src_guild_id)
            dst_client = _guild_client.get(dst_guild_id)
            if src_client is None or dst_client is None:
                continue
            src_guild = src_client.get_guild(src_guild_id)
            dst_guild = dst_client.get_guild(dst_guild_id)
            if src_guild is None or dst_guild is None:
                continue

            async with db.execute(
                "SELECT source_channel_id, dest_channel_id, unreadable FROM server_mirror_channels"
            ) as cur:
                channel_rows = await cur.fetchall()
            async with db.execute(
                "SELECT source_forum_id, dest_forum_id FROM server_mirror_forums"
            ) as cur:
                forum_rows = await cur.fetchall()

            dst_archive_cat: discord.CategoryChannel | None = None
            dst_unreadable_cat: discord.CategoryChannel | None = None

            async def get_archive_cat() -> discord.CategoryChannel | None:
                nonlocal dst_archive_cat
                if dst_archive_cat is not None:
                    return dst_archive_cat
                dst_archive_cat = discord.utils.get(dst_guild.categories, name=ARCHIVE_CATEGORY_NAME)
                if dst_archive_cat is None:
                    try:
                        dst_archive_cat = await dst_guild.create_category(ARCHIVE_CATEGORY_NAME)
                        console.info("Archive sync: created '%s' in %s", ARCHIVE_CATEGORY_NAME, dst_guild.name)
                    except Exception as exc:
                        console.warning("Archive sync: could not create archive category: %s", exc)
                return dst_archive_cat

            async def get_unreadable_cat() -> discord.CategoryChannel | None:
                nonlocal dst_unreadable_cat
                if dst_unreadable_cat is not None:
                    return dst_unreadable_cat
                dst_unreadable_cat = discord.utils.get(dst_guild.categories, name=UNREADABLE_CATEGORY_NAME)
                if dst_unreadable_cat is None:
                    try:
                        dst_unreadable_cat = await dst_guild.create_category(UNREADABLE_CATEGORY_NAME)
                        console.info("Archive sync: created '%s' in %s", UNREADABLE_CATEGORY_NAME, dst_guild.name)
                    except Exception as exc:
                        console.warning("Archive sync: could not create unreadable category: %s", exc)
                return dst_unreadable_cat

            category_cache: dict[str, discord.CategoryChannel] = {}

            for row in channel_rows:
                src_id, dst_id, is_unreadable = row[0], row[1], row[2]
                src_ch = src_guild.get_channel(src_id)
                dst_ch = dst_guild.get_channel(dst_id)
                if dst_ch is None:
                    continue

                if src_ch is None:
                    # Source gone → archive regardless of readable state
                    cat = await get_archive_cat()
                    if cat is None or dst_ch.category_id == cat.id:
                        continue
                    try:
                        await dst_ch.edit(category=cat)
                        console.info("Archive sync: archived #%s in %s (source gone)", dst_ch.name, dst_guild.name)
                    except Exception as exc:
                        console.warning("Archive sync: could not archive #%s: %s", dst_ch.name, exc)
                elif is_unreadable:
                    # Re-probe: maybe permissions changed
                    now_readable = await _probe_readable(src_ch)
                    if now_readable:
                        target_cat = (
                            await _get_or_create_category(dst_guild, src_ch.category.name, 0, category_cache)
                            if src_ch.category else None
                        )
                        try:
                            await dst_ch.edit(category=target_cat)
                            await db.execute(
                                "UPDATE server_mirror_channels SET unreadable = 0 WHERE source_channel_id = ?",
                                (src_id,),
                            )
                            await db.commit()
                            cat_name = target_cat.name if target_cat else "no category"
                            console.info(
                                "Archive sync: #%s became readable, moved to '%s' in %s",
                                dst_ch.name, cat_name, dst_guild.name,
                            )
                        except Exception as exc:
                            console.warning("Archive sync: could not move newly-readable #%s: %s", dst_ch.name, exc)
                    else:
                        # Still unreadable — ensure it's in the unreadable category
                        unreadable_cat = await get_unreadable_cat()
                        if unreadable_cat is not None and dst_ch.category_id != unreadable_cat.id:
                            try:
                                await dst_ch.edit(category=unreadable_cat)
                                console.info(
                                    "Archive sync: moved #%s to '%s' in %s",
                                    dst_ch.name, UNREADABLE_CATEGORY_NAME, dst_guild.name,
                                )
                            except Exception as exc:
                                console.warning("Archive sync: could not move unreadable #%s: %s", dst_ch.name, exc)
                else:
                    # Readable channel — unarchive if it ended up in the archive category
                    archive_cat = await get_archive_cat()
                    if archive_cat is None or dst_ch.category_id != archive_cat.id:
                        continue
                    target_cat = (
                        await _get_or_create_category(dst_guild, src_ch.category.name, 0, category_cache)
                        if src_ch.category else None
                    )
                    try:
                        await dst_ch.edit(category=target_cat)
                        cat_name = target_cat.name if target_cat else "no category"
                        console.info("Archive sync: unarchived #%s → '%s' in %s", dst_ch.name, cat_name, dst_guild.name)
                    except Exception as exc:
                        console.warning("Archive sync: could not unarchive #%s: %s", dst_ch.name, exc)

            for row in forum_rows:
                src_id, dst_id = row[0], row[1]
                src_ch = src_guild.get_channel(src_id)
                dst_ch = dst_guild.get_channel(dst_id)
                if dst_ch is None:
                    continue

                if src_ch is None:
                    cat = await get_archive_cat()
                    if cat is None or dst_ch.category_id == cat.id:
                        continue
                    try:
                        await dst_ch.edit(category=cat)
                        console.info("Archive sync: archived forum #%s in %s (source gone)", dst_ch.name, dst_guild.name)
                    except Exception as exc:
                        console.warning("Archive sync: could not archive forum #%s: %s", dst_ch.name, exc)
                else:
                    archive_cat = await get_archive_cat()
                    if archive_cat is None or dst_ch.category_id != archive_cat.id:
                        continue
                    target_cat = (
                        await _get_or_create_category(dst_guild, src_ch.category.name, 0, category_cache)
                        if src_ch.category else None
                    )
                    try:
                        await dst_ch.edit(category=target_cat)
                        cat_name = target_cat.name if target_cat else "no category"
                        console.info("Archive sync: unarchived forum #%s → '%s' in %s", dst_ch.name, cat_name, dst_guild.name)
                    except Exception as exc:
                        console.warning("Archive sync: could not unarchive forum #%s: %s", dst_ch.name, exc)


# ── Client ────────────────────────────────────────────────────────────────────

class MessageLogger(discord.Client):
    def __init__(self, db: aiosqlite.Connection, token_index: int = 0, poster_only: bool = False) -> None:
        super().__init__()
        self._db = db
        self._token_index = token_index
        self._poster_only = poster_only
        self._session: aiohttp.ClientSession | None = None
        self._log_channel: discord.TextChannel | None = None

    def _is_watched_guild(self, guild_id: int | None) -> bool:
        if self._poster_only:
            return False
        if guild_id is None:
            return not bool(WATCHED_GUILDS)
        if WATCHED_GUILDS and guild_id not in WATCHED_GUILDS:
            return False
        return _guild_owner.get(guild_id) == id(self)

    def _is_watched(self, message: discord.Message) -> bool:
        return self._is_watched_guild(message.guild.id if message.guild else None)

    async def setup_hook(self) -> None:
        self._session = aiohttp.ClientSession()

    async def close(self) -> None:
        if self._session:
            await self._session.close()
        await super().close()

    async def _cache_message(self, message: discord.Message,
                              attachments: list[dict] | None = None,
                              stickers: list[dict] | None = None) -> None:
        att_json = json.dumps(attachments if attachments is not None else [
            {"filename": a.filename, "url": a.url} for a in message.attachments
        ])
        stk_json = json.dumps(stickers if stickers is not None else [
            {"id": s.id, "name": s.name, "format": s.format.name} for s in message.stickers
        ])
        await self._db.execute(
            """INSERT OR REPLACE INTO messages
               (id, author, author_id, channel, guild_name, content, created_at, attachments, stickers, avatar_url)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                message.id,
                str(message.author),
                message.author.id,
                str(message.channel),
                message.guild.name if message.guild else None,
                message.content,
                message.created_at.isoformat(),
                att_json,
                stk_json,
                str(message.author.display_avatar.url),
            ),
        )
        await self._db.commit()

    async def _pop_cached(self, message_id: int) -> CachedMessage | None:
        async with self._db.execute(
            "SELECT id, author, author_id, channel, guild_name, content, created_at, attachments, stickers "
            "FROM messages WHERE id = ?",
            (message_id,),
        ) as cursor:
            row = await cursor.fetchone()
        if not row:
            return None
        return CachedMessage(
            id=row[0],
            author=row[1],
            author_id=row[2],
            channel=row[3],
            guild_name=row[4],
            content=row[5] or "",
            created_at=datetime.fromisoformat(row[6]),
            attachments=json.loads(row[7]) if row[7] else [],
            stickers=json.loads(row[8]) if row[8] else [],
        )

    # ── Events ────────────────────────────────────────────────────────────────

    async def _send_chunked(self, text: str, files: list[discord.File]) -> None:
        """Send text to the log channel, splitting at Discord's 2000-char limit."""
        if self._log_channel is None:
            return
        chunks: list[str] = []
        while text:
            if len(text) <= 2000:
                chunks.append(text)
                break
            split_at = text.rfind("\n", 0, 2000)
            if split_at == -1:
                split_at = 2000
            chunks.append(text[:split_at])
            text = text[split_at:].lstrip("\n")
        for i, chunk in enumerate(chunks):
            await self._log_channel.send(
                chunk,
                files=files if i == len(chunks) - 1 else [],
            )

    async def _log_to_channel(self, text: str, files: list[discord.File] | None = None) -> None:
        """Enqueue a post; _post_worker sends it sequentially with retry."""
        await _post_queue.put((text, files or []))

    async def on_ready(self) -> None:
        global _log_poster, _ready_count
        label = "poster" if self._poster_only else f"token[{self._token_index}]"
        console.info("%s: logged in as %s (id: %s)", label, self.user, self.user.id)

        token = LOG_POSTER_TOKEN if self._poster_only else TOKENS[self._token_index]
        await self._db.execute(
            """INSERT OR REPLACE INTO accounts
               (user_id, username, avatar_url, token, poster_only, token_index)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                self.user.id, str(self.user), str(self.user.display_avatar.url),
                token, self._poster_only, None if self._poster_only else self._token_index,
            ),
        )
        await self._db.commit()

        if self._poster_only:
            for guild in self.guilds:
                _guild_client.setdefault(guild.id, self)
            if LOG_CHANNEL_ID:
                ch = self.get_channel(LOG_CHANNEL_ID)
                if isinstance(ch, discord.TextChannel):
                    _log_poster = self
                    self._log_channel = ch
                    console.info("Log channel: #%s (%s) (dedicated poster: %s)", ch.name, ch.id, self.user)
            _ready_count += 1
            if _server_mirror_ready is not None and _ready_count >= _total_clients:
                _server_mirror_ready.set()
            return

        claimed = []
        for guild in self.guilds:
            if guild.id not in _guild_owner:
                _guild_owner[guild.id] = id(self)
                claimed.append(guild.name)
            _guild_client.setdefault(guild.id, self)
        if claimed:
            console.info("Claimed guilds: %s", claimed)
        if LOG_CHANNEL_ID and _log_poster is None:
            ch = self.get_channel(LOG_CHANNEL_ID)
            if isinstance(ch, discord.TextChannel):
                _log_poster = self
                self._log_channel = ch
                console.info("Log channel: #%s (%s) (poster: %s)", ch.name, ch.id, self.user)
        _ready_count += 1
        if _server_mirror_ready is not None and _ready_count >= _total_clients:
            _server_mirror_ready.set()

    async def on_message(self, message: discord.Message) -> None:
        if not self._is_watched(message):
            return
        if self._log_channel and message.channel.id == self._log_channel.id:
            return

        if message.guild:
            _write_guild_name(message.guild)

        att_records: list[dict] = []
        for att in message.attachments:
            local = await _save_attachment(self._session, message, att)
            rec: dict = {"filename": att.filename, "url": att.url}
            if local:
                rec["local_path"] = local
            att_records.append(rec)

        stk_records: list[dict] = []
        for s in message.stickers:
            local = await _save_sticker(self._session, message, s)
            rec = {"id": s.id, "name": s.name, "format": s.format.name}
            if local:
                rec["local_path"] = local
            stk_records.append(rec)

        await self._cache_message(message, att_records, stk_records)

        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        path = _log_path(_guild_name(message), str(message.channel), date)

        lines: list[str] = [
            f"[{_format_ts(message.created_at)}] "
            f"[NEW] {message.author} ({message.author.id}) "
            f"in {_channel_label(message)}\n",
            f"  {message.content}\n" if message.content else "",
        ]

        if att_records:
            lines.append(f"  Attachments: {len(att_records)}\n")
            for rec in att_records:
                local_label = f"  →  {rec['local_path']}" if "local_path" in rec else ""
                lines.append(f"    - {rec['filename']}  ({rec['url']}){local_label}\n")

        if stk_records:
            lines.append(f"  Stickers: {len(stk_records)}\n")
            for rec in stk_records:
                lines.append(f"    - {rec['name']} (id: {rec['id']}, format: {rec['format']})\n")

        if message.embeds:
            lines.append(f"  Embeds: {len(message.embeds)}\n")

        lines.append("\n")
        _write(path, "".join(lines))

        if not self._poster_only and message.channel.id in MIRROR_MAP:
            if not message.guild or _guild_owner.get(message.guild.id) == id(self):
                reply_to: int | None = message.reference.message_id if message.reference else None
                for wurl in MIRROR_MAP[message.channel.id]:
                    await self._db.execute(
                        "INSERT OR IGNORE INTO mirror_queue (message_id, webhook_url, reply_to) VALUES (?, ?, ?)",
                        (message.id, wurl, reply_to),
                    )
                await self._db.commit()

        if message.guild and message.guild.id in _server_mirror_src:
            reply_to_srv: int | None = message.reference.message_id if message.reference else None
            async with self._db.execute(
                "SELECT webhook_url, dest_channel_id FROM server_mirror_channels WHERE source_channel_id = ?",
                (message.channel.id,),
            ) as cur:
                row = await cur.fetchone()
            if row:
                dest_thread_id = row[1] if isinstance(message.channel, discord.Thread) else None
                await self._db.execute(
                    "INSERT OR IGNORE INTO mirror_queue (message_id, webhook_url, dest_thread_id, reply_to) VALUES (?, ?, ?, ?)",
                    (message.id, row[0], dest_thread_id, reply_to_srv),
                )
                await self._db.commit()
            elif isinstance(message.channel, discord.Thread) and isinstance(message.channel.parent, discord.ForumChannel):
                async with self._db.execute(
                    "SELECT webhook_url FROM server_mirror_forums WHERE source_forum_id = ?",
                    (message.channel.parent_id,),
                ) as cur:
                    forum_row = await cur.fetchone()
                if forum_row:
                    await self._create_forum_thread_and_send(message, forum_row[0], att_records, stk_records)

    async def on_message_edit(self,
                               before: discord.Message,
                               after: discord.Message) -> None:
        if not self._is_watched(after):
            return
        if self._log_channel and after.channel.id == self._log_channel.id:
            return
        if before.content == after.content:
            return

        async with self._db.execute(
            "SELECT attachments, stickers FROM messages WHERE id = ?", (before.id,)
        ) as cur:
            existing = await cur.fetchone()
        att_records: list[dict] = (
            json.loads(existing[0]) if existing and existing[0] else
            [{"filename": a.filename, "url": a.url} for a in after.attachments]
        )
        stk_records: list[dict] = (
            json.loads(existing[1]) if existing and existing[1] else
            [{"id": s.id, "name": s.name, "format": s.format.name} for s in after.stickers]
        )
        await self._cache_message(after, att_records, stk_records)

        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        path = _log_path(_guild_name(after), str(after.channel), date)

        text = (
            f"[{_format_ts(after.edited_at or datetime.now(timezone.utc))}] "
            f"[EDIT] {after.author} ({after.author.id}) "
            f"in {_channel_label(after)}\n"
            f"  BEFORE: {before.content}\n"
            f"  AFTER:  {after.content}\n\n"
        )
        _write(path, text)
        guild_dir = MEDIA_DIR / (str(after.guild.id) if after.guild else "DMs")
        files = [
            discord.File(p)
            for att in att_records
            if (p := _att_path(att, guild_dir, before.id)).exists()
        ]
        edited_ts = int((after.edited_at or datetime.now(timezone.utc)).timestamp())
        channel_post = "\n".join([
            f"✏️ Message Edited",
            f"Channel: {_channel_label(after)}" + (f"  ·  {after.guild.name}" if after.guild else ""),
            f"Author: {discord.utils.escape_markdown(str(after.author))} ({after.author.id})",
            f"Edited: <t:{edited_ts}:R>",
            f"Before: {discord.utils.escape_markdown(before.content)}",
            f"After: {discord.utils.escape_markdown(after.content)}",
        ])
        await self._log_to_channel(channel_post, files=files)

        mirror_urls = await _mirror_webhooks_for_channel(self._db, after.channel.id)
        for wurl in mirror_urls:
            notif = "\n".join([
                f"✏️ **{discord.utils.escape_markdown(str(after.author))}** edited their message",
                f"Before: {discord.utils.escape_markdown(before.content)}",
                f"After: {discord.utils.escape_markdown(after.content)}",
            ])
            await self._db.execute(
                "INSERT INTO mirror_notifications (webhook_url, content) VALUES (?, ?)",
                (wurl, notif),
            )
        if mirror_urls:
            await self._db.commit()

        console.info("Edit logged: %s in %s", after.author, _channel_label(after))

    async def on_raw_message_delete(self, payload: discord.RawMessageDeleteEvent) -> None:
        if not self._is_watched_guild(payload.guild_id):
            return
        if self._log_channel and payload.channel_id == self._log_channel.id:
            return

        cached = await self._pop_cached(payload.message_id)

        if cached is None and payload.cached_message is not None:
            msg = payload.cached_message
            cached = CachedMessage(
                id=msg.id,
                author=str(msg.author),
                author_id=msg.author.id,
                channel=str(msg.channel),
                guild_name=msg.guild.name if msg.guild else None,
                content=msg.content,
                created_at=msg.created_at,
                attachments=[{"filename": a.filename, "url": a.url} for a in msg.attachments],
                stickers=[{"id": s.id, "name": s.name, "format": s.format.name} for s in msg.stickers],
            )

        guild_dir = MEDIA_DIR / (str(payload.guild_id) if payload.guild_id else "DMs")
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        if cached is None:
            ch = self.get_channel(payload.channel_id)
            channel_str = getattr(ch, "name", str(payload.channel_id))
            guild_name: str | None = None
            if payload.guild_id:
                g = self.get_guild(payload.guild_id)
                guild_name = g.name if g else None
            channel_label = f"#{channel_str}" if guild_name else f"DM:{channel_str}"
            path = _log_path(guild_name, channel_str, date)
            _write(path, (
                f"[{_format_ts(datetime.now(timezone.utc))}] "
                f"[DELETE] <unknown> in {channel_label}\n"
                f"  Message ID: {payload.message_id}\n"
                f"  Content: <unknown>\n\n"
            ))
            mirror_urls = await _mirror_webhooks_for_channel(self._db, payload.channel_id)
            for wurl in mirror_urls:
                await self._db.execute(
                    "INSERT INTO mirror_notifications (webhook_url, content) VALUES (?, ?)",
                    (wurl, f"🗑️ A message was deleted (id: {payload.message_id})"),
                )
            if mirror_urls:
                await self._db.commit()
            console.info("Delete logged (no cache): msg %s in %s", payload.message_id, channel_label)
            return

        channel_label = f"#{cached.channel}" if cached.guild_name else f"DM:{cached.channel}"
        path = _log_path(cached.guild_name, cached.channel, date)

        lines: list[str] = [
            f"[{_format_ts(datetime.now(timezone.utc))}] "
            f"[DELETE] {cached.author} ({cached.author_id}) "
            f"in {channel_label}\n"
            f"  Originally sent: {_format_ts(cached.created_at)}\n",
            f"  Content: {cached.content}\n" if cached.content else "  Content: <unknown>\n",
        ]

        if cached.attachments:
            lines.append(f"  Attachments ({len(cached.attachments)}):\n")
            for att in cached.attachments:
                local = _att_path(att, guild_dir, cached.id)
                label = str(local.relative_to(MEDIA_DIR)) if local.exists() else att['url']
                lines.append(f"    - {att['filename']}  →  {label}\n")

        if cached.stickers:
            lines.append(f"  Stickers ({len(cached.stickers)}):\n")
            for s in cached.stickers:
                lines.append(f"    - {s['name']} (id: {s['id']}, format: {s['format']})\n")

        lines.append("\n")
        _write(path, "".join(lines))
        files = [
            discord.File(p)
            for att in cached.attachments
            if (p := _att_path(att, guild_dir, cached.id)).exists()
        ]
        for s in cached.stickers:
            ext = "gif" if s["format"].lower() == "gif" else "png"
            p = MEDIA_DIR / s["local_path"] if "local_path" in s else guild_dir / "stickers" / f"{s['id']}.{ext}"
            if p.exists():
                files.append(discord.File(p, filename=f"{s['name']}.{ext}"))
        sent_ts = int(cached.created_at.timestamp())
        post_lines = [
            f"🗑️ Message Deleted",
            f"Channel: {channel_label}" + (f"  ·  {cached.guild_name}" if cached.guild_name else ""),
            f"Author: {discord.utils.escape_markdown(cached.author)} ({cached.author_id})",
            f"Sent: <t:{sent_ts}:R>",
            f"Content: {discord.utils.escape_markdown(cached.content) if cached.content else '<no text>'}",
        ]
        if cached.attachments:
            post_lines.append("Attachments: " + "  ".join(a['filename'] for a in cached.attachments))
        if cached.stickers:
            post_lines.append("Stickers: " + "  ".join(s['name'] for s in cached.stickers))
        await self._log_to_channel("\n".join(post_lines), files=files)

        mirror_urls = await _mirror_webhooks_for_channel(self._db, payload.channel_id)
        for wurl in mirror_urls:
            async with self._db.execute(
                "SELECT jump_url FROM mirror_message_map WHERE source_message_id = ? AND webhook_url = ?",
                (payload.message_id, wurl),
            ) as cur:
                jump_row = await cur.fetchone()
            notif = f"🗑️ **{discord.utils.escape_markdown(cached.author)}** deleted their message"
            if jump_row:
                notif += f" — [jump to mirror]({jump_row[0]})"
            await self._db.execute(
                "INSERT INTO mirror_notifications (webhook_url, content) VALUES (?, ?)",
                (wurl, notif),
            )
        if mirror_urls:
            await self._db.commit()

        console.info("Delete logged: %s in %s", cached.author, channel_label)

    async def on_raw_bulk_message_delete(self, payload: discord.RawBulkMessageDeleteEvent) -> None:
        if not self._is_watched_guild(payload.guild_id):
            return
        if self._log_channel and payload.channel_id == self._log_channel.id:
            return

        dpy_cached: dict[int, discord.Message] = {m.id: m for m in payload.cached_messages}

        ch = self.get_channel(payload.channel_id)
        channel_str = getattr(ch, "name", str(payload.channel_id))
        guild_name: str | None = None
        if payload.guild_id:
            g = self.get_guild(payload.guild_id)
            guild_name = g.name if g else None
        channel_label = f"#{channel_str}" if guild_name else f"DM:{channel_str}"

        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        entries: list[tuple] = []

        for message_id in payload.message_ids:
            cached = await self._pop_cached(message_id)

            if cached is None and message_id in dpy_cached:
                msg = dpy_cached[message_id]
                cached = CachedMessage(
                    id=msg.id,
                    author=str(msg.author),
                    author_id=msg.author.id,
                    channel=str(msg.channel),
                    guild_name=msg.guild.name if msg.guild else None,
                    content=msg.content,
                    created_at=msg.created_at,
                    attachments=[{"filename": a.filename, "url": a.url} for a in msg.attachments],
                    stickers=[{"id": s.id, "name": s.name, "format": s.format.name} for s in msg.stickers],
                )

            author_str = cached.author if cached else "<unknown>"
            author_id = cached.author_id if cached else 0
            content = cached.content if cached else "<unknown>"
            stickers = cached.stickers if cached else []

            entries.append((author_str, author_id, content, stickers))
            path = _log_path(guild_name, channel_str, date)
            log_lines = [
                f"[{_format_ts(datetime.now(timezone.utc))}] "
                f"[BULK-DELETE] {author_str} ({author_id}) "
                f"in {channel_label}\n",
                f"  Content: {content}\n",
            ]
            if stickers:
                log_lines.append(
                    f"  Stickers ({len(stickers)}): "
                    + ", ".join(s["name"] for s in stickers) + "\n"
                )
            log_lines.append("\n")
            _write(path, "".join(log_lines))

        post_lines = [
            f"🗑️ Bulk Delete ({len(payload.message_ids)} messages)",
            f"Channel: {channel_label}" + (f"  ·  {guild_name}" if guild_name else ""),
        ]
        MAX_PREVIEW = 10
        for author_str, _, content, _ in entries[:MAX_PREVIEW]:
            author = discord.utils.escape_markdown(author_str)
            preview = discord.utils.escape_markdown(content[:80])
            if len(content) > 80:
                preview += "…"
            post_lines.append(f"  {author}: {preview}")
        if len(entries) > MAX_PREVIEW:
            post_lines.append(f"  [+ {len(entries) - MAX_PREVIEW} more]")
        await self._log_to_channel("\n".join(post_lines))
        console.info("Bulk delete: %d messages", len(payload.message_ids))

    async def _create_forum_thread_and_send(
        self,
        message: discord.Message,
        webhook_url: str,
        att_records: list[dict],
        stk_records: list[dict],
    ) -> None:
        """Create a thread in the dest forum and post the first message; store the thread mapping."""
        try:
            wh = discord.Webhook.from_url(webhook_url, session=self._session)
            files: list[discord.File] = []
            extra_urls: list[str] = []

            for att in att_records:
                try:
                    async with _download_sem:
                        async with self._session.get(att["url"]) as resp:
                            if resp.status == 200:
                                data = await resp.read()
                                if len(data) < 8 * 1024 * 1024:
                                    files.append(discord.File(io.BytesIO(data), filename=att["filename"]))
                                else:
                                    extra_urls.append(att["url"])
                            else:
                                extra_urls.append(att["url"])
                except Exception as exc:
                    console.warning("Forum mirror attachment fetch failed (%s): %s", att["filename"], exc)
                    extra_urls.append(att["url"])

            for s in stk_records:
                extra_urls.append(f"https://media.discordapp.net/stickers/{s['id']}.webp")

            post_content = message.content or ""
            if extra_urls:
                post_content = (post_content + "\n" + "\n".join(extra_urls)).strip()

            sent = await wh.send(
                content=post_content or "​",
                username=str(message.author),
                avatar_url=str(message.author.display_avatar.url),
                thread_name=message.channel.name,
                wait=True,
            )
            dest_thread_id = sent.channel.id
            await self._db.execute(
                "INSERT OR REPLACE INTO server_mirror_channels (source_channel_id, dest_channel_id, webhook_url, unreadable) VALUES (?, ?, ?, ?)",
                (message.channel.id, dest_thread_id, webhook_url, 0),
            )
            await self._db.commit()
            console.info("Forum mirror: created thread '%s' in dest forum", message.channel.name)
        except Exception as exc:
            console.warning("Forum thread mirror failed for '%s': %s", message.channel.name, exc)

    async def on_guild_channel_create(self, channel: discord.abc.GuildChannel) -> None:
        if not isinstance(channel, (discord.TextChannel, discord.VoiceChannel, discord.ForumChannel)):
            return
        for src_id, dst_id in MIRROR_SERVERS:
            if channel.guild.id == src_id:
                dst_client = _guild_client.get(dst_id)
                if dst_client:
                    dst_guild = dst_client.get_guild(dst_id)
                    if dst_guild:
                        if isinstance(channel, discord.TextChannel):
                            await _ensure_server_mirror_channel(self._db, dst_guild, channel, {})
                        elif isinstance(channel, discord.VoiceChannel):
                            await _ensure_server_mirror_voice_channel(self._db, dst_guild, channel, {})
                        else:
                            await _ensure_server_mirror_forum(self._db, dst_guild, channel, {})
                break

    async def on_error(self, event: str, *args, **kwargs) -> None:  # type: ignore[override]
        import traceback
        console.error("Error in %s:\n%s", event, traceback.format_exc())


async def _mirror_webhooks_for_channel(db: aiosqlite.Connection, channel_id: int) -> list[str]:
    """Return all webhook URLs that should receive mirror posts for channel_id."""
    urls = list(MIRROR_MAP.get(channel_id, []))
    async with db.execute(
        "SELECT webhook_url FROM server_mirror_channels WHERE source_channel_id = ?",
        (channel_id,),
    ) as cur:
        for row in await cur.fetchall():
            urls.append(row[0])
    return urls


async def _mirror_worker(db: aiosqlite.Connection, session: aiohttp.ClientSession) -> None:
    while True:
        try:
            await _mirror_worker_tick(db, session)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            console.warning("Mirror worker: unexpected error, restarting: %s", exc)
            await asyncio.sleep(5.0)


async def _mirror_worker_tick(db: aiosqlite.Connection, session: aiohttp.ClientSession) -> None:
    try:
        async with db.execute(
            """SELECT q.id, q.message_id, q.webhook_url, q.dest_thread_id, q.reply_to,
                      m.author, m.avatar_url, m.content, m.attachments, m.stickers
               FROM mirror_queue q
               JOIN messages m ON q.message_id = m.id
               ORDER BY q.id
               LIMIT 1"""
        ) as cursor:
            row = await cursor.fetchone()
    except Exception as exc:
        console.warning("Mirror worker: DB fetch error: %s", exc)
        await asyncio.sleep(5.0)
        return

    if row is None:
        try:
            async with db.execute(
                "SELECT id, webhook_url, content FROM mirror_notifications ORDER BY id LIMIT 1"
            ) as cur:
                notif = await cur.fetchone()
        except Exception as exc:
            console.warning("Mirror worker: DB fetch error (notifications): %s", exc)
            await asyncio.sleep(5.0)
            return
        if notif is not None:
            try:
                wh = discord.Webhook.from_url(notif[1], session=session)
                await wh.send(content=notif[2], username="Message Logger")
            except Exception as exc:
                console.warning("Mirror notification to %s failed: %s", notif[1][:40], exc)
            finally:
                try:
                    await db.execute("DELETE FROM mirror_notifications WHERE id = ?", (notif[0],))
                    await db.commit()
                except Exception as exc:
                    console.warning("Mirror worker: failed to delete notification %s: %s", notif[0], exc)
            return
        await asyncio.sleep(0.5)
        return

    queue_id, source_message_id, webhook_url, dest_thread_id, reply_to = row[0], row[1], row[2], row[3], row[4]
    author, avatar_url, content, attachments_json, stickers_json = row[5], row[6], row[7], row[8], row[9]
    attachments: list[dict] = json.loads(attachments_json) if attachments_json else []
    stickers: list[dict] = json.loads(stickers_json) if stickers_json else []

    try:
        wh = discord.Webhook.from_url(webhook_url, session=session)
        files: list[discord.File] = []
        extra_urls: list[str] = []

        for att in attachments:
            try:
                async with _download_sem:
                    async with session.get(att["url"], timeout=aiohttp.ClientTimeout(total=30)) as resp:
                        if resp.status == 200:
                            data = await resp.read()
                            if len(data) < 8 * 1024 * 1024:
                                files.append(discord.File(io.BytesIO(data), filename=att["filename"]))
                            else:
                                extra_urls.append(att["url"])
                        else:
                            extra_urls.append(att["url"])
            except Exception as exc:
                console.warning("Mirror attachment fetch failed (%s): %s", att["filename"], exc)
                extra_urls.append(att["url"])

        for s in stickers:
            extra_urls.append(f"https://media.discordapp.net/stickers/{s['id']}.webp")

        post_content = content or ""
        if reply_to is not None:
            async with db.execute(
                "SELECT author, content FROM messages WHERE id = ?", (reply_to,)
            ) as cur:
                ref_row = await cur.fetchone()
            if ref_row:
                ref_full = ref_row[1] or ""
                ref_preview = ref_full[:100] + ("…" if len(ref_full) > 100 else "")
                async with db.execute(
                    "SELECT jump_url FROM mirror_message_map WHERE source_message_id = ? AND webhook_url = ?",
                    (reply_to, webhook_url),
                ) as cur:
                    jump_row = await cur.fetchone()
                if jump_row:
                    ref_preview += f" [↗]({jump_row[0]})"
                post_content = f"> **{ref_row[0]}**: {ref_preview}\n{post_content}"
        if extra_urls:
            post_content = (post_content + "\n" + "\n".join(extra_urls)).strip()

        sent = await wh.send(
            content=post_content or "​",
            username=author,
            avatar_url=avatar_url,
            files=files or discord.utils.MISSING,
            thread=discord.Object(id=dest_thread_id) if dest_thread_id else discord.utils.MISSING,
            wait=True,
        )
        await db.execute(
            "INSERT OR IGNORE INTO mirror_message_map (source_message_id, webhook_url, jump_url) VALUES (?, ?, ?)",
            (source_message_id, webhook_url, sent.jump_url),
        )
        await db.commit()
        console.info("Mirrored message to %s", webhook_url[:40])
    except Exception as exc:
        console.warning("Mirror post to %s failed: %s", webhook_url[:40], exc)
    finally:
        try:
            await db.execute("DELETE FROM mirror_queue WHERE id = ?", (queue_id,))
            await db.commit()
        except Exception as exc:
            console.warning("Mirror worker: failed to delete queue item %s: %s", queue_id, exc)


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    global _total_clients, _server_mirror_ready
    console.info("Starting %d account(s)", len(TOKENS))

    worker = asyncio.create_task(_post_worker(), name="log-poster")
    db = await aiosqlite.connect("data/cache.db")
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id          INTEGER PRIMARY KEY,
            author      TEXT    NOT NULL,
            author_id   INTEGER NOT NULL,
            channel     TEXT    NOT NULL,
            guild_name  TEXT,
            content     TEXT,
            created_at  TEXT    NOT NULL,
            attachments TEXT,
            stickers    TEXT,
            avatar_url  TEXT
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS mirror_queue (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id     INTEGER NOT NULL,
            webhook_url    TEXT    NOT NULL,
            dest_thread_id INTEGER,
            reply_to       INTEGER,
            UNIQUE(message_id, webhook_url)
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS server_mirror_channels (
            source_channel_id  INTEGER PRIMARY KEY,
            dest_channel_id    INTEGER NOT NULL,
            webhook_url        TEXT    NOT NULL,
            unreadable         INTEGER NOT NULL DEFAULT 0
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS server_mirror_forums (
            source_forum_id INTEGER PRIMARY KEY,
            dest_forum_id   INTEGER NOT NULL,
            webhook_url     TEXT    NOT NULL
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS mirror_notifications (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            webhook_url TEXT NOT NULL,
            content     TEXT NOT NULL
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS mirror_message_map (
            source_message_id INTEGER NOT NULL,
            webhook_url       TEXT    NOT NULL,
            jump_url          TEXT    NOT NULL,
            PRIMARY KEY (source_message_id, webhook_url)
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            user_id     INTEGER PRIMARY KEY,
            username    TEXT    NOT NULL,
            avatar_url  TEXT,
            token       TEXT    NOT NULL,
            poster_only INTEGER NOT NULL DEFAULT 0,
            token_index INTEGER
        )
    """)
    # Migrations for existing DBs
    for migration in [
        "ALTER TABLE messages ADD COLUMN avatar_url TEXT",
        "ALTER TABLE mirror_queue ADD COLUMN dest_thread_id INTEGER",
        "ALTER TABLE mirror_queue ADD COLUMN reply_to INTEGER",
        "ALTER TABLE server_mirror_channels ADD COLUMN unreadable INTEGER NOT NULL DEFAULT 0",
    ]:
        try:
            await db.execute(migration)
        except Exception:
            pass
    await db.commit()

    mirror_session = aiohttp.ClientSession()
    mirror_worker = asyncio.create_task(
        _mirror_worker(db, mirror_session), name="mirror-worker"
    )

    clients: list[MessageLogger] = [MessageLogger(db, token_index=i) for i in range(len(TOKENS))]
    tokens: list[str] = list(TOKENS)
    _total_clients = len(clients)
    _server_mirror_ready = asyncio.Event()
    if LOG_POSTER_TOKEN:
        clients.append(MessageLogger(db, poster_only=True))
        tokens.append(LOG_POSTER_TOKEN)
        _total_clients += 1

    server_mirror_setup = asyncio.create_task(_setup_server_mirrors(db), name="server-mirror-setup")
    archive_sync = asyncio.create_task(_archive_sync_worker(db), name="archive-sync")

    async def _start_client(client: MessageLogger, token: str) -> None:
        global _total_clients
        try:
            await client.start(token)
        except (discord.LoginFailure, discord.HTTPException) as exc:
            label = "poster" if client._poster_only else f"token[{client._token_index}]"
            console.error("%s: login failed, skipping: %s", label, exc)
            _total_clients -= 1
            if _server_mirror_ready is not None and _ready_count >= _total_clients:
                _server_mirror_ready.set()

    try:
        await asyncio.gather(*[
            _start_client(client, token)
            for client, token in zip(clients, tokens)
        ])
    finally:
        await asyncio.gather(*[client.close() for client in clients])
        await db.close()
        for task in (worker, mirror_worker, server_mirror_setup, archive_sync):
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        await mirror_session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
