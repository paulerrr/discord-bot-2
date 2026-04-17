import discord
import asyncio
import io
import os
import json
import aiohttp
import asyncpg
import logging
import logging.handlers
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

# Comma-separated tokens, one per account.
TOKENS: list[str] = [
    t.strip()
    for t in os.environ.get("DISCORD_TOKENS", os.environ.get("DISCORD_TOKEN", "")).split(",")
    if t.strip()
]
if not TOKENS:
    raise RuntimeError("Set DISCORD_TOKENS (or DISCORD_TOKEN) in your .env")

# Comma-separated guild IDs to watch across all accounts, or empty to watch all.
WATCHED_GUILDS: list[int] = [
    int(g) for g in os.environ.get("WATCHED_GUILDS", "").split(",") if g.strip()
]

# Optional channel ID to mirror log entries into Discord.
LOG_CHANNEL_ID: int | None = (
    int(os.environ["LOG_CHANNEL_ID"]) if os.environ.get("LOG_CHANNEL_ID") else None
)

# Optional dedicated token whose sole job is posting to LOG_CHANNEL_ID.
# This account does not need to be in any WATCHED_GUILDS.
LOG_POSTER_TOKEN: str | None = os.environ.get("LOG_POSTER_TOKEN", "").strip() or None

# channel_id:webhook_url pairs, comma-separated.
# A channel can appear multiple times to fan out to multiple webhooks.
MIRROR_MAP: dict[int, list[str]] = {}
for _pair in os.environ.get("MIRROR_CHANNELS", "").split(","):
    _pair = _pair.strip()
    if not _pair:
        continue
    _cid, _, _wurl = _pair.partition(":")
    _cid, _wurl = _cid.strip(), _wurl.strip()
    if _cid and _wurl:
        MIRROR_MAP.setdefault(int(_cid), []).append(_wurl)

DATABASE_URL: str = os.environ.get("SUPABASE_DATABASE_URL", "")
if not DATABASE_URL:
    raise RuntimeError("Set SUPABASE_DATABASE_URL in your .env")

BASE_LOG_DIR = Path("logs")
MEDIA_DIR = Path("media")
# ─────────────────────────────────────────────────────────────────────────────

BASE_LOG_DIR.mkdir(exist_ok=True)
MEDIA_DIR.mkdir(exist_ok=True)

# Internal discord.py library logs → logs/discord.log (rotating, 10 MiB × 3)
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

# Application-level console logger
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


@dataclass
class CachedMessage:
    id: int
    author: str
    author_id: int
    channel: str
    guild_name: str | None
    content: str
    created_at: datetime
    attachments: list[dict]  # [{filename, url}, ...]
    stickers: list[dict]     # [{id, name, format}, ...]


# Maps guild_id → the client instance responsible for logging it.
# First account to connect claims each guild; others skip it.
_guild_owner: dict[int, int] = {}  # guild_id → id(client)

# The first client that can see LOG_CHANNEL_ID claims it for posting.
# This may differ from the guild owner if that account lacks channel access.
_log_poster: "MessageLogger | None" = None

# Serialises all log-channel posts; prevents burst rate-limit exposure.
_post_queue: asyncio.Queue[tuple[str, list]] = asyncio.Queue()
# Caps concurrent media downloads.
_download_sem = asyncio.Semaphore(5)




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


def _att_path(att: dict, fallback_dir: Path, msg_id: int) -> Path:
    """Resolve attachment local path; falls back for pre-migration records without local_path."""
    if "local_path" in att:
        return MEDIA_DIR / att["local_path"]
    return fallback_dir / f"{msg_id}_{att['filename']}"


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
        return None  # JSON, not a viewable image
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


# ── Client ────────────────────────────────────────────────────────────────────

class MessageLogger(discord.Client):
    def __init__(self, db: asyncpg.Pool, poster_only: bool = False) -> None:
        super().__init__()
        self._db = db
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
        attachments = json.dumps(attachments if attachments is not None else [
            {"filename": a.filename, "url": a.url} for a in message.attachments
        ])
        stickers = json.dumps(stickers if stickers is not None else [
            {"id": s.id, "name": s.name, "format": s.format.name} for s in message.stickers
        ])
        await self._db.execute(
            """INSERT INTO messages
               (id, author, author_id, channel, guild_name, content, created_at, attachments, stickers, avatar_url)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
               ON CONFLICT (id) DO UPDATE SET
                   author=EXCLUDED.author, author_id=EXCLUDED.author_id,
                   channel=EXCLUDED.channel, guild_name=EXCLUDED.guild_name,
                   content=EXCLUDED.content, created_at=EXCLUDED.created_at,
                   attachments=EXCLUDED.attachments, stickers=EXCLUDED.stickers,
                   avatar_url=EXCLUDED.avatar_url""",
            message.id,
            str(message.author),
            message.author.id,
            str(message.channel),
            message.guild.name if message.guild else None,
            message.content,
            message.created_at.isoformat(),
            attachments,
            stickers,
            str(message.author.display_avatar.url),
        )

    async def _pop_cached(self, message_id: int) -> CachedMessage | None:
        row = await self._db.fetchrow(
            "SELECT id, author, author_id, channel, guild_name, content, created_at, attachments, stickers "
            "FROM messages WHERE id = $1",
            message_id,
        )
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
        global _log_poster
        console.info("Logged in as %s (id: %s)", self.user, self.user.id)
        if self._poster_only:
            if LOG_CHANNEL_ID:
                ch = self.get_channel(LOG_CHANNEL_ID)
                if isinstance(ch, discord.TextChannel):
                    _log_poster = self
                    self._log_channel = ch
                    console.info("Log channel: #%s (%s) (dedicated poster: %s)", ch.name, ch.id, self.user)
            return
        claimed = []
        for guild in self.guilds:
            if guild.id not in _guild_owner:
                _guild_owner[guild.id] = id(self)
                claimed.append(guild.name)
        if claimed:
            console.info("Claimed guilds: %s", claimed)
        if LOG_CHANNEL_ID and _log_poster is None:
            ch = self.get_channel(LOG_CHANNEL_ID)
            if isinstance(ch, discord.TextChannel):
                _log_poster = self
                self._log_channel = ch
                console.info("Log channel: #%s (%s) (poster: %s)", ch.name, ch.id, self.user)

    async def on_message(self, message: discord.Message) -> None:
        if not self._is_watched(message):
            return
        if self._log_channel and message.channel.id == self._log_channel.id:
            return

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
                for wurl in MIRROR_MAP[message.channel.id]:
                    await self._db.execute(
                        "INSERT INTO mirror_queue (message_id, webhook_url) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                        message.id, wurl,
                    )

    async def on_message_edit(self,
                               before: discord.Message,
                               after: discord.Message) -> None:
        if not self._is_watched(after):
            return
        if self._log_channel and after.channel.id == self._log_channel.id:
            return
        if before.content == after.content:
            return  # pin, embed-load, etc. — not a real edit

        existing = await self._db.fetchrow(
            "SELECT attachments, stickers FROM messages WHERE id = $1", before.id
        )
        att_records: list[dict] = (
            json.loads(existing["attachments"])
            if existing and existing["attachments"]
            else [{"filename": a.filename, "url": a.url} for a in after.attachments]
        )
        stk_records: list[dict] = (
            json.loads(existing["stickers"])
            if existing and existing["stickers"]
            else [{"id": s.id, "name": s.name, "format": s.format.name} for s in after.stickers]
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
            if "local_path" in s:
                p = MEDIA_DIR / s["local_path"]
            else:
                p = guild_dir / "stickers" / f"{s['id']}.{ext}"
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

        # One summary post instead of N individual posts.
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

    async def on_error(self, event: str, *args, **kwargs) -> None:  # type: ignore[override]
        import traceback
        console.error("Error in %s:\n%s", event, traceback.format_exc())


async def _mirror_worker(db: asyncpg.Pool, session: aiohttp.ClientSession) -> None:
    """Reads mirror_queue rows and posts each to its webhook with spoofed identity."""
    while True:
        row = await db.fetchrow(
            """SELECT q.id, q.webhook_url,
                      m.author, m.avatar_url, m.content, m.attachments, m.stickers
               FROM mirror_queue q
               JOIN messages m ON q.message_id = m.id
               ORDER BY q.id
               LIMIT 1"""
        )

        if row is None:
            await asyncio.sleep(0.5)
            continue

        queue_id, webhook_url, author, avatar_url, content, attachments_json, stickers_json = row
        attachments: list[dict] = json.loads(attachments_json) if attachments_json else []
        stickers: list[dict] = json.loads(stickers_json) if stickers_json else []

        try:
            wh = discord.Webhook.from_url(webhook_url, session=session)
            files: list[discord.File] = []
            extra_urls: list[str] = []

            for att in attachments:
                try:
                    async with _download_sem:
                        async with session.get(att["url"]) as resp:
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
            if extra_urls:
                post_content = (post_content + "\n" + "\n".join(extra_urls)).strip()

            await wh.send(
                content=post_content or "\u200b",
                username=author,
                avatar_url=avatar_url,
                files=files or discord.utils.MISSING,
            )
            console.info("Mirrored message to %s", webhook_url[:40])
        except Exception as exc:
            console.warning("Mirror post to %s failed: %s", webhook_url[:40], exc)
        finally:
            await db.execute("DELETE FROM mirror_queue WHERE id = $1", queue_id)


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    console.info("Starting %d account(s)", len(TOKENS))

    worker = asyncio.create_task(_post_worker(), name="log-poster")
    db = await asyncpg.create_pool(DATABASE_URL)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id          BIGINT PRIMARY KEY,
            author      TEXT NOT NULL,
            author_id   BIGINT NOT NULL,
            channel     TEXT NOT NULL,
            guild_name  TEXT,
            content     TEXT,
            created_at  TEXT NOT NULL,
            attachments TEXT,
            stickers    TEXT,
            avatar_url  TEXT
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS mirror_queue (
            id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            message_id  BIGINT NOT NULL,
            webhook_url TEXT NOT NULL,
            UNIQUE(message_id, webhook_url)
        )
    """)

    mirror_session = aiohttp.ClientSession()
    mirror_worker = asyncio.create_task(
        _mirror_worker(db, mirror_session), name="mirror-worker"
    )

    clients: list[MessageLogger] = [MessageLogger(db) for _ in TOKENS]
    tokens: list[str] = list(TOKENS)
    if LOG_POSTER_TOKEN:
        clients.append(MessageLogger(db, poster_only=True))
        tokens.append(LOG_POSTER_TOKEN)
    try:
        await asyncio.gather(*[
            client.start(token)
            for client, token in zip(clients, tokens)
        ])
    finally:
        await asyncio.gather(*[client.close() for client in clients])
        await db.close()
        for task in (worker, mirror_worker):
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
