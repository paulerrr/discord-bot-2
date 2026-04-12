import discord
import asyncio
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


def _is_watched(message: discord.Message, client_id: int) -> bool:
    if WATCHED_GUILDS and (not message.guild or message.guild.id not in WATCHED_GUILDS):
        return False
    if message.guild and _guild_owner.get(message.guild.id) != client_id:
        return False
    return True


async def _save_attachment(session: aiohttp.ClientSession,
                            message: discord.Message,
                            attachment: discord.Attachment) -> Path | None:
    guild_dir = MEDIA_DIR / (str(message.guild.id) if message.guild else "DMs")
    guild_dir.mkdir(parents=True, exist_ok=True)
    dest = guild_dir / f"{message.id}_{attachment.filename}"
    if dest.exists():
        return dest
    try:
        async with session.get(attachment.url) as resp:
            if resp.status == 200:
                dest.write_bytes(await resp.read())
                return dest
    except Exception as exc:
        console.warning("Failed to save attachment %s: %s", attachment.filename, exc)
    return None


async def _save_sticker(session: aiohttp.ClientSession,
                         message: discord.Message,
                         sticker: discord.StickerItem) -> Path | None:
    if sticker.format == discord.StickerFormatType.lottie:
        return None  # JSON, not a viewable image
    guild_dir = MEDIA_DIR / (str(message.guild.id) if message.guild else "DMs") / "stickers"
    guild_dir.mkdir(parents=True, exist_ok=True)
    ext = "gif" if sticker.format == discord.StickerFormatType.gif else "png"
    dest = guild_dir / f"{sticker.id}.{ext}"
    if dest.exists():
        return dest
    try:
        async with session.get(sticker.url) as resp:
            if resp.status == 200:
                dest.write_bytes(await resp.read())
                return dest
    except Exception as exc:
        console.warning("Failed to save sticker %s: %s", sticker.name, exc)
    return None


# ── Client ────────────────────────────────────────────────────────────────────

class MessageLogger(discord.Client):
    def __init__(self, db: aiosqlite.Connection) -> None:
        super().__init__()
        self._db = db
        self._session: aiohttp.ClientSession | None = None
        self._log_channel: discord.TextChannel | None = None

    async def setup_hook(self) -> None:
        self._session = aiohttp.ClientSession()

    async def close(self) -> None:
        if self._session:
            await self._session.close()
        await super().close()

    async def _cache_message(self, message: discord.Message) -> None:
        attachments = json.dumps([
            {"filename": a.filename, "url": a.url} for a in message.attachments
        ])
        stickers = json.dumps([
            {"id": s.id, "name": s.name, "format": s.format.name} for s in message.stickers
        ])
        await self._db.execute(
            """INSERT OR REPLACE INTO messages
               (id, author, author_id, channel, guild_name, content, created_at, attachments, stickers)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                message.id,
                str(message.author),
                message.author.id,
                str(message.channel),
                message.guild.name if message.guild else None,
                message.content,
                message.created_at.isoformat(),
                attachments,
                stickers,
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

    async def _log_to_channel(self, text: str, files: list[discord.File] | None = None) -> None:
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
        try:
            for i, chunk in enumerate(chunks):
                await self._log_channel.send(
                    chunk,
                    files=(files or []) if i == len(chunks) - 1 else [],
                )
        except Exception as exc:
            console.warning("Failed to send to log channel: %s", exc)

    async def on_ready(self) -> None:
        global _log_poster
        console.info("Logged in as %s (id: %s)", self.user, self.user.id)
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
        if not _is_watched(message, id(self)):
            return
        if self._log_channel and message.channel.id == self._log_channel.id:
            return

        await self._cache_message(message)

        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        path = _log_path(_guild_name(message), str(message.channel), date)

        lines: list[str] = [
            f"[{_format_ts(message.created_at)}] "
            f"[NEW] {message.author} ({message.author.id}) "
            f"in {_channel_label(message)}\n",
            f"  {message.content}\n" if message.content else "",
        ]

        if message.attachments:
            lines.append(f"  Attachments: {len(message.attachments)}\n")
            for att in message.attachments:
                saved = await _save_attachment(self._session, message, att)
                local = f"  →  {saved}" if saved else ""
                lines.append(f"    - {att.filename}  ({att.url}){local}\n")

        if message.stickers:
            lines.append(f"  Stickers: {len(message.stickers)}\n")
            for s in message.stickers:
                await _save_sticker(self._session, message, s)
                lines.append(f"    - {s.name} (id: {s.id}, format: {s.format.name})\n")

        if message.embeds:
            lines.append(f"  Embeds: {len(message.embeds)}\n")

        lines.append("\n")
        _write(path, "".join(lines))

    async def on_message_edit(self,
                               before: discord.Message,
                               after: discord.Message) -> None:
        if not _is_watched(after, id(self)):
            return
        if self._log_channel and after.channel.id == self._log_channel.id:
            return
        if before.content == after.content:
            return  # pin, embed-load, etc. — not a real edit

        await self._cache_message(after)

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
            for att in before.attachments
            if (p := guild_dir / f"{before.id}_{att.filename}").exists()
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
        await (_log_poster or self)._log_to_channel(channel_post, files=files)
        console.info("Edit logged: %s in %s", after.author, _channel_label(after))

    async def on_message_delete(self, message: discord.Message) -> None:
        if not _is_watched(message, id(self)):
            return

        cached = await self._pop_cached(message.id)
        if cached is None:
            cached = CachedMessage(
                id=message.id,
                author=str(message.author),
                author_id=message.author.id,
                channel=str(message.channel),
                guild_name=message.guild.name if message.guild else None,
                content=message.content,
                created_at=message.created_at,
                attachments=[{"filename": a.filename, "url": a.url} for a in message.attachments],
                stickers=[{"id": s.id, "name": s.name, "format": s.format.name} for s in message.stickers],
            )

        channel_label = f"#{cached.channel}" if cached.guild_name else f"DM:{cached.channel}"
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
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
                local = MEDIA_DIR / (str(message.guild.id) if message.guild else "DMs") / f"{cached.id}_{att['filename']}"
                label = str(local) if local.exists() else att['url']
                lines.append(f"    - {att['filename']}  →  {label}\n")

        if cached.stickers:
            lines.append(f"  Stickers ({len(cached.stickers)}):\n")
            for s in cached.stickers:
                lines.append(f"    - {s['name']} (id: {s['id']}, format: {s['format']})\n")

        lines.append("\n")
        _write(path, "".join(lines))
        guild_dir = MEDIA_DIR / (str(message.guild.id) if message.guild else "DMs")
        files = [
            discord.File(p)
            for att in cached.attachments
            if (p := guild_dir / f"{cached.id}_{att['filename']}").exists()
        ]
        for s in cached.stickers:
            ext = "gif" if s["format"].lower() == "gif" else "png"
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
        channel_post = "\n".join(post_lines)
        await (_log_poster or self)._log_to_channel(channel_post, files=files)
        console.info("Delete logged: %s in %s", cached.author, channel_label)

    async def on_bulk_message_delete(self,
                                      messages: list[discord.Message]) -> None:
        for message in messages:
            if _is_watched(message, id(self)):
                cached = await self._pop_cached(message.id)
                guild_name = message.guild.name if message.guild else None
                channel_str = str(message.channel)
                channel_label = f"#{channel_str}" if guild_name else f"DM:{channel_str}"
                content = cached.content if cached else (message.content or "<unknown>")
                stickers = cached.stickers if cached else [{"id": s.id, "name": s.name, "format": s.format.name} for s in message.stickers]
                date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                path = _log_path(guild_name, channel_str, date)
                log_lines = [
                    f"[{_format_ts(datetime.now(timezone.utc))}] "
                    f"[BULK-DELETE] {message.author} ({message.author.id}) "
                    f"in {channel_label}\n",
                    f"  Content: {content}\n",
                ]
                if stickers:
                    log_lines.append(f"  Stickers ({len(stickers)}): " + ", ".join(s['name'] for s in stickers) + "\n")
                log_lines.append("\n")
                _write(path, "".join(log_lines))
                sent_ts = int(message.created_at.timestamp())
                post_lines = [
                    f"🗑️ Bulk Delete",
                    f"Channel: {channel_label}" + (f"  ·  {guild_name}" if guild_name else ""),
                    f"Author: {discord.utils.escape_markdown(str(message.author))} ({message.author.id})",
                    f"Sent: <t:{sent_ts}:R>",
                    f"Content: {discord.utils.escape_markdown(content) if content != '<unknown>' else '<unknown>'}",
                ]
                if stickers:
                    post_lines.append("Stickers: " + "  ".join(s['name'] for s in stickers))
                channel_post = "\n".join(post_lines)
                await (_log_poster or self)._log_to_channel(channel_post)

        console.info("Bulk delete: %d messages", len(messages))

    async def on_error(self, event: str, *args, **kwargs) -> None:  # type: ignore[override]
        import traceback
        console.error("Error in %s:\n%s", event, traceback.format_exc())


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    console.info("Starting %d account(s)", len(TOKENS))

    db = await aiosqlite.connect("cache.db")
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
            stickers    TEXT
        )
    """)
    await db.commit()

    clients = [MessageLogger(db) for _ in TOKENS]
    try:
        await asyncio.gather(*[
            client.start(token)
            for client, token in zip(clients, TOKENS)
        ])
    finally:
        await asyncio.gather(*[client.close() for client in clients])
        await db.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
