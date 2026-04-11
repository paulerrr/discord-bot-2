import discord
import asyncio
import os
import aiohttp
import logging
import logging.handlers
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
TOKEN = os.environ["DISCORD_TOKEN"]

# Set to a list of channel IDs to only log those channels, or empty to log all.
WATCHED_CHANNELS: list[int] = []

# Set to a list of guild IDs to only log those guilds, or empty to log all.
WATCHED_GUILDS: list[int] = []

# Save media attachments to disk.
SAVE_ATTACHMENTS = True

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


def _is_watched(message: discord.Message) -> bool:
    if WATCHED_GUILDS and (not message.guild or message.guild.id not in WATCHED_GUILDS):
        return False
    if WATCHED_CHANNELS and message.channel.id not in WATCHED_CHANNELS:
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


# ── Client ────────────────────────────────────────────────────────────────────

class MessageLogger(discord.Client):
    def __init__(self) -> None:
        # message_content intent is required to read content
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(intents=intents)
        # Cache messages so we can log edits / deletes when Discord omits content
        self._msg_cache: dict[int, discord.Message] = {}
        self._session: aiohttp.ClientSession | None = None

    async def setup_hook(self) -> None:
        self._session = aiohttp.ClientSession()

    async def close(self) -> None:
        if self._session:
            await self._session.close()
        await super().close()

    # ── Events ────────────────────────────────────────────────────────────────

    async def on_ready(self) -> None:
        console.info("Logged in as %s (id: %s)", self.user, self.user.id)
        console.info("Watching %d guilds", len(self.guilds))

    async def on_message(self, message: discord.Message) -> None:
        if not _is_watched(message):
            return

        self._msg_cache[message.id] = message

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
            if SAVE_ATTACHMENTS and self._session:
                for att in message.attachments:
                    saved = await _save_attachment(self._session, message, att)
                    label = str(saved) if saved else att.url
                    lines.append(f"    - {att.filename}  →  {label}\n")
            else:
                for att in message.attachments:
                    lines.append(f"    - {att.filename}  ({att.url})\n")

        if message.embeds:
            lines.append(f"  Embeds: {len(message.embeds)}\n")

        lines.append("\n")
        _write(path, "".join(lines))

    async def on_message_edit(self,
                               before: discord.Message,
                               after: discord.Message) -> None:
        if not _is_watched(after):
            return
        if before.content == after.content:
            return  # pin, embed-load, etc. — not a real edit

        self._msg_cache[after.id] = after

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
        console.info("Edit logged: %s in %s", after.author, _channel_label(after))

    async def on_message_delete(self, message: discord.Message) -> None:
        if not _is_watched(message):
            return

        cached = self._msg_cache.pop(message.id, message)
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        path = _log_path(_guild_name(cached), str(cached.channel), date)

        lines: list[str] = [
            f"[{_format_ts(datetime.now(timezone.utc))}] "
            f"[DELETE] {cached.author} ({cached.author.id}) "
            f"in {_channel_label(cached)}\n"
            f"  Originally sent: {_format_ts(cached.created_at)}\n",
            f"  Content: {cached.content}\n" if cached.content else "  Content: <unknown>\n",
        ]

        if cached.attachments:
            lines.append(f"  Attachments ({len(cached.attachments)}):\n")
            for att in cached.attachments:
                lines.append(f"    - {att.filename}\n")

        lines.append("\n")
        _write(path, "".join(lines))
        console.info("Delete logged: %s in %s", cached.author, _channel_label(cached))

    async def on_bulk_message_delete(self,
                                      messages: list[discord.Message]) -> None:
        for message in messages:
            if _is_watched(message):
                self._msg_cache.pop(message.id, None)
                date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                path = _log_path(_guild_name(message), str(message.channel), date)
                text = (
                    f"[{_format_ts(datetime.now(timezone.utc))}] "
                    f"[BULK-DELETE] {message.author} ({message.author.id}) "
                    f"in {_channel_label(message)}\n"
                    f"  Content: {message.content or '<unknown>'}\n\n"
                )
                _write(path, text)

        console.info("Bulk delete: %d messages", len(messages))

    async def on_error(self, event: str, *args, **kwargs) -> None:  # type: ignore[override]
        import traceback
        console.error("Error in %s:\n%s", event, traceback.format_exc())


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    client = MessageLogger()
    client.run(TOKEN, log_handler=None)  # we manage logging ourselves
