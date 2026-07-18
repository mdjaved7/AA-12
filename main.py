import os
import re
import time
import logging
import asyncio
import tempfile
import aiohttp
from typing import Dict, Any, Optional, Tuple

# Telethon Imports
from telethon import TelegramClient, events
from telethon.tl.types import DocumentAttributeAudio, Message
from telethon.errors import FloodWaitError, MessageNotModifiedError

# Mutagen Metadata Imports
from mutagen.mp3 import MP3
from mutagen.id3 import ID3, TIT2, TPE1, TALB, APIC, error as ID3Error
from mutagen.mp4 import MP4, MP4Cover

# --- Logging Configuration ---
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("TaggerBot")

# --- Environment Variables ---
API_ID = 34801155
API_HASH = "d7846c4d0f2c343dd5b67c80d45409e8"
BOT_TOKEN = "8881589159:AAE_3rsISHa9DrWZuAMcDkMClmVZmem2Acc"

# --- Constants ---
ARTIST_NAME = "@AllstoryFM2"
STATE_TTL_SECONDS = 600  

# --- State Management & Queue System ---
pending_files: Dict[int, Dict[str, Any]] = {}
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(5)  # Max 5 parallel downloads/processing

class ChatQueue:
    def __init__(self):
        self.next_assign_seq = 0
        self.current_upload_seq = 0
        self.condition = asyncio.Condition()

chat_queues: Dict[int, ChatQueue] = {}

def get_chat_queue(chat_id: int) -> ChatQueue:
    if chat_id not in chat_queues:
        chat_queues[chat_id] = ChatQueue()
    return chat_queues[chat_id]

# --- Helper Functions for Watchdog ---
async def wait_for_turn(chat_queue: ChatQueue, seq: int):
    while chat_queue.current_upload_seq != seq:
        await chat_queue.condition.wait()

# --- Health Check Server ---
from aiohttp import web

async def health_check_handler(request: web.Request) -> web.Response:
    return web.Response(text="Bot is alive and running successfully!", content_type="text/plain")

async def start_health_server() -> None:
    app = web.Application()
    app.router.add_get('/', health_check_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"🖥️ Native Async Health check server active on port {port}")

async def track_and_expire_states() -> None:
    while True:
        await asyncio.sleep(60)
        now = time.time()
        expired_chats = [
            chat_id for chat_id, state in pending_files.items()
            if now - state.get("timestamp", 0) > STATE_TTL_SECONDS
        ]
        for chat_id in expired_chats:
            pending_files.pop(chat_id, None)

# --- Helper Functions ---
def sanitize_filename(filename: str) -> str:
    name = os.path.basename(filename)
    return re.sub(r'[\\/*?:"<>|]', "", name)

def extract_episode_number(filename: str, caption: str = "") -> str:
    for text in [filename, caption]:
        match = re.search(r'(?:ep|episode|story)[-_\s]*(\d+)', text, re.IGNORECASE)
        if match: return match.group(1)
    fallback = re.search(r'\d+', filename)
    if fallback: return fallback.group()
    if caption:
        fallback_cap = re.search(r'\d+', caption)
        if fallback_cap: return fallback_cap.group()
    return "Unknown"

def get_image_mime_and_format(data: bytes) -> Tuple[str, int]:
    if data.startswith(b'\xff\xd8'): return 'image/jpeg', MP4Cover.FORMAT_JPEG
    elif data.startswith(b'\x89PNG\r\n\x1a\n'): return 'image/png', MP4Cover.FORMAT_PNG
    return 'image/jpeg', MP4Cover.FORMAT_JPEG

def get_audio_duration(file_path: str, ext: str) -> int:
    try:
        if ext == '.mp3': return int(MP3(file_path).info.length)
        elif ext == '.m4a': return int(MP4(file_path).info.length)
    except Exception as e:
        logger.error(f"Failed to read audio duration: {e}")
    return 0

# --- Messaging Helpers ---
async def safe_edit_message(message: Any, text: str) -> None:
    try:
        await message.edit(text)
    except MessageNotModifiedError:
        pass
    except FloodWaitError as e:
        await asyncio.sleep(e.seconds + 2)
        await safe_edit_message(message, text)
    except Exception as e:
        logger.error(f"Edit message error: {e}")

async def safe_send_file(client, chat_id, file, **kwargs):
    retries = 5
    for attempt in range(retries):
        try:
            return await client.send_file(chat_id, file, **kwargs)
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + 2)
        except Exception as e:
            if attempt == retries - 1: raise e
            await asyncio.sleep(3 * (attempt + 1))

# --- Downloaders ---
async def download_image_from_url(url: str) -> Optional[bytes]:
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as response:
                if response.status != 200: return None
                return await response.read()
    except Exception: return None

async def download_image_from_tg(client: TelegramClient, url: str) -> Optional[bytes]:
    match = re.match(r'https?://t\.me/(?:c/)?([^/]+)/(\d+)', url)
    if not match: return None
    try:
        channel_ref = match.group(1)
        message_id = int(match.group(2))
        entity = channel_ref if not channel_ref.isdigit() else int(f"-100{channel_ref}")
        msg = await client.get_messages(entity, ids=message_id)
        if not msg: return None
        return await client.download_media(msg.media, bytes)
    except Exception: return None

# --- Metadata Engine ---
def process_mp3_metadata(file_path: str, title: str, artist: str, album: str, image_data: bytes) -> bool:
    try:
        try: audio = MP3(file_path, ID3=ID3)
        except ID3Error: audio = MP3(file_path); audio.add_tags()
        if audio.tags is None: audio.add_tags()
        
        for tag in ["TIT2", "TPE1", "TALB"]: audio.tags.delall(tag)
        keys_to_delete = [k for k in audio.tags.keys() if k.startswith("APIC")]
        for key in keys_to_delete: audio.tags.pop(key, None)
            
        audio.tags.add(TIT2(encoding=3, text=title))
        audio.tags.add(TPE1(encoding=3, text=artist))
        audio.tags.add(TALB(encoding=3, text=album))
        mime_type, _ = get_image_mime_and_format(image_data)
        audio.tags.add(APIC(encoding=3, mime=mime_type, type=3, desc=u'Cover', data=image_data))
        audio.save()
        return True
    except Exception: return False

def process_m4a_metadata(file_path: str, title: str, artist: str, album: str, image_data: bytes) -> bool:
    try:
        audio = MP4(file_path)
        audio["\xa9nam"] = [title]; audio["\xa9ART"] = [artist]; audio["\xa9alb"] = [album]
        _, img_format = get_image_mime_and_format(image_data)
        audio["covr"] = [MP4Cover(image_data, imageformat=img_format)]
        audio.save()
        return True
    except Exception: return False

# --- Bot Handler ---
bot = TelegramClient('tagger_bot_session', API_ID, API_HASH)

@bot.on(events.NewMessage(incoming=True, pattern='/start'))
async def start_handler(event: events.NewMessage.Event) -> None:
    await event.respond("✅ **Bot Online & Ready!**")

@bot.on(events.NewMessage(incoming=True))
async def incoming_message_handler(event: events.NewMessage.Event) -> None:
    message: Message = event.message
    chat_id = event.chat_id
    if message.text and message.text.startswith('/start'): return

    if message.file and message.file.ext.lower() in ['.mp3', '.m4a']:
        file_name = sanitize_filename(message.file.name or f"audio{message.file.ext}")
        caption_text = message.message or ""
        url_match = re.search(r'(https?://[^\s]+)', caption_text)
        
        if url_match:
            chat_queue = get_chat_queue(chat_id)
            seq = chat_queue.next_assign_seq
            chat_queue.next_assign_seq += 1
            asyncio.create_task(hybrid_pipeline_worker(event, seq, chat_queue, message.media, file_name, url_match.group(1), caption_text))
            return
        pending_files[chat_id] = {"media": message.media, "file_name": file_name, "timestamp": time.time()}
        await event.respond("📥 **File received!** Now send the image link.")
    
    elif message.text and not message.text.startswith('/'):
        input_url = message.text.strip()
        url_match = re.search(r'(https?://[^\s]+)', input_url)
        if url_match and chat_id in pending_files:
            file_data = pending_files.pop(chat_id)
            chat_queue = get_chat_queue(chat_id)
            seq = chat_queue.next_assign_seq
            chat_queue.next_assign_seq += 1
            asyncio.create_task(hybrid_pipeline_worker(event, seq, chat_queue, file_data["media"], file_data["file_name"], url_match.group(1), ""))

# --- FIXED PIPELINE (WITH SELF-HEALING WATCHDOG) ---
async def hybrid_pipeline_worker(event: events.NewMessage.Event, seq: int, chat_queue: ChatQueue, file_media: Any, file_name: str, image_url: str, caption_text: str) -> None:
    chat_id = event.chat_id
    ep_num = extract_episode_number(file_name, caption_text)
    status_msg = await event.respond(f"⏳ **Ep {ep_num}** in queue... (#{seq+1})")
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            local_audio_path = os.path.join(temp_dir, file_name)
            thumb_path = os.path.join(temp_dir, "thumb.jpg")
            
            # --- PHASE 1: DOWNLOAD & PROCESS (Semaphore Limited) ---
            async with DOWNLOAD_SEMAPHORE:
                await safe_edit_message(status_msg, f"📥 **Downloading Ep {ep_num}...**")
                await bot.download_media(file_media, local_audio_path)
                
                image_data = await download_image_from_tg(bot, image_url) or await download_image_from_url(image_url)
                if not image_data:
                    await safe_edit_message(status_msg, f"❌ **Image error.**")
                    return

                await safe_edit_message(status_msg, f"✍️ **Processing Metadata...**")
                ext = os.path.splitext(file_name)[1].lower()
                title, album = f"Ep {ep_num}", f"Ep {ep_num} - Single"
                
                success = await asyncio.to_thread(process_mp3_metadata if ext == '.mp3' else process_m4a_metadata, local_audio_path, title, ARTIST_NAME, album, image_data)
                if not success:
                    await safe_edit_message(status_msg, f"❌ **Metadata error.**")
                    return
                
                duration = get_audio_duration(local_audio_path, ext)
                with open(thumb_path, "wb") as f: f.write(image_data)
                audio_attributes = [DocumentAttributeAudio(duration=duration, title=title, performer=ARTIST_NAME)]
            
            # --- PHASE 2: UPLOAD (Sequential + Watchdog Timeout) ---
            async with chat_queue.condition:
                if chat_queue.current_upload_seq != seq:
                    await safe_edit_message(status_msg, f"⏸️ **Waiting for Queue (Ep {ep_num})...**")
                    # WATCHDOG: Timeout if waiting for queue > 10 mins
                    try:
                        await asyncio.wait_for(wait_for_turn(chat_queue, seq), timeout=600)
                    except asyncio.TimeoutError:
                        logger.error(f"Watchdog: Queue Timeout on Ep {ep_num}")
                        await safe_edit_message(status_msg, f"⚠️ **Queue Timeout! Skipping...**")
                        return # Stop, but finally block will unlock queue
                
                await safe_edit_message(status_msg, f"📤 **Uploading Ep {ep_num}...**")
                # WATCHDOG: Timeout if upload takes > 15 mins
                try:
                    await asyncio.wait_for(
                        safe_send_file(bot, chat_id, local_audio_path, caption=f"✅ **Done!**", attributes=audio_attributes, thumb=thumb_path, supports_streaming=True),
                        timeout=900
                    )
                except asyncio.TimeoutError:
                    logger.error(f"Watchdog: Upload Timeout on Ep {ep_num}")
                    await safe_edit_message(status_msg, f"⚠️ **Upload Timeout!**")
                    return # Stop, finally block unlocks queue
                
                await status_msg.delete()

    except Exception as e:
        logger.error(f"Pipeline error: {e}", exc_info=True)
        await safe_edit_message(status_msg, f"❌ **Crash:** `{str(e)}`")
    finally:
        async with chat_queue.condition:
            if chat_queue.current_upload_seq == seq:
                chat_queue.current_upload_seq += 1
                chat_queue.condition.notify_all()

async def main() -> None:
    await bot.start(bot_token=BOT_TOKEN)
    await start_health_server()
    asyncio.create_task(track_and_expire_states())
    await bot.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
    
