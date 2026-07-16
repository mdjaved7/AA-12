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

# --- लॉगिंग कॉन्फिगरेशन ---
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("TaggerBot")

# --- एनवायरनमेंट वेरिएबल्स ---
API_ID = 34801155
API_HASH = "d7846c4d0f2c343dd5b67c80d45409e8"
BOT_TOKEN = "8918721301:AAGQomTKJ5vtViPRyAhHAZ51_eEmJk1v25I"

# --- कॉन्स्टेंट्स ---
ARTIST_NAME = "@AllstoryFM2"
STATE_TTL_SECONDS = 600  

# --- State Management & Queue System ---
pending_files: Dict[int, Dict[str, Any]] = {}
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(5)  # Phase 1: Max 5 parallel downloads/processing at a time

class ChatQueue:
    """हर यूजर/चैट के लिए एक अलग Sequential Queue Manager"""
    def __init__(self):
        self.next_assign_seq = 0
        self.current_upload_seq = 0
        self.condition = asyncio.Condition()

chat_queues: Dict[int, ChatQueue] = {}

def get_chat_queue(chat_id: int) -> ChatQueue:
    if chat_id not in chat_queues:
        chat_queues[chat_id] = ChatQueue()
    return chat_queues[chat_id]

# --- नेटिव एसिंक्रोनस हेल्थ चेक वेब सर्वर ---
from aiohttp import web

async def health_check_handler(request: web.Request) -> web.Response:
    return web.Response(text="Bot is alive and running successfully in Production Mode!", content_type="text/plain")

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

# --- हेल्पर फंक्शन्स ---
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

# --- एंटी-फ्लड सेफ एडिट और सेंड (Exponential Backoff) ---
async def safe_edit_message(message: Any, text: str) -> None:
    try:
        await message.edit(text)
    except MessageNotModifiedError:
        pass  # इग्नोर करें, मैसेज पहले से ही इस टेक्स्ट पर है
    except FloodWaitError as e:
        logger.warning(f"FloodWait on edit! Sleeping for {e.seconds}s")
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
            logger.warning(f"FloodWait on upload! Sleeping for {e.seconds}s")
            await asyncio.sleep(e.seconds + 2)
        except Exception as e:
            logger.error(f"Upload attempt {attempt+1} failed: {e}")
            if attempt == retries - 1:
                raise e
            await asyncio.sleep(3 * (attempt + 1))

# --- इमेज डाउनलोडर्स ---
async def download_image_from_url(url: str) -> Optional[bytes]:
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as response:
                if response.status != 200: return None
                content_type = response.headers.get('content-type', '').lower()
                if 'image' not in content_type: return None
                return await response.read()
    except Exception as e:
        logger.error(f"Error fetching direct image URL: {e}")
    return None

async def download_image_from_tg(client: TelegramClient, url: str) -> Optional[bytes]:
    match = re.match(r'https?://t\.me/(?:c/)?([^/]+)/(\d+)', url)
    if not match: return None
    try:
        channel_ref = match.group(1)
        message_id = int(match.group(2))
        if channel_ref.isdigit():
            channel_ref = int(f"-100{channel_ref}") if not channel_ref.startswith("-100") else int(channel_ref)
        try: entity = await client.get_entity(channel_ref)
        except Exception: entity = channel_ref
            
        msg = await client.get_messages(entity, ids=message_id)
        if not msg: return None
        if msg.photo: return await client.download_media(msg.photo, bytes)
        elif msg.document and msg.document.mime_type and msg.document.mime_type.startswith('image/'):
            return await client.download_media(msg.document, bytes)
        elif msg.media and hasattr(msg.media, 'webpage') and msg.media.webpage and getattr(msg.media.webpage, 'photo', None):
            return await client.download_media(msg.media.webpage.photo, bytes)
    except Exception as e:
        logger.error(f"Error resolving Telegram link: {e}")
    return None

# --- मेटाडेटा इंजन (Strict Cleanup) ---
def process_mp3_metadata(file_path: str, title: str, artist: str, album: str, image_data: bytes) -> bool:
    try:
        try: audio = MP3(file_path, ID3=ID3)
        except ID3Error:
            audio = MP3(file_path)
            audio.add_tags()
        if audio.tags is None: audio.add_tags()
        
        # Remove all duplicates before writing
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
    except Exception as e:
        logger.error(f"Error processing MP3 metadata: {e}")
        return False

def process_m4a_metadata(file_path: str, title: str, artist: str, album: str, image_data: bytes) -> bool:
    try:
        audio = MP4(file_path)
        # Overwrites inherently
        audio["\xa9nam"] = [title]
        audio["\xa9ART"] = [artist]
        audio["\xa9alb"] = [album]
        _, img_format = get_image_mime_and_format(image_data)
        audio["covr"] = [MP4Cover(image_data, imageformat=img_format)]
        audio.save()
        return True
    except Exception as e:
        logger.error(f"Error processing M4A metadata: {e}")
        return False

# --- मुख्य क्लाइंट ---
bot = TelegramClient('tagger_bot_session', API_ID, API_HASH)

@bot.on(events.NewMessage(incoming=True, pattern='/start'))
async def start_handler(event: events.NewMessage.Event) -> None:
    await event.respond("👋 **नमस्ते! मैं आपका Production-Grade ऑटोमैटिक ऑडियो टैगर बॉट हूँ।**\n\nआप एक साथ अनलिमिटेड फाइल्स भेज सकते हैं। प्रोसेसिंग पैरेलल होगी, लेकिन अपलोडिंग **100% सही क्रम** में ही होगी!")

@bot.on(events.NewMessage(incoming=True))
async def incoming_message_handler(event: events.NewMessage.Event) -> None:
    message: Message = event.message
    chat_id = event.chat_id
    
    if message.text and message.text.startswith('/start'): return

    if message.file and message.file.ext.lower() in ['.mp3', '.m4a']:
        raw_name = message.file.name or f"audio{message.file.ext}"
        file_name = sanitize_filename(raw_name)
        caption_text = message.message or ""
        
        url_match = re.search(r'(https?://[^\s]+)', caption_text)
        if url_match:
            chat_queue = get_chat_queue(chat_id)
            seq = chat_queue.next_assign_seq
            chat_queue.next_assign_seq += 1
            # Parallel processing task create
            asyncio.create_task(hybrid_pipeline_worker(event, seq, chat_queue, message.media, file_name, url_match.group(1), caption_text))
            return
            
        pending_files[chat_id] = {"media": message.media, "file_name": file_name, "timestamp": time.time()}
        await event.respond("📥 **ऑडियो फाइल मिल गई!** अब फोटो का लिंक भेजें।")
        return

    elif message.text and not message.text.startswith('/'):
        input_url = message.text.strip()
        url_match = re.search(r'(https?://[^\s]+)', input_url)
        if not url_match: return
            
        if chat_id in pending_files:
            file_data = pending_files.pop(chat_id)
            chat_queue = get_chat_queue(chat_id)
            seq = chat_queue.next_assign_seq
            chat_queue.next_assign_seq += 1
            asyncio.create_task(hybrid_pipeline_worker(event, seq, chat_queue, file_data["media"], file_data["file_name"], url_match.group(1), ""))
        else:
            await event.respond("ℹ️ **पहले मुझे एक ऑडियो फाइल भेजें**।")

# --- Hybrid Pipeline Worker (The Core Engine) ---
async def hybrid_pipeline_worker(event: events.NewMessage.Event, seq: int, chat_queue: ChatQueue, file_media: Any, file_name: str, image_url: str, caption_text: str) -> None:
    chat_id = event.chat_id
    ep_num = extract_episode_number(file_name, caption_text)
    status_msg = await event.respond(f"⏳ **Ep {ep_num}** कतार में है... (Order: #{seq+1})")
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            local_audio_path = os.path.join(temp_dir, file_name)
            
            # Phase 1: Parallel Processing (Controlled by Semaphore)
            async with DOWNLOAD_SEMAPHORE:
                await safe_edit_message(status_msg, f"📥 **Downloading Ep {ep_num}...**")
                await bot.download_media(file_media, local_audio_path)
                
                await safe_edit_message(status_msg, f"🖼️ **Image downloading for Ep {ep_num}...**")
                image_data = await download_image_from_tg(bot, image_url)
                if not image_data:
                    # Fallback to direct URL
                    image_data = await download_image_from_url(image_url)
                    
                if not image_data:
                    await safe_edit_message(status_msg, f"❌ **Image error for Ep {ep_num}.**")
                    return

                await safe_edit_message(status_msg, f"✍️ **Writing Metadata Ep {ep_num}...**")
                title = f"Ep {ep_num}"
                album = f"Ep {ep_num} - Single"
                ext = os.path.splitext(file_name)[1].lower()
                
                success = False
                if ext == '.mp3':
                    success = await asyncio.to_thread(process_mp3_metadata, local_audio_path, title, ARTIST_NAME, album, image_data)
                elif ext == '.m4a':
                    success = await asyncio.to_thread(process_m4a_metadata, local_audio_path, title, ARTIST_NAME, album, image_data)
                    
                if not success:
                    await safe_edit_message(status_msg, f"❌ **Metadata writing error for Ep {ep_num}!**")
                    return
                
                duration = get_audio_duration(local_audio_path, ext)
                thumb_path = os.path.join(temp_dir, "thumb.jpg")
                with open(thumb_path, "wb") as f: f.write(image_data)
                audio_attributes = [DocumentAttributeAudio(duration=duration, title=title, performer=ARTIST_NAME)]

            # Phase 2: Sequential Upload Phase (Strictly one-by-one ordering)
            async with chat_queue.condition:
                if chat_queue.current_upload_seq != seq:
                    await safe_edit_message(status_msg, f"⏸️ **Waiting for Upload Queue (Ep {ep_num})...**")
                    while chat_queue.current_upload_seq != seq:
                        await chat_queue.condition.wait()
                
                # Turn has come!
                await safe_edit_message(status_msg, f"📤 **Uploading Ep {ep_num}...**")
                await safe_send_file(
                    bot, chat_id, local_audio_path,
                    caption=f"✅ **Completed!**\n\n📌 **Title:** {title}\n🎤 **Artist:** {ARTIST_NAME}\n⏱️ **Duration:** {duration}s",
                    attributes=audio_attributes, thumb=thumb_path, supports_streaming=True
                )
                await status_msg.delete()

    except Exception as e:
        logger.error(f"Error in pipeline for Ep {ep_num}: {e}", exc_info=True)
        await safe_edit_message(status_msg, f"❌ **Pipeline Crashed:** `{str(e)}`")
    finally:
        # CRUCIAL: Always unlock the queue for the next file, even if this task failed!
        async with chat_queue.condition:
            if chat_queue.current_upload_seq == seq:
                chat_queue.current_upload_seq += 1
                chat_queue.condition.notify_all()

async def main() -> None:
    await bot.start(bot_token=BOT_TOKEN)
    logger.info("🚀 Production Bot authenticated successfully.")
    await start_health_server()
    asyncio.create_task(track_and_expire_states())
    await bot.run_until_disconnected()

if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): logger.info("Bot stopped cleanly.")
        
