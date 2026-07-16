import os
import re
import logging
import requests
import threading
from http.server import SimpleHTTPRequestHandler, HTTPServer
from telethon import TelegramClient, events
from telethon.tl.types import DocumentAttributeAudio
from mutagen.mp3 import MP3
from mutagen.id3 import ID3, TIT2, TPE1, TALB, APIC, error as ID3Error
from mutagen.mp4 import MP4, MP4Cover

# लॉगिंग सेट करें
logging.basicConfig(level=logging.INFO)

# --- Render के लिए वेब सर्वर (ताकि एरर न आए और बॉट हमेशा चलता रहे) ---
def run_web_server():
    class SimpleHandler(SimpleHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header('Content-type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(b"Bot is alive and running successfully!")

    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(('0.0.0.0', port), SimpleHandler)
    logging.info(f"🖥️ Web server started on port {port}")
    server.serve_forever()

# बैकग्राउंड थ्रेड में वेब सर्वर शुरू करें
threading.Thread(target=run_web_server, daemon=True).start()

# --- मुख्य बॉट कोड ---
API_ID = 34801155             
API_HASH = "d7846c4d0f2c343dd5b67c80d45409e8"   
BOT_TOKEN = "8918721301:AAGnaBqk8DVKlIbZY5ITuqJUb0Sk2f2wTcw" 

# क्लाइंट शुरू करें
bot = TelegramClient('tagger_bot_session', API_ID, API_HASH).start(bot_token=BOT_TOKEN)

pending_files = {}

async def download_image(url, client):
    """URL या Telegram पोस्ट लिंक से इमेज डाउनलोड करने के लिए"""
    tg_match = re.match(r'https?://t\.me/([^/]+)/(\d+)', url)
    if tg_match:
        try:
            channel = tg_match.group(1)
            msg_id = int(tg_match.group(2))
            msg = await client.get_messages(channel, ids=msg_id)
            if msg and msg.photo:
                image_data = await client.download_media(msg.photo, bytes)
                return image_data
        except Exception as e:
            logging.error(f"Telegram लिंक से फोटो डाउनलोड करने में एरर: {e}")

    try:
        response = requests.get(url, timeout=15)
        if response.status_code == 200:
            return response.content
    except Exception as e:
        logging.error(f"वेब URL से इमेज डाउनलोड करने में समस्या: {e}")
    return None

def extract_episode_number(filename):
    """फाइल के नाम से एपिसोड नंबर निकालने के लिए"""
    match = re.search(r'\d+', filename)
    if match:
        return match.group()
    return "Unknown"

def process_mp3(file_path, title, artist, album, image_data):
    """MP3 फाइल एडिट करने के लिए"""
    try:
        try:
            audio = MP3(file_path, ID3=ID3)
            audio.add_tags()
        except ID3Error:
            pass

        audio.tags.add(TIT2(encoding=3, text=title))
        audio.tags.add(TPE1(encoding=3, text=artist))
        audio.tags.add(TALB(encoding=3, text=album))
        audio.tags.add(
            APIC(
                encoding=3,
                mime='image/jpeg',
                type=3,
                desc=u'Cover',
                data=image_data
            )
        )
        audio.save()
        return True
    except Exception as e:
        logging.error(f"MP3 एरर: {e}")
        return False

def process_m4a(file_path, title, artist, album, image_data):
    """M4A फाइल एडिट करने के लिए"""
    try:
        audio = MP4(file_path)
        audio["\xa9nam"] = title
        audio["\xa9ART"] = artist
        audio["\xa9alb"] = album
        audio["covr"] = [MP4Cover(image_data, imageformat=MP4Cover.FORMAT_JPEG)]
        audio.save()
        return True
    except Exception as e:
        logging.error(f"M4A एरर: {e}")
        return False

@bot.on(events.NewMessage(pattern='/start'))
async def start(event):
    """बॉट स्टार्ट करने पर मैसेज"""
    await event.respond(
        "👋 नमस्ते! मैं एक सुपर-फास्ट ऑटोमैटिक ऑडियो टैगर बॉट हूँ।\n\n"
        "💪 **अब मैं बिना किसी लिमिट के 2 GB तक की बड़ी फाइल्स भी आसानी से प्रोसेस कर सकता हूँ!**\n\n"
        "**इस्तेमाल करने का तरीका:**\n"
        "1. सबसे पहले अपनी **MP3** या **M4A** फाइल भेजें।\n"
        "2. फाइल के कैप्शन (Caption) में या उसके तुरंत बाद **फोटो का URL (Link)** भेजें।"
    )

@bot.on(events.NewMessage)
async def handle_message(event):
    """सभी मैसेजेस को हैंडल करने के लिए"""
    if event.text and event.text.startswith('/start'):
        return

    chat_id = event.chat_id

    if event.message.file and event.message.file.ext.lower() in ['.mp3', '.m4a']:
        file_obj = event.message.media
        file_name = event.message.file.name or f"audio{event.message.file.ext}"
        photo_url = event.message.message
        
        if not photo_url or not re.match(r'^https?://', photo_url.strip()):
            pending_files[chat_id] = {
                "file": file_obj,
                "file_name": file_name
            }
            await event.respond("📥 ऑडियो मिल गया! अब इस फाइल के लिए **फोटो का URL (Link)** भेजें।")
            return
        
        await process_and_send(event, file_obj, file_name, photo_url.strip())

    elif event.text and not event.text.startswith('/'):
        text = event.text.strip()
        if not re.match(r'^https?://', text):
            await event.respond("⚠️ कृपया एक सही फोटो URL (http/https से शुरू होने वाला) भेजें।")
            return

        if chat_id in pending_files:
            file_data = pending_files.pop(chat_id)
            await process_and_send(event, file_data["file"], file_data["file_name"], text)
        else:
            await event.respond("ℹ️ पहले मुझे एक ऑडियो फाइल (MP3/M4A) भेजें, फिर यह फोटो लिंक काम करेगा।")

async def process_and_send(event, file_obj, file_name, image_url):
    """फाइल डाउनलोड, एडिट और वापस भेजने की मुख्य प्रक्रिया"""
    status_message = await event.respond("⚡ प्रोसेसिंग शुरू हो रही है...")
    local_file_path = os.path.join(".", file_name)

    try:
        await status_message.edit("📥 फ़ाइल डाउनलोड की जा रही है... (कृपया धैर्य रखें)")
        await bot.download_media(file_obj, local_file_path)

        ep_number = extract_episode_number(file_name)
        title = f"Ep {ep_number}"
        album = f"Ep {ep_number} - Single"
        artist_name = "@AllstoryFM2"

        await status_message.edit("🖼️ फोटो डाउनलोड की जा रही है...")
        image_data = await download_image(image_url, bot)
        if not image_data:
            await status_message.edit("❌ फोटो डाउनलोड नहीं हो सकी।")
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
            return

        await status_message.edit("✍️ ऑडियो में डेटा और फोटो डाली जा रही है...")
        ext = os.path.splitext(file_name)[1].lower()
        success = False
        if ext == '.mp3':
            success = process_mp3(local_file_path, title, artist_name, album, image_data)
        elif ext == '.m4a':
            success = process_m4a(local_file_path, title, artist_name, album, image_data)

        if success:
            await status_message.edit("📤 फाइल तैयार है! टेलीग्राम पर अपलोड की जा रही है...")
            audio_attributes = [DocumentAttributeAudio(duration=0, title=title, performer=artist_name)]
            await bot.send_file(
                event.chat_id,
                local_file_path,
                caption=f"✅ सफलतापूर्वक अपडेट किया गया!\n📌 Title: {title}\n🎤 Artist: {artist_name}",
                attributes=audio_attributes
            )
            await status_message.delete()
        else:
            await status_message.edit("❌ मेटाडेटा अपडेट करने में कोई एरर आया।")

    except Exception as e:
        await status_message.edit(f"❌ एक समस्या आई: {str(e)}")
    finally:
        if os.path.exists(local_file_path):
            os.remove(local_file_path)

if __name__ == "__main__":
    print("🚀 Render पर बॉट सफलतापूर्वक चालू हो गया है...")
    bot.run_until_disconnected()
  
