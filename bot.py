import os
import time
import asyncio
import aiohttp
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application,
    MessageHandler, 
    CommandHandler, 
    CallbackQueryHandler,
    ChatJoinRequestHandler,
    ContextTypes, 
    filters
)
from pymongo import MongoClient

# Environment Credentials
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
MONGO_URI = os.getenv("MONGO_URI", "") 

# ShrinkBixby API & Tutorial Configuration
SHRINK_API_TOKEN = os.getenv("SHRINK_API_TOKEN", "81f51fb11c1b277ee3dc2edc0b21fe5c5b95cd6a")
TOKEN_VALIDITY_DURATION = 4 * 3600  # 4 Hours in seconds
TUTORIAL_VIDEO_LINK = os.getenv("TUTORIAL_VIDEO_LINK", "https://t.me/your_tutorial_link")

# Multiple Admin IDs Setup
ADMIN_IDS_RAW = os.getenv("ADMIN_ID", "0")
ADMIN_IDS = [int(aid.strip()) for aid in ADMIN_IDS_RAW.split(",") if aid.strip().isdigit()]
ADMIN_ID = ADMIN_IDS[0] if ADMIN_IDS else 0

CHANNEL_INVITE_LINK = os.getenv("CHANNEL_INVITE_LINK", "") 
PRIVATE_STORE_ID = int(os.getenv("PRIVATE_STORE_ID", "0"))  

# MongoDB Setup
client = MongoClient(MONGO_URI)

# Primary Database Configuration
primary_db = client['bot_primary_db']
user_col = primary_db['users']
delete_col = primary_db['delete_queue'] 
history_col = primary_db['user_history']  
token_col = primary_db['user_tokens'] 
registry_col = primary_db['batch_registry']
config_col = primary_db['bot_config']

# Dynamic Multi Force Join Collection
fsub_col = primary_db['force_sub_channels']
join_req_col = primary_db['join_requests_data'] # Collection for pending requests

user_queues = {}
backup_queues = {}
cancel_status = {}
processing_tasks = {}

# --- Dynamic File Database Selector ---
def get_active_file_db():
    config = config_col.find_one({"_id": "file_db_config"})
    idx = config.get("index", 0) if config else 0
    
    db_name = f"bot_file_db_{idx}"
    current_db = client[db_name]
    
    try:
        stats_data = current_db.command("dbStats")
        storage_size_mb = stats_data.get("storageSize", 0) / (1024 * 1024)
        
        if storage_size_mb >= 450.0:
            idx += 1
            config_col.update_one(
                {"_id": "file_db_config"},
                {"$set": {"index": idx}},
                upsert=True
            )
            db_name = f"bot_file_db_{idx}"
            current_db = client[db_name]
            print(f"⚠️ Database full! Switched to new database: {db_name}")
    except Exception as e:
        print(f"File DB check error: {e}")
        
    return current_db, db_name

# --- File Size Formatter ---
def get_readable_size(size_in_bytes):
    if not size_in_bytes:
        return "Unknown Size"
    for unit in ['Bytes', 'KB', 'MB', 'GB', 'TB']:
        if size_in_bytes < 1024.0:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024.0
    return f"{size_in_bytes:.2f} PB"

# --- Shortener API ---
async def get_short_link(long_url):
    api_url = f"https://shrinkbixby.com/api?api={SHRINK_API_TOKEN}&url={long_url}"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(api_url) as response:
                data = await response.json()
                if data.get("status") == "success":
                    return data.get("shortenedUrl")
        except Exception as e:
            print(f"ShrinkBixby API Error: {e}")
    return long_url  

# --- Token Logic ---
def is_token_valid(user_id):
    user_record = token_col.find_one({"user_id": user_id})
    if not user_record or "expiry" not in user_record:
        return False
    return time.time() < user_record["expiry"]

def renew_user_token(user_id):
    expiry_time = time.time() + TOKEN_VALIDITY_DURATION
    token_col.update_one(
        {"user_id": user_id},
        {"$set": {"expiry": expiry_time}},
        upsert=True
    )

# --- Dynamic Multi Force Sub Checker ---
async def get_fsub_buttons(context, user_id, start_param):
    channels = list(fsub_col.find())
    if not channels:
        return True, [] 

    unjoined_buttons = []
    joined_buttons = []
    has_unjoined = False

    for ch in channels:
        ch_id = ch["channel_id"]
        ch_link = ch["invite_link"]
        ch_title = ch.get("title", "Join Channel")

        # Check 1: DB check for pending request
        has_requested = join_req_col.find_one({"user_id": user_id, "channel_id": ch_id})
        
        if has_requested:
            joined_buttons.append([InlineKeyboardButton(f"✅ {ch_title}", url=ch_link)])
            continue

        # Check 2: Regular member check if no request found in DB
        try:
            member = await context.bot.get_chat_member(chat_id=ch_id, user_id=user_id)
            if member.status in ['member', 'administrator', 'creator']:
                joined_buttons.append([InlineKeyboardButton(f"✅ {ch_title}", url=ch_link)])
            else:
                has_unjoined = True
                unjoined_buttons.append([InlineKeyboardButton(f"📢 Request {ch_title}", url=ch_link)])
        except Exception as e:
            has_unjoined = True
            unjoined_buttons.append([InlineKeyboardButton(f"📢 Request {ch_title}", url=ch_link)])

    if has_unjoined:
        bot_info = await context.bot.get_me()
        try_again_link = f"https://t.me/{bot_info.username}?start={start_param}"
        unjoined_buttons.append([InlineKeyboardButton("🔄 Try Again", url=try_again_link)])
        return False, unjoined_buttons
    else:
        return True, []

# --- System Background Operations ---
async def database_storage_checker(app):
    while True:
        try:
            active_db, active_name = get_active_file_db()
            stats_data = active_db.command("dbStats")
            storage_size_mb = stats_data.get("storageSize", 0) / (1024 * 1024)
            
            if storage_size_mb >= 450.0:
                alert_text = (
                    f"⚠️ <b>MONGODB STORAGE WARNING!</b> ⚠️\n\n"
                    f"Current Active File DB ({active_name}) full hone wala hai!\n"
                    f"<b>Current Usage:</b> {storage_size_mb:.2f} MB / 512 MB\n\n"
                    f"System automatically naye database par shift ho raha hai."
                )
                for adm in ADMIN_IDS:
                    try: await app.bot.send_message(chat_id=adm, text=alert_text, parse_mode="HTML")
                    except: pass
        except Exception as e:
            print(f"Database storage check error: {e}")
        await asyncio.sleep(3600)

async def auto_delete_monitor(app):
    while True:
        try:
            current_time = time.time()
            all_pending = delete_col.find({"delete_at": {"$lte": current_time}})
            for task in all_pending:
                chat_id = task['chat_id']
                for msg_id in task['message_ids']:
                    try: await app.bot.delete_message(chat_id=chat_id, message_id=msg_id)
                    except: pass
                    await asyncio.sleep(0.1) 
                delete_col.delete_one({"_id": task['_id']})
        except Exception as e: 
            print(f"Auto-Delete Monitor Error: {e}")
        await asyncio.sleep(15)

async def run_post_init(application):
    asyncio.create_task(auto_delete_monitor(application))
    asyncio.create_task(database_storage_checker(application))

# --- Send Files Logic ---
async def send_files_logic(update, context, batch_key):
    user = update.effective_user
    cancel_status[user.id] = False 
    
    reg_record = registry_col.find_one({"batch_key": batch_key})
    batch = None
    if reg_record:
        target_db_name = reg_record["db_name"]
        batch = client[target_db_name]['file_batches'].find_one({"batch_key": batch_key})
    else:
        batch = client['bot_database']['file_batches'].find_one({"batch_key": batch_key})
    
    if not batch:
        await update.message.reply_text("❌ Yeh link amanaye (invalid) hai.")
        return

    try:
        history_col.insert_one({
            "user_id": user.id, 
            "first_name": user.first_name, 
            "username": user.username, 
            "action": "requested_files", 
            "batch_key": batch_key, 
            "time": datetime.now(ZoneInfo("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')
        })
    except:
        pass
    
    info_msg = await update.message.reply_text(
        "⏳ Sending files...", 
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("• Cancel", callback_data="cancel_action")],
            [InlineKeyboardButton("📟 UPDATE CHANNEL", url=CHANNEL_INVITE_LINK)]
        ])
    )
    
    sent_message_ids = [info_msg.message_id]
    is_cancelled = False
    
    for file in batch["files"]:
        if cancel_status.get(user.id): 
            is_cancelled = True
            break 

        try:
            sent_msg = None
            file_bytes = file.get('file_size', 0)
            readable_size = get_readable_size(file_bytes)
            file_type = file.get('file_type')
            original_caption = file.get('caption', '')
            
            if file_type == 'video' and original_caption:
                custom_caption = f"{original_caption}\n\n👉 FILE SIZE :- {readable_size} 👑\n>> JOIN > @AllstoryFM2 🔥"
            else:
                custom_caption = (
                    f">> JOIN > @AllstoryFM2 🔥\n"
                    f"✅✨\n\n"
                    f"👉 FILE SIZE :- {readable_size} 👑\n"
                    f"🔥"
                )

            if file['file_type'] == 'document': 
                sent_msg = await context.bot.send_document(update.message.chat_id, file['file_id'], protect_content=True, caption=custom_caption)
            elif file['file_type'] == 'video': 
                sent_msg = await context.bot.send_video(update.message.chat_id, file['file_id'], protect_content=True, caption=custom_caption)
            elif file['file_type'] == 'photo': 
                sent_msg = await context.bot.send_photo(update.message.chat_id, file['file_id'], protect_content=True, caption=custom_caption)
            elif file['file_type'] == 'audio': 
                sent_msg = await context.bot.send_audio(update.message.chat_id, file['file_id'], protect_content=True, caption=custom_caption)

            if sent_msg: 
                sent_message_ids.append(sent_msg.message_id)
            await asyncio.sleep(0.5) 
        except Exception as e:
            print(f"File send error: {e}")
            break

    if len(sent_message_ids) > 0:
        try:
            delete_col.insert_one({
                "chat_id": update.message.chat_id, 
                "message_ids": sent_message_ids, 
                "delete_at": time.time() + 14400 
            })
        except:
            pass

    try: await context.bot.delete_message(chat_id=update.message.chat_id, message_id=info_msg.message_id)
    except: pass

    alert_text = "𝙷𝙸𝙽𝙳𝙸 𝚂𝚃𝙾𝚁𝚈\n❤️ 𝙷𝙴𝚈 𝙱𝚁𝙾 🇮🇳 \n\n📂 𝙵𝙸𝙻𝙴𝚂 𝚆𝙸𝙻𝙻 𝙱𝙴 𝙳𝙴𝙻𝙴𝚃𝙴𝙳 \n𝙰𝙵𝚃𝙴𝚁 [ 4 𝙷𝙾𝚄𝚁𝚂 ] 𝙿𝙻𝙴𝙰𝚂𝙴 \n𝚂𝙰𝚅𝙴 𝚃𝙷𝙴𝙼 𝚂𝙾𝙼𝙴𝚆𝙷𝙴𝚁𝙴 𝚂𝙰𝙵𝙴."
    if is_cancelled: alert_text += "\n\n⚠️ *Process was cancelled by user.*"

    try:
        final_msg = await update.message.reply_text(
            alert_text, 
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📟 UPDATE CHANNEL", url=CHANNEL_INVITE_LINK)]])
        )
        delete_col.insert_one({
            "chat_id": update.message.chat_id, 
            "message_ids": [final_msg.message_id], 
            "delete_at": time.time() + 14400
        })
    except: pass

# --- Command Handler: /start ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    try:
        if not user_col.find_one({"user_id": user.id}):
            user_col.insert_one({"user_id": user.id, "username": user.username, "first_name": user.first_name})
    except: pass
        
    args = context.args
    
    if args and args[0].startswith("verify_"):
        try:
            token_user_id = int(args[0].split("_")[1])
            if token_user_id == user.id:
                renew_user_token(user.id)
                await update.message.reply_text("✅ <b>Your Access Token has been successfully renewed for the next 4 hours!</b>\n\nAb aap apne file link par dubara click karke files le sakte hain.", parse_mode="HTML")
                return
        except Exception as e:
            print(f"Token Verification Error: {e}")

    if args:
        start_param = args[0]
        
        has_joined_all, fsub_buttons = await get_fsub_buttons(context, user.id, start_param)
        if not has_joined_all:
            await update.message.reply_text(
                "⚠️ <b>Access Restricted!</b>\n\nFiles receive karne ke liye niche diye gaye remaining channels ko join karein:",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(fsub_buttons)
            )
            return

        if not is_token_valid(user.id):
            bot_info = await context.bot.get_me()
            long_target_url = f"https://t.me/{bot_info.username}?start=verify_{user.id}"
            short_token_url = await get_short_link(long_target_url)
            
            token_msg = (
                "⚠️ <b>ACCESS TOKEN EXPIRED!</b> ⚠️\n\n"
                "<i>Your previous access session has ended. Please renew your token to continue downloading files smoothly.</i> ♻️\n\n"
                "⏳ <b>Token Validity:</b> 4 Hours\n\n"
                "💡 <i>This is a quick ads-based verification. Completing just 1 token grants you uninterrupted access to all shareable file links for the next 4 hours!</i> ✨"
            )
            
            token_buttons = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔑 Renew Access Token", url=short_token_url)],
                [InlineKeyboardButton("Tutorial Video", url=TUTORIAL_VIDEO_LINK)],
                [InlineKeyboardButton("♻️ Try Again", callback_data="check_token")]
            ])
            
            await update.message.reply_text(token_msg, parse_mode="HTML", reply_markup=token_buttons)
            return

        asyncio.create_task(send_files_logic(update, context, start_param))
        return
        
    await update.message.reply_text("🗄️ Your automation scripts are securely archived 🛡️, fully optimized ⚙️, and ready for instant deployment 🚀💻⚡. Ready when you are! 🎯🔥")

# --- Dynamic Admin Multi-Force Join Management Commands ---
async def add_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS: return
    if len(context.args) < 3:
        await update.message.reply_text("❌ <b>Format:</b> `/addchannel <Channel_ID> <Invite_Link> <Channel_Title>`", parse_mode="Markdown")
        return
    
    try:
        ch_id = int(context.args[0])
        ch_link = context.args[1]
        ch_title = " ".join(context.args[2:])
        
        fsub_col.update_one(
            {"channel_id": ch_id},
            {"$set": {"invite_link": ch_link, "title": ch_title}},
            upsert=True
        )
        await update.message.reply_text(f"✅ Channel Added Successfully!\n\n📌 <b>Title:</b> {ch_title}\n🆔 <b>ID:</b> <code>{ch_id}</code>\n🔗 <b>Link:</b> {ch_link}", parse_mode="HTML")
    except Exception as e:
        await update.message.reply_text(f"❌ Error adding channel: {e}")

async def del_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS: return
    if not context.args:
        await update.message.reply_text("❌ <b>Format:</b> `/delchannel <Channel_ID>`", parse_mode="Markdown")
        return
    try:
        ch_id = int(context.args[0])
        res = fsub_col.delete_one({"channel_id": ch_id})
        if res.deleted_count > 0:
            await update.message.reply_text(f"✅ Channel <code>{ch_id}</code> removed from Force-Sub list.", parse_mode="HTML")
        else:
            await update.message.reply_text("❌ Specified Channel ID not found.")
    except Exception as e:
        await update.message.reply_text(f"❌ Error removing channel: {e}")

async def list_channels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS: return
    channels = list(fsub_col.find())
    if not channels:
        await update.message.reply_text("📁 Force Join list is currently empty.")
        return
    
    msg = "📢 <b>Active Force Join Channels:</b>\n\n"
    for idx, ch in enumerate(channels, 1):
        msg += f"{idx}. <b>{ch.get('title')}</b>\n🆔 <code>{ch.get('channel_id')}</code>\n🔗 {ch.get('invite_link')}\n\n"
    await update.message.reply_text(msg, parse_mode="HTML")

# --- Admin Monitoring Commands ---
async def check_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS: return
    try:
        logs = list(history_col.find().sort("_id", -1).limit(15))
        log_text = "📊 Recent Logs:\n\n" + "".join([f"👤 {e.get('first_name')}\n📥 {e.get('batch_key')}\n⏰ {e.get('time')}\n\n" for e in logs])
        await update.message.reply_text(log_text)
    except Exception as e:
        await update.message.reply_text(f"❌ Logs load karne me error: {e}")

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id in ADMIN_IDS: 
        try:
            total_users = user_col.count_documents({})
            total_reqs = history_col.count_documents({})
            
            active_db, active_name = get_active_file_db()
            try:
                stats_cmd = active_db.command("dbStats")
                storage_bytes = stats_cmd.get("storageSize", stats_cmd.get("dataSize", 0))
                
                if storage_bytes < 1024 * 1024:
                    storage_kb = storage_bytes / 1024
                    storage_text = f"{storage_kb:.2f} KB ({active_name})"
                else:
                    storage_mb = storage_bytes / (1024 * 1024)
                    storage_text = f"{storage_mb:.2f} MB ({active_name})"
            except Exception as db_err:
                storage_text = "Unavailable"

            await update.message.reply_text(
                f"👥 Total Users: {total_users}\n"
                f"📥 Total Requests: {total_reqs}\n"
                f"🗄️ Active DB Storage: {storage_text}"
            )
        except Exception as e:
            await update.message.reply_text(f"❌ Stats calculation error: {e}")

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS: return
    if not context.args and not update.message.reply_to_message:
        await update.message.reply_text("❌ Kuch text likhein ya kisi message ko reply karein.")
        return
        
    await update.message.reply_text("📢 Broadcast shuru ho raha hai...")
    success = 0
    failed = 0
    for user in user_col.find():
        try:
            if update.message.reply_to_message: 
                await context.bot.copy_message(user['user_id'], update.message.chat_id, update.message.reply_to_message.message_id)
            else: 
                await context.bot.send_message(user['user_id'], " ".join(context.args))
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
            if "Forbidden" in str(e): 
                try: user_col.delete_one({"user_id": user['user_id']})
                except: pass
    await update.message.reply_text(f"✅ Broadcast Complete!\n🟢 Success: {success}\n🔴 Failed/Blocked: {failed}")

# --- Callbacks & Batch Storage ---
async def cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    cancel_status[user_id] = True 
    try: await query.message.delete()
    except: pass
    await query.answer("❌ Files bhejna rok diya gaya hai.")

async def token_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    
    if is_token_valid(user_id):
        await query.answer("✅ Aapka Access Token active hai! Dubara /start command bhejein.", show_alert=True)
        try: await query.message.delete()
        except: pass
    else:
        await query.answer("❌ Token abhi verified nahi hai! Pehle 'Renew Access Token' par click karke token generate karein.", show_alert=True)

async def process_batch_queue(user_id, context, message):
    await asyncio.sleep(15)
    if user_id not in user_queues: return
    raw_files = user_queues.pop(user_id)
    saved_files = []
    
    for msg in raw_files:
        if not msg: continue
        file_obj = msg.document or msg.video or (msg.photo[-1] if msg.photo else None) or msg.audio
        file_id = file_obj.file_id if file_obj else None
        file_size = file_obj.file_size if file_obj and hasattr(file_obj, 'file_size') else 0
        file_caption = msg.caption or "" 
        
        if file_id:
            while True:  
                try:
                    if PRIVATE_STORE_ID != 0:
                        await context.bot.forward_message(PRIVATE_STORE_ID, msg.chat_id, msg.message_id)
                    saved_files.append({
                        "file_id": file_id, 
                        "file_size": file_size,
                        "file_type": 'document' if msg.document else ('video' if msg.video else ('photo' if msg.photo else 'audio')),
                        "caption": file_caption 
                    })
                    await asyncio.sleep(0.2)
                    break
                except Exception as e:
                    error_str = str(e)
                    if "FloodWait" in error_str:
                        seconds = int(re.search(r'\d+', error_str).group()) if re.search(r'\d+', error_str) else 5
                        await asyncio.sleep(seconds + 1)
                    else:
                        break

    backup_queues[user_id] = saved_files
    await message.reply_text("✅ Batch stored! Now send /getlink command to get the shareable batch link.")

async def handle_incoming_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS: return

    if user_id not in user_queues:
        user_queues[user_id] = []
    
    user_queues[user_id].append(update.message)
    
    if user_id in processing_tasks:
        processing_tasks[user_id].cancel()

    processing_tasks[user_id] = asyncio.create_task(process_batch_queue(user_id, context, update.message))

async def get_link_manually(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS: return
    
    if user_id not in backup_queues or not backup_queues[user_id]: 
        await update.message.reply_text("❌ Queue khali hai! Pehle files bhejein.")
        return
        
    batch_key = f"batch_{int(time.time())}"
    try:
        active_db, active_name = get_active_file_db()
        file_batch_col = active_db['file_batches']
        
        file_batch_col.insert_one({"batch_key": batch_key, "files": backup_queues[user_id], "timestamp": time.time()})
        registry_col.insert_one({"batch_key": batch_key, "db_name": active_name})
        
        backup_queues.pop(user_id, None)
        bot_info = await context.bot.get_me()
        await update.message.reply_text(f"🔗 Link: https://t.me/{bot_info.username}?start={batch_key}\n📂 Stored in: {active_name}")
    except Exception as e:
        await update.message.reply_text(f"❌ Link generation error: {e}")

# --- Handle Join Requests (Without Approving them to Channel) ---
async def handle_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.chat_join_request.from_user
        chat = update.chat_join_request.chat
        
        # Request aane par user ko DB mein save karlo as 'requested' (taaki bypass mil jaye)
        join_req_col.update_one(
            {"user_id": user.id, "channel_id": chat.id},
            {"$set": {"status": "requested", "time": time.time()}},
            upsert=True
        )
            
    except Exception as e:
        print(f"Join Request Handling Error: {e}")

# --- Application Startup ---
def main():
    if not TELEGRAM_BOT_TOKEN:
        print("❌ TELEGRAM_BOT_TOKEN missing in environment configuration.")
        return

    request_kwargs = HTTPXRequest(connect_timeout=20.0, read_timeout=20.0)
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).request(request_kwargs).post_init(run_post_init).build()

    # Core Handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("logs", check_logs))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("getlink", get_link_manually))
    
    # Dynamic Multi Force-Sub Admin Handlers
    app.add_handler(CommandHandler("addchannel", add_channel))
    app.add_handler(CommandHandler("delchannel", del_channel))
    app.add_handler(CommandHandler("channels", list_channels))
    
    # Inline Callback Handlers
    app.add_handler(CallbackQueryHandler(cancel_callback, pattern="^cancel_action$"))
    app.add_handler(CallbackQueryHandler(token_callback, pattern="^check_token$"))
    
    # File Media Handlers
    app.add_handler(MessageHandler(filters.Document.ALL | filters.VIDEO | filters.PHOTO | filters.AUDIO, handle_incoming_files))

    # Catch Join Requests (Store to DB but DO NOT add to channel)
    app.add_handler(ChatJoinRequestHandler(handle_join_request))

    print("🤖 Bot with Request-Based Bypass (No Auto-Join) successfully running...")
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    main() 
