import asyncio
import json
import os
import logging
import random
import secrets
import tempfile
import threading
from functools import wraps
from pathlib import Path

from flask import Flask, jsonify, redirect, render_template, request, session, url_for, flash
from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from werkzeug.utils import secure_filename

# ================= LOG =================
logging.basicConfig(
    format='[%(levelname)5s/%(asctime)s] %(name)s: %(message)s',
    level=logging.INFO
)
log = logging.getLogger("telegram-dashboard")

# ================= ENV =================
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")
ADMIN_USER = os.environ.get("ADMIN_USER", "admin")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "")
SECRET_KEY = os.environ.get("SECRET_KEY") or secrets.token_hex(32)
PORT = int(os.environ.get("PORT", "8080"))

if not API_ID or not API_HASH or not SESSION_STRING:
    raise RuntimeError("ENV API_ID, API_HASH, dan SESSION_STRING wajib diisi")
if not ADMIN_PASSWORD:
    raise RuntimeError("ENV ADMIN_PASSWORD wajib diisi")

BASE_DIR = Path(__file__).resolve().parent
DATA_FILE = Path(os.environ.get("DATA_FILE", str(BASE_DIR / "bot_data.json")))
LEGACY_DATA_FILE = BASE_DIR / "data.json"

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
telegram_loop = None
broadcast_task = None
broadcast_lock = asyncio.Lock()
data_lock = threading.RLock()

DEFAULT_DATA = {
    "caption": "",
    "groups": [],
    "is_active": False,
    "media_message_id": None,
    "media_name": "",
    "media_type": "",
    "buttons": [],
    "forward_link": None,
    "delay_min": 150,
    "delay_max": 210,
    "cycle_delay": 1800
}


def normalize_data(data):
    if not isinstance(data, dict):
        data = {}
    if "grup" in data and "groups" not in data:
        data["groups"] = data.pop("grup")
    if "aktif" in data and "is_active" not in data:
        data["is_active"] = data.pop("aktif")
    if "media_id" in data and "media_message_id" not in data:
        data["media_message_id"] = data.pop("media_id")

    out = DEFAULT_DATA.copy()
    out.update(data)
    out["groups"] = sorted({str(x).strip().lower() for x in out.get("groups", []) if str(x).strip()})
    out["buttons"] = [
        {"text": str(b.get("text", "")).strip(), "url": str(b.get("url", "")).strip()}
        for b in out.get("buttons", []) if isinstance(b, dict) and b.get("text") and b.get("url")
    ]
    for key, default in (("delay_min", 150), ("delay_max", 210), ("cycle_delay", 1800)):
        try:
            out[key] = int(out.get(key, default))
        except Exception:
            out[key] = default
    out["delay_min"] = max(30, out["delay_min"])
    out["delay_max"] = max(out["delay_min"], out["delay_max"])
    out["cycle_delay"] = max(60, out["cycle_delay"])
    return out


def load_data():
    source = DATA_FILE if DATA_FILE.exists() else LEGACY_DATA_FILE
    if not source.exists():
        return DEFAULT_DATA.copy()
    try:
        with source.open("r", encoding="utf-8") as f:
            return normalize_data(json.load(f))
    except Exception:
        log.exception("Gagal membaca data; menggunakan default")
        return DEFAULT_DATA.copy()


def save_data(data=None):
    global bot_data
    with data_lock:
        if data is not None:
            bot_data = normalize_data(data)
        DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = DATA_FILE.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(bot_data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, DATA_FILE)


bot_data = load_data()
save_data(bot_data)  # migrasi data.json -> bot_data.json bila perlu

# ================= TELEGRAM UTIL =================
def build_buttons():
    with data_lock:
        buttons = list(bot_data.get("buttons", []))
    rows = []
    for b in buttons:
        rows.append([Button.url(b["text"], b["url"])])
    return rows or None


def bold(text):
    return f"<b>{text}</b>" if text else ""


async def send_forward(group):
    try:
        with data_lock:
            link = bot_data.get("forward_link")
        if not link:
            return
        parts = link.rstrip("/").split("/")
        if len(parts) < 2:
            raise ValueError("Format link Telegram tidak valid")
        chat = parts[-2]
        msg_id = int(parts[-1].split("?")[0])
        msg = await client.get_messages(chat, ids=msg_id)
        if not msg:
            raise ValueError("Pesan sumber tidak ditemukan")
        await client.forward_messages(group, msg)
        await client.send_message("me", f"✅ {group}")
    except Exception as e:
        log.warning("Forward ke %s gagal: %s", group, e)
        try:
            await client.send_message("me", f"❌ {group}\n{e}")
        except Exception:
            pass


async def send_custom(group):
    try:
        with data_lock:
            media_id = bot_data.get("media_message_id")
            caption_raw = bot_data.get("caption", "")
        buttons = build_buttons()
        caption = bold(caption_raw)

        if media_id:
            msg = await client.get_messages("me", ids=int(media_id))
            if not msg or not msg.media:
                raise ValueError("Media tersimpan tidak ditemukan di Saved Messages")
            await client.send_file(
                group,
                msg.media,
                caption=caption or bold(msg.message or ""),
                buttons=buttons,
                parse_mode="html"
            )
        elif caption_raw:
            await client.send_message(group, caption, buttons=buttons, parse_mode="html")
        else:
            raise ValueError("Caption/media kosong")

        await client.send_message("me", f"✅ {group}")
    except Exception as e:
        log.warning("Kirim ke %s gagal: %s", group, e)
        try:
            await client.send_message("me", f"❌ {group}\n{e}")
        except Exception:
            pass


async def send_one(group):
    with data_lock:
        forward_link = bot_data.get("forward_link")
    if forward_link:
        await send_forward(group)
    else:
        await send_custom(group)


async def broadcast_loop():
    global broadcast_task
    try:
        async with broadcast_lock:
            while True:
                with data_lock:
                    if not bot_data.get("is_active"):
                        break
                    groups = list(dict.fromkeys(bot_data.get("groups", [])))
                    delay_min = bot_data.get("delay_min", 150)
                    delay_max = bot_data.get("delay_max", 210)
                    cycle_delay = bot_data.get("cycle_delay", 1800)

                if not groups:
                    await asyncio.sleep(5)
                    continue

                for group in groups:
                    with data_lock:
                        if not bot_data.get("is_active"):
                            break
                    await send_one(group)
                    await asyncio.sleep(random.randint(delay_min, delay_max))

                with data_lock:
                    still_active = bot_data.get("is_active")
                if still_active:
                    await asyncio.sleep(cycle_delay)
    except asyncio.CancelledError:
        pass
    finally:
        broadcast_task = None


async def ensure_broadcast_started():
    global broadcast_task
    with data_lock:
        active = bot_data.get("is_active")
    if active and (broadcast_task is None or broadcast_task.done()):
        broadcast_task = asyncio.create_task(broadcast_loop())


async def stop_broadcast_task():
    global broadcast_task
    if broadcast_task and not broadcast_task.done():
        broadcast_task.cancel()
        try:
            await broadcast_task
        except asyncio.CancelledError:
            pass
    broadcast_task = None


def run_async(coro, timeout=60):
    if telegram_loop is None or not telegram_loop.is_running():
        raise RuntimeError("Telegram client belum siap")
    future = asyncio.run_coroutine_threadsafe(coro, telegram_loop)
    return future.result(timeout=timeout)


async def upload_to_saved_messages(path, caption=""):
    msg = await client.send_file("me", path, caption=caption or "")
    return msg.id


# ================= FLASK =================
app = Flask(__name__, template_folder=str(BASE_DIR / "templates"))
app.secret_key = SECRET_KEY
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    MAX_CONTENT_LENGTH=50 * 1024 * 1024,
)


def login_required(fn):
    @wraps(fn)
    def wrapped(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        return fn(*args, **kwargs)
    return wrapped


@app.get("/health")
def health():
    return jsonify({"ok": True, "telegram": bool(client.is_connected())})


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        if secrets.compare_digest(username, ADMIN_USER) and secrets.compare_digest(password, ADMIN_PASSWORD):
            session.clear()
            session["logged_in"] = True
            return redirect(url_for("dashboard"))
        flash("ID atau password salah.", "error")
    return render_template("login.html")


@app.post("/logout")
@login_required
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.get("/")
@login_required
def dashboard():
    with data_lock:
        data = json.loads(json.dumps(bot_data))
    return render_template("dashboard.html", data=data, connected=client.is_connected())


@app.post("/settings")
@login_required
def update_settings():
    global bot_data
    caption = request.form.get("caption", "").strip()
    forward_link = request.form.get("forward_link", "").strip() or None

    try:
        delay_min = max(30, int(request.form.get("delay_min", "150")))
        delay_max = max(delay_min, int(request.form.get("delay_max", "210")))
        cycle_delay = max(60, int(request.form.get("cycle_delay", "1800")))
    except ValueError:
        flash("Nilai jeda harus berupa angka.", "error")
        return redirect(url_for("dashboard"))

    buttons = []
    texts = request.form.getlist("button_text[]")
    urls = request.form.getlist("button_url[]")
    for text, url in zip(texts, urls):
        text, url = text.strip(), url.strip()
        if text and url:
            buttons.append({"text": text, "url": url})

    with data_lock:
        bot_data["caption"] = caption
        bot_data["forward_link"] = forward_link
        bot_data["buttons"] = buttons
        bot_data["delay_min"] = delay_min
        bot_data["delay_max"] = delay_max
        bot_data["cycle_delay"] = cycle_delay
        if forward_link:
            # mode forward tidak memakai custom media/caption saat pengiriman
            pass
        save_data(bot_data)

    flash("Pengaturan pesan berhasil disimpan.", "success")
    return redirect(url_for("dashboard"))


@app.post("/groups/add")
@login_required
def add_groups_web():
    raw = request.form.get("groups", "")
    items = raw.replace(",", "\n").splitlines()
    added = []
    with data_lock:
        existing = set(bot_data.get("groups", []))
        for item in items:
            g = item.strip().lower()
            if not g:
                continue
            if not (g.startswith("@") or g.lstrip("-").isdigit()):
                continue
            if g not in existing:
                existing.add(g)
                added.append(g)
        bot_data["groups"] = sorted(existing)
        save_data(bot_data)
    flash(f"{len(added)} grup ditambahkan.", "success")
    return redirect(url_for("dashboard"))


@app.post("/groups/delete")
@login_required
def delete_group_web():
    group = request.form.get("group", "").strip().lower()
    with data_lock:
        bot_data["groups"] = [g for g in bot_data.get("groups", []) if g != group]
        save_data(bot_data)
    flash(f"Grup {group} dihapus.", "success")
    return redirect(url_for("dashboard"))


@app.post("/media/upload")
@login_required
def upload_media_web():
    file = request.files.get("media")
    if not file or not file.filename:
        flash("Pilih foto atau video terlebih dahulu.", "error")
        return redirect(url_for("dashboard"))

    filename = secure_filename(file.filename) or "media.bin"
    suffix = Path(filename).suffix.lower()
    allowed = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".mp4", ".mov", ".mkv", ".webm"}
    if suffix not in allowed:
        flash("Format media tidak didukung.", "error")
        return redirect(url_for("dashboard"))

    tmp_path = None
    try:
        fd, tmp_path = tempfile.mkstemp(prefix="tg_media_", suffix=suffix)
        os.close(fd)
        file.save(tmp_path)
        with data_lock:
            caption = bot_data.get("caption", "")
        msg_id = run_async(upload_to_saved_messages(tmp_path, caption), timeout=120)
        with data_lock:
            bot_data["media_message_id"] = int(msg_id)
            bot_data["media_name"] = filename
            bot_data["media_type"] = "video" if suffix in {".mp4", ".mov", ".mkv", ".webm"} else "photo"
            bot_data["forward_link"] = None
            save_data(bot_data)
        flash("Media berhasil disimpan ke Telegram Saved Messages.", "success")
    except Exception as e:
        log.exception("Upload media gagal")
        flash(f"Upload media gagal: {e}", "error")
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
    return redirect(url_for("dashboard"))


@app.post("/media/delete")
@login_required
def delete_media_web():
    with data_lock:
        bot_data["media_message_id"] = None
        bot_data["media_name"] = ""
        bot_data["media_type"] = ""
        save_data(bot_data)
    flash("Media dihapus dari konfigurasi.", "success")
    return redirect(url_for("dashboard"))


@app.post("/broadcast/toggle")
@login_required
def toggle_broadcast_web():
    desired = request.form.get("state") == "on"
    with data_lock:
        bot_data["is_active"] = desired
        save_data(bot_data)
    try:
        if desired:
            run_async(ensure_broadcast_started(), timeout=10)
            flash("Broadcast diaktifkan.", "success")
        else:
            run_async(stop_broadcast_task(), timeout=10)
            flash("Broadcast dimatikan.", "success")
    except Exception as e:
        flash(f"Status tersimpan, tetapi runtime gagal diperbarui: {e}", "error")
    return redirect(url_for("dashboard"))


@app.post("/broadcast/send-now")
@login_required
def send_now_web():
    group = request.form.get("group", "").strip().lower()
    with data_lock:
        groups = list(bot_data.get("groups", []))
    targets = [group] if group else groups
    if not targets:
        flash("Belum ada grup tujuan.", "error")
        return redirect(url_for("dashboard"))

    async def _send_targets():
        for target in targets:
            await send_one(target)
            await asyncio.sleep(1)

    try:
        run_async(_send_targets(), timeout=max(60, len(targets) * 15))
        flash(f"Pesan dikirim ke {len(targets)} grup.", "success")
    except Exception as e:
        flash(f"Kirim sekarang gagal: {e}", "error")
    return redirect(url_for("dashboard"))


# ================= TELEGRAM COMMANDS =================
@client.on(events.NewMessage(outgoing=True, pattern=r'^/on$'))
async def cmd_on(event):
    with data_lock:
        if bot_data.get("is_active"):
            return await event.respond("Sudah ON")
        bot_data["is_active"] = True
        save_data(bot_data)
    await ensure_broadcast_started()
    await event.respond("ON")


@client.on(events.NewMessage(outgoing=True, pattern=r'^/off$'))
async def cmd_off(event):
    with data_lock:
        bot_data["is_active"] = False
        save_data(bot_data)
    await stop_broadcast_task()
    await event.respond("OFF")


@client.on(events.NewMessage(outgoing=True, pattern=r'^/status$'))
async def cmd_status(event):
    with data_lock:
        active = bot_data.get("is_active")
        groups = len(bot_data.get("groups", []))
        mode = "FORWARD" if bot_data.get("forward_link") else "CUSTOM"
    await event.respond(f"Status: {'ON' if active else 'OFF'}\nGrup: {groups}\nMode: {mode}")


@client.on(events.NewMessage(outgoing=True, pattern=r'^/addgroup'))
async def cmd_addgroup(event):
    lines = event.raw_text.split('\n')[1:]
    added = []
    with data_lock:
        for g in lines:
            g = g.strip().lower()
            if (g.startswith("@") or g.lstrip("-").isdigit()) and g not in bot_data["groups"]:
                bot_data["groups"].append(g)
                added.append(g)
        bot_data["groups"] = sorted(set(bot_data["groups"]))
        save_data(bot_data)
    await event.respond("✅ Ditambahkan:\n" + "\n".join(added) if added else "⚠️ Tidak ada grup baru")


@client.on(events.NewMessage(outgoing=True, pattern=r'^/delgroup'))
async def cmd_delgroup(event):
    parts = event.raw_text.split()
    if len(parts) < 2:
        return await event.respond("Format salah")
    g = parts[1].lower()
    with data_lock:
        bot_data["groups"] = [x for x in bot_data["groups"] if x != g]
        save_data(bot_data)
    await event.respond("OK")


@client.on(events.NewMessage(outgoing=True, pattern=r'^/listgroup$'))
async def cmd_listgroup(event):
    with data_lock:
        groups = "\n".join(bot_data["groups"]) or "Kosong"
    await event.respond(groups)


@client.on(events.NewMessage(outgoing=True, pattern=r'^/setcaption$'))
async def cmd_setcaption(event):
    if not event.is_reply:
        return await event.respond("Reply pesan untuk ambil caption")
    msg = await event.get_reply_message()
    with data_lock:
        bot_data["caption"] = msg.message or ""
        bot_data["forward_link"] = None
        save_data(bot_data)
    await event.respond("Caption OK")


@client.on(events.NewMessage(outgoing=True, pattern=r'^/setmedia$'))
async def cmd_setmedia(event):
    if not event.is_reply:
        return await event.respond("Reply media")
    msg = await event.get_reply_message()
    if not msg.media:
        return await event.respond("Pesan itu tidak memiliki media")
    with data_lock:
        bot_data["media_message_id"] = msg.id
        bot_data["media_name"] = "Media dari Telegram"
        bot_data["media_type"] = "telegram"
        bot_data["caption"] = msg.message or ""
        bot_data["forward_link"] = None
        save_data(bot_data)
    await event.respond("Media OK")


@client.on(events.NewMessage(outgoing=True, pattern=r'^/setbutton'))
async def cmd_setbutton(event):
    raw = event.raw_text.replace("/setbutton", "", 1).strip()
    buttons = []
    try:
        for b in raw.split("||"):
            t, u = b.split("|", 1)
            buttons.append({"text": t.strip(), "url": u.strip()})
    except Exception:
        return await event.respond("Format salah")
    with data_lock:
        bot_data["buttons"] = buttons
        save_data(bot_data)
    await event.respond("Button OK")


@client.on(events.NewMessage(outgoing=True, pattern=r'^/forward'))
async def cmd_forward(event):
    parts = event.raw_text.split()
    if len(parts) < 2:
        return await event.respond("Masukkan link")
    with data_lock:
        bot_data["forward_link"] = parts[1]
        save_data(bot_data)
    await event.respond("Forward ON")


# ================= RUN =================
def run_flask():
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)


async def main():
    global telegram_loop
    telegram_loop = asyncio.get_running_loop()
    await client.start()
    log.info("Telegram connected as %s", await client.get_me())
    await ensure_broadcast_started()
    threading.Thread(target=run_flask, daemon=True, name="flask").start()
    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
