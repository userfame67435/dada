import os
import logging
from uuid import uuid4
from urllib.parse import urlencode
import hashlib
import requests
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web
import psycopg2.pool
from config import bots_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
lg = logging.getLogger(__name__)

DB_URL = "postgresql://postgres.iylthyqzwovudjcyfubg:Alex4382!@aws-0-eu-central-1.pooler.supabase.com:6543/postgres"
SITE = os.environ.get("SITE", "")
ENV = "koyeb"

db = psycopg2.pool.SimpleConnectionPool(1, 5, DB_URL)

def setup_db():
    c = db.getconn()
    cr = c.cursor()
    for k in bots_data():
        cr.execute(f"CREATE TABLE IF NOT EXISTS t_{k} (id TEXT PRIMARY KEY, u TEXT, s TEXT, m TEXT)")
    c.commit()
    cr.close()
    db.putconn(c)
    lg.info("DB init")

setup_db()

b = {k: Bot(v["T"]) for k, v in bots_data().items()}
d = {k: Dispatcher(v) for k, v in b.items()}

def get_rates():
    try:
        r = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=the-open-network,bitcoin,tether&vs_currencies=usd").json()
        return r["the-open-network"]["usd"], r["bitcoin"]["usd"], r["tether"]["usd"]
    except:
        return 5.0, 60000.0, 1.0

def rub_to_usd(rub):
    return rub / 100.0

def pay_btns(u):
    kb = InlineKeyboardMarkup()
    for t, c in [("ЮMoney", f"y_{u}"), ("TON", f"t_{u}"), ("BTC", f"b_{u}"), ("USDT", f"u_{u}")]:
        kb.add(InlineKeyboardButton(t, callback_data=c))
    return kb

for k, dp in d.items():
    async def cmd_s(m: types.Message, k=k):
        u = str(m.from_user.id)
        cfg = bots_data()[k]
        await b[k].send_message(m.chat.id, f"Оплата {cfg['P']} ₽\nВыберите:", reply_markup=pay_btns(u))
        lg.info(f"{k}: start {u}")

    async def y_pay(c: types.CallbackQuery, k=k):
        u = c.data.split("_")[1]
        cfg = bots_data()[k]
        await b[k].answer_callback_query(c.id)
        i = str(uuid4())
        p = {"quickpay-form": "shop", "paymentType": "AC", "targets": f"Sub {u}", "sum": cfg["P"], "label": i, "receiver": cfg["Y"], "successURL": f"https://t.me/{(await b[k].get_me()).username}"}
        u = f"https://yoomoney.ru/quickpay/confirm.xml?{urlencode(p)}"
        cnx = db.getconn()
        cr = cnx.cursor()
        cr.execute(f"INSERT INTO t_{k} (id, u, s, m) VALUES (%s, %s, %s, %s)", (i, u, "p", "y"))
        cnx.commit()
        cr.close()
        db.putconn(cnx)
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("Оплатить", url=u))
        await b[k].send_message(c.message.chat.id, "ЮMoney:", reply_markup=kb)
        lg.info(f"{k}: Y link {u}")

    async def t_pay(c: types.CallbackQuery, k=k):
        u = c.data.split("_")[1]
        cfg = bots_data()[k]
        await b[k].answer_callback_query(c.id)
        i = str(uuid4())
        t_r, _, _ = get_rates()
        usd = rub_to_usd(cfg["P"])
        amt = round(usd / t_r, 4)
        cnx = db.getconn()
        cr = cnx.cursor()
        cr.execute(f"INSERT INTO t_{k} (id, u, s, m) VALUES (%s, %s, %s, %s)", (i, u, "p", "t"))
        cnx.commit()
        cr.close()
        db.putconn(cnx)
        await b[k].send_message(c.message.chat.id, f"Send {amt:.4f} TON to: {cfg['C']['TON']}")
        lg.info(f"{k}: TON {u}")

    async def b_pay(c: types.CallbackQuery, k=k):
        u = c.data.split("_")[1]
        cfg = bots_data()[k]
        await b[k].answer_callback_query(c.id)
        i = str(uuid4())
        _, b_r, _ = get_rates()
        usd = rub_to_usd(cfg["P"])
        amt = f"{usd / b_r:.8f}".rstrip("0")
        cnx = db.getconn()
        cr = cnx.cursor()
        cr.execute(f"INSERT INTO t_{k} (id, u, s, m) VALUES (%s, %s, %s, %s)", (i, u, "p", "b"))
        cnx.commit()
        cr.close()
        db.putconn(cnx)
        await b[k].send_message(c.message.chat.id, f"Send {amt} BTC to: {cfg['C']['BTC']}")
        lg.info(f"{k}: BTC {u}")

    async def u_pay(c: types.CallbackQuery, k=k):
        u = c.data.split("_")[1]
        cfg = bots_data()[k]
        await b[k].answer_callback_query(c.id)
        i = str(uuid4())
        _, _, u_r = get_rates()
        usd = rub_to_usd(cfg["P"])
        amt = round(usd / u_r, 2)
        cnx = db.getconn()
        cr = cnx.cursor()
        cr.execute(f"INSERT INTO t_{k} (id, u, s, m) VALUES (%s, %s, %s, %s)", (i, u, "p", "u"))
        cnx.commit()
        cr.close()
        db.putconn(cnx)
        await b[k].send_message(c.message.chat.id, f"Send {amt:.2f} USDT to: {cfg['C']['USDT']}")
        lg.info(f"{k}: USDT {u}")

    dp.register_message_handler(cmd_s, commands=["start"])
    dp.register_callback_query_handler(y_pay, lambda c: c.data.startswith("y_"))
    dp.register_callback_query_handler(t_pay, lambda c: c.data.startswith("t_"))
    dp.register_callback_query_handler(b_pay, lambda c: c.data.startswith("b_"))
    dp.register_callback_query_handler(u_pay, lambda c: c.data.startswith("u_"))

async def y_hook(r):
    d = await r.post()
    i = d.get("label")
    if not i:
        return web.Response(status=400)
    for k in bots_data():
        cnx = db.getconn()
        cr = cnx.cursor()
        cr.execute(f"SELECT u FROM t_{k} WHERE id = %s", (i,))
        r = cr.fetchone()
        if r:
            cfg = bots_data()[k]
            p = [d.get(x, "") for x in ["notification_type", "operation_id", "amount", "currency", "datetime", "sender", "codepro"]] + [cfg["S"], i]
            if hashlib.sha1("&".join(p).encode()).hexdigest() == d.get("sha1_hash"):
                cr.execute(f"UPDATE t_{k} SET s = %s WHERE id = %s", ("ok", i))
                cnx.commit()
                try:
                    inv = await b[k].create_chat_invite_link(chat_id=cfg["CH"], member_limit=1, name=f"x{r[0]}")
                    await b[k].send_message(r[0], f"Оплата прошла! Канал: {inv.invite_link}")
                    lg.info(f"{k}: Pay {i} done")
                except:
                    await b[k].send_message(r[0], "Проблема с доступом. Пишите @Support")
                    lg.error(f"{k}: Invite fail")
            cr.close()
            db.putconn(cnx)
            return web.Response(status=200)
        cr.close()
        db.putconn(cnx)
    return web.Response(status=400)

async def chk(r):
    return web.Response(status=200, text="Up")

async def b_hook(r, k):
    if k not in d:
        return web.Response(status=400)
    u = await r.json()
    await d[k].process_update(types.Update(**u))
    return web.Response(status=200)

async def go():
    for k in b:
        await b[k].delete_webhook(drop_pending_updates=True)
        await b[k].set_webhook(f"{SITE}/h/{k}")
        lg.info(f"{k}: Hook set")
    app = web.Application()
    app.router.add_post("/y", y_hook)
    app.router.add_get("/chk", chk)
    for k in bots_data():
        app.router.add_post(f"/h/{k}", lambda r, k=k: b_hook(r, k))
    p = int(os.environ.get("PORT", 8000))
    r = web.AppRunner(app)
    await r.setup()
    s = web.TCPSite(r, "0.0.0.0", p)
    await s.start()
    lg.info(f"Run on {p}")
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    import asyncio
    asyncio.run(go())
