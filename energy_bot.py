from telegram import Update
from telegram.ext import Updater, CommandHandler, CallbackContext, MessageHandler, Filters
import os
os.chdir(os.path.join(*os.path.split(__file__)[:-1]))

from db_api import ElectricityDB
print("Welcome to energy bot!!!")


with open("api_key.txt", 'r') as f:
    API_KEY = f.readline().strip()

DB_LOCATION = "electricity.sqlite"

MAX_MSG_LENGTH = 4096


# noinspection PyUnusedLocal
def handle_text(update: Update, context: CallbackContext) -> None:
    tg_id = update.message.chat_id
    txt = update.message.text
    txt_split = txt.split(" ")
    value, *rest = txt_split
    time, date = None, None
    value = float(value)
    if rest:
        time, *date = rest
        if date:
            date = date[0]
    with ElectricityDB(DB_LOCATION) as db:
        db.add_record(tg_id, value, time, date)
    msg = "Record added successfully"
    update.message.reply_text(msg)
    get_stats(update, context)
    return None


def cmd(cmd_text: str):
    # noinspection PyUnusedLocal
    def cmd_inner(update: Update, context: CallbackContext) -> None:
        tg_id = update.message.chat_id
        with ElectricityDB(DB_LOCATION) as db:
            func = getattr(db, cmd_text)
            msg = func(tg_id)
        if not msg:
            msg = "Command executed"
        info = msg
        if len(info) > MAX_MSG_LENGTH:
            for x in range(0, len(info), MAX_MSG_LENGTH):
                update.message.reply_text(info[x:x + MAX_MSG_LENGTH])
        else:
            update.message.reply_text(info)
        return None
    return cmd_inner


# noinspection PyUnusedLocal
def execute_command(update: Update, context: CallbackContext) -> None:
    tg_id = update.message.chat_id
    cmd_ = update.message.text
    cmd_txt = "/execute"
    if cmd_.startswith(cmd_txt):
        cmd_ = cmd_[len(cmd_txt):]
    with ElectricityDB(DB_LOCATION) as db:
        res = db.execute_command(tg_id, cmd_)
    update.message.reply_text(res)
    return None


# noinspection PyUnusedLocal
def get_stats(update: Update, context: CallbackContext) -> None:
    tg_id = update.message.chat_id
    with ElectricityDB(DB_LOCATION) as db:
        txt, pics = db.get_stats(tg_id)
    update.message.reply_text(txt)
    for pic in pics:
        update.message.reply_photo(photo=open(pic, 'rb'))
        os.remove(pic)
    return None


# noinspection PyUnusedLocal
def info(update: Update, context: CallbackContext) -> None:
    update.message.reply_text("This is a bot for tracking your energy consumption, like a fancy google doc.")
    return None


def main():
    updater = Updater(API_KEY, workers=1)

    for command in ["delete_records", "drop_tables", "list_hourly_records", "list_records"]:
        cmd_func = cmd(command)
        updater.dispatcher.add_handler(CommandHandler(command, cmd_func))

    updater.dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_text))
    updater.dispatcher.add_handler(CommandHandler("execute", execute_command))
    updater.dispatcher.add_handler(CommandHandler("stats", get_stats))
    updater.dispatcher.add_handler(CommandHandler("info", info))

    updater.start_polling()
    updater.idle()
    return None


if __name__ == '__main__':
    main()
