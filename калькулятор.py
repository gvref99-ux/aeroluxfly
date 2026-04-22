import os
import sqlite3
import tempfile
from datetime import datetime, timezone

import discord
from discord import app_commands
from discord.ext import commands, tasks
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle

# ==============================
# НАСТРОЙКИ
# ==============================
TOKEN = "MTQ5NjE3MjAzMzExNjIxMzI3OA.Gxur6W.wsNz389dg2evBBn4L5YDFzRTV1ywQju73gYe4c"
GUILD_ID = 1124758777623761039
BOOKING_CHANNEL_ID = 1124758778408075266

DB_PATH = "bookings.db"
DATETIME_FORMAT = "%m-%d %H:%M"
TIMEZONE_LABEL = "UTC"


# ==============================
# ВРЕМЯ
# ==============================
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_utc_datetime(value: str) -> datetime:
    dt = datetime.strptime(value.strip(), DATETIME_FORMAT)
    current_year_utc = utc_now().year
    return dt.replace(year=current_year_utc, tzinfo=timezone.utc)


def format_utc_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime(DATETIME_FORMAT)


# ==============================
# БАЗА ДАННЫХ
# ==============================
def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bookings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT NOT NULL,
                callsign TEXT NOT NULL,
                flight_number TEXT NOT NULL,
                board_number TEXT NOT NULL,
                dep_icao TEXT NOT NULL,
                arr_icao TEXT NOT NULL,
                departure_time TEXT NOT NULL,
                estimated_return_time TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL,
                returned_at TEXT,
                return_confirmed_by INTEGER,
                booking_message_id INTEGER
            )
            """
        )
        conn.commit()


def normalize_code(value: str) -> str:
    return value.strip().upper()


def create_booking(
    user_id: int,
    username: str,
    callsign: str,
    flight_number: str,
    board_number: str,
    dep_icao: str,
    arr_icao: str,
    departure_time: datetime,
    estimated_return_time: datetime,
) -> int:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            """
            INSERT INTO bookings (
                user_id,
                username,
                callsign,
                flight_number,
                board_number,
                dep_icao,
                arr_icao,
                departure_time,
                estimated_return_time,
                status,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?)
            """,
            (
                user_id,
                username,
                callsign,
                flight_number,
                board_number,
                dep_icao,
                arr_icao,
                format_utc_datetime(departure_time),
                format_utc_datetime(estimated_return_time),
                format_utc_datetime(utc_now()),
            ),
        )
        conn.commit()
        return cur.lastrowid


def get_booking_by_id(booking_id: int):
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            "SELECT * FROM bookings WHERE id = ? LIMIT 1",
            (booking_id,),
        ).fetchone()


def get_user_bookings(user_id: int):
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            """
            SELECT *
            FROM bookings
            WHERE user_id = ?
            ORDER BY departure_time DESC
            LIMIT 10
            """,
            (user_id,),
        ).fetchall()


def get_active_bookings():
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            """
            SELECT *
            FROM bookings
            WHERE status = 'active'
            ORDER BY departure_time ASC
            """
        ).fetchall()


def get_all_bookings_history():
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            """
            SELECT *
            FROM bookings
            ORDER BY created_at DESC, departure_time DESC
            """
        ).fetchall()


def set_booking_message_id(booking_id: int, message_id: int):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "UPDATE bookings SET booking_message_id = ? WHERE id = ?",
            (message_id, booking_id),
        )
        conn.commit()


def mark_booking_returned(
    booking_id: int,
    confirmed_by_user_id: int | None = None,
    returned_at: datetime | None = None,
):
    actual_return = returned_at or utc_now()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            UPDATE bookings
            SET status = 'returned',
                returned_at = ?,
                return_confirmed_by = COALESCE(?, return_confirmed_by)
            WHERE id = ?
            """,
            (format_utc_datetime(actual_return), confirmed_by_user_id, booking_id),
        )
        conn.commit()


def cancel_booking(booking_id: int, cancelled_at: datetime | None = None):
    actual_cancel_time = cancelled_at or utc_now()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            UPDATE bookings
            SET status = 'cancelled',
                returned_at = ?
            WHERE id = ?
            """,
            (format_utc_datetime(actual_cancel_time), booking_id),
        )
        conn.commit()


def find_conflict(board_number: str, new_departure: datetime, new_return: datetime):
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT *
            FROM bookings
            WHERE UPPER(board_number) = UPPER(?)
              AND status = 'active'
            ORDER BY departure_time ASC
            """,
            (board_number,),
        ).fetchall()

    for row in rows:
        existing_departure = parse_utc_datetime(row["departure_time"])
        existing_return = parse_utc_datetime(row["estimated_return_time"])
        overlaps = new_departure < existing_return and new_return > existing_departure
        if overlaps:
            return row

    return None


def get_booking_for_manual_return(board_number: str, now_utc: datetime):
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT *
            FROM bookings
            WHERE UPPER(board_number) = UPPER(?)
              AND status = 'active'
            ORDER BY departure_time ASC
            """,
            (board_number,),
        ).fetchall()

    for row in rows:
        departure = parse_utc_datetime(row["departure_time"])
        estimated_return = parse_utc_datetime(row["estimated_return_time"])
        if departure <= now_utc < estimated_return:
            return row

    return None


def get_expired_bookings(now_utc: datetime):
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT *
            FROM bookings
            WHERE status = 'active'
            ORDER BY estimated_return_time ASC
            """
        ).fetchall()

    expired = []
    for row in rows:
        estimated_return = parse_utc_datetime(row["estimated_return_time"])
        if estimated_return <= now_utc:
            expired.append(row)

    return expired


def get_booking_for_cancel(board_number: str, departure_time: datetime):
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            """
            SELECT *
            FROM bookings
            WHERE UPPER(board_number) = UPPER(?)
              AND departure_time = ?
              AND status = 'active'
            LIMIT 1
            """,
            (board_number, format_utc_datetime(departure_time)),
        ).fetchone()


# ==============================
# DISCORD BOT
# ==============================
intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)


async def get_booking_channel():
    channel = bot.get_channel(BOOKING_CHANNEL_ID)
    if channel is None:
        channel = await bot.fetch_channel(BOOKING_CHANNEL_ID)
    return channel


async def send_booking_message(booking_row):
    channel = await get_booking_channel()

    embed = discord.Embed(
        title="Активная бронь борта",
        description=f"Бронь создана. Всё время указано в {TIMEZONE_LABEL}.",
        timestamp=utc_now(),
    )
    embed.add_field(name="Позывной", value=booking_row["callsign"], inline=True)
    embed.add_field(name="Рейс", value=booking_row["flight_number"], inline=True)
    embed.add_field(name="Номер борта", value=booking_row["board_number"], inline=True)
    embed.add_field(name="Вылет ICAO", value=booking_row["dep_icao"], inline=True)
    embed.add_field(name="Прибытие ICAO", value=booking_row["arr_icao"], inline=True)
    embed.add_field(
        name=f"Дата и время вылета ({TIMEZONE_LABEL})",
        value=booking_row["departure_time"],
        inline=False,
    )
    embed.add_field(
        name=f"Расчётное время возвращения ({TIMEZONE_LABEL})",
        value=booking_row["estimated_return_time"],
        inline=False,
    )
    embed.add_field(name="Кто забронировал", value=booking_row["username"], inline=False)
    embed.set_footer(text=f"ID брони: {booking_row['id']}")

    message = await channel.send(embed=embed)
    set_booking_message_id(booking_row["id"], message.id)


async def delete_booking_message(message_id: int | None):
    if not message_id:
        return

    try:
        channel = await get_booking_channel()
        message = await channel.fetch_message(message_id)
        await message.delete()
    except Exception:
        pass


def build_history_pdf() -> str:
    rows = get_all_bookings_history()
    timestamp = utc_now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(tempfile.gettempdir(), f"booking_history_{timestamp}.pdf")

    doc = SimpleDocTemplate(
        output_path,
        pagesize=A4,
        leftMargin=12 * mm,
        rightMargin=12 * mm,
        topMargin=12 * mm,
        bottomMargin=12 * mm,
    )

    styles = getSampleStyleSheet()
    story = []
    story.append(Paragraph("История броней бортов", styles["Title"]))
    story.append(Spacer(1, 6))
    story.append(Paragraph(f"Сформировано: {format_utc_datetime(utc_now())} UTC", styles["Normal"]))
    story.append(Spacer(1, 10))

    if not rows:
        story.append(Paragraph("История пуста.", styles["Normal"]))
        doc.build(story)
        return output_path

    table_data = [[
        "Статус",
        "Борт",
        "Рейс",
        "Позывной",
        "Маршрут",
        "Вылет UTC",
        "Возврат UTC",
        "Пилот",
    ]]

    for row in rows:
        table_data.append([
            str(row["status"]),
            str(row["board_number"]),
            str(row["flight_number"]),
            str(row["callsign"]),
            f"{row['dep_icao']}-{row['arr_icao']}",
            str(row["departure_time"]),
            str(row["estimated_return_time"]),
            str(row["username"]),
        ])

    table = Table(
        table_data,
        repeatRows=1,
        colWidths=[18 * mm, 22 * mm, 22 * mm, 24 * mm, 25 * mm, 28 * mm, 28 * mm, 30 * mm],
    )
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.grey),
        ("FONTSIZE", (0, 0), (-1, -1), 7),
        ("LEADING", (0, 0), (-1, -1), 8),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.beige]),
    ]))

    story.append(table)
    doc.build(story)
    return output_path


@tasks.loop(minutes=1)
async def auto_return_check():
    now_utc = utc_now()
    expired_rows = get_expired_bookings(now_utc)

    for row in expired_rows:
        mark_booking_returned(row["id"], returned_at=now_utc)
        await delete_booking_message(row["booking_message_id"])


@auto_return_check.before_loop
async def before_auto_return_check():
    await bot.wait_until_ready()


@bot.event
async def on_ready():
    print(f"Бот запущен как {bot.user}")
    try:
        guild = discord.Object(id=GUILD_ID)
        synced = await bot.tree.sync(guild=guild)
        print(f"Команды синхронизированы: {len(synced)}")
    except Exception as e:
        print("Ошибка синхронизации команд:", e)

    if not auto_return_check.is_running():
        auto_return_check.start()


# ==============================
# КОМАНДЫ
# ==============================
@bot.tree.command(
    name="booking_flight",
    description="Создать бронь борта",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(
    callsign="Позывной",
    flight_number="Рейс",
    board_number="Номер борта",
    dep_icao="ICAO вылета",
    arr_icao="ICAO прибытия",
    departure_time=f"Дата и время вылета в формате MM-DD HH:MM ({TIMEZONE_LABEL})",
    estimated_return_time=f"Расчётное время возвращения в формате MM-DD HH:MM ({TIMEZONE_LABEL})",
)
async def booking_flight(
    interaction: discord.Interaction,
    callsign: str,
    flight_number: str,
    board_number: str,
    dep_icao: str,
    arr_icao: str,
    departure_time: str,
    estimated_return_time: str,
):
    await interaction.response.defer(ephemeral=True)

    callsign = normalize_code(callsign)
    flight_number = normalize_code(flight_number)
    board_number = normalize_code(board_number)
    dep_icao = normalize_code(dep_icao)
    arr_icao = normalize_code(arr_icao)

    if len(dep_icao) != 4 or len(arr_icao) != 4:
        await interaction.followup.send(
            "ICAO коды должны быть по 4 символа. Пример: UUEE, ULLI.",
            ephemeral=True,
        )
        return

    try:
        departure_dt = parse_utc_datetime(departure_time)
        return_dt = parse_utc_datetime(estimated_return_time)
    except ValueError:
        await interaction.followup.send(
            f"Неверный формат даты. Используй MM-DD HH:MM. Всё время вводится в {TIMEZONE_LABEL}.",
            ephemeral=True,
        )
        return

    if return_dt <= departure_dt:
        await interaction.followup.send(
            "Расчётное время возвращения должно быть позже времени вылета.",
            ephemeral=True,
        )
        return

    conflict = find_conflict(board_number, departure_dt, return_dt)
    if conflict:
        await interaction.followup.send(
            (
                "Этот борт уже занят на пересекающееся время.\n"
                f"Текущая бронь: рейс {conflict['flight_number']}, позывной {conflict['callsign']}.\n"
                f"Интервал ({TIMEZONE_LABEL}): {conflict['departure_time']} — {conflict['estimated_return_time']}\n"
                "Можно бронировать этот же борт только на непересекающееся время."
            ),
            ephemeral=True,
        )
        return

    booking_id = create_booking(
        user_id=interaction.user.id,
        username=str(interaction.user),
        callsign=callsign,
        flight_number=flight_number,
        board_number=board_number,
        dep_icao=dep_icao,
        arr_icao=arr_icao,
        departure_time=departure_dt,
        estimated_return_time=return_dt,
    )

    booking = get_booking_by_id(booking_id)
    if booking:
        await send_booking_message(booking)

    await interaction.followup.send(
        (
            "Бронь создана.\n"
            f"Позывной: {callsign}\n"
            f"Рейс: {flight_number}\n"
            f"Борт: {board_number}\n"
            f"Маршрут: {dep_icao} → {arr_icao}\n"
            f"Вылет ({TIMEZONE_LABEL}): {format_utc_datetime(departure_dt)}\n"
            f"Расчётный возврат ({TIMEZONE_LABEL}): {format_utc_datetime(return_dt)}"
        ),
        ephemeral=True,
    )


@bot.tree.command(
    name="return_flight",
    description="Подтвердить возвращение борта",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(board_number="Номер борта")
async def return_flight(interaction: discord.Interaction, board_number: str):
    await interaction.response.defer(ephemeral=True)

    board_number = normalize_code(board_number)
    now_utc = utc_now()
    booking = get_booking_for_manual_return(board_number, now_utc)

    if not booking:
        await interaction.followup.send(
            (
                f"Для борта {board_number} сейчас нет активного полёта для возврата.\n"
                f"Проверь номер борта или дождись времени вылета. Всё время у бота в {TIMEZONE_LABEL}."
            ),
            ephemeral=True,
        )
        return

    mark_booking_returned(booking["id"], confirmed_by_user_id=interaction.user.id, returned_at=now_utc)
    await delete_booking_message(booking["booking_message_id"])

    await interaction.followup.send(
        f"Возврат борта {board_number} подтверждён. Борт снова доступен для бронирования.",
        ephemeral=True,
    )


@bot.tree.command(
    name="cancel_flight",
    description="Отменить бронь по борту и времени вылета",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(
    board_number="Номер борта",
    departure_time=f"Время вылета в формате MM-DD HH:MM ({TIMEZONE_LABEL})",
)
async def cancel_flight(interaction: discord.Interaction, board_number: str, departure_time: str):
    await interaction.response.defer(ephemeral=True)

    board_number = normalize_code(board_number)

    try:
        departure_dt = parse_utc_datetime(departure_time)
    except ValueError:
        await interaction.followup.send(
            f"Неверный формат даты. Используй MM-DD HH:MM. Всё время вводится в {TIMEZONE_LABEL}.",
            ephemeral=True,
        )
        return

    booking = get_booking_for_cancel(board_number, departure_dt)
    if not booking:
        await interaction.followup.send(
            f"Активная бронь для борта {board_number} с вылетом {format_utc_datetime(departure_dt)} {TIMEZONE_LABEL} не найдена.",
            ephemeral=True,
        )
        return

    cancel_booking(booking["id"], cancelled_at=utc_now())
    await delete_booking_message(booking["booking_message_id"])

    await interaction.followup.send(
        f"Бронь борта {board_number} с вылетом {format_utc_datetime(departure_dt)} {TIMEZONE_LABEL} отменена.",
        ephemeral=True,
    )


@bot.tree.command(
    name="check_booking",
    description="Показать активные брони только тебе",
    guild=discord.Object(id=GUILD_ID),
)
async def check_booking(interaction: discord.Interaction):
    rows = get_active_bookings()

    if not rows:
        await interaction.response.send_message(
            f"Сейчас нет активных броней. Всё время отображается в {TIMEZONE_LABEL}.",
            ephemeral=True,
        )
        return

    embed = discord.Embed(title=f"Активные брони ({TIMEZONE_LABEL})")

    for row in rows[:25]:
        embed.add_field(
            name=f"{row['board_number']} | {row['flight_number']}",
            value=(
                f"Позывной: {row['callsign']}\n"
                f"Маршрут: {row['dep_icao']} → {row['arr_icao']}\n"
                f"Вылет: {row['departure_time']}\n"
                f"Возврат: {row['estimated_return_time']}\n"
                f"Пилот: {row['username']}"
            ),
            inline=False,
        )

    if len(rows) > 25:
        embed.set_footer(text=f"Показаны первые 25 из {len(rows)} активных броней")

    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(
    name="history_booking",
    description="Отправить PDF с историей всех броней",
    guild=discord.Object(id=GUILD_ID),
)
async def history_booking(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    pdf_path = build_history_pdf()
    file = discord.File(pdf_path, filename="booking_history.pdf")

    await interaction.followup.send(
        content="Готово. Вот PDF с историей всех броней.",
        file=file,
        ephemeral=True,
    )

    try:
        os.remove(pdf_path)
    except Exception:
        pass


@bot.tree.command(
    name="my_brons",
    description="Показать мои последние брони",
    guild=discord.Object(id=GUILD_ID),
)
async def my_brons(interaction: discord.Interaction):
    rows = get_user_bookings(interaction.user.id)

    if not rows:
        await interaction.response.send_message("У тебя пока нет броней.", ephemeral=True)
        return

    embed = discord.Embed(title=f"Мои брони ({TIMEZONE_LABEL})")

    for row in rows:
        status_text = row["status"]
        returned_line = ""
        if row["returned_at"]:
            returned_line = f"\nВозвращён: {row['returned_at']}"

        embed.add_field(
            name=f"ID {row['id']} | {row['flight_number']} | {row['board_number']}",
            value=(
                f"Позывной: {row['callsign']}\n"
                f"Маршрут: {row['dep_icao']} → {row['arr_icao']}\n"
                f"Вылет: {row['departure_time']}\n"
                f"Расчётный возврат: {row['estimated_return_time']}\n"
                f"Статус: {status_text}{returned_line}"
            ),
            inline=False,
        )

    await interaction.response.send_message(embed=embed, ephemeral=True)


if __name__ == "__main__":
    if not TOKEN or TOKEN == "ВСТАВЬ_СЮДА_НОВЫЙ_ТОКЕН":
        raise RuntimeError("Вставь токен бота в переменную TOKEN.")
    init_db()
    bot.run(TOKEN)