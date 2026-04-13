"""Generate heatmap methodology PDF."""
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.lib.colors import HexColor
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import os

# Register a font that supports Cyrillic
# Try common macOS paths
font_paths = [
    "/System/Library/Fonts/Supplemental/Arial.ttf",
    "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
    "/System/Library/Fonts/Supplemental/Arial Italic.ttf",
    "/Library/Fonts/Arial.ttf",
]

pdfmetrics.registerFont(TTFont("Arial", "/System/Library/Fonts/Supplemental/Arial.ttf"))
pdfmetrics.registerFont(TTFont("Arial-Bold", "/System/Library/Fonts/Supplemental/Arial Bold.ttf"))
pdfmetrics.registerFont(TTFont("Arial-Italic", "/System/Library/Fonts/Supplemental/Arial Italic.ttf"))

# Colors
DARK = HexColor("#1a1a2e")
ACCENT = HexColor("#0f3460")
GREEN = HexColor("#16a34a")
RED = HexColor("#dc2626")
GRAY = HexColor("#6b7280")
LIGHT_BG = HexColor("#f8fafc")
BORDER = HexColor("#e2e8f0")

styles = getSampleStyleSheet()

title_style = ParagraphStyle(
    "CustomTitle", fontName="Arial-Bold", fontSize=20, leading=26,
    textColor=DARK, spaceAfter=4*mm, alignment=TA_CENTER
)
subtitle_style = ParagraphStyle(
    "Subtitle", fontName="Arial", fontSize=10, leading=14,
    textColor=GRAY, spaceAfter=8*mm, alignment=TA_CENTER
)
h1_style = ParagraphStyle(
    "H1", fontName="Arial-Bold", fontSize=14, leading=18,
    textColor=ACCENT, spaceBefore=8*mm, spaceAfter=3*mm
)
h2_style = ParagraphStyle(
    "H2", fontName="Arial-Bold", fontSize=11, leading=15,
    textColor=DARK, spaceBefore=5*mm, spaceAfter=2*mm
)
body_style = ParagraphStyle(
    "Body", fontName="Arial", fontSize=10, leading=14,
    textColor=DARK, spaceAfter=2*mm
)
formula_style = ParagraphStyle(
    "Formula", fontName="Arial-Bold", fontSize=10, leading=14,
    textColor=DARK, spaceAfter=2*mm, leftIndent=10*mm,
    backColor=LIGHT_BG
)
note_style = ParagraphStyle(
    "Note", fontName="Arial-Italic", fontSize=9, leading=13,
    textColor=GRAY, spaceAfter=2*mm, leftIndent=10*mm
)
bullet_style = ParagraphStyle(
    "Bullet", fontName="Arial", fontSize=10, leading=14,
    textColor=DARK, spaceAfter=1.5*mm, leftIndent=8*mm, bulletIndent=3*mm
)

output_path = os.path.join(os.path.dirname(__file__), "heatmap_methodology.pdf")

doc = SimpleDocTemplate(
    output_path, pagesize=A4,
    topMargin=20*mm, bottomMargin=20*mm,
    leftMargin=20*mm, rightMargin=20*mm
)

story = []

# Title
story.append(Paragraph("Карта рынка", title_style))
story.append(Paragraph("Методология расчёта", ParagraphStyle(
    "Title2", fontName="Arial", fontSize=14, leading=18,
    textColor=GRAY, spaceAfter=3*mm, alignment=TA_CENTER
)))
story.append(Paragraph("таймфрейм.рф  |  13 апреля 2026", subtitle_style))
story.append(HRFlowable(width="100%", thickness=1, color=BORDER, spaceAfter=5*mm))

# 1. General
story.append(Paragraph("1. Общее описание", h1_style))
story.append(Paragraph(
    "Карта рынка (хитмэп) отображает акции Московской биржи в виде прямоугольников. "
    "Размер прямоугольника определяется капитализацией, оборотом или весом в индексе. "
    "Цвет показывает процентное изменение цены за выбранный период.", body_style
))

story.append(Paragraph("Два режима отображения:", body_style))
story.append(Paragraph(
    "<bullet>&bull;</bullet><b>IMOEX</b> — акции из индекса МосБиржи. "
    "Размер блока = вес в индексе (данные из ISS MOEX API).", bullet_style
))
story.append(Paragraph(
    "<bullet>&bull;</bullet><b>Все акции</b> — все акции из базы данных. "
    "Размер по капитализации или обороту.", bullet_style
))

story.append(Spacer(1, 3*mm))
story.append(Paragraph("Доступные периоды:", body_style))

periods_data = [
    ["Кнопка", "Период", "Описание"],
    ["1Д", "1 день", "Изменение за текущий торговый день"],
    ["1Н", "1 неделя", "Изменение за 7 дней (snapshot-to-snapshot)"],
    ["1М", "1 месяц", "Изменение за 30 дней (snapshot-to-snapshot)"],
    ["1Г", "1 год", "Изменение за 365 дней (snapshot-to-snapshot)"],
]
t = Table(periods_data, colWidths=[25*mm, 30*mm, 100*mm])
t.setStyle(TableStyle([
    ("FONTNAME", (0, 0), (-1, 0), "Arial-Bold"),
    ("FONTNAME", (0, 1), (-1, -1), "Arial"),
    ("FONTSIZE", (0, 0), (-1, -1), 9),
    ("BACKGROUND", (0, 0), (-1, 0), ACCENT),
    ("TEXTCOLOR", (0, 0), (-1, 0), HexColor("#ffffff")),
    ("BACKGROUND", (0, 1), (-1, -1), LIGHT_BG),
    ("GRID", (0, 0), (-1, -1), 0.5, BORDER),
    ("TOPPADDING", (0, 0), (-1, -1), 3),
    ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ("LEFTPADDING", (0, 0), (-1, -1), 5),
]))
story.append(t)

# 2. Data sources
story.append(Paragraph("2. Источники данных", h1_style))

sources_data = [
    ["Данные", "Источник", "API", "Назначение"],
    ["Дневные свечи\n(interval=24)", "ISS MOEX\n(публичный)", "iss.moex.com", "Prev close, цены N дней\nназад, объёмы торгов"],
    ["5-мин свечи\n(interval=5)", "Algopack MOEX\n(JWT-токен)", "apim.moex.com", "Текущая цена\nвнутри дня"],
    ["Капитализация", "Таблица\nstock_market_cap", "БД", "Размер блоков"],
    ["Веса IMOEX", "ISS MOEX\n(публичный)", "iss.moex.com", "Размер блоков\nв режиме IMOEX"],
]
t2 = Table(sources_data, colWidths=[32*mm, 32*mm, 35*mm, 55*mm])
t2.setStyle(TableStyle([
    ("FONTNAME", (0, 0), (-1, 0), "Arial-Bold"),
    ("FONTNAME", (0, 1), (-1, -1), "Arial"),
    ("FONTSIZE", (0, 0), (-1, -1), 9),
    ("BACKGROUND", (0, 0), (-1, 0), ACCENT),
    ("TEXTCOLOR", (0, 0), (-1, 0), HexColor("#ffffff")),
    ("BACKGROUND", (0, 1), (-1, -1), LIGHT_BG),
    ("GRID", (0, 0), (-1, -1), 0.5, BORDER),
    ("TOPPADDING", (0, 0), (-1, -1), 3),
    ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ("LEFTPADDING", (0, 0), (-1, -1), 4),
    ("VALIGN", (0, 0), (-1, -1), "TOP"),
]))
story.append(t2)

story.append(Spacer(1, 3*mm))
story.append(Paragraph(
    "ISS-дневки агрегируют все торговые сессии дня (утреннюю, основную, вечернюю) "
    "в одну свечу. Для Сургутнефтегаза (SNGS/SNGSP) капитализация делится на 2.", note_style
))

# 3. Formulas
story.append(Paragraph("3. Формулы расчёта", h1_style))

# 1D
story.append(Paragraph("1Д (1 день) — change_1d", h2_style))
story.append(Paragraph("Текущая цена в сравнении с ценой закрытия последнего рабочего дня.", body_style))
story.append(Paragraph("change_1d = (текущая_цена - prev_close) / prev_close * 100%", formula_style))
story.append(Paragraph(
    "<bullet>&bull;</bullet><b>текущая_цена</b> = close последней 5-мин свечи сегодня. "
    "Если торги ещё не начались — close дневной свечи.", bullet_style
))
story.append(Paragraph(
    "<bullet>&bull;</bullet><b>prev_close</b> = close последнего будня (пн-пт) из дневных свечей. "
    "Это аналог поля PREVPRICE на Московской бирже.", bullet_style
))
story.append(Paragraph(
    "На выходных prev_close = close пятницы. В понедельник до открытия торгов — тоже пятница.", note_style
))

# 1W
story.append(Paragraph("1Н (1 неделя) — change_1w", h2_style))
story.append(Paragraph(
    "Snapshot-to-snapshot: сравнение текущей цены с ценой ровно 7 дней назад.", body_style
))
story.append(Paragraph("change_1w = (текущая_цена - цена_7д_назад) / цена_7д_назад * 100%", formula_style))
story.append(Paragraph(
    "цена_7д_назад = close ближайшей свечи в окне [NOW()-10 дней ... NOW()-7 дней]. "
    "Используются 5-мин и дневные свечи, берётся ближайшая к отметке.", note_style
))

# 1M
story.append(Paragraph("1М (1 месяц) — change_1m", h2_style))
story.append(Paragraph("change_1m = (текущая_цена - цена_30д_назад) / цена_30д_назад * 100%", formula_style))
story.append(Paragraph(
    "цена_30д_назад = close ближайшей свечи в окне [NOW()-35 дней ... NOW()-30 дней].", note_style
))

# 1Y
story.append(Paragraph("1Г (1 год) — change_1y", h2_style))
story.append(Paragraph("change_1y = (текущая_цена - цена_365д_назад) / цена_365д_назад * 100%", formula_style))
story.append(Paragraph(
    "цена_365д_назад = close ближайшей свечи в окне [NOW()-379 дней ... NOW()-365 дней].", note_style
))

# 4. Block sizes
story.append(Paragraph("4. Размер блоков", h1_style))

sizes_data = [
    ["Режим", "Параметр", "Что определяет размер"],
    ["IMOEX", "Вес в индексе", "Поле weight из ISS MOEX API (обновляется раз в час)"],
    ["Все акции", "Капитализация", "Рыночная капитализация компании (таблица stock_market_cap)"],
    ["Все акции", "Оборот", "Объём торгов в рублях: value_1d (1Д), value_1w (1Н), value_1m (1М, 1Г)"],
]
t3 = Table(sizes_data, colWidths=[25*mm, 35*mm, 95*mm])
t3.setStyle(TableStyle([
    ("FONTNAME", (0, 0), (-1, 0), "Arial-Bold"),
    ("FONTNAME", (0, 1), (-1, -1), "Arial"),
    ("FONTSIZE", (0, 0), (-1, -1), 9),
    ("BACKGROUND", (0, 0), (-1, 0), ACCENT),
    ("TEXTCOLOR", (0, 0), (-1, 0), HexColor("#ffffff")),
    ("BACKGROUND", (0, 1), (-1, -1), LIGHT_BG),
    ("GRID", (0, 0), (-1, -1), 0.5, BORDER),
    ("TOPPADDING", (0, 0), (-1, -1), 3),
    ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ("LEFTPADDING", (0, 0), (-1, -1), 5),
    ("VALIGN", (0, 0), (-1, -1), "TOP"),
]))
story.append(t3)

# 5. Updates
story.append(Paragraph("5. Обновление данных", h1_style))

updates_data = [
    ["Компонент", "Частота", "Источник"],
    ["Materialized View\n(mv_heatmap_stocks)", "Каждые 5 минут", "Оркестратор"],
    ["5-мин свечи", "Реальное время", "Algopack API"],
    ["Дневные свечи", "Ежедневно\n(перезапись 14 дней)", "ISS MOEX API"],
    ["Кэш API (Redis)", "TTL 5 минут", "Автоматически"],
    ["Веса IMOEX", "TTL 1 час", "ISS MOEX API"],
]
t4 = Table(updates_data, colWidths=[45*mm, 40*mm, 55*mm])
t4.setStyle(TableStyle([
    ("FONTNAME", (0, 0), (-1, 0), "Arial-Bold"),
    ("FONTNAME", (0, 1), (-1, -1), "Arial"),
    ("FONTSIZE", (0, 0), (-1, -1), 9),
    ("BACKGROUND", (0, 0), (-1, 0), ACCENT),
    ("TEXTCOLOR", (0, 0), (-1, 0), HexColor("#ffffff")),
    ("BACKGROUND", (0, 1), (-1, -1), LIGHT_BG),
    ("GRID", (0, 0), (-1, -1), 0.5, BORDER),
    ("TOPPADDING", (0, 0), (-1, -1), 3),
    ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ("LEFTPADDING", (0, 0), (-1, -1), 5),
    ("VALIGN", (0, 0), (-1, -1), "TOP"),
]))
story.append(t4)

# 6. Reference
story.append(Paragraph("6. Верификация", h1_style))
story.append(Paragraph(
    "Расчёт <b>1Д</b> совпадает с данными data.moex.com (поле LASTTOPREVPRICE). "
    "Проверено на GAZP (+2.71%), ROSN (+2.92%) — точное совпадение.", body_style
))
story.append(Paragraph(
    "Расчёты <b>1Н / 1М / 1Г</b> используют snapshot-to-snapshot методологию: "
    "сравнение цены в текущий момент с ценой в аналогичный момент N дней назад. "
    "Окно поиска (3-14 дней) компенсирует выходные и праздники.", body_style
))

story.append(Spacer(1, 10*mm))
story.append(HRFlowable(width="100%", thickness=0.5, color=BORDER, spaceAfter=3*mm))
story.append(Paragraph(
    "Документ подготовлен автоматически из кодовой базы проекта таймфрейм.рф", note_style
))

doc.build(story)
print(f"PDF saved: {output_path}")
