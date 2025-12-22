#!/usr/bin/env python3
"""
Backend API для визуализации Open Interest и свечей
"""

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)

# === ⚙️ Настройки БД ===
DB_URL = "postgresql+pg8000://postgres:1803@localhost:5432/moex_db"
engine = create_engine(DB_URL)


# =============================================================================
# 📋 ИНСТРУМЕНТЫ
# =============================================================================

@app.route('/api/instruments', methods=['GET'])
def get_instruments():
    """
    Возвращает список инструментов, сгруппированных по категориям
    """
    try:
        query = text("""
            SELECT DISTINCT 
                sectype,
                name,
                iss_code,
                "group"
            FROM instruments
            WHERE type = 'futures'
            ORDER BY "group", name
        """)

        with engine.connect() as conn:
            result = conn.execute(query)
            rows = result.fetchall()

        print(f"DEBUG: Found {len(rows)} instruments")

        # Группируем по категориям
        groups = {}
        for row in rows:
            group_name = row[3] if row[3] else 'Другое'
            if group_name not in groups:
                groups[group_name] = []

            groups[group_name].append({
                'sectype': row[0],
                'name': row[1],
                'iss_code': row[2],
                'group': group_name,
                'has_oi': True,  # Упрощаем
                'has_candles': True
            })

        print(f"DEBUG: Groups = {list(groups.keys())}")

        return jsonify({
            'success': True,
            'groups': groups,
            'total': len(rows)
        })

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/stocks', methods=['GET'])
def get_stocks():
    """
    Возвращает список акций (без OI)
    """
    try:
        query = text("""
            SELECT DISTINCT sectype, name
            FROM instruments
            WHERE type = 'stock'
            ORDER BY name
        """)

        with engine.connect() as conn:
            result = conn.execute(query)
            stocks = [{'sectype': row[0], 'name': row[1]} for row in result.fetchall()]

        return jsonify({
            'success': True,
            'stocks': stocks
        })

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# =============================================================================
# 📊 ДОСТУПНЫЕ ДАННЫЕ
# =============================================================================

@app.route('/api/available-data', methods=['POST'])
def get_available_data():
    """
    Возвращает доступные таймфреймы и периоды для инструмента
    """
    try:
        data = request.json
        sectype = data.get('sectype', 'IMOEXF')

        # Проверяем OI
        oi_query = text("""
            SELECT 
                interval,
                COUNT(*) as records,
                MIN(tradedate) as date_from, MAX(tradedate) as date_to
            FROM open_interest
            WHERE ticker = :sectype
            GROUP BY interval
            ORDER BY interval
        """)

        # Проверяем свечи
        candles_query = text("""
            SELECT 
                interval,
                COUNT(*) as records,
                MIN(begin_time)::date as date_from,
                MAX(begin_time)::date as date_to
            FROM candles_futures
            WHERE secid LIKE :pattern
            GROUP BY interval
            ORDER BY interval
        """)

        with engine.connect() as conn:
            # OI данные
            oi_result = conn.execute(oi_query, {"sectype": sectype})
            oi_intervals = {}
            for row in oi_result.fetchall():
                oi_intervals[row[0]] = {
                    'records': row[1],
                    'from': str(row[2]) if row[2] else None,
                    'to': str(row[3]) if row[3] else None
                }

            # Candles данные
            candles_result = conn.execute(candles_query, {"pattern": f"{sectype}%"})
            candles_intervals = {}
            for row in candles_result.fetchall():
                candles_intervals[row[0]] = {
                    'records': row[1],
                    'from': str(row[2]) if row[2] else None,
                    'to': str(row[3]) if row[3] else None
                }

        # Формируем доступные таймфреймы
        available_timeframes = []

        interval_names = {5: '5 мин', 60: '1 час', 24: '1 день'}

        for interval in [5, 60, 24]:
            has_oi = interval in oi_intervals
            has_candles = interval in candles_intervals

            if has_oi or has_candles:
                available_timeframes.append({
                    'value': interval,
                    'label': interval_names.get(interval, str(interval)),
                    'has_oi': has_oi,
                    'has_candles': has_candles,
                    'oi_data': oi_intervals.get(interval),
                    'candles_data': candles_intervals.get(interval)
                })

        # Общий диапазон дат
        all_dates = []
        for data in list(oi_intervals.values()) + list(candles_intervals.values()):
            if data.get('from'):
                all_dates.append(data['from'])
            if data.get('to'):
                all_dates.append(data['to'])

        date_range = {
            'from': min(all_dates) if all_dates else None,
            'to': max(all_dates) if all_dates else None
        }

        return jsonify({
            'success': True,
            'sectype': sectype,
            'timeframes': available_timeframes,
            'date_range': date_range,
            'oi_intervals': oi_intervals,
            'candles_intervals': candles_intervals
        })

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# =============================================================================
# 📈 ОТКРЫТЫЙ ИНТЕРЕС
# =============================================================================

@app.route('/api/open-interest', methods=['POST'])
def get_open_interest():
    """
    Возвращает данные открытого интереса

    Параметры:
    - sectype: код инструмента (IMOEXF, Si, BR...)
    - interval: 5, 60, 24
    - start_date: начало периода
    - end_date: конец периода
    - clgroup: FIZ, YUR или ALL
    - data_type: positions, traders, net_position, open_interest
    """
    try:
        data = request.json
        sectype = data.get('sectype', 'IMOEXF')
        interval = data.get('interval', 24)
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        clgroup = data.get('clgroup', 'ALL')  # FIZ, YUR, ALL
        data_type = data.get('data_type', 'positions')  # positions, traders, net_position, open_interest

        # Строим запрос
        clgroup_filter = ""
        if clgroup != 'ALL':
            clgroup_filter = "AND clgroup = :clgroup"

        query = text(f"""
            SELECT 
                tradedate,
                tradetime,
                clgroup,
                COALESCE(pos, 0) as pos,
                COALESCE(pos_long, 0) as pos_long,
                COALESCE(pos_short, 0) as pos_short,
                COALESCE(pos_long_num, 0) as pos_long_num,
                COALESCE(pos_short_num, 0) as pos_short_num
            FROM open_interest
            WHERE ticker = :sectype
              AND interval = :interval
              AND tradedate BETWEEN :start_date AND :end_date
              {clgroup_filter}
            ORDER BY tradedate, tradetime, clgroup
        """)

        params = {
            "sectype": sectype,
            "interval": interval,
            "start_date": start_date,
            "end_date": end_date
        }
        if clgroup != 'ALL':
            params["clgroup"] = clgroup

        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params=params)

        if df.empty:
            return jsonify({
                'success': True,
                'data': [],
                'message': 'Нет данных для выбранных параметров'
            })

        # Обрабатываем данные в зависимости от типа
        result_data = process_oi_data(df, data_type, clgroup)

        return jsonify({
            'success': True,
            'data': result_data,
            'sectype': sectype,
            'interval': interval,
            'clgroup': clgroup,
            'data_type': data_type,
            'records': len(result_data)
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)}), 500


def process_oi_data(df, data_type, clgroup):
    """
    Обрабатывает данные OI в нужный формат
    """
    # Создаём datetime колонку
    df['datetime'] = pd.to_datetime(df['tradedate'].astype(str) + ' ' + df['tradetime'].astype(str))

    result = []

    if clgroup == 'ALL':
        # Группируем по datetime и агрегируем FIZ + YUR
        grouped = df.groupby('datetime').agg({
            'pos': 'sum',
            'pos_long': 'sum',
            'pos_short': 'sum',
            'pos_long_num': 'sum',
            'pos_short_num': 'sum'
        }).reset_index()

        for _, row in grouped.iterrows():
            item = build_oi_item(row, data_type)
            if item:
                result.append(item)
    else:
        # Данные уже отфильтрованы по clgroup
        for _, row in df.iterrows():
            item = build_oi_item(row, data_type)
            if item:
                result.append(item)

    return result


def build_oi_item(row, data_type):
    """
    Формирует элемент данных в зависимости от типа
    """
    dt = row['datetime'] if 'datetime' in row else row.name
    dt_str = dt.strftime('%Y-%m-%d %H:%M') if hasattr(dt, 'strftime') else str(dt)

    pos_long = float(row['pos_long']) if pd.notna(row['pos_long']) else 0
    pos_short = float(row['pos_short']) if pd.notna(row['pos_short']) else 0
    pos_long_num = int(row['pos_long_num']) if pd.notna(row['pos_long_num']) else 0
    pos_short_num = int(row['pos_short_num']) if pd.notna(row['pos_short_num']) else 0

    # Чистая позиция
    net_position = pos_long + pos_short  # pos_short уже отрицательный

    # Открытый интерес = сумма абсолютных значений
    open_interest = abs(pos_long) + abs(pos_short)

    item = {'datetime': dt_str}

    if data_type == 'positions':
        item['long'] = pos_long
        item['short'] = pos_short
        item['net'] = net_position
        item['total'] = open_interest

    elif data_type == 'traders':
        item['long'] = pos_long_num
        item['short'] = pos_short_num
        item['net'] = pos_long_num - pos_short_num
        item['total'] = pos_long_num + pos_short_num

    elif data_type == 'net_position':
        item['value'] = net_position

    elif data_type == 'open_interest':
        item['value'] = open_interest

    elif data_type == 'long':
        item['value'] = pos_long

    elif data_type == 'short':
        item['value'] = pos_short

    elif data_type == 'long_traders':
        item['value'] = pos_long_num

    elif data_type == 'short_traders':
        item['value'] = pos_short_num

    return item


# =============================================================================
# 🕯️ СВЕЧИ
# =============================================================================

@app.route('/api/candles', methods=['POST'])
def get_candles():
    """
    Возвращает данные свечей для инструмента
    """
    try:
        data = request.json
        sectype = data.get('sectype', 'IMOEXF')
        interval = data.get('interval', 24)
        start_date = data.get('start_date')
        end_date = data.get('end_date')

        query = text("""
            SELECT 
                begin_time,
                COALESCE(open, close) as open,
                COALESCE(high, GREATEST(open, close)) as high,
                COALESCE(low, LEAST(open, close)) as low,
                close,
                COALESCE(volume, 0) as volume,
                secid
            FROM candles_futures
            WHERE secid LIKE :pattern
              AND interval = :interval
              AND begin_time BETWEEN :start_date AND :end_date
            ORDER BY begin_time
        """)

        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={
                "pattern": f"{sectype}%",
                "interval": interval,
                "start_date": start_date,
                "end_date": end_date
            })

        if df.empty:
            return jsonify({
                'success': True,
                'data': [],
                'message': 'Нет данных для выбранных параметров'
            })

        # Формируем результат
        result = []
        for _, row in df.iterrows():
            result.append({
                'datetime': row['begin_time'].strftime('%Y-%m-%d %H:%M'),
                'open': float(row['open']) if pd.notna(row['open']) else None,
                'high': float(row['high']) if pd.notna(row['high']) else None,
                'low': float(row['low']) if pd.notna(row['low']) else None,
                'close': float(row['close']) if pd.notna(row['close']) else None,
                'volume': float(row['volume']) if pd.notna(row['volume']) else 0,
                'secid': row['secid']
            })

        return jsonify({
            'success': True,
            'data': result,
            'sectype': sectype,
            'interval': interval,
            'records': len(result)
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)}), 500


# =============================================================================
# 📊 КОМБИНИРОВАННЫЕ ДАННЫЕ (OI + Свечи)
# =============================================================================

@app.route('/api/combined-data', methods=['POST'])
def get_combined_data():
    """
    Возвращает объединённые данные OI и свечей для одного графика
    """
    try:
        data = request.json
        sectype = data.get('sectype', 'IMOEXF')
        interval = data.get('interval', 24)
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        clgroup = data.get('clgroup', 'ALL')

        # Получаем OI
        oi_query = text("""
            SELECT 
                tradedate,
                tradetime,
                clgroup,
                COALESCE(pos_long, 0) as pos_long,
                COALESCE(pos_short, 0) as pos_short,
                COALESCE(pos_long_num, 0) as pos_long_num,
                COALESCE(pos_short_num, 0) as pos_short_num
            FROM open_interest
            WHERE ticker = :sectype
              AND interval = :interval
              AND tradedate BETWEEN :start_date AND :end_date
            ORDER BY tradedate, tradetime
        """)

        # Получаем свечи
        candles_query = text("""
            SELECT 
                begin_time,
                COALESCE(open, close) as open,
                COALESCE(high, GREATEST(open, close)) as high,
                COALESCE(low, LEAST(open, close)) as low,
                close,
                COALESCE(volume, 0) as volume
            FROM candles_futures
            WHERE secid LIKE :pattern
              AND interval = :interval
              AND begin_time BETWEEN :start_date AND :end_date
            ORDER BY begin_time
        """)

        with engine.connect() as conn:
            oi_df = pd.read_sql(oi_query, conn, params={
                "sectype": sectype,
                "interval": interval,
                "start_date": start_date,
                "end_date": end_date
            })

            candles_df = pd.read_sql(candles_query, conn, params={
                "pattern": f"{sectype}%",
                "interval": interval,
                "start_date": start_date,
                "end_date": end_date
            })

        # Обрабатываем OI
        oi_data = {}
        if not oi_df.empty:
            oi_df['datetime'] = pd.to_datetime(
                oi_df['tradedate'].astype(str) + ' ' + oi_df['tradetime'].astype(str)
            )

            # Фильтруем по clgroup если нужно
            if clgroup != 'ALL':
                oi_df = oi_df[oi_df['clgroup'] == clgroup]

            # Группируем
            grouped = oi_df.groupby('datetime').agg({
                'pos_long': 'sum',
                'pos_short': 'sum',
                'pos_long_num': 'sum',
                'pos_short_num': 'sum'
            }).reset_index()

            for _, row in grouped.iterrows():
                dt_str = row['datetime'].strftime('%Y-%m-%d %H:%M')
                oi_data[dt_str] = {
                    'pos_long': float(row['pos_long']),
                    'pos_short': float(row['pos_short']),
                    'pos_long_num': int(row['pos_long_num']),
                    'pos_short_num': int(row['pos_short_num'])
                }

        # Обрабатываем свечи
        candles_data = {}
        if not candles_df.empty:
            for _, row in candles_df.iterrows():
                dt_str = row['begin_time'].strftime('%Y-%m-%d %H:%M')
                candles_data[dt_str] = {
                    'open': float(row['open']) if pd.notna(row['open']) else None,
                    'high': float(row['high']) if pd.notna(row['high']) else None,
                    'low': float(row['low']) if pd.notna(row['low']) else None,
                    'close': float(row['close']) if pd.notna(row['close']) else None,
                    'volume': float(row['volume']) if pd.notna(row['volume']) else 0
                }

        # Объединяем по datetime
        all_datetimes = sorted(set(list(oi_data.keys()) + list(candles_data.keys())))

        result = []
        for dt in all_datetimes:
            item = {'datetime': dt}

            # Добавляем OI если есть
            if dt in oi_data:
                oi = oi_data[dt]
                item['pos_long'] = oi['pos_long']
                item['pos_short'] = oi['pos_short']
                item['net_position'] = oi['pos_long'] + oi['pos_short']
                item['open_interest'] = abs(oi['pos_long']) + abs(oi['pos_short'])
                item['traders_long'] = oi['pos_long_num']
                item['traders_short'] = oi['pos_short_num']
                item['traders_net'] = oi['pos_long_num'] - oi['pos_short_num']

            # Добавляем свечи если есть
            if dt in candles_data:
                candle = candles_data[dt]
                item['open'] = candle['open']
                item['high'] = candle['high']
                item['low'] = candle['low']
                item['close'] = candle['close']
                item['volume'] = candle['volume']

            result.append(item)

        return jsonify({
            'success': True,
            'data': result,
            'sectype': sectype,
            'interval': interval,
            'clgroup': clgroup,
            'has_oi': len(oi_data) > 0,
            'has_candles': len(candles_data) > 0,
            'records': len(result)
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)}), 500


# =============================================================================
# 🏥 СЛУЖЕБНЫЕ
# =============================================================================

@app.route('/api/health', methods=['GET'])
def health_check():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return jsonify({'success': True, 'status': 'OK'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Общая статистика по БД"""
    try:
        with engine.connect() as conn:
            # Инструменты
            instruments = conn.execute(text(
                "SELECT COUNT(DISTINCT sectype) FROM instruments WHERE type = 'futures'"
            )).fetchone()[0]

            # Свечи
            candles = conn.execute(text(
                "SELECT COUNT(*) FROM candles_futures"
            )).fetchone()[0]

            # OI
            oi = conn.execute(text(
                "SELECT COUNT(*) FROM open_interest"
            )).fetchone()[0]

        return jsonify({
            'success': True,
            'instruments': instruments,
            'candles': candles,
            'open_interest': oi
        })

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/')
def serve_frontend():
    return send_from_directory('static', 'index.html')


@app.route('/static/<path:path>')
def serve_static(path):
    return send_from_directory('static', path)


if __name__ == '__main__':
    print("🚀 Запуск Flask сервера...")
    print(f"📊 БД: {DB_URL}")

    try:
        with engine.connect() as conn:
            oi_count = conn.execute(text("SELECT COUNT(*) FROM open_interest")).fetchone()[0]
            candles_count = conn.execute(text("SELECT COUNT(*) FROM candles_futures")).fetchone()[0]
            print(f"✅ БД подключена! OI: {oi_count:,}, Свечи: {candles_count:,}")
    except Exception as e:
        print(f"❌ Ошибка: {e}")

    app.run(debug=True, host='0.0.0.0', port=5000)