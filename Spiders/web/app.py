from flask import Flask, render_template, request, jsonify
import sqlite3
import os
from datetime import datetime

app = Flask(__name__)

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'stock_signals.db')

def migrate_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute('ALTER TABLE stock_signals ADD COLUMN buy_day_change_rate REAL')
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute('ALTER TABLE stock_signals ADD COLUMN next_day_change_rate REAL')
    except sqlite3.OperationalError:
        pass
    conn.commit()
    conn.close()

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def index():
    return render_template('calendar.html')

@app.route('/list')
def list_view():
    return render_template('index.html')

@app.route('/api/signals')
def get_signals():
    conn = get_db_connection()
    
    stock_code = request.args.get('stock_code', '')
    stock_name = request.args.get('stock_name', '')
    signal_type = request.args.get('signal_type', '')
    min_success_rate = request.args.get('min_success_rate', '')
    min_signal_count = request.args.get('min_signal_count', '')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    sort_by = request.args.get('sort_by', 'created_at')
    order = request.args.get('order', 'desc')
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 20))
    
    query = "SELECT * FROM stock_signals WHERE 1=1"
    params = []
    
    if stock_code:
        query += " AND stock_code LIKE ?"
        params.append(f"%{stock_code}%")
    
    if stock_name:
        query += " AND stock_name LIKE ?"
        params.append(f"%{stock_name}%")
    
    if signal_type:
        query += " AND signal LIKE ?"
        params.append(f"%{signal_type}%")
    
    if min_success_rate:
        query += " AND overall_success_rate >= ?"
        params.append(float(min_success_rate))
    
    if min_signal_count:
        query += " AND signal_count >= ?"
        params.append(int(min_signal_count))
    
    if date_from:
        query += " AND insert_date >= ?"
        params.append(date_from)
    
    if date_to:
        query += " AND insert_date <= ?"
        params.append(date_to)
    
    valid_sort_columns = [
        'created_at', 'stock_code', 'stock_name', 'overall_success_rate', 
        'signal_count', 'insert_date', 'insert_price', 'highest_change_rate', 
        'highest_price_date', 'highest_days', 'lowest_change_rate', 
        'lowest_price_date', 'lowest_days', 'buy_day_change_rate', 'next_day_change_rate'
    ]
    if sort_by not in valid_sort_columns:
        sort_by = 'created_at'
    
    valid_orders = ['asc', 'desc']
    if order not in valid_orders:
        order = 'desc'
    
    # 先计算总数（使用COUNT，不需要ORDER BY）
    count_query = query.replace("SELECT *", "SELECT COUNT(*)")
    cursor = conn.cursor()
    cursor.execute(count_query, params)
    total = cursor.fetchone()[0]
    
    # 然后执行分页查询
    query += f" ORDER BY {sort_by} {order.upper()}"
    query += " LIMIT ? OFFSET ?"
    params.extend([per_page, (page - 1) * per_page])
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    
    signals = []
    for row in rows:
        signal = dict(row)
        signals.append(signal)
    
    conn.close()
    
    return jsonify({
        'signals': signals,
        'total': total,
        'page': page,
        'per_page': per_page,
        'total_pages': (total + per_page - 1) // per_page
    })

@app.route('/api/stats')
def get_stats():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) as total FROM stock_signals")
    total_signals = cursor.fetchone()['total']
    
    cursor.execute("SELECT AVG(overall_success_rate) as avg_rate FROM stock_signals")
    avg_rate = cursor.fetchone()['avg_rate'] or 0
    
    cursor.execute("SELECT COUNT(DISTINCT stock_code) as total_stocks FROM stock_signals")
    total_stocks = cursor.fetchone()['total_stocks']
    
    cursor.execute("SELECT AVG(highest_change_rate) as avg_highest FROM stock_signals WHERE highest_change_rate IS NOT NULL")
    avg_highest = cursor.fetchone()['avg_highest'] or 0
    
    conn.close()
    
    return jsonify({
        'total_signals': total_signals,
        'avg_success_rate': round(avg_rate, 2),
        'total_stocks': total_stocks,
        'avg_highest_change': round(avg_highest, 2)
    })

@app.route('/api/calendar/events')
def get_calendar_events():
    conn = get_db_connection()
    cursor = conn.cursor()
    stock_code = request.args.get('stock_code', '')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    query = """
        SELECT id, stock_code, stock_name, insert_date, insert_price,
               highest_price, highest_price_date, highest_change_rate, highest_days,
               lowest_price, lowest_price_date, lowest_change_rate, lowest_days,
               buy_day_change_rate, next_day_change_rate
        FROM stock_signals
        WHERE 1=1
    """
    params = []
    if stock_code:
        query += " AND stock_code LIKE ?"
        params.append(f"%{stock_code}%")
    if date_from:
        query += " AND date(insert_date) >= date(?)"
        params.append(date_from)
    if date_to:
        query += " AND date(insert_date) <= date(?)"
        params.append(date_to)
    query += " ORDER BY insert_date DESC"
    cursor.execute(query, params)
    rows = cursor.fetchall()
    events = []
    for row in rows:
        signal_id = row[0]
        insert_date = row[3]
        insert_price = row[4]
        highest_price = row[5]
        highest_price_date = row[6]
        highest_change_rate = row[7]
        highest_days = row[8]
        lowest_price = row[9]
        lowest_price_date = row[10]
        lowest_change_rate = row[11]
        lowest_days = row[12]
        buy_day_change_rate = row[13] if len(row) > 13 else None
        next_day_change_rate = row[14] if len(row) > 14 else None
        events.append({
            'id': signal_id,
            'stock_code': row[1],
            'stock_name': row[2],
            'insert_date': insert_date,
            'insert_price': insert_price,
            'highest_price': highest_price,
            'highest_price_date': highest_price_date,
            'highest_change_rate': highest_change_rate,
            'highest_days': highest_days,
            'lowest_price': lowest_price,
            'lowest_price_date': lowest_price_date,
            'lowest_change_rate': lowest_change_rate,
            'lowest_days': lowest_days,
            'buy_day_change_rate': buy_day_change_rate,
            'next_day_change_rate': next_day_change_rate
        })
    conn.close()
    return jsonify({'events': events})

@app.route('/api/stock-codes')
def get_stock_codes():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT stock_code, stock_name FROM stock_signals ORDER BY stock_code")
    rows = cursor.fetchall()
    stock_codes = []
    for row in rows:
        stock_codes.append({
            'code': row[0],
            'name': row[1] if row[1] else ''
        })
    conn.close()
    return jsonify({'stock_codes': stock_codes})

@app.route('/api/filter-options')
def get_filter_options():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 获取所有唯一的股票代码和名称
    cursor.execute("SELECT DISTINCT stock_code, stock_name FROM stock_signals WHERE stock_code IS NOT NULL AND stock_code != '' ORDER BY stock_code")
    stock_rows = cursor.fetchall()
    stock_codes = []
    for row in stock_rows:
        stock_codes.append({
            'code': row[0],
            'name': row[1] if row[1] else ''
        })
    
    # 获取所有唯一的股票名称
    cursor.execute("SELECT DISTINCT stock_name FROM stock_signals WHERE stock_name IS NOT NULL AND stock_name != '' ORDER BY stock_name")
    stock_names = [row[0] for row in cursor.fetchall()]
    
    # 获取所有唯一的信号类型（从signal字段中提取）
    cursor.execute("SELECT DISTINCT signal FROM stock_signals WHERE signal IS NOT NULL AND signal != ''")
    signal_rows = cursor.fetchall()
    signal_types = set()
    for row in signal_rows:
        if row[0]:
            # 信号可能是逗号分隔的多个信号
            signals = [s.strip() for s in row[0].split(',') if s.strip()]
            signal_types.update(signals)
    signal_types = sorted(list(signal_types))
    
    conn.close()
    return jsonify({
        'stock_codes': stock_codes,
        'stock_names': stock_names,
        'signal_types': signal_types
    })

@app.route('/api/signal-daily-prices')
def get_signal_daily_prices():
    """获取信号的每日价格数据"""
    signal_id = request.args.get('signal_id', type=int)
    stock_code = request.args.get('stock_code', '')
    insert_date = request.args.get('insert_date', '')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if signal_id:
        # 通过 signal_id 查询
        cursor.execute('''
            SELECT date, open, high, low, close, days_from_signal
            FROM stock_signal_daily_prices
            WHERE signal_id = ?
            ORDER BY days_from_signal ASC
        ''', (signal_id,))
    elif stock_code and insert_date:
        # 通过 stock_code 和 insert_date 查找 signal_id，然后查询价格数据
        cursor.execute('''
            SELECT p.date, p.open, p.high, p.low, p.close, p.days_from_signal
            FROM stock_signal_daily_prices p
            JOIN stock_signals s ON p.signal_id = s.id
            WHERE s.stock_code = ? AND s.insert_date = ?
            ORDER BY p.days_from_signal ASC
        ''', (stock_code, insert_date))
    else:
        conn.close()
        return jsonify({'error': '需要提供 signal_id 或 (stock_code + insert_date)'}), 400
    
    rows = cursor.fetchall()
    prices = []
    for row in rows:
        prices.append({
            'date': row[0],
            'open': row[1],
            'high': row[2],
            'low': row[3],
            'close': row[4],
            'days_from_signal': row[5]
        })
    
    conn.close()
    return jsonify({'prices': prices})

if __name__ == '__main__':
    migrate_database()
    app.run(debug=True, host='0.0.0.0', port=5001)

