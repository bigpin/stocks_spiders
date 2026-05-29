const cloud = require('wx-server-sdk')

cloud.init({ env: 'cloudbase-4g6zx8vx290da64e' })
const db = cloud.database()
const _ = db.command
const aggr = db.command.aggregate

// ============ Handler: /api/stats ============
async function handleStats() {
  const countRes = await db.collection('web_signals').count()
  const total_signals = countRes.total

  // 用聚合求 AVG 和 DISTINCT
  const aggRes = await db.collection('web_signals')
    .aggregate()
    .group({
      _id: null,
      avg_success_rate: aggr.avg('$overall_success_rate'),
      avg_highest_change: aggr.avg({
        $cond: [
          { $ne: ['$highest_change_rate', null] },
          '$highest_change_rate',
          '$$REMOVE'
        ]
      }),
      stock_codes: aggr.addToSet('$stock_code')
    })
    .end()

  const agg = aggRes.list[0] || {}
  return {
    total_signals,
    avg_success_rate: agg.avg_success_rate != null ? Math.round(agg.avg_success_rate * 100) / 100 : 0,
    total_stocks: agg.stock_codes ? agg.stock_codes.length : 0,
    avg_highest_change: agg.avg_highest_change != null ? Math.round(agg.avg_highest_change * 100) / 100 : 0
  }
}

// ============ Handler: /api/stock-codes ============
async function handleStockCodes() {
  const res = await db.collection('web_signals')
    .field({ stock_code: true, stock_name: true })
    .limit(1000)
    .get()

  const seen = new Set()
  const stock_codes = []
  for (const doc of res.data) {
    if (doc.stock_code && !seen.has(doc.stock_code)) {
      seen.add(doc.stock_code)
      stock_codes.push({ code: doc.stock_code, name: doc.stock_name || '' })
    }
  }
  stock_codes.sort((a, b) => a.code.localeCompare(b.code))
  return { stock_codes }
}

// ============ Handler: /api/calendar/events ============
async function handleCalendarEvents(qs) {
  const whereObj = {}
  if (qs.stock_code) {
    whereObj.stock_code = db.RegExp({ regexp: qs.stock_code, options: 'i' })
  }
  if (qs.date_from && qs.date_to) {
    whereObj.insert_date = _.and(_.gte(qs.date_from), _.lte(qs.date_to))
  } else if (qs.date_from) {
    whereObj.insert_date = _.gte(qs.date_from)
  } else if (qs.date_to) {
    whereObj.insert_date = _.lte(qs.date_to)
  }
  if (qs.heat_min && qs.heat_max) {
    whereObj.trade_heat_score = _.and(_.gte(parseFloat(qs.heat_min)), _.lte(parseFloat(qs.heat_max)))
  } else if (qs.heat_min) {
    whereObj.trade_heat_score = _.gte(parseFloat(qs.heat_min))
  } else if (qs.heat_max) {
    whereObj.trade_heat_score = _.lte(parseFloat(qs.heat_max))
  }

  const res = await db.collection('web_signals')
    .where(whereObj)
    .orderBy('insert_date', 'desc')
    .limit(500)
    .get()

  // 云数据库返回 _id，前端需要 id
  const events = res.data.map(doc => {
    const { _id, ...rest } = doc
    return { id: _id, ...rest }
  })
  return { events }
}

// ============ Handler: /api/signals ============
async function handleSignals(qs) {
  const sort_by = qs.sort_by || 'created_at'
  const order = qs.order || 'desc'
  const page = parseInt(qs.page) || 1
  const per_page = parseInt(qs.per_page) || 20

  const validSortColumns = [
    'created_at', 'stock_code', 'stock_name', 'overall_success_rate',
    'signal_count', 'insert_date', 'insert_price', 'highest_change_rate',
    'highest_price_date', 'highest_days', 'lowest_change_rate',
    'lowest_price_date', 'lowest_days', 'buy_day_change_rate', 'next_day_change_rate'
  ]
  const safeSortBy = validSortColumns.includes(sort_by) ? sort_by : 'created_at'
  const safeOrder = order === 'asc' ? 'asc' : 'desc'

  // 构建 where
  const whereObj = {}
  if (qs.stock_code) {
    whereObj.stock_code = db.RegExp({ regexp: qs.stock_code, options: 'i' })
  }
  if (qs.stock_name) {
    whereObj.stock_name = db.RegExp({ regexp: qs.stock_name, options: 'i' })
  }
  if (qs.signal_type) {
    whereObj.signal = db.RegExp({ regexp: qs.signal_type, options: 'i' })
  }
  if (qs.min_success_rate) {
    whereObj.overall_success_rate = _.gte(parseFloat(qs.min_success_rate))
  }
  if (qs.min_signal_count) {
    whereObj.signal_count = _.gte(parseInt(qs.min_signal_count))
  }
  if (qs.date_from && qs.date_to) {
    whereObj.insert_date = _.and(_.gte(qs.date_from), _.lte(qs.date_to))
  } else if (qs.date_from) {
    whereObj.insert_date = _.gte(qs.date_from)
  } else if (qs.date_to) {
    whereObj.insert_date = _.lte(qs.date_to)
  }

  const countRes = await db.collection('web_signals').where(whereObj).count()
  const total = countRes.total

  const skip = (page - 1) * per_page
  const res = await db.collection('web_signals')
    .where(whereObj)
    .orderBy(safeSortBy, safeOrder)
    .skip(skip)
    .limit(per_page)
    .get()

  const signals = res.data.map(doc => {
    const { _id, ...rest } = doc
    return { id: _id, ...rest }
  })
  return {
    signals,
    total,
    page,
    per_page,
    total_pages: Math.ceil(total / per_page)
  }
}

// ============ Handler: /api/filter-options ============
async function handleFilterOptions() {
  const res = await db.collection('web_signals')
    .field({ stock_code: true, stock_name: true, signal: true })
    .limit(1000)
    .get()

  const codeSeen = new Set()
  const nameSeen = new Set()
  const signalTypes = new Set()
  const stock_codes = []
  const stock_names = []

  for (const doc of res.data) {
    if (doc.stock_code && !codeSeen.has(doc.stock_code)) {
      codeSeen.add(doc.stock_code)
      stock_codes.push({ code: doc.stock_code, name: doc.stock_name || '' })
    }
    if (doc.stock_name && !nameSeen.has(doc.stock_name)) {
      nameSeen.add(doc.stock_name)
      stock_names.push(doc.stock_name)
    }
    if (doc.signal) {
      const signals = doc.signal.split(',').map(s => s.trim()).filter(Boolean)
      signals.forEach(s => signalTypes.add(s))
    }
  }

  stock_codes.sort((a, b) => a.code.localeCompare(b.code))
  stock_names.sort()

  return {
    stock_codes,
    stock_names,
    signal_types: [...signalTypes].sort()
  }
}

// ============ Handler: /api/signal-daily-prices ============
async function handleSignalDailyPrices(qs) {
  if (qs.signal_id) {
    const res = await db.collection('web_daily_prices')
      .where({ signal_id: parseInt(qs.signal_id) })
      .orderBy('days_from_signal', 'asc')
      .limit(100)
      .get()
    const prices = res.data.map(({ _id, ...rest }) => rest)
    return { prices }
  }

  if (qs.stock_code && qs.insert_date) {
    // 先查 signal 的 _id（同步时用 SQLite id 作为 _id）
    const sigRes = await db.collection('web_signals')
      .where({ stock_code: qs.stock_code, insert_date: qs.insert_date })
      .limit(1)
      .get()

    if (!sigRes.data.length) return { prices: [] }
    const signalId = parseInt(sigRes.data[0]._id)

    const priceRes = await db.collection('web_daily_prices')
      .where({ signal_id: signalId })
      .orderBy('days_from_signal', 'asc')
      .limit(100)
      .get()
    const prices = priceRes.data.map(({ _id, ...rest }) => rest)
    return { prices }
  }

  return { error: '需要提供 signal_id 或 (stock_code + insert_date)' }
}

// ============ Router ============
exports.main = async (event, context) => {
  const path = (event.$url && event.$url.path) || event.path || ''
  const qs = (event.$url && event.$url.queryStringObject) || event.queryStringObject || event.queryStringParameters || {}

  try {
    let result
    // 去掉前缀 /api（HTTP 触发器路径可能是 /api/stats 或 /stats）
    const route = path.replace(/^\/api/, '') || path

    switch (route) {
      case '/stats':
        result = await handleStats()
        break
      case '/stock-codes':
        result = await handleStockCodes()
        break
      case '/calendar/events':
        result = await handleCalendarEvents(qs)
        break
      case '/signals':
        result = await handleSignals(qs)
        break
      case '/filter-options':
        result = await handleFilterOptions()
        break
      case '/signal-daily-prices':
        result = await handleSignalDailyPrices(qs)
        break
      default:
        return { statusCode: 404, body: { error: `Unknown path: ${path}` } }
    }

    return { statusCode: 200, body: result }
  } catch (err) {
    console.error('Cloud function error:', err)
    return { statusCode: 500, body: { error: err.message } }
  }
}
