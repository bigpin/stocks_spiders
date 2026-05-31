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
  const aggRes = await db.collection('web_signals')
    .aggregate()
    .group({
      _id: '$stock_code',
      name: aggr.first('$stock_name')
    })
    .sort({ _id: 1 })
    .end()

  const stock_codes = aggRes.list.map(doc => ({
    code: doc._id,
    name: doc.name || ''
  }))
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

  const page = parseInt(qs.page) || 1
  const perPage = parseInt(qs.per_page) || 200
  const skip = (page - 1) * perPage

  const countRes = await db.collection('web_signals').where(whereObj).count()
  const total = countRes.total

  const res = await db.collection('web_signals')
    .where(whereObj)
    .orderBy('insert_date', 'desc')
    .skip(skip)
    .limit(perPage)
    .get()

  const events = res.data.map(doc => {
    const { _id, ...rest } = doc
    return { id: _id, ...rest }
  })
  return {
    events, total, page, per_page: perPage,
    total_pages: Math.ceil(total / perPage),
  }
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
  const [codesRes, namesRes, signalsRes] = await Promise.all([
    db.collection('web_signals').aggregate()
      .group({ _id: '$stock_code', name: aggr.first('$stock_name') })
      .sort({ _id: 1 })
      .end(),
    db.collection('web_signals').aggregate()
      .group({ _id: '$stock_name' })
      .match({ _id: aggr.neq(null) })
      .sort({ _id: 1 })
      .end(),
    db.collection('web_signals').aggregate()
      .match({ signal: aggr.neq(null) })
      .project({ signals: { $split: ['$signal', ','] } })
      .unwind('$signals')
      .group({ _id: { $trim: { input: '$signals' } } })
      .match({ _id: aggr.neq('') })
      .sort({ _id: 1 })
      .end()
  ])

  const stock_codes = codesRes.list.map(doc => ({
    code: doc._id,
    name: doc.name || ''
  }))
  const stock_names = namesRes.list.map(doc => doc._id)
  const signal_types = signalsRes.list.map(doc => doc._id)

  return { stock_codes, stock_names, signal_types }
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

// ============ Handler: /api/signal-daily-prices-batch ============
async function handleSignalDailyPricesBatch(qs) {
  const idsRaw = qs.signal_ids || ''
  if (!idsRaw) return { error: '需要提供 signal_ids 参数（逗号分隔）' }
  const signalIds = idsRaw.split(',').map(s => parseInt(s.trim())).filter(n => !isNaN(n))
  if (!signalIds.length) return {}

  // 云数据库 where in 一次最多查 500 条，分批
  const batchSize = 500
  const result = {}
  for (let i = 0; i < signalIds.length; i += batchSize) {
    const batch = signalIds.slice(i, i + batchSize)
    const res = await db.collection('web_daily_prices')
      .where({ signal_id: _.in(batch) })
      .orderBy('signal_id', 'asc')
      .orderBy('days_from_signal', 'asc')
      .limit(100 * batch.length)
      .get()
    for (const doc of res.data) {
      const { _id, signal_id, ...rest } = doc
      const key = String(signal_id)
      if (!result[key]) result[key] = []
      result[key].push(rest)
    }
  }
  return result
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
      case '/signal-daily-prices-batch':
        result = await handleSignalDailyPricesBatch(qs)
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
