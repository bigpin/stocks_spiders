// API 基础地址。空字符串 = 本地 Flask；部署时改为云函数 HTTP 触发器地址。
const API_BASE = "";

let records = [];
let selectedGroupId = null;
let selectedRecord = null;
let tooltipEl = null;
let tooltipHideTimer = null;
let tooltipHovered = false;
let dateRange = { start: null, end: null };
let pixelsPerDay = 2;
let minPixelsPerDay = 0.5;
let maxPixelsPerDay = 20;

function switchTab(tabId) {
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.tab === tabId);
    });
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.toggle('active', content.id === 'tab-' + tabId);
    });
    document.getElementById('timeline-controls').style.display = tabId === 'timeline' ? 'block' : 'none';
    document.getElementById('matrix-controls').style.display = tabId === 'matrix' ? 'block' : 'none';
    document.getElementById('data-list-controls').style.display = tabId === 'data-list' ? 'block' : 'none';
    if (tabId === 'data-list') {
        loadDataListFilterOptions();
        loadDataList(1);
    } else if (tabId === 'timeline') {
        reloadTimeline();
    } else if (tabId === 'matrix') {
        loadMatrix();
    }
}

document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        switchTab(btn.dataset.tab);
    });
});

function createTooltip() {
    if (tooltipEl) return tooltipEl;
    const el = document.createElement("div");
    el.className = "tooltip";
    el.style.display = "none";
    el.addEventListener("mouseenter", function() {
        tooltipHovered = true;
        if (tooltipHideTimer) { clearTimeout(tooltipHideTimer); tooltipHideTimer = null; }
    });
    el.addEventListener("mouseleave", function() {
        tooltipHovered = false;
        hideTooltipDelayed();
    });
    document.body.appendChild(el);

    // 鼠标在tooltip区域内时启用pointer-events，允许交互
    document.addEventListener("mousemove", function(e) {
        if (!tooltipEl || tooltipEl.style.display === "none") return;
        const r = tooltipEl.getBoundingClientRect();
        const inside = e.clientX >= r.left && e.clientX <= r.right && e.clientY >= r.top && e.clientY <= r.bottom;
        tooltipEl.style.pointerEvents = inside ? "auto" : "none";
    });

    tooltipEl = el;
    return el;
}
function showTooltip(html, x, y) {
    if (tooltipHideTimer) { clearTimeout(tooltipHideTimer); tooltipHideTimer = null; }
    const el = createTooltip();
    el.innerHTML = html;
    el.style.display = "block";

    const padding = 12;
    const viewportWidth = window.innerWidth || document.documentElement.clientWidth;
    const viewportHeight = window.innerHeight || document.documentElement.clientHeight;

    // 先放到大致位置再测量尺寸
    let left = x + padding;
    let top = y + padding;
    el.style.left = left + "px";
    el.style.top = top + "px";

    const rect = el.getBoundingClientRect();

    // 水平方向：如果超出右侧，则放到鼠标左侧
    if (rect.right + 8 > viewportWidth) {
        left = x - rect.width - padding;
    }
    // 如果仍然超出左侧，则钉在左边缘
    if (left < 4) {
        left = 4;
    }

    // 垂直方向：如果超出下方，则放到鼠标上方
    if (rect.bottom + 8 > viewportHeight) {
        top = y - rect.height - padding;
    }
    // 如果仍然超出上方，则钉在上边缘
    if (top < 4) {
        top = 4;
    }

    el.style.left = left + "px";
    el.style.top = top + "px";
}
function hideTooltip() {
    if (tooltipHovered) return;
    if (tooltipEl) {
        tooltipEl.style.display = "none";
    }
}
function hideTooltipDelayed() {
    if (tooltipHideTimer) clearTimeout(tooltipHideTimer);
    tooltipHideTimer = setTimeout(function() {
        tooltipHideTimer = null;
        hideTooltip();
    }, 200);
}
function parseDate(str) {
    if (!str) return null;
    const s = str.split(" ")[0];
    const parts = s.split("-");
    if (parts.length === 3) {
        const y = parseInt(parts[0], 10);
        const m = parseInt(parts[1], 10) - 1;
        const d = parseInt(parts[2], 10);
        return new Date(y, m, d);
    }
    const p2 = s.split(".");
    if (p2.length === 3) {
        const y2 = parseInt(p2[0], 10);
        const m2 = parseInt(p2[1], 10) - 1;
        const d2 = parseInt(p2[2], 10);
        return new Date(y2, m2, d2);
    }
    return new Date(s);
}
function formatDate(date) {
    if (!date) return "-";
    const y = date.getFullYear();
    const m = String(date.getMonth() + 1).padStart(2, "0");
    const d = String(date.getDate()).padStart(2, "0");
    return y + "-" + m + "-" + d;
}
function formatDateShort(date) {
    if (!date) return "";
    const m = String(date.getMonth() + 1).padStart(2, "0");
    const d = String(date.getDate()).padStart(2, "0");
    return m + "-" + d;
}
function getDaysBetween(start, end) {
    return Math.round((end - start) / (24 * 60 * 60 * 1000));
}

function adjustColorBrightness(color, percent) {
    // 简单的颜色亮度调整函数
    if (!color || color.length < 7) return color;
    const num = parseInt(color.replace("#", ""), 16);
    const r = Math.max(0, Math.min(255, (num >> 16) + percent));
    const g = Math.max(0, Math.min(255, ((num >> 8) & 0x00FF) + percent));
    const b = Math.max(0, Math.min(255, (num & 0x0000FF) + percent));
    return "#" + ((1 << 24) + (r << 16) + (g << 8) + b).toString(16).slice(1);
}

function dateToY(date) {
    if (!dateRange.start || !dateRange.end || !date) return 0;
    // 反转时间轴：最新的日期在顶部，最旧的日期在底部
    const totalDays = getDaysBetween(dateRange.start, dateRange.end);
    const daysFromStart = getDaysBetween(dateRange.start, date);
    // 从总高度减去从开始到当前日期的距离，实现反转
    return (totalDays - daysFromStart) * pixelsPerDay;
}
function colorForChange(change, isHigh) {
    if (change == null) return isHigh ? "rgba(229,57,53,0.6)" : "rgba(67,160,71,0.6)";
    const v = Math.abs(change);
    const alpha = Math.min(0.25 + v / 80, 0.95);
    if (isHigh) {
        return "rgba(229,57,53," + alpha + ")";
    }
    return "rgba(67,160,71," + alpha + ")";
}
function colorForBuyPoint(buyDayChangeRate, nextDayChangeRate) {
    const hasBuyDay = buyDayChangeRate !== null && buyDayChangeRate !== undefined;
    const hasNextDay = nextDayChangeRate !== null && nextDayChangeRate !== undefined;
    if (!hasBuyDay && !hasNextDay) {
        return null;
    }
    if (hasBuyDay && hasNextDay) {
        if (buyDayChangeRate > 0 && nextDayChangeRate > 0) {
            return "#c62828";
        } else if (buyDayChangeRate < 0 && nextDayChangeRate < 0) {
            return "#2e7d32";
        } else if (buyDayChangeRate > 0 && nextDayChangeRate < 0) {
            return "#ff6f00";
        } else if (buyDayChangeRate < 0 && nextDayChangeRate > 0) {
            return "#66bb6a";
        } else {
            return "#757575";
        }
    } else if (hasBuyDay) {
        if (buyDayChangeRate > 0) {
            return "#e53935";
        } else if (buyDayChangeRate < 0) {
            return "#43a047";
        }
    } else if (hasNextDay) {
        if (nextDayChangeRate > 0) {
            return "#e53935";
        } else if (nextDayChangeRate < 0) {
            return "#43a047";
        }
    }
    return null;
}
function buildTooltip(rec, kind) {
    let title = "";
    if (kind === "high") {
        title = "最高价线";
    } else if (kind === "low") {
        title = "最低价线";
    } else if (kind === "buy") {
        title = "买入点";
    }
    let html = '<div class="tooltip-title">' + title + "</div>";
    html += '<div class="tooltip-row"><span class="tooltip-label">代码</span><span>' + (rec.stockCode || "-") + "</span></div>";
    if (document.getElementById("toggle-show-name").checked) {
        html += '<div class="tooltip-row"><span class="tooltip-label">名称</span><span>' + (rec.stockName || "-") + "</span></div>";
    }
    if (rec.buyDate) {
        html += '<div class="tooltip-row"><span class="tooltip-label">买入日</span><span>' + formatDate(rec.buyDate) + "</span></div>";
    }
    if (rec.buyPrice != null) {
        html += '<div class="tooltip-row"><span class="tooltip-label">买入价</span><span>' + rec.buyPrice.toFixed(2) + "</span></div>";
    }
    if (kind === "buy") {
        if (rec.buyDayChangeRate !== null && rec.buyDayChangeRate !== undefined) {
            const sign = rec.buyDayChangeRate > 0 ? "+" : "";
            const color = rec.buyDayChangeRate >= 0 ? "#e53935" : "#43a047";
            html += '<div class="tooltip-row"><span class="tooltip-label">买入当天涨跌</span><span style="color:' + color + ';">' + sign + rec.buyDayChangeRate.toFixed(2) + "%</span></div>";
        }
        if (rec.nextDayChangeRate !== null && rec.nextDayChangeRate !== undefined) {
            const sign2 = rec.nextDayChangeRate > 0 ? "+" : "";
            const color2 = rec.nextDayChangeRate >= 0 ? "#e53935" : "#43a047";
            html += '<div class="tooltip-row"><span class="tooltip-label">第二天涨跌</span><span style="color:' + color2 + ';">' + sign2 + rec.nextDayChangeRate.toFixed(2) + "%</span></div>";
        }
    }
    if (kind === "high" && rec.highDate) {
        html += '<div class="tooltip-row"><span class="tooltip-label">卖出日</span><span>' + formatDate(rec.highDate) + "</span></div>";
        if (rec.highPrice != null) {
            html += '<div class="tooltip-row"><span class="tooltip-label">卖出价</span><span>' + rec.highPrice.toFixed(2) + "</span></div>";
        }
        if (rec.highDays != null) {
            html += '<div class="tooltip-row"><span class="tooltip-label">持有时间</span><span>' + rec.highDays + " 天</span></div>";
        }
        if (document.getElementById("toggle-show-profit").checked && rec.highChange != null) {
            const c = rec.highChange;
            const sign = c > 0 ? "+" : "";
            const color = c >= 0 ? "#e53935" : "#43a047";
            html += '<div class="tooltip-row"><span class="tooltip-label">收益</span><span style="color:' + color + ';">' + sign + c.toFixed(2) + "%</span></div>";
        }
    }
    if (kind === "low" && rec.lowDate) {
        html += '<div class="tooltip-row"><span class="tooltip-label">卖出日</span><span>' + formatDate(rec.lowDate) + "</span></div>";
        if (rec.lowPrice != null) {
            html += '<div class="tooltip-row"><span class="tooltip-label">卖出价</span><span>' + rec.lowPrice.toFixed(2) + "</span></div>";
        }
        if (rec.lowDays != null) {
            html += '<div class="tooltip-row"><span class="tooltip-label">持有时间</span><span>' + rec.lowDays + " 天</span></div>";
        }
        if (document.getElementById("toggle-show-profit").checked && rec.lowChange != null) {
            const c2 = rec.lowChange;
            const sign2 = c2 > 0 ? "+" : "";
            const color2 = c2 >= 0 ? "#e53935" : "#43a047";
            html += '<div class="tooltip-row"><span class="tooltip-label">收益</span><span style="color:' + color2 + ';">' + sign2 + c2.toFixed(2) + "%</span></div>";
        }
    }
    return html;
}
function highlightGroup(groupId) {
    const elems = document.querySelectorAll(".stock-elem");
    elems.forEach(el => {
        const g = el.dataset.groupId;
        if (!g) return;
        if (groupId && g === groupId) {
            el.style.opacity = "1";
        } else if (groupId) {
            el.style.opacity = "0.18";
        } else {
            el.style.opacity = "1";
        }
    });
}
function bindElemEvents(el, rec, kind) {
    const isPoint = el.classList.contains("stock-point");
    el.addEventListener("mouseenter", function(evt) {
        if (!selectedGroupId) {
            el.style.opacity = "1";
            if (isPoint) {
                el.style.transform = "translate(-50%, -50%) scale(1.5)";
            } else {
                el.style.transform = "scaleX(1.5)";
            }
        }
        const html = buildTooltip(rec, kind);
        showTooltip(html, evt.clientX, evt.clientY);
    });
    el.addEventListener("mouseleave", function() {
        if (!selectedGroupId) {
            el.style.opacity = "";
            if (isPoint) {
                el.style.transform = "translate(-50%, -50%)";
            } else {
                el.style.transform = "";
            }
        }
        hideTooltipDelayed();
    });
    el.addEventListener("click", function() {
        if (selectedGroupId === rec.groupId) {
            selectedGroupId = null;
        } else {
            selectedGroupId = rec.groupId;
        }
        renderTimeline();
    });
}
// options: { forceSell: bool, forceSellDays: number }
function calculateProfitForStock(rec, profitTarget, stopLoss, options) {
    if (!rec.buyPrice) return null;

    const forceSell     = options && options.forceSell;
    const forceSellDays = options && options.forceSellDays > 0 ? options.forceSellDays : 30;

    const buyPrice    = rec.buyPrice;
    const profitPrice = buyPrice * (1 + profitTarget / 100);
    const lossPrice   = buyPrice * (1 + stopLoss  / 100);

    if (rec.dailyPrices && rec.dailyPrices.length > 0) {
        // ── 逐日路径（有每日 OHLC 数据）──────────────────────────────────
        // 先找到强制卖出截止点：forceSellDays 对应的 dailyPrices 下标
        // days_from_signal 是信号发出后的第 N 个交易日
        let forceSellIdx = rec.dailyPrices.length - 1;  // 默认窗口最后一天
        if (forceSell) {
            for (let i = 0; i < rec.dailyPrices.length; i++) {
                if ((rec.dailyPrices[i].days_from_signal || 0) >= forceSellDays) {
                    forceSellIdx = i;
                    break;
                }
            }
        }

        const scanEnd = forceSell ? forceSellIdx : rec.dailyPrices.length - 1;

        for (let i = 0; i <= scanEnd; i++) {
            const dayData   = rec.dailyPrices[i];
            const high      = dayData.high;
            const low       = dayData.low;
            const hitProfit = high != null && high >= profitPrice;
            const hitLoss   = low  != null && low  <= lossPrice;

            if (hitProfit && hitLoss) {
                // 同日双触发：用开盘价优先判断，开盘在中间时保守取止损
                const openPrice = dayData.open != null ? dayData.open : buyPrice;
                if (openPrice <= lossPrice) {
                    return { profit: stopLoss,    days: dayData.days_from_signal, type: "loss" };
                }
                if (openPrice >= profitPrice) {
                    return { profit: profitTarget, days: dayData.days_from_signal, type: "profit" };
                }
                // 开盘在中间，盘中顺序不确定 → 保守：先止损
                return { profit: stopLoss, days: dayData.days_from_signal, type: "loss_ambiguous" };
            } else if (hitProfit) {
                return { profit: profitTarget, days: dayData.days_from_signal, type: "profit" };
            } else if (hitLoss) {
                return { profit: stopLoss, days: dayData.days_from_signal, type: "loss" };
            }
        }

        // 遍历完仍未触发
        if (forceSell) {
            // 强制卖出：用截止日的收盘价计算盈亏
            const sellDay   = rec.dailyPrices[forceSellIdx];
            const sellClose = sellDay.close != null ? sellDay.close : buyPrice;
            const sellReturn = (sellClose - buyPrice) / buyPrice * 100;
            return { profit: sellReturn, days: sellDay.days_from_signal, type: "force_sell" };
        }
        // 不强制卖出 → miss
        const windowDays = rec.dailyPrices[rec.dailyPrices.length - 1].days_from_signal || 0;
        return { profit: null, days: windowDays, type: "miss" };

    } else {
        // ── 旧路径（只有聚合字段，无逐日 OHLC）────────────────────────────
        const highPrice = rec.highPrice;
        const lowPrice  = rec.lowPrice;
        const highDays  = rec.highDays != null ? rec.highDays : 999999;
        const lowDays   = rec.lowDays  != null ? rec.lowDays  : 999999;

        const canReachProfit = highPrice != null && highPrice >= profitPrice;
        const canReachLoss   = lowPrice  != null && lowPrice  <= lossPrice;

        if (canReachProfit && canReachLoss) {
            if (highDays < lowDays) {
                return { profit: profitTarget, days: highDays, type: "profit" };
            } else {
                return { profit: stopLoss, days: lowDays, type: "loss" };
            }
        } else if (canReachProfit) {
            return { profit: profitTarget, days: highDays, type: "profit" };
        } else if (canReachLoss) {
            return { profit: stopLoss, days: lowDays, type: "loss" };
        }

        // 旧路径无逐日收盘，强制卖出无法精确计算 → 统一 miss
        return { profit: null, days: 0, type: "miss" };
    }
}
function getFilteredRecords() {
    const dateRangeInput = document.getElementById("filter-date-range").value;
    const daysMinInput = document.getElementById("filter-days-min").value;
    const daysMaxInput = document.getElementById("filter-days-max").value;
    const priceMinInput = document.getElementById("filter-price-min").value;
    const priceMaxInput = document.getElementById("filter-price-max").value;
    const highChangeMinInput = document.getElementById("filter-high-change-min").value;
    const highChangeMaxInput = document.getElementById("filter-high-change-max").value;
    const buyDayChangeMinInput = document.getElementById("filter-buy-day-change-min").value;
    const buyDayChangeMaxInput = document.getElementById("filter-buy-day-change-max").value;
    const nextDayChangeMinInput = document.getElementById("filter-next-day-change-min").value;
    const nextDayChangeMaxInput = document.getElementById("filter-next-day-change-max").value;
    const heatMinInput = document.getElementById("filter-heat-min").value;
    const heatMaxInput = document.getElementById("filter-heat-max").value;
    const daysMin = daysMinInput ? parseFloat(daysMinInput) : 0;
    const daysMax = daysMaxInput ? parseFloat(daysMaxInput) : Infinity;
    const priceMin = priceMinInput ? parseFloat(priceMinInput) : 0;
    const priceMax = priceMaxInput ? parseFloat(priceMaxInput) : Infinity;
    const highChangeMin = highChangeMinInput ? parseFloat(highChangeMinInput) : -Infinity;
    const highChangeMax = highChangeMaxInput ? parseFloat(highChangeMaxInput) : Infinity;
    const buyDayChangeMin = buyDayChangeMinInput ? parseFloat(buyDayChangeMinInput) : -Infinity;
    const buyDayChangeMax = buyDayChangeMaxInput ? parseFloat(buyDayChangeMaxInput) : Infinity;
    const nextDayChangeMin = nextDayChangeMinInput ? parseFloat(nextDayChangeMinInput) : -Infinity;
    const nextDayChangeMax = nextDayChangeMaxInput ? parseFloat(nextDayChangeMaxInput) : Infinity;
    const heatMin = heatMinInput ? parseFloat(heatMinInput) : null;
    const heatMax = heatMaxInput ? parseFloat(heatMaxInput) : null;
    let dateThreshold = null;
    if (dateRangeInput) {
        const days = parseInt(dateRangeInput);
        const thresholdDate = new Date();
        thresholdDate.setDate(thresholdDate.getDate() - days);
        thresholdDate.setHours(0, 0, 0, 0);
        dateThreshold = thresholdDate;
    }
    return records.filter(rec => {
        if (!rec.buyPrice) return false;
        if (dateThreshold && rec.buyDate) {
            const buyDate = new Date(rec.buyDate);
            buyDate.setHours(0, 0, 0, 0);
            if (buyDate < dateThreshold) return false;
        }
        if (priceMinInput && rec.buyPrice < priceMin) return false;
        if (priceMaxInput && rec.buyPrice > priceMax) return false;
        if (rec.highChange !== null) {
            if (highChangeMinInput && rec.highChange < highChangeMin) return false;
            if (highChangeMaxInput && rec.highChange > highChangeMax) return false;
        }
        const enableBuyDayChange = document.getElementById("filter-enable-buy-day-change").checked;
        if (enableBuyDayChange) {
            if (rec.buyDayChangeRate === null || rec.buyDayChangeRate === undefined) return false;
            if (buyDayChangeMinInput && rec.buyDayChangeRate < buyDayChangeMin) return false;
            if (buyDayChangeMaxInput && rec.buyDayChangeRate > buyDayChangeMax) return false;
        }
        const enableNextDayChange = document.getElementById("filter-enable-next-day-change").checked;
        if (enableNextDayChange) {
            if (rec.nextDayChangeRate === null || rec.nextDayChangeRate === undefined) return false;
            if (nextDayChangeMinInput && rec.nextDayChangeRate < nextDayChangeMin) return false;
            if (nextDayChangeMaxInput && rec.nextDayChangeRate > nextDayChangeMax) return false;
        }
        // 热度评分筛选：有值时，无评分信号也被过滤
        if (heatMin !== null) {
            if (rec.tradeHeatScore == null || rec.tradeHeatScore < heatMin) return false;
        }
        if (heatMax !== null) {
            if (rec.tradeHeatScore == null || rec.tradeHeatScore > heatMax) return false;
        }
        const maxDays = Math.max(rec.highDays || 0, rec.lowDays || 0);
        if (daysMinInput && maxDays < daysMin) return false;
        if (daysMaxInput && maxDays > daysMax) return false;
        return true;
    });
}
function updateSliderValue(sliderId, valueId) {
    const slider = document.getElementById(sliderId);
    const valueEl = document.getElementById(valueId);
    if (slider && valueEl) {
        const value = parseFloat(slider.value);
        valueEl.textContent = (value > 0 ? "+" : "") + value.toFixed(1) + "%";
    }
}
function resetCalcFilters() {
    document.getElementById("filter-date-range").value = "";
    document.getElementById("filter-days-min").value = "10";
    document.getElementById("filter-days-max").value = "";
    document.getElementById("filter-price-min").value = "";
    document.getElementById("filter-price-max").value = "";
    document.getElementById("filter-high-change-min").value = "";
    document.getElementById("filter-high-change-max").value = "";
    document.getElementById("filter-enable-buy-day-change").checked = false;
    document.getElementById("filter-enable-next-day-change").checked = false;
    document.getElementById("filter-buy-day-change-min").value = "-10";
    document.getElementById("filter-buy-day-change-max").value = "10";
    document.getElementById("filter-next-day-change-min").value = "-10";
    document.getElementById("filter-next-day-change-max").value = "10";
    document.getElementById("filter-reach-rate").value = "80";
    document.getElementById("filter-buy-day-change-min").disabled = true;
    document.getElementById("filter-buy-day-change-max").disabled = true;
    document.getElementById("filter-next-day-change-min").disabled = true;
    document.getElementById("filter-next-day-change-max").disabled = true;
    document.getElementById("filter-heat-min").value = "50";
    document.getElementById("filter-heat-max").value = "";
    document.getElementById("filter-enable-force-sell").checked = true;
    document.getElementById("filter-force-sell-days").value = "30";
    document.getElementById("filter-force-sell-days").disabled = false;
    updateSliderValue("filter-buy-day-change-min", "filter-buy-day-change-min-value");
    updateSliderValue("filter-buy-day-change-max", "filter-buy-day-change-max-value");
    updateSliderValue("filter-next-day-change-min", "filter-next-day-change-min-value");
    updateSliderValue("filter-next-day-change-max", "filter-next-day-change-max-value");
    updateCalc();
}
function updateCalc() {
    const reachRateInput = document.getElementById("filter-reach-rate");
    const minReachRate = reachRateInput && reachRateInput.value !== ""
        ? Math.max(0, Math.min(100, parseFloat(reachRateInput.value)))
        : 0;
    const filteredRecords = getFilteredRecords();
    const profitTargets = [];
    const stopLosses = [];
    for (let i = 2; i <= 30; i += 2)  { profitTargets.push(i); }
    for (let i = -2; i >= -30; i -= 2) { stopLosses.push(i); }

    const thead = document.getElementById("calc-table-header");
    const tbody  = document.getElementById("calc-table-body");
    thead.innerHTML = "";
    tbody.innerHTML  = "";

    const headerRow = document.createElement("tr");
    headerRow.innerHTML = "<th>止损 \\ 止盈</th>";
    profitTargets.forEach(pt => {
        const th = document.createElement("th");
        th.className = "text-center";
        th.textContent = "+" + pt + "%";
        headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);

    // totalStocks = 所有有买入价的记录数（全程固定分母）
    const totalStocks = filteredRecords.filter(r => r.buyPrice).length;
    document.getElementById("calc-total-stocks").textContent = totalStocks;

    if (totalStocks === 0) return;

    // ── 读取强制卖出选项 ─────────────────────────────────────────────────
    const forceSellEnabled = document.getElementById("filter-enable-force-sell").checked;
    const forceSellDays    = parseInt(document.getElementById("filter-force-sell-days").value) || 30;
    const calcOptions      = { forceSell: forceSellEnabled, forceSellDays };

    // ── 构建每格统计 ─────────────────────────────────────────────────────
    const cellData = [];

    stopLosses.forEach((sl) => {
        const rowData = [];
        profitTargets.forEach((pt) => {
            let profitCount = 0, lossCount = 0, missCount = 0, forceSellCount = 0, ambigCount = 0;
            let totalProfitDays = 0, totalLossDays = 0;
            let totalForceSellProfit = 0, totalForceSellDays = 0;
            const hitStocks       = [];   // 触发止盈/止损的个股
            const forceSellStocks = [];   // 强制卖出的个股
            const missStocks      = [];   // 未触发（强制卖出关闭时）

            filteredRecords.forEach(rec => {
                if (!rec.buyPrice) return;
                const result = calculateProfitForStock(rec, pt, sl, calcOptions);
                if (result === null) return;

                const isLoss = result.type === "loss" || result.type === "loss_ambiguous";

                if (result.type === "profit") {
                    profitCount++;
                    totalProfitDays += (result.days || 0);
                    hitStocks.push({ code: rec.stockCode || "-", name: rec.stockName || "-",
                                     profit: result.profit, days: result.days, type: "profit" });
                } else if (isLoss) {
                    lossCount++;
                    totalLossDays += (result.days || 0);
                    if (result.type === "loss_ambiguous") ambigCount++;
                    hitStocks.push({ code: rec.stockCode || "-", name: rec.stockName || "-",
                                     profit: result.profit, days: result.days, type: result.type });
                } else if (result.type === "force_sell") {
                    forceSellCount++;
                    totalForceSellProfit += result.profit;
                    totalForceSellDays   += (result.days || 0);
                    forceSellStocks.push({ code: rec.stockCode || "-", name: rec.stockName || "-",
                                           profit: result.profit, days: result.days, type: "force_sell" });
                } else if (result.type === "miss") {
                    missCount++;
                    missStocks.push({ code: rec.stockCode || "-", name: rec.stockName || "-" });
                }
            });

            const hitCount = profitCount + lossCount;
            // 触达率 = 止盈+止损 / 总信号（不含强制卖出）
            const hitRate  = totalStocks > 0 ? hitCount / totalStocks * 100 : 0;
            const profitRateAmongHits = hitCount > 0 ? profitCount / hitCount * 100 : 0;
            const lossRateAmongHits   = hitCount > 0 ? lossCount   / hitCount * 100 : 0;
            // 触发股票均值（确定性）
            const avgHitProfit = hitCount > 0
                ? (profitCount * pt + lossCount * sl) / hitCount
                : null;
            const avgProfitDays  = profitCount    > 0 ? totalProfitDays    / profitCount    : null;
            const avgLossDays    = lossCount      > 0 ? totalLossDays      / lossCount      : null;
            const avgForceSellP  = forceSellCount > 0 ? totalForceSellProfit / forceSellCount : null;
            const avgForceSellD  = forceSellCount > 0 ? totalForceSellDays   / forceSellCount : null;
            // 全量均值（触发 + 强制卖出）
            const totalWithResult = hitCount + forceSellCount;
            const totalProfit     = (hitCount > 0 ? profitCount * pt + lossCount * sl : 0)
                                  + totalForceSellProfit;
            const avgAllProfit    = totalWithResult > 0 ? totalProfit / totalWithResult : null;

            rowData.push({
                hitCount, profitCount, lossCount, missCount, forceSellCount, ambigCount,
                hitRate, profitRateAmongHits, lossRateAmongHits,
                avgHitProfit, avgProfitDays, avgLossDays,
                avgForceSellP, avgForceSellD,
                avgAllProfit, totalWithResult,
                hitStocks, forceSellStocks, missStocks,
                forceSellEnabled,
            });
        });
        cellData.push(rowData);
    });

    // ── 排序：综合得分（先满足触达率筛选，再参与 Top10/Bottom10）──────
    const allScores = [];
    cellData.forEach((row, r) => {
        row.forEach((d, c) => {
            const passReachRate = d.hitRate >= minReachRate;
            if (passReachRate && (d.hitCount > 0 || d.forceSellCount > 0)) {
                const score = d.forceSellEnabled
                    ? (d.avgAllProfit != null ? d.avgAllProfit : -999)
                    : ((d.avgHitProfit != null ? d.avgHitProfit : -999) * (d.hitRate / 100.0));
                allScores.push({ score, r, c });
            }
        });
    });
    allScores.sort((a, b) => b.score - a.score);
    const top10Set    = new Set(allScores.slice(0, 10).map(x => x.r + "_" + x.c));
    const bottom10Set = new Set(allScores.slice(-10).map(x => x.r + "_" + x.c));
    const topRankMap  = new Map(allScores.slice(0, 10).map((x, idx) => [x.r + "_" + x.c, idx + 1]));

    // ── 渲染 ─────────────────────────────────────────────────────────────
    stopLosses.forEach((sl, slIdx) => {
        const row = document.createElement("tr");
        const labelCell = document.createElement("td");
        labelCell.textContent  = sl + "%";
        labelCell.style.fontWeight = "600";
        row.appendChild(labelCell);

        profitTargets.forEach((pt, ptIdx) => {
            const d    = cellData[slIdx][ptIdx];
            const cell = document.createElement("td");
            cell.style.cursor = "pointer";
            const key  = slIdx + "_" + ptIdx;

            const hasAnyResult = d.hitCount > 0 || d.forceSellCount > 0;
            if (!hasAnyResult) {
                cell.className = "text-center";
                cell.innerHTML = '<div style="color:#ccc;font-size:11px;">-</div>';
                row.appendChild(cell);
                return;
            }

            // ── 取参考均值（用于正负着色的统一判断）────────────────────────
            const refAvg = d.forceSellEnabled ? d.avgAllProfit : d.avgHitProfit;
            const isPositive = refAvg != null && refAvg > 0;
            const isNegative = refAvg != null && refAvg < 0;

            const isBelowReachRate = d.hitRate < minReachRate;

            // ── 着色逻辑 ─────────────────────────────────────────────────
            // 底色：先按正/负给一个基础色，再由强规则覆盖
            let bgColor = isPositive ? "#f1f8e9" : isNegative ? "#fff3f3" : "";
            let borderLeft = isPositive ? "3px solid #81c784" : isNegative ? "3px solid #e57373" : "3px solid transparent";
            let fw = "400";

            if (d.forceSellEnabled) {
                const avg = d.avgAllProfit;
                if (avg != null && avg > 2 && d.profitRateAmongHits >= 50) {
                    bgColor = "#c8e6c9"; borderLeft = "3px solid #4caf50"; fw = "700";
                } else if (avg != null && avg < 0) {
                    bgColor = "#ffcdd2"; borderLeft = "3px solid #e57373";
                } else if (top10Set.has(key)) {
                    bgColor = "#fff3cd"; borderLeft = "3px solid #ffc107"; fw = "700";
                } else if (bottom10Set.has(key)) {
                    bgColor = "#f8d7da"; borderLeft = "3px solid #e57373"; fw = "700";
                }
            } else {
                if (d.hitRate >= 60 && d.profitRateAmongHits >= 60) {
                    bgColor = "#c8e6c9"; borderLeft = "3px solid #4caf50"; fw = "700";
                } else if (d.hitRate >= 60 && d.profitRateAmongHits < 40) {
                    bgColor = "#ffcdd2"; borderLeft = "3px solid #e57373";
                } else if (d.hitRate < 15) {
                    bgColor = "#f5f5f5"; borderLeft = "3px solid #ddd";
                } else if (top10Set.has(key)) {
                    bgColor = "#fff3cd"; borderLeft = "3px solid #ffc107"; fw = "700";
                } else if (bottom10Set.has(key)) {
                    bgColor = "#f8d7da"; borderLeft = "3px solid #e57373"; fw = "700";
                }
            }

            if (isBelowReachRate) {
                bgColor = "#f1f3f5";
                borderLeft = "3px solid #c7cdd4";
                fw = "400";
            }

            cell.className = "text-center";
            cell.style.backgroundColor = bgColor;
            cell.style.borderLeft = borderLeft;
            cell.style.fontWeight = fw;
            cell.style.padding = "4px 3px";
            if (isBelowReachRate) {
                cell.style.color = "#8a949e";
                cell.style.filter = "grayscale(0.85)";
            } else {
                cell.style.color = "";
                cell.style.filter = "";
            }
            if (top10Set.has(key)) {
                cell.classList.add("matrix-top10");
                const r = topRankMap.get(key);
                if (r != null) {
                    cell.setAttribute("data-top-rank", String(r));
                }
            }

            // ── 格子文本 ─────────────────────────────────────────────────
            if (d.forceSellEnabled) {
                const allSign  = d.avgAllProfit > 0 ? "+" : "";
                const allStr   = d.avgAllProfit != null ? allSign + d.avgAllProfit.toFixed(2) + "%" : "-";
                const allColor = d.avgAllProfit != null && d.avgAllProfit >= 0 ? "#c62828" : "#2e7d32";
                const fsRate   = d.totalWithResult > 0
                    ? (d.forceSellCount / d.totalWithResult * 100).toFixed(0) + "%" : "-";
                const pRate2   = d.totalWithResult > 0
                    ? (d.profitCount / d.totalWithResult * 100).toFixed(0) + "%" : "-";
                const lRate2   = d.totalWithResult > 0
                    ? (d.lossCount / d.totalWithResult * 100).toFixed(0) + "%" : "-";

                cell.innerHTML =
                    '<div style="font-size:16px;font-weight:800;color:' + allColor + ';line-height:1.2;">' + allStr + '</div>'
                    + '<div style="font-size:11px;margin-top:2px;">'
                        + '<span style="color:#c62828;">' + pRate2 + '↑</span>'
                        + '<span style="color:#bbb;margin:0 2px;">|</span>'
                        + '<span style="color:#2e7d32;">' + lRate2 + '↓</span>'
                        + '<span style="color:#bbb;margin:0 2px;">|</span>'
                        + '<span style="color:#888;">' + fsRate + '强卖</span>'
                    + '</div>'
                    + '<div style="font-size:10px;color:#1565c0;margin-top:1px;">'
                        + d.hitRate.toFixed(0) + '% 触达</div>';
            } else {
                const hitRateStr = d.hitRate.toFixed(0) + "%";
                const pAmongStr  = d.profitRateAmongHits.toFixed(0) + "%";
                const lAmongStr  = d.lossRateAmongHits.toFixed(0) + "%";
                const avgSign    = d.avgHitProfit != null && d.avgHitProfit > 0 ? "+" : "";
                const avgStr     = d.avgHitProfit != null ? avgSign + d.avgHitProfit.toFixed(2) + "%" : "-";
                const avgColor   = d.avgHitProfit != null && d.avgHitProfit >= 0 ? "#c62828" : "#2e7d32";

                cell.innerHTML =
                    '<div style="font-size:12px;font-weight:700;color:#1565c0;line-height:1.2;">' + hitRateStr + ' 触达</div>'
                    + '<div style="font-size:16px;font-weight:800;color:' + avgColor + ';line-height:1.3;">' + avgStr + '</div>'
                    + '<div style="font-size:11px;margin-top:1px;">'
                        + '<span style="color:#c62828;">' + pAmongStr + '止盈</span>'
                        + '<span style="color:#bbb;margin:0 2px;">|</span>'
                        + '<span style="color:#2e7d32;">' + lAmongStr + '止损</span>'
                    + '</div>';
            }

            // ── Tooltip ──────────────────────────────────────────────────
            const tooltipData = {
                pt, sl, totalStocks, forceSellEnabled: d.forceSellEnabled,
                hitCount: d.hitCount, profitCount: d.profitCount, lossCount: d.lossCount,
                missCount: d.missCount, forceSellCount: d.forceSellCount, ambigCount: d.ambigCount,
                hitRate: d.hitRate, profitRateAmongHits: d.profitRateAmongHits,
                lossRateAmongHits: d.lossRateAmongHits,
                avgHitProfit: d.avgHitProfit, avgAllProfit: d.avgAllProfit,
                avgProfitDays: d.avgProfitDays, avgLossDays: d.avgLossDays,
                avgForceSellP: d.avgForceSellP, avgForceSellD: d.avgForceSellD,
                hitStocks: d.hitStocks, forceSellStocks: d.forceSellStocks, missStocks: d.missStocks,
            };
            cell.dataset.tooltip = JSON.stringify(tooltipData);

            cell.addEventListener("mouseenter", function(e) {
                const td = JSON.parse(this.dataset.tooltip || "{}");
                let html = '<div class="tooltip-title">止盈 +' + td.pt + '% / 止损 ' + td.sl + '%</div>';

                html += '<div style="padding:6px 0;border-bottom:1px solid #eee;margin-bottom:6px;">';
                html += '<div style="font-size:12px;">触达率：<b style="color:#1565c0;">'
                    + td.hitRate.toFixed(1) + '%</b>'
                    + ' <span style="color:#999;font-size:11px;">（' + td.hitCount + '/' + td.totalStocks + ' 只触及止盈/止损）</span></div>';
                if (td.avgHitProfit != null) {
                    const s = td.avgHitProfit > 0 ? "+" : "";
                    html += '<div style="font-size:12px;">触达均值：<b style="color:'
                        + (td.avgHitProfit >= 0 ? "#e53935" : "#43a047") + ';">'
                        + s + td.avgHitProfit.toFixed(2) + '%</b></div>';
                }
                if (td.forceSellEnabled && td.avgAllProfit != null) {
                    const s = td.avgAllProfit > 0 ? "+" : "";
                    html += '<div style="font-size:12px;">全量均值（含强卖）：<b style="color:'
                        + (td.avgAllProfit >= 0 ? "#e53935" : "#43a047") + ';">'
                        + s + td.avgAllProfit.toFixed(2) + '%</b></div>';
                }
                html += '</div>';

                if (td.profitCount > 0) {
                    const dStr = td.avgProfitDays != null ? "平均 " + td.avgProfitDays.toFixed(1) + " 天" : "";
                    html += '<div style="font-size:11px;margin-bottom:3px;">'
                        + '<span style="color:#e53935;">▲ 止盈触发：' + td.profitCount + ' 只</span>'
                        + (dStr ? ' <span style="color:#999;">(' + dStr + ')</span>' : '') + '</div>';
                }
                if (td.lossCount > 0) {
                    const dStr = td.avgLossDays != null ? "平均 " + td.avgLossDays.toFixed(1) + " 天" : "";
                    const ambig = td.ambigCount > 0 ? "，含 " + td.ambigCount + " 只同日双触发保守" : "";
                    html += '<div style="font-size:11px;margin-bottom:3px;">'
                        + '<span style="color:#43a047;">▼ 止损触发：' + td.lossCount + ' 只</span>'
                        + (dStr ? ' <span style="color:#999;">(' + dStr + ambig + ')</span>' : '') + '</div>';
                }
                if (td.forceSellCount > 0) {
                    const dStr = td.avgForceSellD != null ? "平均持有 " + td.avgForceSellD.toFixed(1) + " 天" : "";
                    const pStr = td.avgForceSellP != null
                        ? (td.avgForceSellP > 0 ? "+" : "") + td.avgForceSellP.toFixed(2) + "%" : "-";
                    html += '<div style="font-size:11px;margin-bottom:3px;">'
                        + '<span style="color:#888;">⏱ 强制卖出：' + td.forceSellCount + ' 只</span>'
                        + ' <span style="color:#999;">（' + dStr + '，均值 ' + pStr + '）</span></div>';
                }
                if (td.missCount > 0) {
                    html += '<div style="font-size:11px;margin-bottom:6px;">'
                        + '<span style="color:#bbb;">◇ 窗口内未触发：' + td.missCount + ' 只</span></div>';
                }

                // 触发个股明细（止盈/止损）
                const allHitList = [...(td.hitStocks || []), ...(td.forceSellStocks || [])];
                if (allHitList.length > 0) {
                    html += '<div style="border-top:1px solid #eee;padding-top:6px;">';
                    html += '<div style="font-size:11px;color:#555;margin-bottom:4px;">个股明细（最多9只）</div>';
                    html += '<div class="tooltip-stocks">';
                    const showN = Math.min(9, allHitList.length);
                    for (let i = 0; i < showN; i++) {
                        const s = allHitList[i];
                        const typeLabel = s.type === "profit" ? "止盈"
                                        : (s.type === "loss" || s.type === "loss_ambiguous") ? "止损"
                                        : "强卖";
                        const pColor = s.profit >= 0 ? "#e53935" : "#43a047";
                        html += '<div class="tooltip-stock-item">';
                        html += '<div class="tooltip-stock-code">' + s.code + '</div>';
                        html += '<div class="tooltip-stock-name">' + (s.name || "-") + '</div>';
                        html += '<div class="tooltip-stock-info">' + (s.days || 0) + '天</div>';
                        html += '<div class="tooltip-stock-info" style="color:' + pColor + ';">'
                            + (s.profit > 0 ? "+" : "") + s.profit.toFixed(1) + '%</div>';
                        html += '<div class="tooltip-stock-info">' + typeLabel + '</div>';
                        html += '</div>';
                    }
                    html += '</div>';
                    if (allHitList.length > 9) {
                        html += '<div style="font-size:11px;color:#999;margin-top:4px;">还有 '
                            + (allHitList.length - 9) + ' 只</div>';
                    }
                    html += '</div>';
                }

                showTooltip(html, e.clientX, e.clientY);
            });
            cell.addEventListener("mouseleave", function() { hideTooltipDelayed(); });

            row.appendChild(cell);
        });

        tbody.appendChild(row);
    });
}
function renderTimeMarkers() {
    const content = document.getElementById("timeline-content");
    const markers = content.querySelectorAll(".time-marker");
    markers.forEach(m => m.remove());
    if (!dateRange.start || !dateRange.end) return;
    const dayMs = 24 * 60 * 60 * 1000;
    let currentDate = new Date(dateRange.start);
    currentDate.setHours(0, 0, 0, 0);
    const endDate = new Date(dateRange.end);
    endDate.setHours(23, 59, 59, 999);
    let lastLabelY = -Infinity;
    let lastMonth = -1;
    
    // 计算总天数和总高度
    const totalDays = getDaysBetween(dateRange.start, dateRange.end);
    const totalHeight = totalDays * pixelsPerDay;
    
    // 根据缩放级别和总高度动态调整标注密度
    // 目标：确保在整个时间轴上显示足够多的日期标签
    let minLabelSpacing;
    let showWeekly;
    let showDaily;
    
    if (pixelsPerDay >= 5) {
        // 缩放很大：显示每日
        minLabelSpacing = Math.max(20, pixelsPerDay * 0.8);
        showWeekly = true;
        showDaily = true;
    } else if (pixelsPerDay >= 2) {
        // 缩放较大：显示每周和重要日期
        minLabelSpacing = Math.max(30, pixelsPerDay * 1.5);
        showWeekly = true;
        showDaily = false;
    } else if (pixelsPerDay >= 0.5) {
        // 缩放中等：显示每周
        minLabelSpacing = Math.max(40, pixelsPerDay * 2);
        showWeekly = true;
        showDaily = false;
    } else {
        // 缩放很小：至少显示每月和一些重要日期
        // 根据总高度计算合适的间距，确保至少显示10-15个标签
        const targetLabels = Math.min(15, Math.max(10, Math.floor(totalHeight / 60)));
        minLabelSpacing = totalHeight / targetLabels;
        showWeekly = pixelsPerDay >= 0.3;
        showDaily = false;
    }
    
    while (currentDate <= endDate) {
        const y = dateToY(currentDate);
        const isWeekStart = currentDate.getDay() === 1; // 周一
        const currentMonth = currentDate.getMonth();
        const isNewMonth = currentMonth !== lastMonth;
        
        // 更新月份记录
        if (isNewMonth) {
            lastMonth = currentMonth;
        }
        
        const dayOfMonth = currentDate.getDate();
        const isFirstDay = dayOfMonth === 1;
        
        // 每月1日：必须显示，线条更明显
        if (isFirstDay) {
            const marker = document.createElement("div");
            marker.className = "time-marker first-day-marker";
            marker.style.top = y + "px";
            
            const label = document.createElement("div");
            label.className = "time-label week-label";
            label.textContent = formatDateShort(currentDate);
            marker.appendChild(label);
            
            content.appendChild(marker);
            lastLabelY = y;
        }
        // 日标记：缩放很大时显示每日
        else if (showDaily && (y - lastLabelY >= minLabelSpacing * 0.6)) {
            const marker = document.createElement("div");
            marker.className = "time-marker";
            marker.style.top = y + "px";
            
            const label = document.createElement("div");
            label.className = "time-label";
            label.textContent = formatDateShort(currentDate);
            marker.appendChild(label);
            
            content.appendChild(marker);
            lastLabelY = y;
        }
        // 周标记：显示周一
        else if (showWeekly && isWeekStart && (y - lastLabelY >= minLabelSpacing * 0.6)) {
            const marker = document.createElement("div");
            marker.className = "time-marker week-marker";
            marker.style.top = y + "px";
            
            const label = document.createElement("div");
            label.className = "time-label week-label";
            label.textContent = formatDateShort(currentDate);
            marker.appendChild(label);
            
            content.appendChild(marker);
            lastLabelY = y;
        }
        // 其他日期：根据缩放级别显示不同密度的日期
        else {
            let shouldShow = false;
            
            // 计算实际需要的间距（考虑标签高度）
            const actualSpacing = Math.abs(y - lastLabelY);
            const minSpacing = Math.max(30, pixelsPerDay * 2); // 最小间距，确保标签不重叠
            
            if (showWeekly) {
                // 缩放较大时：显示每3天、每5天或每10天
                if (dayOfMonth % 3 === 0 || dayOfMonth % 5 === 0 || dayOfMonth % 10 === 0) {
                    shouldShow = (actualSpacing >= minSpacing * 0.3);
                }
            } else if (pixelsPerDay >= 0.3) {
                // 缩放中等时：显示每5天、每10天或15号
                if (dayOfMonth === 15 || dayOfMonth % 5 === 0 || dayOfMonth % 10 === 0) {
                    shouldShow = (actualSpacing >= minSpacing * 0.4);
                }
            } else {
                // 缩放很小时：显示每10天或15号，间距更宽松
                if (dayOfMonth === 15 || dayOfMonth % 10 === 0) {
                    shouldShow = (actualSpacing >= minSpacing * 0.5);
                }
            }
            
            // 如果间距足够大，即使不满足上述条件，也显示一些日期（每7天）
            if (!shouldShow && actualSpacing >= minSpacing * 0.6) {
                if (dayOfMonth % 7 === 0) {
                    shouldShow = true;
                }
            }
            
            // 如果距离上次标签已经很远，强制显示一些日期（每10天）
            if (!shouldShow && actualSpacing >= minSpacing * 1.2) {
                if (dayOfMonth % 10 === 0 || dayOfMonth === 15) {
                    shouldShow = true;
                }
            }
            
            if (shouldShow) {
                const marker = document.createElement("div");
                marker.className = "time-marker week-marker";
                marker.style.top = y + "px";
                
                const label = document.createElement("div");
                label.className = "time-label week-label";
                label.textContent = formatDateShort(currentDate);
                marker.appendChild(label);
                
                content.appendChild(marker);
                lastLabelY = y;
            }
            
            // 普通日期线：总是显示，但不一定有标签（1日已经在上面处理过了，这里不再创建）
            if (!isFirstDay) {
                const marker = document.createElement("div");
                marker.className = "time-marker";
                marker.style.top = y + "px";
                content.appendChild(marker);
            }
        }
        
        currentDate = new Date(currentDate.getTime() + dayMs);
    }
}
function getStockTimeRange(rec) {
    const dates = [];
    if (rec.buyDate) dates.push(rec.buyDate);
    if (rec.highDate) dates.push(rec.highDate);
    if (rec.lowDate) dates.push(rec.lowDate);
    if (dates.length === 0) return null;
    return {
        start: new Date(Math.min(...dates)),
        end: new Date(Math.max(...dates))
    };
}
function rangesOverlap(range1, range2) {
    if (!range1 || !range2) return false;
    return range1.start <= range2.end && range2.start <= range1.end;
}
function assignColumns(records) {
    const columns = [];
    records.forEach(rec => {
        const range = getStockTimeRange(rec);
        let assigned = false;
        for (let i = 0; i < columns.length; i++) {
            const canFit = !columns[i].some(existingRec => {
                const existingRange = getStockTimeRange(existingRec);
                return rangesOverlap(range, existingRange);
            });
            if (canFit) {
                columns[i].push(rec);
                assigned = true;
                break;
            }
        }
        if (!assigned) {
            columns.push([rec]);
        }
    });
    return columns;
}
function renderStockLines() {
    const content = document.getElementById("timeline-content");
    const lines = content.querySelectorAll(".stock-line");
    lines.forEach(l => l.remove());
    const points = content.querySelectorAll(".stock-point");
    points.forEach(p => p.remove());
    const visibleRecords = records.filter(rec => !selectedGroupId || rec.groupId === selectedGroupId);
    const screenWidth = window.innerWidth || 1400;
    // Adjust width for sidebar
    const sidebarWidth = 320 + 20; // 320px + 20px gap
    const availableScreenWidth = document.querySelector('.main-content').offsetWidth || (screenWidth - sidebarWidth);
    
    const startX = 80;
    const endX = availableScreenWidth - 40;
    const availableWidth = endX - startX;
    const minSpacing = 30;
    const columns = assignColumns(visibleRecords);
    const columnWidth = Math.max(minSpacing, availableWidth / Math.max(1, columns.length));
    columns.forEach((columnRecords, colIndex) => {
        const baseX = startX + (colIndex * columnWidth);
        columnRecords.forEach((rec) => {
            const showHigh = document.getElementById("toggle-sell-high").checked;
            const showLow = document.getElementById("toggle-sell-low").checked;
            const showBuy = document.getElementById("toggle-buy").checked;
            const highLineX = baseX;
            const lowLineX = baseX + 6;
            const lineWidth = 8;
            const buyRectWidth = showHigh && showLow ? 14 : 8;
            const buyRectX = showHigh && showLow ? (highLineX + lowLineX + lineWidth) / 2 : (showHigh ? highLineX + lineWidth / 2 : lowLineX + lineWidth / 2);
            if (rec.buyDate && rec.highDate && showHigh) {
                const buyY = dateToY(rec.buyDate);
                const highY = dateToY(rec.highDate);
                const startY = Math.min(buyY, highY);
                const endY = Math.max(buyY, highY);
                const height = endY - startY;
                if (height > 0) {
                    const line = document.createElement("div");
                    line.className = "stock-line stock-line-high stock-elem";
                    line.dataset.groupId = rec.groupId;
                    line.style.left = highLineX + "px";
                    line.style.top = startY + "px";
                    line.style.height = height + "px";
                    line.style.backgroundColor = colorForChange(rec.highChange, true);
                    content.appendChild(line);
                    bindElemEvents(line, rec, "high");
                }
                if (rec.highDate) {
                    const highY2 = dateToY(rec.highDate);
                    const highPoint = document.createElement("div");
                    highPoint.className = "stock-point stock-point-high stock-elem";
                    highPoint.dataset.groupId = rec.groupId;
                    highPoint.style.left = (highLineX + lineWidth / 2) + "px";
                    highPoint.style.top = highY2 + "px";
                    content.appendChild(highPoint);
                    bindElemEvents(highPoint, rec, "high");
                }
            }
            if (rec.buyDate && rec.lowDate && showLow) {
                const buyY = dateToY(rec.buyDate);
                const lowY = dateToY(rec.lowDate);
                const startY = Math.min(buyY, lowY);
                const endY = Math.max(buyY, lowY);
                const height = endY - startY;
                if (height > 0) {
                    const line = document.createElement("div");
                    line.className = "stock-line stock-line-low stock-elem";
                    line.dataset.groupId = rec.groupId;
                    line.style.left = lowLineX + "px";
                    line.style.top = startY + "px";
                    line.style.height = height + "px";
                    line.style.backgroundColor = colorForChange(rec.lowChange, false);
                    content.appendChild(line);
                    bindElemEvents(line, rec, "low");
                }
                if (rec.lowDate) {
                    const lowY2 = dateToY(rec.lowDate);
                    const lowPoint = document.createElement("div");
                    lowPoint.className = "stock-point stock-point-low stock-elem";
                    lowPoint.dataset.groupId = rec.groupId;
                    lowPoint.style.left = (lowLineX + lineWidth / 2) + "px";
                    lowPoint.style.top = lowY2 + "px";
                    content.appendChild(lowPoint);
                    bindElemEvents(lowPoint, rec, "low");
                }
            }
            if (showBuy && rec.buyDate) {
                const buyY2 = dateToY(rec.buyDate);
                const buyPoint = document.createElement("div");
                buyPoint.className = "stock-point stock-point-buy rect stock-elem";
                buyPoint.dataset.groupId = rec.groupId;
                buyPoint.style.width = buyRectWidth + "px";
                buyPoint.style.left = buyRectX + "px";
                buyPoint.style.top = buyY2 + "px";
                const buyPointColor = colorForBuyPoint(rec.buyDayChangeRate, rec.nextDayChangeRate);
                if (buyPointColor) {
                    // 使用渐变背景让颜色更明显
                    buyPoint.style.background = `linear-gradient(135deg, ${buyPointColor} 0%, ${adjustColorBrightness(buyPointColor, -10)} 100%)`;
                    buyPoint.style.backgroundColor = buyPointColor; // 备用
                } else {
                    // 如果没有颜色数据，使用默认灰色渐变
                    buyPoint.style.background = 'linear-gradient(135deg, #757575 0%, #616161 100%)';
                }
                content.appendChild(buyPoint);
                bindElemEvents(buyPoint, rec, "buy");
            }
        });
    });
    if (selectedGroupId) {
        highlightGroup(selectedGroupId);
    } else {
        highlightGroup(null);
    }
}
function renderTimeline() {
    const content = document.getElementById("timeline-content");
    renderTimeMarkers();
    const totalDays = getDaysBetween(dateRange.start, dateRange.end);
    const totalHeight = totalDays * pixelsPerDay;
    content.style.height = totalHeight + "px";
    renderStockLines();
}
function setupZoom() {
    const axis = document.getElementById("timeline-axis");
    axis.addEventListener("wheel", function(e) {
        if (e.ctrlKey) {
            e.preventDefault();
            const rect = axis.getBoundingClientRect();
            const mouseY = e.clientY - rect.top;
            const scrollTop = axis.scrollTop;
            const mouseYInContent = mouseY + scrollTop;
            const oldPixelsPerDay = pixelsPerDay;
            const mouseDate = yToDate(mouseYInContent, oldPixelsPerDay);
            const delta = e.deltaY > 0 ? -0.1 : 0.1;
            pixelsPerDay = Math.max(minPixelsPerDay, Math.min(maxPixelsPerDay, pixelsPerDay * (1 + delta)));
            if (pixelsPerDay !== oldPixelsPerDay && mouseDate) {
                renderTimeline();
                const newMouseY = dateToY(mouseDate);
                const newScrollTop = newMouseY - mouseY;
                axis.scrollTop = Math.max(0, newScrollTop);
            }
        }
    });
}
function yToDate(y, ppd) {
    if (!dateRange.start || !dateRange.end) return null;
    const pixelsPerDayValue = ppd || pixelsPerDay;
    // 反转时间轴：Y坐标从顶部（最新日期）开始
    const totalDays = getDaysBetween(dateRange.start, dateRange.end);
    const daysFromTop = y / pixelsPerDayValue; // 从顶部开始的天数
    const daysFromStart = totalDays - daysFromTop; // 转换为从开始日期的天数
    const date = new Date(dateRange.start.getTime() + daysFromStart * 24 * 60 * 60 * 1000);
    return date;
}
async function loadStats() {
    try {
        const res = await fetch(API_BASE + "/api/stats");
        const data = await res.json();
        document.getElementById("stat-total").textContent = data.total_signals || 0;
        document.getElementById("stat-stocks").textContent = data.total_stocks || 0;
        document.getElementById("stat-success").textContent = (data.avg_success_rate || 0) + "%";
        const highest = data.avg_highest_change || 0;
        document.getElementById("stat-highest").textContent = (highest > 0 ? "+" : "") + highest.toFixed(2) + "%";
    } catch (error) {
        console.error("Failed to load stats:", error);
    }
}

function setStatChangeClass(el, avg) {
    if (!el) return;
    el.classList.remove("positive", "negative");
    if (avg == null || isNaN(avg)) return;
    if (avg > 0) el.classList.add("positive");
    else if (avg < 0) el.classList.add("negative");
}

function updateFilteredTimelineStats() {
    const n = records.length;
    const elSig = document.getElementById("stat-filter-signals");
    const elStocks = document.getElementById("stat-filter-stocks");
    const elSucc = document.getElementById("stat-filter-success");
    const elHigh = document.getElementById("stat-filter-highest");
    const elLow = document.getElementById("stat-filter-lowest");
    if (!elSig || !elStocks || !elSucc || !elHigh || !elLow) return;

    if (n === 0) {
        elSig.textContent = "0";
        elStocks.textContent = "0";
        elSucc.textContent = "-";
        elHigh.textContent = "-";
        elLow.textContent = "-";
        setStatChangeClass(elHigh, null);
        setStatChangeClass(elLow, null);
        return;
    }

    elSig.textContent = String(n);
    const stockSet = new Set();
    records.forEach(r => {
        if (r.stockCode) stockSet.add(r.stockCode);
    });
    elStocks.textContent = String(stockSet.size);

    const osrVals = records
        .map(r => r.overallSuccessRate)
        .filter(v => v !== null && v !== undefined && !isNaN(v));
    if (osrVals.length > 0) {
        const avgOsr = osrVals.reduce((a, b) => a + b, 0) / osrVals.length;
        elSucc.textContent = avgOsr.toFixed(2) + "%";
    } else {
        elSucc.textContent = "-";
    }

    const highVals = records
        .map(r => r.highChange)
        .filter(v => v !== null && v !== undefined && !isNaN(v));
    if (highVals.length > 0) {
        const avgH = highVals.reduce((a, b) => a + b, 0) / highVals.length;
        elHigh.textContent = (avgH > 0 ? "+" : "") + avgH.toFixed(2) + "%";
        setStatChangeClass(elHigh, avgH);
    } else {
        elHigh.textContent = "-";
        setStatChangeClass(elHigh, null);
    }

    const lowVals = records
        .map(r => r.lowChange)
        .filter(v => v !== null && v !== undefined && !isNaN(v));
    if (lowVals.length > 0) {
        const avgL = lowVals.reduce((a, b) => a + b, 0) / lowVals.length;
        elLow.textContent = (avgL > 0 ? "+" : "") + avgL.toFixed(2) + "%";
        setStatChangeClass(elLow, avgL);
    } else {
        elLow.textContent = "-";
        setStatChangeClass(elLow, null);
    }
}
async function loadStockCodes() {
    try {
        const res = await fetch(API_BASE + "/api/stock-codes");
        const data = await res.json();
        const select = document.getElementById("filter-stock-code");
        data.stock_codes.forEach(item => {
            const option = document.createElement("option");
            option.value = item.code;
            option.textContent = item.code + (item.name ? " - " + item.name : "");
            select.appendChild(option);
        });
    } catch (error) {
        console.error("Failed to load stock codes:", error);
    }
}
async function loadDailyPricesForEvents(events) {
    // 为事件列表批量加载每日价格数据
    if (!events || events.length === 0) return;
    
    // 分批加载，每批50个，避免一次性请求太多数据
    const batchSize = 50;
    const dailyPricesMap = {};
    
    for (let i = 0; i < events.length; i += batchSize) {
        const batch = events.slice(i, i + batchSize);
        const promises = batch.map(async (event) => {
            try {
                const params = new URLSearchParams();
                // 优先使用 signal_id，如果没有则使用 stock_code + insert_date
                if (event.id) {
                    params.append("signal_id", event.id);
                } else {
                    params.append("stock_code", event.stock_code);
                    params.append("insert_date", event.insert_date);
                }
                
                const res = await fetch(API_BASE + "/api/signal-daily-prices?" + params.toString());
                const data = await res.json();
                
                if (data.prices && data.prices.length > 0) {
                    const key = event.id ? `id_${event.id}` : `${event.stock_code}_${event.insert_date}`;
                    return {
                        key: key,
                        prices: data.prices
                    };
                }
                return null;
            } catch (error) {
                console.error(`Failed to load daily prices for ${event.stock_code}:`, error);
                return null;
            }
        });
        
        const results = await Promise.all(promises);
        results.forEach(result => {
            if (result) {
                dailyPricesMap[result.key] = result.prices;
            }
        });
    }
    
    // 将每日价格数据附加到事件对象
    events.forEach(event => {
        const key = event.id ? `id_${event.id}` : `${event.stock_code}_${event.insert_date}`;
        if (dailyPricesMap[key]) {
            event.dailyPrices = dailyPricesMap[key];
        }
    });
}
async function reloadTimeline() {
    const stockCode = document.getElementById("filter-stock-code").value;
    const dateFrom  = document.getElementById("filter-date-from").value;
    const dateTo    = document.getElementById("filter-date-to").value;
    const heatMin   = document.getElementById("filter-timeline-heat-min").value;
    const heatMax   = document.getElementById("filter-timeline-heat-max").value;
    const params = new URLSearchParams();
    if (stockCode) params.append("stock_code", stockCode);
    if (dateFrom)  params.append("date_from", dateFrom);
    if (dateTo)    params.append("date_to", dateTo);
    // heat 筛选传给后端（NULL 字段不受影响）
    if (heatMin)   params.append("heat_min", heatMin);
    if (heatMax)   params.append("heat_max", heatMax);
    const res = await fetch(API_BASE + "/api/calendar/events?" + params.toString());
    const data = await res.json();
    const events = data.events || [];
    
    // 批量加载每日价格数据
    await loadDailyPricesForEvents(events);
    
    records = [];
    const priceMinInput = document.getElementById("filter-timeline-price-min").value;
    const priceMaxInput = document.getElementById("filter-timeline-price-max").value;
    const buyDayChangeMinInput = document.getElementById("filter-timeline-buy-day-change-min").value;
    const buyDayChangeMaxInput = document.getElementById("filter-timeline-buy-day-change-max").value;
    const nextDayChangeMinInput = document.getElementById("filter-timeline-next-day-change-min").value;
    const nextDayChangeMaxInput = document.getElementById("filter-timeline-next-day-change-max").value;
    const priceMin = priceMinInput ? parseFloat(priceMinInput) : 0;
    const priceMax = priceMaxInput ? parseFloat(priceMaxInput) : Infinity;
    const buyDayChangeMin = buyDayChangeMinInput ? parseFloat(buyDayChangeMinInput) : -Infinity;
    const buyDayChangeMax = buyDayChangeMaxInput ? parseFloat(buyDayChangeMaxInput) : Infinity;
    const nextDayChangeMin = nextDayChangeMinInput ? parseFloat(nextDayChangeMinInput) : -Infinity;
    const nextDayChangeMax = nextDayChangeMaxInput ? parseFloat(nextDayChangeMaxInput) : Infinity;
    events.forEach((e, idx) => {
        const buyPrice = e.insert_price != null ? Number(e.insert_price) : null;
        if (priceMinInput && (buyPrice === null || buyPrice < priceMin)) return;
        if (priceMaxInput && (buyPrice === null || buyPrice > priceMax)) return;
        const enableBuyDayChange = document.getElementById("filter-timeline-enable-buy-day-change").checked;
        if (enableBuyDayChange) {
            const buyDayChangeRate = e.buy_day_change_rate != null ? Number(e.buy_day_change_rate) : null;
            if (buyDayChangeRate === null || buyDayChangeRate === undefined) return;
            if (buyDayChangeRate < buyDayChangeMin || buyDayChangeRate > buyDayChangeMax) return;
        }
        const enableNextDayChange = document.getElementById("filter-timeline-enable-next-day-change").checked;
        if (enableNextDayChange) {
            const nextDayChangeRate = e.next_day_change_rate != null ? Number(e.next_day_change_rate) : null;
            if (nextDayChangeRate === null || nextDayChangeRate === undefined) return;
            if (nextDayChangeRate < nextDayChangeMin || nextDayChangeRate > nextDayChangeMax) return;
        }
        const buyDate = parseDate(e.insert_date);
        const highDate = parseDate(e.highest_price_date);
        const lowDate = parseDate(e.lowest_price_date);
        const rec = {
            groupId: e.stock_code + "_" + idx,
            stockCode: e.stock_code,
            stockName: e.stock_name,
            buyDate: buyDate,
            buyPrice: buyPrice,
            highDate: highDate,
            highPrice: e.highest_price != null ? Number(e.highest_price) : null,
            highChange: e.highest_change_rate != null ? Number(e.highest_change_rate) : null,
            highDays: e.highest_days != null ? Number(e.highest_days) : null,
            lowDate: lowDate,
            lowPrice: e.lowest_price != null ? Number(e.lowest_price) : null,
            lowChange: e.lowest_change_rate != null ? Number(e.lowest_change_rate) : null,
            lowDays: e.lowest_days != null ? Number(e.lowest_days) : null,
            buyDayChangeRate: e.buy_day_change_rate != null ? Number(e.buy_day_change_rate) : null,
            nextDayChangeRate: e.next_day_change_rate != null ? Number(e.next_day_change_rate) : null,
            tradeHeatScore: e.trade_heat_score != null ? Number(e.trade_heat_score) : null,
            overallSuccessRate: e.overall_success_rate != null ? Number(e.overall_success_rate) : null,
            dailyPrices: e.dailyPrices || null  // 每日价格数据
        };
        records.push(rec);
    });
    if (records.length === 0) {
        dateRange.start = new Date();
        dateRange.end = new Date();
    } else {
        const allDates = [];
        records.forEach(rec => {
            if (rec.buyDate) allDates.push(rec.buyDate);
            if (rec.highDate) allDates.push(rec.highDate);
            if (rec.lowDate) allDates.push(rec.lowDate);
        });
        const minDate = new Date(Math.min(...allDates));
        const maxDate = new Date(Math.max(...allDates));
        dateRange.start = new Date(minDate);
        dateRange.start.setDate(dateRange.start.getDate() - 14);
        dateRange.start.setHours(0, 0, 0, 0);
        dateRange.end = new Date(maxDate);
        dateRange.end.setDate(dateRange.end.getDate() + 14);
        dateRange.end.setHours(23, 59, 59, 999);
    }
    // 计算合适的pixelsPerDay，使时间轴显示到最大值
    const screenHeight = window.innerHeight || 800;
    const availableHeight = screenHeight - 400; // 减去header、stats等高度
    const totalDays = getDaysBetween(dateRange.start, dateRange.end);
    if (totalDays > 0) {
        // 使用最大pixelsPerDay，让时间轴放大到最大值
        pixelsPerDay = maxPixelsPerDay;
    } else {
        const daysToShow = 60;
        pixelsPerDay = Math.max(0.5, Math.min(20, screenHeight / daysToShow));
    }
    renderTimeline();
    updateFilteredTimelineStats();
    // 滚动到时间轴顶部（最新日期，因为时间轴已反转）
    const axis = document.getElementById("timeline-axis");
    if (axis) {
        // 使用requestAnimationFrame确保DOM已更新
        requestAnimationFrame(() => {
            requestAnimationFrame(() => {
                axis.scrollTop = 0; // 滚动到顶部，显示最新日期
            });
        });
    }
    updateCalc();
}
function resetFilters() {
    document.getElementById("filter-stock-code").value = "";
    document.getElementById("filter-date-from").value = "";
    document.getElementById("filter-date-to").value = "";
    document.getElementById("filter-timeline-heat-min").value = "50";
    document.getElementById("filter-timeline-heat-max").value = "";
    document.getElementById("filter-timeline-price-min").value = "";
    document.getElementById("filter-timeline-price-max").value = "";
    document.getElementById("filter-timeline-enable-buy-day-change").checked = false;
    document.getElementById("filter-timeline-enable-next-day-change").checked = false;
    document.getElementById("filter-timeline-buy-day-change-min").value = "-10";
    document.getElementById("filter-timeline-buy-day-change-max").value = "10";
    document.getElementById("filter-timeline-next-day-change-min").value = "-10";
    document.getElementById("filter-timeline-next-day-change-max").value = "10";
    document.getElementById("filter-timeline-buy-day-change-min").disabled = true;
    document.getElementById("filter-timeline-buy-day-change-max").disabled = true;
    document.getElementById("filter-timeline-next-day-change-min").disabled = true;
    document.getElementById("filter-timeline-next-day-change-max").disabled = true;
    updateSliderValue("filter-timeline-buy-day-change-min", "filter-timeline-buy-day-change-min-value");
    updateSliderValue("filter-timeline-buy-day-change-max", "filter-timeline-buy-day-change-max-value");
    updateSliderValue("filter-timeline-next-day-change-min", "filter-timeline-next-day-change-min-value");
    updateSliderValue("filter-timeline-next-day-change-max", "filter-timeline-next-day-change-max-value");
    document.getElementById("toggle-buy").checked = true;
    document.getElementById("toggle-sell-high").checked = true;
    document.getElementById("toggle-sell-low").checked = true;
    document.getElementById("toggle-show-name").checked = true;
    document.getElementById("toggle-show-profit").checked = true;
    selectedGroupId = null;
    reloadTimeline();
}
function setupSliderListeners() {
    const sliders = [
        { id: "filter-buy-day-change-min", valueId: "filter-buy-day-change-min-value", checkboxId: "filter-enable-buy-day-change" },
        { id: "filter-buy-day-change-max", valueId: "filter-buy-day-change-max-value", checkboxId: "filter-enable-buy-day-change" },
        { id: "filter-next-day-change-min", valueId: "filter-next-day-change-min-value", checkboxId: "filter-enable-next-day-change" },
        { id: "filter-next-day-change-max", valueId: "filter-next-day-change-max-value", checkboxId: "filter-enable-next-day-change" },
        { id: "filter-timeline-buy-day-change-min", valueId: "filter-timeline-buy-day-change-min-value", checkboxId: "filter-timeline-enable-buy-day-change" },
        { id: "filter-timeline-buy-day-change-max", valueId: "filter-timeline-buy-day-change-max-value", checkboxId: "filter-timeline-enable-buy-day-change" },
        { id: "filter-timeline-next-day-change-min", valueId: "filter-timeline-next-day-change-min-value", checkboxId: "filter-timeline-enable-next-day-change" },
        { id: "filter-timeline-next-day-change-max", valueId: "filter-timeline-next-day-change-max-value", checkboxId: "filter-timeline-enable-next-day-change" }
    ];
    sliders.forEach(({ id, valueId, checkboxId }) => {
        const slider = document.getElementById(id);
        const checkbox = document.getElementById(checkboxId);
        if (slider) {
            updateSliderValue(id, valueId);
            slider.addEventListener("input", function() {
                updateSliderValue(id, valueId);
                if (id.startsWith("filter-timeline-")) {
                    reloadTimeline();
                } else {
                    updateCalc();
                }
            });
        }
        if (checkbox && slider) {
            checkbox.addEventListener("change", function() {
                slider.disabled = !checkbox.checked;
                if (id.startsWith("filter-timeline-")) {
                    reloadTimeline();
                } else {
                    updateCalc();
                }
            });
        }
    });
}
// 侧边栏收起/展开功能
function initSidebarToggle() {
    const sidebar = document.getElementById('sidebar');
    const toggleBtn = document.getElementById('sidebar-toggle');
    
    if (!sidebar || !toggleBtn) return;
    
    // 从localStorage读取状态
    const isCollapsed = localStorage.getItem('sidebarCollapsed') === 'true';
    if (isCollapsed) {
        sidebar.classList.add('collapsed');
        // 更新按钮位置
        toggleBtn.style.left = '8px';
        toggleBtn.querySelector('.toggle-icon').style.transform = 'rotate(180deg)';
    } else {
        toggleBtn.style.left = '280px';
        toggleBtn.querySelector('.toggle-icon').style.transform = 'rotate(0deg)';
    }
    
    // 更新main-content的左边距
    const mainContent = document.querySelector('.main-content');
    if (mainContent) {
        // 展开和收起时，主区域左侧留相同的间距（24px），减少按钮右侧空隙
        mainContent.style.marginLeft = '5px';
    }
    
    toggleBtn.addEventListener('click', function(e) {
        e.stopPropagation();
        const isCollapsed = sidebar.classList.contains('collapsed');
        const mainContent = document.querySelector('.main-content');
        
        if (isCollapsed) {
            sidebar.classList.remove('collapsed');
            toggleBtn.style.left = '280px';
            toggleBtn.querySelector('.toggle-icon').style.transform = 'rotate(0deg)';
            if (mainContent) mainContent.style.marginLeft = '24px';
        } else {
            sidebar.classList.add('collapsed');
            toggleBtn.style.left = '8px';
            toggleBtn.querySelector('.toggle-icon').style.transform = 'rotate(180deg)';
            if (mainContent) mainContent.style.marginLeft = '24px';
        }
        
        // 保存状态到localStorage
        localStorage.setItem('sidebarCollapsed', !isCollapsed);
        
        // 如果切换到时间轴视图，需要重新渲染以适应新的布局
        const activeTab = document.querySelector('.tab-btn.active');
        if (activeTab && activeTab.dataset.tab === 'timeline') {
            setTimeout(() => {
                renderTimeline();
            }, 300); // 等待动画完成
        }
    });
}

document.addEventListener("DOMContentLoaded", function() {
    // 初始化侧边栏收起/展开功能
    initSidebarToggle();
    setupZoom();
    loadStats();
    loadStockCodes();
    setupSliderListeners();
    reloadTimeline();
    document.getElementById("toggle-buy").addEventListener("change", function() {
        renderTimeline();
    });
    document.getElementById("toggle-sell-high").addEventListener("change", function() {
        renderTimeline();
    });
    document.getElementById("toggle-sell-low").addEventListener("change", function() {
        renderTimeline();
    });
    document.getElementById("toggle-show-name").addEventListener("change", function() {
        renderTimeline();
    });
    document.getElementById("toggle-show-profit").addEventListener("change", function() {
        renderTimeline();
    });
    
    // Add resize listener to re-render timeline when window size changes
    window.addEventListener("resize", function() {
        renderTimeline();
    });

    // 强制卖出勾选框联动（默认已勾选，天数框默认可用）
    const forceSellCheckbox = document.getElementById("filter-enable-force-sell");
    const forceSellDaysInput = document.getElementById("filter-force-sell-days");
    if (forceSellCheckbox && forceSellDaysInput) {
        forceSellDaysInput.disabled = !forceSellCheckbox.checked;
        forceSellCheckbox.addEventListener("change", function() {
            forceSellDaysInput.disabled = !this.checked;
            updateCalc();
        });
        forceSellDaysInput.addEventListener("change", function() {
            updateCalc();
        });
    }

    const reachRateInput = document.getElementById("filter-reach-rate");
    if (reachRateInput) {
        reachRateInput.addEventListener("input", function() {
            updateCalc();
        });
    }
});

// 数据列表相关变量和函数
let dataListCurrentPage = 1;
let dataListCurrentSort = 'created_at';
let dataListCurrentOrder = 'desc';

async function loadDataList(page = 1) {
    dataListCurrentPage = page;
    const loading = document.getElementById('data-list-loading');
    const tableWrapper = document.getElementById('data-list-table-wrapper');
    const table = document.getElementById('data-list-table');
    const empty = document.getElementById('data-list-empty');
    const tbody = document.getElementById('data-list-table-body');
    const pagination = document.getElementById('data-list-pagination');
    
    loading.style.display = 'flex';
    tableWrapper.style.display = 'none';
    empty.style.display = 'none';
    pagination.style.display = 'none';
    
    const params = new URLSearchParams({
        page: page,
        per_page: parseInt(document.getElementById('data-list-per-page').value) || 20,
        sort_by: dataListCurrentSort,
        order: dataListCurrentOrder
    });
    
    const stockCode = document.getElementById('data-list-stock-code').value;
    const stockName = document.getElementById('data-list-stock-name').value;
    const signalType = document.getElementById('data-list-signal-type').value;
    const minSuccessRate = document.getElementById('data-list-min-success-rate').value;
    const minSignalCount = document.getElementById('data-list-min-signal-count').value;
    const dateFrom = document.getElementById('data-list-date-from').value;
    const dateTo = document.getElementById('data-list-date-to').value;
    
    if (stockCode) params.append('stock_code', stockCode);
    if (stockName) params.append('stock_name', stockName);
    if (signalType) params.append('signal_type', signalType);
    if (minSuccessRate) params.append('min_success_rate', minSuccessRate);
    if (minSignalCount) params.append('min_signal_count', minSignalCount);
    if (dateFrom) params.append('date_from', dateFrom);
    if (dateTo) params.append('date_to', dateTo);
    
    try {
        const response = await fetch(API_BASE + `/api/signals?${params}`);
        const data = await response.json();
        
        loading.style.display = 'none';
        
        if (data.signals.length === 0) {
            empty.style.display = 'flex';
            return;
        }
        
        tableWrapper.style.display = 'block';
        tbody.innerHTML = '';
        
        data.signals.forEach(signal => {
            const row = document.createElement('tr');
            const signals = signal.signal ? signal.signal.split(',') : [];
            
            const successRate = signal.overall_success_rate || 0;
            let rateClass = 'low';
            if (successRate >= 70) rateClass = 'high';
            else if (successRate >= 50) rateClass = 'medium';
            
            const highestChange = signal.highest_change_rate || 0;
            const lowestChange = signal.lowest_change_rate || 0;
            const buyDayChange = signal.buy_day_change_rate != null ? signal.buy_day_change_rate : null;
            const nextDayChange = signal.next_day_change_rate != null ? signal.next_day_change_rate : null;
            
            // 辅助函数：判断值是否为0，并返回相应的CSS类
            const getZeroClass = (value) => {
                if (value === 0 || value === '0' || (typeof value === 'number' && Math.abs(value) < 0.01)) {
                    return ' zero-value';
                }
                return '';
            };
            
            const signalCount = signal.signal_count || 0;
            const insertPrice = signal.insert_price;
            const highestDays = signal.highest_days !== null ? signal.highest_days : null;
            const lowestDays = signal.lowest_days !== null ? signal.lowest_days : null;
            
            row.innerHTML = `
                <td>${signal.stock_code || '-'}</td>
                <td>${signal.stock_name || '-'}</td>
                <td class="signal-count-cell${getZeroClass(signalCount)}" data-signals="${signal.signal || ''}">${signalCount}</td>
                <td><span class="success-rate ${rateClass}${getZeroClass(successRate)}">${successRate.toFixed(2)}%</span></td>
                <td>${signal.insert_date ? signal.insert_date.split(' ')[0] : '-'}</td>
                <td class="${insertPrice != null && insertPrice === 0 ? getZeroClass(insertPrice) : ''}">${insertPrice != null ? insertPrice.toFixed(2) : '-'}</td>
                <td><span class="change-rate ${highestChange >= 0 ? 'positive' : 'negative'}${getZeroClass(highestChange)}">${highestChange >= 0 ? '+' : ''}${highestChange.toFixed(2)}%</span></td>
                <td>${signal.highest_price_date || '-'}</td>
                <td class="${getZeroClass(highestDays)}">${highestDays !== null ? highestDays + '天' : '-'}</td>
                <td><span class="change-rate ${lowestChange >= 0 ? 'positive' : 'negative'}${getZeroClass(lowestChange)}">${lowestChange >= 0 ? '+' : ''}${lowestChange.toFixed(2)}%</span></td>
                <td>${signal.lowest_price_date || '-'}</td>
                <td class="${getZeroClass(lowestDays)}">${lowestDays !== null ? lowestDays + '天' : '-'}</td>
                <td>${buyDayChange !== null ? `<span class="change-rate ${buyDayChange >= 0 ? 'positive' : 'negative'}${getZeroClass(buyDayChange)}">${buyDayChange >= 0 ? '+' : ''}${buyDayChange.toFixed(2)}%</span>` : '-'}</td>
                <td>${nextDayChange !== null ? `<span class="change-rate ${nextDayChange >= 0 ? 'positive' : 'negative'}${getZeroClass(nextDayChange)}">${nextDayChange >= 0 ? '+' : ''}${nextDayChange.toFixed(2)}%</span>` : '-'}</td>
            `;
            
            // 为信号数列添加tooltip
            const signalCountCell = row.querySelector('.signal-count-cell');
            if (signalCountCell && signals.length > 0) {
                signalCountCell.style.cursor = 'help';
                
                // 添加鼠标事件
                signalCountCell.addEventListener('mouseenter', function(e) {
                    const allSignals = signals.map(s => s.trim());
                    const html = `<div style="font-weight: 600; margin-bottom: 8px; color: #fff;">信号列表 (${signals.length}个)</div><div style="display: flex; flex-wrap: wrap; gap: 6px; max-width: 400px;">${allSignals.map(s => `<span style="background: rgba(255,255,255,0.2); padding: 4px 8px; border-radius: 4px; font-size: 12px; white-space: nowrap;">${s}</span>`).join('')}</div>`;
                    showTooltip(html, e.clientX, e.clientY);
                });
                
                signalCountCell.addEventListener('mouseleave', function() {
                    hideTooltipDelayed();
                });
                
                signalCountCell.addEventListener('mousemove', function(e) {
                    if (tooltipEl && tooltipEl.style.display === 'block') {
                        tooltipEl.style.left = e.clientX + 12 + "px";
                        tooltipEl.style.top = e.clientY + 12 + "px";
                    }
                });
            }
            
            tbody.appendChild(row);
        });
        
        updateDataListPagination(data);
        updateDataListSortHeaders();
    } catch (error) {
        console.error('加载数据失败:', error);
        loading.style.display = 'none';
        empty.style.display = 'block';
        empty.textContent = '加载失败，请刷新页面重试';
    }
}

function updateDataListPagination(data) {
    const pagination = document.getElementById('data-list-pagination');
    if (data.total_pages <= 1) {
        pagination.style.display = 'none';
        return;
    }
    
    pagination.style.display = 'flex';
    
    // 计算页码范围
    const maxPages = 10;
    let startPage = Math.max(1, dataListCurrentPage - Math.floor(maxPages / 2));
    let endPage = Math.min(data.total_pages, startPage + maxPages - 1);
    if (endPage - startPage < maxPages - 1) {
        startPage = Math.max(1, endPage - maxPages + 1);
    }
    
    let paginationHTML = `
        <button ${dataListCurrentPage === 1 ? 'disabled' : ''} onclick="loadDataList(1)">首页</button>
        <button ${dataListCurrentPage === 1 ? 'disabled' : ''} onclick="loadDataList(${dataListCurrentPage - 1})">上一页</button>
    `;
    
    if (startPage > 1) {
        paginationHTML += `<button onclick="loadDataList(1)">1</button>`;
        if (startPage > 2) {
            paginationHTML += `<span class="page-info">...</span>`;
        }
    }
    
    for (let i = startPage; i <= endPage; i++) {
        paginationHTML += `
            <button ${i === dataListCurrentPage ? 'style="background: #667eea; color: white; border-color: #667eea;"' : ''} onclick="loadDataList(${i})">${i}</button>
        `;
    }
    
    if (endPage < data.total_pages) {
        if (endPage < data.total_pages - 1) {
            paginationHTML += `<span class="page-info">...</span>`;
        }
        paginationHTML += `<button onclick="loadDataList(${data.total_pages})">${data.total_pages}</button>`;
    }
    
    paginationHTML += `
        <button ${dataListCurrentPage === data.total_pages ? 'disabled' : ''} onclick="loadDataList(${dataListCurrentPage + 1})">下一页</button>
        <button ${dataListCurrentPage === data.total_pages ? 'disabled' : ''} onclick="loadDataList(${data.total_pages})">末页</button>
        <span class="page-info">第 ${dataListCurrentPage} / ${data.total_pages} 页 (共 ${data.total} 条)</span>
    `;
    
    pagination.innerHTML = paginationHTML;
}

function sortDataList(column) {
    if (dataListCurrentSort === column) {
        dataListCurrentOrder = dataListCurrentOrder === 'asc' ? 'desc' : 'asc';
    } else {
        dataListCurrentSort = column;
        dataListCurrentOrder = 'desc';
    }
    loadDataList(1);
}

function updateDataListSortHeaders() {
    document.querySelectorAll('#data-list-table th.sortable').forEach(th => {
        th.classList.remove('sort-asc', 'sort-desc');
    });
    const header = Array.from(document.querySelectorAll('#data-list-table th')).find(th => 
        th.getAttribute('onclick') && th.getAttribute('onclick').includes(dataListCurrentSort)
    );
    if (header) {
        header.classList.add(`sort-${dataListCurrentOrder}`);
    }
}

async function loadDataListFilterOptions() {
    try {
        const response = await fetch(API_BASE + '/api/filter-options');
        const data = await response.json();
        
        // 加载股票代码下拉框（只显示数据库中的）
        const stockCodeSelect = document.getElementById('data-list-stock-code');
        const currentStockCode = stockCodeSelect.value;
        // 清空并只保留"全部"选项
        stockCodeSelect.innerHTML = '<option value="">全部</option>';
        // 只添加数据库中实际存在的股票代码
        if (data.stock_codes && Array.isArray(data.stock_codes)) {
            data.stock_codes.forEach(item => {
                if (item.code) { // 确保代码不为空
                    const option = document.createElement('option');
                    option.value = item.code;
                    option.textContent = item.code + (item.name ? ' - ' + item.name : '');
                    stockCodeSelect.appendChild(option);
                }
            });
        }
        if (currentStockCode) {
            stockCodeSelect.value = currentStockCode;
        }
        
        // 加载股票名称下拉框（只显示数据库中的）
        const stockNameSelect = document.getElementById('data-list-stock-name');
        const currentStockName = stockNameSelect.value;
        stockNameSelect.innerHTML = '<option value="">全部</option>';
        // 只添加数据库中实际存在的股票名称
        if (data.stock_names && Array.isArray(data.stock_names)) {
            data.stock_names.forEach(name => {
                if (name && name.trim()) { // 确保名称不为空
                    const option = document.createElement('option');
                    option.value = name;
                    option.textContent = name;
                    stockNameSelect.appendChild(option);
                }
            });
        }
        if (currentStockName) {
            stockNameSelect.value = currentStockName;
        }
        
        // 加载信号类型下拉框（只显示数据库中的）
        const signalTypeSelect = document.getElementById('data-list-signal-type');
        const currentSignalType = signalTypeSelect.value;
        signalTypeSelect.innerHTML = '<option value="">全部</option>';
        // 只添加数据库中实际存在的信号类型
        if (data.signal_types && Array.isArray(data.signal_types)) {
            data.signal_types.forEach(signal => {
                if (signal && signal.trim()) { // 确保信号不为空
                    const option = document.createElement('option');
                    option.value = signal;
                    option.textContent = signal;
                    signalTypeSelect.appendChild(option);
                }
            });
        }
        if (currentSignalType) {
            signalTypeSelect.value = currentSignalType;
        }
    } catch (error) {
        console.error('加载筛选选项失败:', error);
    }
}

function resetDataListFilters() {
    document.getElementById('data-list-stock-code').value = '';
    document.getElementById('data-list-stock-name').value = '';
    document.getElementById('data-list-signal-type').value = '';
    document.getElementById('data-list-min-success-rate').value = '';
    document.getElementById('data-list-min-signal-count').value = '';
    document.getElementById('data-list-date-from').value = '';
    document.getElementById('data-list-date-to').value = '';
    document.getElementById('data-list-sort-by').value = 'created_at';
    document.getElementById('data-list-sort-order').value = 'desc';
    document.getElementById('data-list-per-page').value = '50';
    dataListCurrentSort = 'created_at';
    dataListCurrentOrder = 'desc';
    loadDataList(1);
}

