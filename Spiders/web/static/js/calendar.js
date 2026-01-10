let records = [];
let selectedGroupId = null;
let selectedRecord = null;
let tooltipEl = null;
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
    document.body.appendChild(el);
    tooltipEl = el;
    return el;
}
function showTooltip(html, x, y) {
    const el = createTooltip();
    el.innerHTML = html;
    el.style.left = x + 12 + "px";
    el.style.top = y + 12 + "px";
    el.style.display = "block";
}
function hideTooltip() {
    if (tooltipEl) {
        tooltipEl.style.display = "none";
    }
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
        hideTooltip();
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
function calculateProfitForStock(rec, profitTarget, stopLoss) {
    if (!rec.buyPrice) return null;
    const buyPrice = rec.buyPrice;
    const highPrice = rec.highPrice;
    const lowPrice = rec.lowPrice;
    const highDays = rec.highDays || 999999;
    const lowDays = rec.lowDays || 999999;
    const profitPrice = buyPrice * (1 + profitTarget / 100);
    const lossPrice = buyPrice * (1 + stopLoss / 100);
    const canReachProfit = highPrice && highPrice >= profitPrice;
    const canReachLoss = lowPrice && lowPrice <= lossPrice;
    if (!canReachProfit && !canReachLoss) {
        return null;
    }
    let firstHit = null;
    let firstHitDays = 999999;
    if (canReachProfit && highDays < firstHitDays) {
        firstHit = "profit";
        firstHitDays = highDays;
    }
    if (canReachLoss && lowDays < firstHitDays) {
        firstHit = "loss";
        firstHitDays = lowDays;
    }
    if (firstHit === "profit") {
        return { profit: profitTarget, days: firstHitDays, type: "profit" };
    } else if (firstHit === "loss") {
        return { profit: stopLoss, days: firstHitDays, type: "loss" };
    }
    return null;
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
    document.getElementById("filter-confidence").value = "80";
    document.getElementById("filter-buy-day-change-min").disabled = true;
    document.getElementById("filter-buy-day-change-max").disabled = true;
    document.getElementById("filter-next-day-change-min").disabled = true;
    document.getElementById("filter-next-day-change-max").disabled = true;
    updateSliderValue("filter-buy-day-change-min", "filter-buy-day-change-min-value");
    updateSliderValue("filter-buy-day-change-max", "filter-buy-day-change-max-value");
    updateSliderValue("filter-next-day-change-min", "filter-next-day-change-min-value");
    updateSliderValue("filter-next-day-change-max", "filter-next-day-change-max-value");
    updateCalc();
}
function updateCalc() {
    const filteredRecords = getFilteredRecords();
    const profitTargets = [];
    const stopLosses = [];
    for (let i = 2; i <= 30; i += 2) {
        profitTargets.push(i);
    }
    for (let i = -2; i >= -30; i -= 2) {
        stopLosses.push(i);
    }
    const thead = document.getElementById("calc-table-header");
    const tbody = document.getElementById("calc-table-body");
    thead.innerHTML = "";
    tbody.innerHTML = "";
    const headerRow = document.createElement("tr");
    headerRow.innerHTML = "<th>止损\\止盈</th>";
    profitTargets.forEach(pt => {
        const th = document.createElement("th");
        th.className = "text-center";
        th.textContent = "+" + pt + "%";
        headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);
    let totalStocks = 0;
    filteredRecords.forEach(rec => {
        if (rec.buyPrice) totalStocks++;
    });
    document.getElementById("calc-total-stocks").textContent = totalStocks;
    const results = [];
    const stockDetails = [];
    stopLosses.forEach((sl, slIdx) => {
        const row = document.createElement("tr");
        const labelCell = document.createElement("td");
        labelCell.textContent = sl + "%";
        labelCell.style.fontWeight = "600";
        row.appendChild(labelCell);
        const rowResults = [];
        const rowDetails = [];
        profitTargets.forEach((pt, ptIdx) => {
            let totalProfit = 0;
            let count = 0;
            let profitCount = 0;
            let lossCount = 0;
            const stocks = [];
            filteredRecords.forEach(rec => {
                const result = calculateProfitForStock(rec, pt, sl);
                if (result !== null) {
                    totalProfit += result.profit;
                    count++;
                    if (result.type === "profit") {
                        profitCount++;
                    } else {
                        lossCount++;
                    }
                    stocks.push({
                        code: rec.stockCode || "-",
                        name: rec.stockName || "-",
                        profit: result.profit,
                        days: result.days,
                        type: result.type
                    });
                }
            });
            const avgProfit = count > 0 ? totalProfit / count : null;
            rowResults.push(avgProfit);
            rowDetails.push(stocks);
            const cell = document.createElement("td");
            cell.style.cursor = "pointer";
            cell.title = "";
            if (avgProfit !== null) {
                const hitRate = totalStocks > 0 ? (profitCount + lossCount) / totalStocks : 0;
                const confidenceFilter = parseFloat(document.getElementById("filter-confidence").value) || 80;
                const meetsConfidence = hitRate * 100 >= confidenceFilter;
                cell.className = "text-center " + (avgProfit >= 0 ? "profit-positive" : "profit-negative");
                if (meetsConfidence) {
                    cell.style.backgroundColor = "#e1bee7";
                }
                const profitSpan = '<span style="color:#b71c1c;opacity:0.7;font-size:11px;">' + profitCount + '</span>';
                const lossSpan = '<span style="color:#1b5e20;opacity:0.7;font-size:11px;">' + lossCount + '</span>';
                cell.innerHTML = (avgProfit > 0 ? "+" : "") + avgProfit.toFixed(2) + "% (" + profitSpan + ", " + lossSpan + ")";
                cell.dataset.stocks = JSON.stringify(stocks);
                cell.addEventListener("mouseenter", function(e) {
                    const stocksData = JSON.parse(this.dataset.stocks || "[]");
                    if (stocksData.length > 0) {
                        let html = '<div class="tooltip-title">股票详情（' + stocksData.length + "只）</div>";
                        html += '<div class="tooltip-stocks">';
                        const displayCount = Math.min(9, stocksData.length);
                        for (let i = 0; i < displayCount; i++) {
                            const s = stocksData[i];
                            html += '<div class="tooltip-stock-item">';
                            html += '<div class="tooltip-stock-code">' + s.code + "</div>";
                            html += '<div class="tooltip-stock-name">' + (s.name || "-") + "</div>";
                            html += '<div class="tooltip-stock-info">持有: ' + s.days + " 天</div>";
                            html += '<div class="tooltip-stock-info" style="color:' + (s.profit >= 0 ? "#e53935" : "#43a047") + ';">收益: ' + (s.profit > 0 ? "+" : "") + s.profit.toFixed(2) + "%</div>";
                            html += '<div class="tooltip-stock-info">' + (s.type === "profit" ? "止盈" : "止损") + "</div>";
                            html += "</div>";
                        }
                        html += "</div>";
                        if (stocksData.length > 9) {
                            html += '<div style="margin-top:8px;font-size:11px;color:#999;">还有 ' + (stocksData.length - 9) + " 只股票，滚动查看</div>";
                        }
                        showTooltip(html, e.clientX, e.clientY);
                    }
                });
                cell.addEventListener("mouseleave", function() {
                    hideTooltip();
                });
            } else {
                cell.className = "text-center";
                cell.textContent = "-";
            }
            row.appendChild(cell);
        });
        results.push(rowResults);
        stockDetails.push(rowDetails);
        tbody.appendChild(row);
    });
    const allResults = [];
    results.forEach((row, r) => {
        row.forEach((val, c) => {
            if (val !== null && typeof val === 'number') {
                allResults.push({ value: val, row: r, col: c });
            }
        });
    });
    allResults.sort((a, b) => b.value - a.value);
    const top10 = allResults.slice(0, 10);
    const bottom10 = allResults.slice(-10);
    const rows = tbody.querySelectorAll("tr");
    results.forEach((row, r) => {
        const cells = rows[r].querySelectorAll("td");
        row.forEach((val, c) => {
            const cell = cells[c + 1];
            const isTop10 = top10.some(item => item.row === r && item.col === c);
            const isBottom10 = bottom10.some(item => item.row === r && item.col === c);
            const currentBg = cell.style.backgroundColor;
            const isHighHitRate = currentBg === "rgb(225, 190, 231)" || currentBg === "#e1bee7";
            if (isTop10 && !isHighHitRate) {
                cell.style.backgroundColor = "#fff3cd";
                cell.style.fontWeight = "700";
            } else if (isBottom10 && !isHighHitRate) {
                cell.style.backgroundColor = "#f8d7da";
                cell.style.fontWeight = "700";
            } else if (isHighHitRate) {
                cell.style.fontWeight = "600";
            }
        });
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
        const res = await fetch("/api/stats");
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
async function loadStockCodes() {
    try {
        const res = await fetch("/api/stock-codes");
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
async function reloadTimeline() {
    const stockCode = document.getElementById("filter-stock-code").value;
    const dateFrom = document.getElementById("filter-date-from").value;
    const dateTo = document.getElementById("filter-date-to").value;
    const params = new URLSearchParams();
    if (stockCode) params.append("stock_code", stockCode);
    if (dateFrom) params.append("date_from", dateFrom);
    if (dateTo) params.append("date_to", dateTo);
    const res = await fetch("/api/calendar/events?" + params.toString());
    const data = await res.json();
    const events = data.events || [];
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
            nextDayChangeRate: e.next_day_change_rate != null ? Number(e.next_day_change_rate) : null
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
document.addEventListener("DOMContentLoaded", function() {
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
        const response = await fetch(`/api/signals?${params}`);
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
            
            row.innerHTML = `
                <td>${signal.stock_code || '-'}</td>
                <td>${signal.stock_name || '-'}</td>
                <td class="signal-count-cell" data-signals="${signal.signal || ''}">${signal.signal_count || 0}</td>
                <td><span class="success-rate ${rateClass}">${successRate.toFixed(2)}%</span></td>
                <td>${signal.insert_date ? signal.insert_date.split(' ')[0] : '-'}</td>
                <td>${signal.insert_price ? signal.insert_price.toFixed(2) : '-'}</td>
                <td><span class="change-rate ${highestChange >= 0 ? 'positive' : 'negative'}">${highestChange >= 0 ? '+' : ''}${highestChange.toFixed(2)}%</span></td>
                <td>${signal.highest_price_date || '-'}</td>
                <td>${signal.highest_days !== null ? signal.highest_days + '天' : '-'}</td>
                <td><span class="change-rate ${lowestChange >= 0 ? 'positive' : 'negative'}">${lowestChange >= 0 ? '+' : ''}${lowestChange.toFixed(2)}%</span></td>
                <td>${signal.lowest_price_date || '-'}</td>
                <td>${signal.lowest_days !== null ? signal.lowest_days + '天' : '-'}</td>
                <td>${buyDayChange !== null ? `<span class="change-rate ${buyDayChange >= 0 ? 'positive' : 'negative'}">${buyDayChange >= 0 ? '+' : ''}${buyDayChange.toFixed(2)}%</span>` : '-'}</td>
                <td>${nextDayChange !== null ? `<span class="change-rate ${nextDayChange >= 0 ? 'positive' : 'negative'}">${nextDayChange >= 0 ? '+' : ''}${nextDayChange.toFixed(2)}%</span>` : '-'}</td>
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
                    hideTooltip();
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
        const response = await fetch('/api/filter-options');
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

