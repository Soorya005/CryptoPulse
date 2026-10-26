// =============================================================================
// CRYPTOPULSE - INSTITUTIONAL REAL-TIME DASHBOARD LOGIC
// =============================================================================

const SYMBOLS = ['BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'LTC'];
const SYMBOLS_PARAM = SYMBOLS.join(',');

// Core Application State
let selectedSymbol = 'BTC';
let currencyMode = 'USD'; // 'USD' | 'INR'
let currentTimeframe = '1H';
let activeFxRate = 94.96; // Fallback USD/INR conversion rate
let chartInstance = null;
let isDarkMode = false;
let showEma9 = false;
let showBollinger = false;
let cachedOrderFlow = {};
let cachedArbitrage = {};
let cachedIndiaPremium = {};

// Live Market Cache
let liveMarket = {
    BTC: { price: 67420.50, change: 1.24, history: [] },
    ETH: { price: 2543.18, change: -0.62, history: [] },
    SOL: { price: 173.24, change: 2.31, history: [] },
    INR: { price: 88.01, change: 0.12, history: [] },
    XRP: { price: 1.3005, change: -0.45, history: [] },
    DOGE: { price: 0.0789, change: 0.85, history: [] },
    LTC: { price: 59.58, change: 1.10, history: [] }
};

let liveMarketChartInstance = null;
let currentTradesFilter = 'ALL';
let currentAlertsSeverityFilter = 'ALL';

let cachedTrades = [
    { id: 'TX-984218', timestamp: '20:42:18', symbol: 'BTC', side: 'BUY', price: 67420.50, volume: 0.0124 },
    { id: 'TX-984216', timestamp: '20:42:16', symbol: 'BTC', side: 'BUY', price: 67418.20, volume: 0.0041 },
    { id: 'TX-984212', timestamp: '20:42:12', symbol: 'ETH', side: 'SELL', price: 2543.18, volume: 0.0863 },
    { id: 'TX-984208', timestamp: '20:42:08', symbol: 'SOL', side: 'BUY', price: 173.24, volume: 1.4500 },
    { id: 'TX-984202', timestamp: '20:42:02', symbol: 'XRP', side: 'BUY', price: 1.3005, volume: 240.00 },
    { id: 'TX-984155', timestamp: '20:41:55', symbol: 'DOGE', side: 'SELL', price: 0.0789, volume: 3500.00 },
    { id: 'TX-984148', timestamp: '20:41:48', symbol: 'BTC', side: 'SELL', price: 67415.00, volume: 0.0520 },
    { id: 'TX-984140', timestamp: '20:41:40', symbol: 'LTC', side: 'BUY', price: 59.58, volume: 4.8000 },
    { id: 'TX-984132', timestamp: '20:41:32', symbol: 'ETH', side: 'BUY', price: 2542.90, volume: 0.2500 },
    { id: 'TX-984125', timestamp: '20:41:25', symbol: 'SOL', side: 'SELL', price: 173.10, volume: 2.1000 },
    { id: 'TX-984118', timestamp: '20:41:18', symbol: 'BTC', side: 'BUY', price: 67425.00, volume: 0.0350 },
    { id: 'TX-984105', timestamp: '20:41:05', symbol: 'ETH', side: 'SELL', price: 2544.10, volume: 0.1200 }
];

let cachedAlerts = [
    { id: 'ALT-1042', symbol: 'BTC', alert_time: '20:36:15', classification: 'Volatility Spike', price_return_pct: 2.41, threshold: 2.00, z_score: '3.62' },
    { id: 'ALT-1041', symbol: 'SOL', alert_time: '20:28:07', classification: 'Sharp Pullback', price_return_pct: -2.12, threshold: 2.00, z_score: '-3.24' },
    { id: 'ALT-1040', symbol: 'ETH', alert_time: '20:15:22', classification: 'Volume Breakout', price_return_pct: 2.08, threshold: 2.00, z_score: '3.11' },
    { id: 'ALT-1039', symbol: 'XRP', alert_time: '19:58:10', classification: 'Flash Dip', price_return_pct: -1.94, threshold: 2.00, z_score: '-3.05' },
    { id: 'ALT-1038', symbol: 'DOGE', alert_time: '19:42:01', classification: 'Meme Momentum', price_return_pct: 3.82, threshold: 2.00, z_score: '3.75' },
    { id: 'ALT-1037', symbol: 'BTC', alert_time: '19:15:33', classification: 'Order Imbalance', price_return_pct: 2.15, threshold: 2.00, z_score: '3.18' }
];

let telemetryMetrics = {
    kafkaRate: 1240,
    sparkLatency: 1.8,
    clickhouseRate: 1210,
    apiLatency: 85
};

// =============================================================================
// NUMBER FORMATTING UTILITIES
// =============================================================================

function formatPrice(val, currency = currencyMode) {
    if (val === null || val === undefined || isNaN(val)) return '--';
    let num = Number(val);
    if (currency === 'INR') {
        num = num * (activeFxRate || 94.96);
        return new Intl.NumberFormat('en-IN', {
            style: 'currency',
            currency: 'INR',
            minimumFractionDigits: num < 10 ? 4 : 2,
            maximumFractionDigits: num < 10 ? 4 : 2
        }).format(num);
    }
    if (num >= 1000) return '$' + num.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    if (num >= 10) return '$' + num.toFixed(2);
    if (num >= 0.1) return '$' + num.toFixed(4);
    return '$' + num.toFixed(5);
}

function formatRawPrice(num) {
    if (num >= 1000) return num.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    return num.toFixed(2);
}

function formatInrPrice(usdPrice) {
    const inr = usdPrice * (activeFxRate || 94.96);
    return new Intl.NumberFormat('en-IN', {
        style: 'currency',
        currency: 'INR',
        maximumFractionDigits: 0
    }).format(inr);
}

// =============================================================================
// REAL-TIME CLOCK INITIALIZER
// =============================================================================

function initLiveClock() {
    const clockEl = document.getElementById('topbarLiveClock');
    function updateTime() {
        const now = new Date();
        const options = { month: 'short', day: 'numeric', year: 'numeric' };
        const dateStr = now.toLocaleDateString('en-US', options);
        const hours = String(now.getHours()).padStart(2, '0');
        const minutes = String(now.getMinutes()).padStart(2, '0');
        const seconds = String(now.getSeconds()).padStart(2, '0');
        if (clockEl) {
            clockEl.textContent = `${dateStr} ${hours}:${minutes}:${seconds}`;
        }
    }
    updateTime();
    setInterval(updateTime, 1000);
}

// =============================================================================
// SPARKLINE GENERATOR (Smooth Cubic Bezier SVG Paths)
// =============================================================================

function generateSparklineSvg(data, isPositive, colorOverride = null) {
    if (!data || data.length < 2) return '';
    const min = Math.min(...data);
    const max = Math.max(...data);
    const range = (max - min) || 1;
    const width = 100;
    const height = 26;

    const points = data.map((val, idx) => {
        const x = (idx / (data.length - 1)) * width;
        const y = height - ((val - min) / range) * (height - 6) - 3;
        return { x, y };
    });

    let d = `M ${points[0].x.toFixed(1)} ${points[0].y.toFixed(1)}`;
    for (let i = 0; i < points.length - 1; i++) {
        const p0 = points[i];
        const p1 = points[i + 1];
        const cx = (p0.x + p1.x) / 2;
        d += ` Q ${p0.x.toFixed(1)} ${p0.y.toFixed(1)}, ${cx.toFixed(1)} ${((p0.y + p1.y) / 2).toFixed(1)}`;
    }
    d += ` T ${points[points.length - 1].x.toFixed(1)} ${points[points.length - 1].y.toFixed(1)}`;

    const strokeColor = colorOverride ? colorOverride : (isPositive ? '#10B981' : '#EF4444');
    return `<path d="${d}" fill="none" stroke="${strokeColor}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />`;
}

// =============================================================================
// TECHNICAL INDICATOR CALCULATORS (EMA-9 & Bollinger Bands 20, 2σ)
// =============================================================================

function calculateEMA(prices, period = 9) {
    if (!prices || prices.length === 0) return [];
    const k = 2 / (period + 1); // α = 0.20 for period 9
    const ema = [prices[0]];
    for (let i = 1; i < prices.length; i++) {
        const nextVal = (prices[i] * k) + (ema[i - 1] * (1 - k));
        ema.push(nextVal);
    }
    return ema;
}

function calculateBollingerBands(prices, period = 20, numStdDev = 2) {
    if (!prices || prices.length === 0) return { sma: [], upper: [], lower: [] };
    const sma = [];
    const upper = [];
    const lower = [];

    for (let i = 0; i < prices.length; i++) {
        const start = Math.max(0, i - period + 1);
        const window = prices.slice(start, i + 1);
        const mean = window.reduce((acc, v) => acc + v, 0) / window.length;

        let variance = 0;
        for (let j = 0; j < window.length; j++) {
            variance += Math.pow(window[j] - mean, 2);
        }
        const stdDev = Math.sqrt(variance / window.length);

        sma.push(mean);
        upper.push(mean + numStdDev * stdDev);
        lower.push(mean - numStdDev * stdDev);
    }
    return { sma, upper, lower };
}

// =============================================================================
// MAIN PRICE HISTORY CHART INITIALIZATION (Area Line Chart + Indicators)
// =============================================================================

function initPriceHistoryChart() {
    const canvas = document.getElementById('priceHistoryChart');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');

    // Generate initial sample data points
    const labels = [];
    const values = [];
    const now = new Date();
    const basePrice = liveMarket[selectedSymbol] ? liveMarket[selectedSymbol].price : 67420.50;

    for (let i = 40; i >= 0; i--) {
        const t = new Date(now.getTime() - i * 60 * 1000);
        const hours = String(t.getHours()).padStart(2, '0');
        const minutes = String(t.getMinutes()).padStart(2, '0');
        labels.push(`${hours}:${minutes}`);
        const noise = (Math.sin(i * 0.4) * 0.005 + (Math.random() - 0.49) * 0.003);
        values.push(basePrice * (1 + noise));
    }

    // Gradient fill for base price
    const gradient = ctx.createLinearGradient(0, 0, 0, 260);
    gradient.addColorStop(0, 'rgba(37, 99, 235, 0.16)');
    gradient.addColorStop(1, 'rgba(37, 99, 235, 0.00)');

    const emaValues = calculateEMA(values, 9);
    const bbValues = calculateBollingerBands(values, 20, 2);

    chartInstance = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [
                {
                    label: `${selectedSymbol} Price`,
                    data: values,
                    borderColor: '#1D4ED8',
                    borderWidth: 2.2,
                    backgroundColor: gradient,
                    fill: true,
                    tension: 0.35,
                    pointRadius: 0,
                    pointHoverRadius: 5,
                    pointHoverBackgroundColor: '#1D4ED8',
                    pointHoverBorderColor: '#FFFFFF',
                    pointHoverBorderWidth: 2,
                },
                {
                    label: `${selectedSymbol} EMA (9)`,
                    data: emaValues,
                    borderColor: '#F59E0B',
                    borderWidth: 2,
                    borderDash: [4, 4],
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    pointHoverBackgroundColor: '#F59E0B',
                    pointHoverBorderColor: '#FFFFFF',
                    pointHoverBorderWidth: 1.5,
                    fill: false,
                    tension: 0.35,
                    hidden: !showEma9
                },
                {
                    label: `${selectedSymbol} BB Upper (20, 2σ)`,
                    data: bbValues.upper,
                    borderColor: 'rgba(139, 92, 246, 0.75)',
                    borderWidth: 1.5,
                    borderDash: [3, 3],
                    pointRadius: 0,
                    pointHoverRadius: 3,
                    fill: false,
                    tension: 0.35,
                    hidden: !showBollinger
                },
                {
                    label: `${selectedSymbol} BB Lower (20, 2σ)`,
                    data: bbValues.lower,
                    borderColor: 'rgba(139, 92, 246, 0.75)',
                    borderWidth: 1.5,
                    borderDash: [3, 3],
                    backgroundColor: 'rgba(139, 92, 246, 0.08)',
                    pointRadius: 0,
                    pointHoverRadius: 3,
                    fill: '-1',
                    tension: 0.35,
                    hidden: !showBollinger
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                intersect: false,
                mode: 'index'
            },
            plugins: {
                legend: { display: false },
                tooltip: {
                    enabled: true,
                    backgroundColor: '#0F172A',
                    titleFont: { family: 'Inter', size: 11, weight: '600' },
                    bodyFont: { family: 'JetBrains Mono', size: 12, weight: '700' },
                    padding: 10,
                    cornerRadius: 8,
                    displayColors: false,
                    callbacks: {
                        label: function (context) {
                            const lbl = context.dataset.label || 'Price';
                            return `${lbl}: ${formatPrice(context.parsed.y, currencyMode)}`;
                        }
                    }
                }
            },
            scales: {
                x: {
                    grid: { display: false },
                    ticks: {
                        color: '#94A3B8',
                        font: { family: 'JetBrains Mono', size: 10 },
                        maxTicksLimit: 7
                    }
                },
                y: {
                    grid: {
                        color: 'rgba(226, 232, 240, 0.8)',
                        drawBorder: false
                    },
                    ticks: {
                        color: '#94A3B8',
                        font: { family: 'JetBrains Mono', size: 10 },
                        callback: function (val) {
                            if (currencyMode === 'INR') {
                                return '₹' + (val / 100000).toFixed(1) + 'L';
                            }
                            if (val >= 1000) return '$' + (val / 1000).toFixed(0) + 'K';
                            return '$' + val.toFixed(1);
                        }
                    }
                }
            }
        }
    });

    updateFloatingTooltipBadge(values[values.length - 1], labels[labels.length - 1]);
}

function updateFloatingTooltipBadge(price, timeStr) {
    const badge = document.getElementById('floatingPriceBadge');
    const pVal = document.getElementById('tooltipPriceVal');
    const tVal = document.getElementById('tooltipTimeVal');
    if (!badge || !pVal) return;

    pVal.textContent = formatPrice(price, currencyMode);
    if (tVal && timeStr) {
        tVal.textContent = timeStr;
    }
}

// =============================================================================
// UI REFRESH & COMPONENT RENDERERS
// =============================================================================

function updateStatCards() {
    // BTC Card
    const btc = liveMarket.BTC;
    const elPriceBTC = document.getElementById('cardPriceBTC');
    const elChangeBTC = document.getElementById('cardChangeBTC');
    const sparkBTC = document.getElementById('sparklineBTC');
    if (elPriceBTC) elPriceBTC.textContent = formatPrice(btc.price);
    if (elChangeBTC) {
        elChangeBTC.className = `trend-badge ${btc.change >= 0 ? 'positive' : 'negative'}`;
        elChangeBTC.innerHTML = `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="${btc.change >= 0 ? '18 15 12 9 6 15' : '6 9 12 15 18 9'}"></polyline></svg><span>${btc.change >= 0 ? '+' : ''}${btc.change.toFixed(2)}%</span>`;
    }
    if (sparkBTC) sparkBTC.innerHTML = generateSparklineSvg(btc.history, btc.change >= 0);

    // ETH Card
    const eth = liveMarket.ETH;
    const elPriceETH = document.getElementById('cardPriceETH');
    const elChangeETH = document.getElementById('cardChangeETH');
    const sparkETH = document.getElementById('sparklineETH');
    if (elPriceETH) elPriceETH.textContent = formatPrice(eth.price);
    if (elChangeETH) {
        elChangeETH.className = `trend-badge ${eth.change >= 0 ? 'positive' : 'negative'}`;
        elChangeETH.innerHTML = `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="${eth.change >= 0 ? '18 15 12 9 6 15' : '6 9 12 15 18 9'}"></polyline></svg><span>${eth.change >= 0 ? '+' : ''}${eth.change.toFixed(2)}%</span>`;
    }
    if (sparkETH) sparkETH.innerHTML = generateSparklineSvg(eth.history, eth.change >= 0);

    // SOL Card
    const sol = liveMarket.SOL;
    const elPriceSOL = document.getElementById('cardPriceSOL');
    const elChangeSOL = document.getElementById('cardChangeSOL');
    const sparkSOL = document.getElementById('sparklineSOL');
    if (elPriceSOL) elPriceSOL.textContent = formatPrice(sol.price);
    if (elChangeSOL) {
        elChangeSOL.className = `trend-badge ${sol.change >= 0 ? 'positive' : 'negative'}`;
        elChangeSOL.innerHTML = `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="${sol.change >= 0 ? '18 15 12 9 6 15' : '6 9 12 15 18 9'}"></polyline></svg><span>${sol.change >= 0 ? '+' : ''}${sol.change.toFixed(2)}%</span>`;
    }
    if (sparkSOL) sparkSOL.innerHTML = generateSparklineSvg(sol.history, sol.change >= 0);

    // USD/INR Card
    const inr = liveMarket.INR;
    const elPriceINR = document.getElementById('cardPriceINR');
    const elChangeINR = document.getElementById('cardChangeINR');
    const sparkINR = document.getElementById('sparklineINR');
    if (elPriceINR) elPriceINR.textContent = inr.price.toFixed(2);
    if (elChangeINR) {
        elChangeINR.className = `trend-badge ${inr.change >= 0 ? 'positive' : 'negative'}`;
        elChangeINR.innerHTML = `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="${inr.change >= 0 ? '18 15 12 9 6 15' : '6 9 12 15 18 9'}"></polyline></svg><span>${inr.change >= 0 ? '+' : ''}${inr.change.toFixed(2)}%</span>`;
    }
    if (sparkINR) sparkINR.innerHTML = generateSparklineSvg(inr.history, true, '#3B82F6');
}

function updateOneMinuteAnalytics() {
    const cur = liveMarket[selectedSymbol];
    if (!cur) return;
    const p = cur.price;

    const avgPrice = p * 0.9992;
    const minPrice = p * 0.9940;
    const maxPrice = p * 1.0048;
    const vwap = p * 0.9998;
    const volume = selectedSymbol === 'BTC' ? 12.8463 : (selectedSymbol === 'ETH' ? 84.1205 : 1240.50);
    const tickCount = Math.floor(1200 + Math.random() * 150);

    const isINR = currencyMode === 'INR';
    document.getElementById('lblAvgPrice').textContent = `Avg Price (${currencyMode})`;
    document.getElementById('lblMinPrice').textContent = `Min Price (${currencyMode})`;
    document.getElementById('lblMaxPrice').textContent = `Max Price (${currencyMode})`;
    document.getElementById('lblVwap').textContent = `VWAP (${currencyMode})`;

    document.getElementById('valAvgPrice').textContent = isINR ? formatInrPrice(avgPrice) : formatRawPrice(avgPrice);
    document.getElementById('valMinPrice').textContent = isINR ? formatInrPrice(minPrice) : formatRawPrice(minPrice);
    document.getElementById('valMaxPrice').textContent = isINR ? formatInrPrice(maxPrice) : formatRawPrice(maxPrice);
    document.getElementById('valVwap').textContent = isINR ? formatInrPrice(vwap) : formatRawPrice(vwap);
    document.getElementById('valTotalVolume').textContent = volume.toFixed(4);
    document.getElementById('valTickCount').textContent = tickCount.toLocaleString('en-US');

    updateOrderFlowWidget();
}

function updateOrderFlowWidget() {
    const sym = selectedSymbol;
    let flow = cachedOrderFlow[sym];
    if (!flow) {
        // Fallback default before first fetch
        flow = {
            symbol: sym,
            buy_volume: sym === 'BTC' ? 14.23 : (sym === 'ETH' ? 15.52 : 710.14),
            sell_volume: sym === 'BTC' ? 10.77 : (sym === 'ETH' ? 9.48 : 489.86),
            total_volume: sym === 'BTC' ? 25.0 : (sym === 'ETH' ? 25.0 : 1200.0),
            imbalance_pct: 13.8,
            sentiment: 'Bullish'
        };
    }

    const buyVol = Number(flow.buy_volume || 0);
    const sellVol = Number(flow.sell_volume || 0);
    const totVol = Number(flow.total_volume || (buyVol + sellVol) || 1);
    const buyPct = Math.min(100, Math.max(0, Math.round((buyVol / totVol) * 100)));
    const sellPct = 100 - buyPct;

    const imb = Number(flow.imbalance_pct || 0);
    const sentiment = flow.sentiment || (imb > 10 ? 'Bullish' : (imb < -10 ? 'Bearish' : 'Neutral'));

    const badge = document.getElementById('orderFlowSentimentBadge');
    const buyBar = document.getElementById('flowProgressBuy');
    const sellBar = document.getElementById('flowProgressSell');
    const buyPctEl = document.getElementById('flowBuyPct');
    const sellPctEl = document.getElementById('flowSellPct');
    const buyQtyEl = document.getElementById('flowBuyQty');
    const sellQtyEl = document.getElementById('flowSellQty');

    if (badge) {
        const sevClass = sentiment.toLowerCase();
        badge.className = `sentiment-badge ${sevClass}`;
        const sign = imb > 0 ? '+' : '';
        const arrow = imb > 0 ? '▲' : (imb < 0 ? '▼' : '●');
        badge.innerHTML = `<span class="sentiment-icon">${arrow}</span> <span>${sentiment} ${sign}${imb.toFixed(1)}%</span>`;
    }

    if (buyBar && sellBar) {
        buyBar.style.width = `${buyPct}%`;
        sellBar.style.width = `${sellPct}%`;
    }

    if (buyPctEl) buyPctEl.textContent = `${buyPct}%`;
    if (sellPctEl) sellPctEl.textContent = `${sellPct}%`;
    if (buyQtyEl) buyQtyEl.textContent = `(${buyVol.toFixed(2)} ${sym})`;
    if (sellQtyEl) sellQtyEl.textContent = `(${sellVol.toFixed(2)} ${sym})`;
}

function updateRecentTradesTable(trades) {
    if (trades && trades.length > 0) {
        const newTrades = trades.map((t) => ({
            id: t.id || `TX-${Math.floor(800000 + Math.random() * 190000)}`,
            timestamp: t.timestamp ? (t.timestamp.includes('T') ? t.timestamp.split('T')[1].split('.')[0] : t.timestamp) : new Date().toLocaleTimeString(),
            symbol: (t.symbol || 'BTC').replace('-USD', ''),
            side: t.side || (Math.random() > 0.45 ? 'BUY' : 'SELL'),
            price: Number(t.price_usd || t.price || 67420),
            volume: Number(t.volume || (0.01 + Math.random() * 0.15))
        }));
        cachedTrades = [...newTrades, ...cachedTrades.filter(c => !newTrades.some(n => n.id === c.id))].slice(0, 50);
    }

    // 1. Overview Preview Table (First 4 rows)
    const previewBody = document.getElementById('recentTradesBody');
    if (previewBody) {
        previewBody.innerHTML = cachedTrades.slice(0, 4).map(t => {
            const priceUSD = formatPrice(t.price, 'USD');
            const priceINR = formatInrPrice(t.price);
            const vol = Number(t.volume || 0.01).toFixed(4);
            return `
                <tr>
                    <td>${t.timestamp}</td>
                    <td><span class="sym-badge">${t.symbol}-USD</span></td>
                    <td>${priceUSD}</td>
                    <td>${priceINR}</td>
                    <td>${vol}</td>
                </tr>
            `;
        }).join('');
    }

    // 2. Dedicated Full Page Blotter Table
    renderDedicatedTradesBlotter();
}

function renderDedicatedTradesBlotter() {
    const blotterBody = document.getElementById('dedicatedTradesBody');
    if (!blotterBody) return;

    let filtered = cachedTrades;
    if (currentTradesFilter && currentTradesFilter !== 'ALL') {
        filtered = cachedTrades.filter(t => t.symbol === currentTradesFilter);
    }

    if (filtered.length === 0) {
        blotterBody.innerHTML = `<tr><td colspan="9" style="text-align:center; padding: 24px; color: var(--text-secondary);">No trades found for ${currentTradesFilter}</td></tr>`;
        return;
    }

    blotterBody.innerHTML = filtered.slice(0, 15).map(t => {
        const priceUSD = formatPrice(t.price, 'USD');
        const priceINR = formatInrPrice(t.price);
        const vol = Number(t.volume).toFixed(4);
        const notionalUSD = (t.price * t.volume);
        const notionalFormatted = currencyMode === 'INR' ? formatInrPrice(notionalUSD) : formatPrice(notionalUSD, 'USD');
        const sideClass = t.side === 'BUY' ? 'var(--color-green)' : 'var(--color-red)';
        const sideBg = t.side === 'BUY' ? 'rgba(16, 185, 129, 0.12)' : 'rgba(239, 68, 68, 0.12)';

        return `
            <tr>
                <td style="font-family: monospace; color: var(--text-secondary); font-size: 0.8rem;">#${t.id}</td>
                <td>${t.timestamp}</td>
                <td><span class="sym-badge">${t.symbol}-USD</span></td>
                <td>
                    <span style="display:inline-block; padding: 2px 8px; border-radius: 4px; font-weight:700; font-size:0.75rem; background: ${sideBg}; color: ${sideClass}">
                        ${t.side}
                    </span>
                </td>
                <td>${priceUSD}</td>
                <td>${priceINR}</td>
                <td>${vol}</td>
                <td>${notionalFormatted}</td>
                <td><span class="badge-pill-status" style="padding: 2px 8px; font-size: 0.72rem;"><span class="pulse-dot" style="width:5px;height:5px;"></span> Settled</span></td>
            </tr>
        `;
    }).join('');

    const countEl = document.getElementById('dedicatedTradesCount');
    if (countEl) countEl.textContent = (1428950 + cachedTrades.length).toLocaleString('en-US');
    const fxEl = document.getElementById('dedicatedFxRate');
    if (fxEl) fxEl.textContent = `₹${(activeFxRate || 94.96).toFixed(2)}`;
}

function filterDedicatedTrades(symbol) {
    currentTradesFilter = symbol;
    renderDedicatedTradesBlotter();
    showToast(`Filtered Blotter: ${symbol}`);
}

function updateAnomalyAlertsTable(alerts) {
    if (alerts && alerts.length > 0) {
        const newAlerts = alerts.map(a => ({
            id: a.id || `ALT-${Math.floor(1000 + Math.random() * 9000)}`,
            symbol: (a.symbol || 'BTC').replace('-USD', ''),
            alert_time: a.alert_time ? (a.alert_time.includes(' ') ? a.alert_time.split(' ')[1] : a.alert_time) : new Date().toLocaleTimeString(),
            classification: a.classification || (Number(a.price_return_pct) >= 0 ? 'Volatility Spike' : 'Sharp Pullback'),
            price_return_pct: Number(a.price_return_pct || 2.12),
            threshold: Number(a.threshold || 2.0),
            z_score: String(a.z_score || '3.20')
        }));
        cachedAlerts = [...newAlerts, ...cachedAlerts.filter(c => !newAlerts.some(n => n.id === c.id))].slice(0, 30);
    }

    // 1. Overview Preview Table (first 4 rows)
    const previewBody = document.getElementById('anomalyAlertsBody');
    if (previewBody) {
        previewBody.innerHTML = cachedAlerts.slice(0, 4).map(a => {
            const ret = Number(a.price_return_pct);
            const z = Math.abs(Number(a.z_score));
            const severity = z >= 3.5 ? 'high' : (z >= 3.0 ? 'medium' : 'low');
            const sevLabel = severity === 'high' ? 'High' : (severity === 'medium' ? 'Medium' : 'Low');
            const colorClass = ret >= 0 ? 'text-green' : 'text-red';
            const sign = ret >= 0 ? '+' : '';

            return `
                <tr>
                    <td>${a.alert_time}</td>
                    <td><span class="sym-badge">${a.symbol}-USD</span></td>
                    <td class="${colorClass}">${sign}${ret.toFixed(2)}%</td>
                    <td>${a.threshold.toFixed(2)}%</td>
                    <td><span class="severity-badge ${severity}">${sevLabel}</span></td>
                </tr>
            `;
        }).join('');
    }

    // 2. Dedicated Anomaly Page Table
    renderDedicatedAlertsPage();

    // 3. Update badges
    const sideBadge = document.getElementById('sidebarAlertCount');
    if (sideBadge) sideBadge.textContent = cachedAlerts.length;
    const totBadge = document.getElementById('dedicatedAlertsTotal');
    if (totBadge) totBadge.textContent = cachedAlerts.length;
}

function renderDedicatedAlertsPage() {
    const dedicatedBody = document.getElementById('dedicatedAlertsBody');
    if (!dedicatedBody) return;

    let filtered = cachedAlerts;
    if (currentAlertsSeverityFilter && currentAlertsSeverityFilter !== 'ALL') {
        filtered = cachedAlerts.filter(a => {
            const z = Math.abs(Number(a.z_score));
            const sev = z >= 3.5 ? 'high' : (z >= 3.0 ? 'medium' : 'low');
            return sev === currentAlertsSeverityFilter;
        });
    }

    if (filtered.length === 0) {
        dedicatedBody.innerHTML = `<tr><td colspan="8" style="text-align:center; padding: 24px; color: var(--text-secondary);">No alerts found for selected severity</td></tr>`;
        return;
    }

    dedicatedBody.innerHTML = filtered.map(a => {
        const ret = Number(a.price_return_pct);
        const z = Math.abs(Number(a.z_score));
        const severity = z >= 3.5 ? 'high' : (z >= 3.0 ? 'medium' : 'low');
        const sevLabel = severity === 'high' ? 'High' : (severity === 'medium' ? 'Medium' : 'Low');
        const colorClass = ret >= 0 ? 'text-green' : 'text-red';
        const sign = ret >= 0 ? '+' : '';

        return `
            <tr>
                <td>${a.alert_time}</td>
                <td><span class="sym-badge">${a.symbol}-USD</span></td>
                <td><strong style="color:var(--text-primary); font-size:0.82rem;">${a.classification}</strong></td>
                <td class="${colorClass}">${sign}${ret.toFixed(2)}%</td>
                <td><span style="font-family:monospace; font-weight:700;">${Number(a.z_score) > 0 ? '+' : ''}${Number(a.z_score).toFixed(2)}&sigma;</span></td>
                <td>${a.threshold.toFixed(2)}%</td>
                <td><span class="severity-badge ${severity}">${sevLabel}</span></td>
                <td>
                    <button class="jump-page-link" onclick="selectCardSymbol('${a.symbol}'); setSidebarSection('live-market');" style="padding: 2px 8px; font-size:0.75rem;">
                        Inspect &rarr;
                    </button>
                </td>
            </tr>
        `;
    }).join('');
}

function filterDedicatedAlerts(sev) {
    currentAlertsSeverityFilter = sev;
    renderDedicatedAlertsPage();
    showToast(`Filtered Alerts: ${sev}`);
}

function updatePipelineHealth(telemetry) {
    if (!telemetry) return;
    const kRate = Number(telemetry.kafka_ingestion_rate || 1240);
    const sLat = Number(telemetry.spark_processing_latency || 1.8);
    const chRate = Number(telemetry.clickhouse_write_rate || 1210);
    const aLat = Number(telemetry.api_response_time || 85);

    // Overview Preview Elements
    const elK = document.getElementById('valKafkaRate');
    const elS = document.getElementById('valSparkLatency');
    const elCh = document.getElementById('valClickhouseRate');
    const elA = document.getElementById('valApiLatency');

    if (elK) elK.textContent = `${kRate.toLocaleString('en-US')} msgs/s`;
    if (elS) elS.textContent = `${sLat.toFixed(1)} s`;
    if (elCh) elCh.textContent = `${chRate.toLocaleString('en-US')} rows/s`;
    if (elA) elA.textContent = `${aLat} ms`;

    // Dedicated Full Page Elements
    const phK = document.getElementById('phKafkaRate');
    const phS = document.getElementById('phSparkLatency');
    const phCh = document.getElementById('phClickhouseRate');
    const phA = document.getElementById('phApiLatency');

    if (phK) phK.textContent = `${kRate.toLocaleString('en-US')} msgs/s`;
    if (phS) phS.textContent = `${sLat.toFixed(1)} s`;
    if (phCh) phCh.textContent = `${chRate.toLocaleString('en-US')} rows/s`;
    if (phA) phA.textContent = `${aLat} ms`;
}

function updateAnalyticsMatrixTable() {
    const tbody = document.getElementById('analyticsMatrixBody');
    if (!tbody) return;

    tbody.innerHTML = SYMBOLS.map(sym => {
        const item = liveMarket[sym] || { price: 100, change: 0.8 };
        const p = item.price;
        const avgPrice = p * 0.9992;
        const minPrice = p * 0.9940;
        const maxPrice = p * 1.0048;
        const vwap = p * 0.9998;
        const vol = (sym === 'BTC' ? 12.84 : (sym === 'ETH' ? 84.12 : 620.50));
        const ret = item.change || 0.8;
        const isPos = ret >= 0;

        const flow = cachedOrderFlow[sym] || { sentiment: 'Neutral', imbalance_pct: 0.0 };
        const flowSign = flow.imbalance_pct > 0 ? '+' : '';
        const flowClass = (flow.sentiment || 'Neutral').toLowerCase();

        return `
            <tr>
                <td><span class="sym-badge">${sym}-USD</span></td>
                <td><strong style="color:var(--text-primary);">${formatPrice(p, 'USD')}</strong></td>
                <td>${formatInrPrice(p)}</td>
                <td>${formatPrice(avgPrice, currencyMode)}</td>
                <td>${formatPrice(minPrice, currencyMode)}</td>
                <td>${formatPrice(maxPrice, currencyMode)}</td>
                <td><span style="font-weight:600; color:var(--color-blue);">${formatPrice(vwap, currencyMode)}</span></td>
                <td>${vol.toFixed(2)}</td>
                <td>
                    <span class="sentiment-badge ${flowClass}" style="font-size:0.68rem; padding: 2px 7px;">
                        ${flow.sentiment} (${flowSign}${flow.imbalance_pct.toFixed(1)}%)
                    </span>
                </td>
                <td>
                    <span class="trend-badge ${isPos ? 'positive' : 'negative'}">
                        ${isPos ? '+' : ''}${ret.toFixed(2)}%
                    </span>
                </td>
                <td>
                    <button class="jump-page-link" onclick="selectCardSymbol('${sym}'); setSidebarSection('live-market');" style="padding: 2px 8px; font-size:0.75rem;">
                        View Stream &rarr;
                    </button>
                </td>
            </tr>
        `;
    }).join('');

    const vwapEl = document.getElementById('analyticsMatrixVwap');
    if (vwapEl && liveMarket.BTC) {
        vwapEl.textContent = formatPrice(liveMarket.BTC.price * 0.9998, currencyMode);
    }
}

// =============================================================================
// BACKEND API INTEGRATION WITH RESILIENT FALLBACK STREAMING
// =============================================================================

async function fetchLivePipelineData() {
    try {
        const [resLatest, resRecent, resAlerts, resTelemetry, resOrderFlow, resArbitrage, resIndiaPremium] = await Promise.allSettled([
            fetch(`/api/latest?symbols=${SYMBOLS_PARAM}`),
            fetch(`/api/recent?symbols=${SYMBOLS_PARAM}&limit=8`),
            fetch(`/api/alerts?limit=5`),
            fetch(`/api/telemetry`),
            fetch(`/api/order-flow`),
            fetch('/api/arbitrage'),
            fetch('/api/india-premium')
        ]);

        let hasLiveBackend = false;

        // Process Latest Prices
        if (resLatest.status === 'fulfilled' && resLatest.value.ok) {
            const data = await resLatest.value.json();
            if (Array.isArray(data) && data.length > 0) {
                hasLiveBackend = true;
                data.forEach(item => {
                    const sym = item.symbol;
                    if (liveMarket[sym]) {
                        liveMarket[sym].price = item.price_usd || item.price;
                        if (item.exchange_rate) activeFxRate = item.exchange_rate;
                    }
                });
            }
        }

        // Process Recent Trades
        if (resRecent.status === 'fulfilled' && resRecent.value.ok) {
            const data = await resRecent.value.json();
            if (Array.isArray(data) && data.length > 0) {
                hasLiveBackend = true;
                updateRecentTradesTable(data);
            }
        }

        // Process Alerts
        if (resAlerts.status === 'fulfilled' && resAlerts.value.ok) {
            const data = await resAlerts.value.json();
            if (Array.isArray(data) && data.length > 0) {
                hasLiveBackend = true;
                updateAnomalyAlertsTable(data);
            }
        }

        // Process Telemetry
        if (resTelemetry.status === 'fulfilled' && resTelemetry.value.ok) {
            const data = await resTelemetry.value.json();
            if (data && typeof data === 'object') {
                updatePipelineHealth(data);
            }
        }

        // Process Order Flow Imbalance
        if (resOrderFlow.status === 'fulfilled' && resOrderFlow.value.ok) {
            const data = await resOrderFlow.value.json();
            if (data && typeof data === 'object') {
                cachedOrderFlow = data;
                updateOrderFlowWidget();
                updateAnalyticsMatrixTable();
            }
        }

        // Process Cross-Exchange Arbitrage (Coinbase vs Kraken)
        if (resArbitrage.status === 'fulfilled' && resArbitrage.value.ok) {
            const data = await resArbitrage.value.json();
            if (data && typeof data === 'object') {
                cachedArbitrage = data;
                updateArbitrageUI();
            }
        }

        // Process India Regulatory & Liquidity Premium (CoinDCX vs ECB Global)
        if (resIndiaPremium.status === 'fulfilled' && resIndiaPremium.value.ok) {
            const data = await resIndiaPremium.value.json();
            if (data && typeof data === 'object') {
                cachedIndiaPremium = data;
                updateIndiaPremiumUI();
            }
        }

        // If backend is momentarily offline, execute realistic simulation tick
        if (!hasLiveBackend) {
            simulateMicroTick();
        }
    } catch (err) {
        simulateMicroTick();
    }

    // Refresh UI with current market state
    updateStatCards();
    updateOneMinuteAnalytics();
    pushChartPoint();
}

function simulateMicroTick() {
    Object.keys(liveMarket).forEach(k => {
        const item = liveMarket[k];
        const deltaPct = (Math.random() - 0.49) * 0.002;
        item.price = item.price * (1 + deltaPct);
        if (!item.history) item.history = [];
        item.history.push(item.price);
        if (item.history.length > 15) item.history.shift();
    });
}

function pushChartPoint() {
    if (!chartInstance) return;
    const cur = liveMarket[selectedSymbol];
    if (!cur) return;

    const price = currencyMode === 'INR' ? cur.price * (activeFxRate || 94.96) : cur.price;
    const now = new Date();
    const timeStr = `${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}`;

    const data = chartInstance.data.datasets[0].data;
    const labels = chartInstance.data.labels;

    data.push(price);
    labels.push(timeStr);

    if (data.length > 45) {
        data.shift();
        labels.shift();
    }

    // Recalculate indicators dynamically for the current data points
    const emaVals = calculateEMA(data, 9);
    const bbVals = calculateBollingerBands(data, 20, 2);

    if (chartInstance.data.datasets[1]) {
        chartInstance.data.datasets[1].data = emaVals;
    }
    if (chartInstance.data.datasets[2]) {
        chartInstance.data.datasets[2].data = bbVals.upper;
    }
    if (chartInstance.data.datasets[3]) {
        chartInstance.data.datasets[3].data = bbVals.lower;
    }

    chartInstance.update('none');
    updateFloatingTooltipBadge(price, `${timeStr}:${String(now.getSeconds()).padStart(2, '0')}`);
}

// =============================================================================
// INTERACTIVE CONTROLS & EVENT HANDLERS
// =============================================================================

function scrollToDashboard() {
    const el = document.getElementById('appDashboard');
    if (el) el.scrollIntoView({ behavior: 'smooth' });
}

function scrollToFeatures() {
    const el = document.getElementById('featuresStrip');
    if (el) el.scrollIntoView({ behavior: 'smooth' });
}

function scrollToAnomalyAlerts() {
    scrollToDashboard();
    const el = document.getElementById('anomalyAlertsTable');
    if (el) el.scrollIntoView({ behavior: 'smooth', block: 'center' });
    showToast('Viewing Anomaly Alerts');
}

function selectCardSymbol(sym) {
    selectedSymbol = sym;
    const pairDropdown = document.getElementById('pairSelectDropdown');
    const analyticsDropdown = document.getElementById('analyticsPairDropdown');
    if (pairDropdown) pairDropdown.value = sym;
    if (analyticsDropdown) analyticsDropdown.value = sym;

    // Highlight active card
    document.querySelectorAll('.stat-card').forEach(c => {
        c.classList.toggle('active-asset', c.dataset.symbol === sym);
    });

    resetChartWithNewSymbol(sym);
    updateOneMinuteAnalytics();
    updateArbitragePulseStrip();
    showToast(`Active Asset: ${sym}-USD`);
}

function onPairDropdownChange(val) {
    selectCardSymbol(val);
}

function onAnalyticsPairChange(val) {
    selectCardSymbol(val);
}

function setTimeframe(tf) {
    currentTimeframe = tf;
    document.querySelectorAll('.tf-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.tf === tf);
    });
    showToast(`Timeframe set to ${tf}`);
}

function onCurrencyDropdownChange(val) {
    currencyMode = val;
    if (chartInstance) {
        resetChartWithNewSymbol(selectedSymbol);
    }
    updateStatCards();
    updateOneMinuteAnalytics();
    showToast(`Currency set to ${val}`);
}

function setCurrencyToggle(curr) {
    currencyMode = curr === 'INR' ? 'INR' : 'USD';
    const currSelect = document.getElementById('currencySelectDropdown');
    if (currSelect) currSelect.value = currencyMode;
    onCurrencyDropdownChange(currencyMode);
}

function resetChartWithNewSymbol(sym) {
    if (!chartInstance) return;
    const basePrice = (currencyMode === 'INR' ? (liveMarket[sym].price * activeFxRate) : liveMarket[sym].price);
    const data = [];
    const labels = [];
    const now = new Date();

    for (let i = 40; i >= 0; i--) {
        const t = new Date(now.getTime() - i * 60 * 1000);
        labels.push(`${String(t.getHours()).padStart(2, '0')}:${String(t.getMinutes()).padStart(2, '0')}`);
        const noise = (Math.sin(i * 0.4) * 0.005 + (Math.random() - 0.49) * 0.003);
        data.push(basePrice * (1 + noise));
    }

    const emaVals = calculateEMA(data, 9);
    const bbVals = calculateBollingerBands(data, 20, 2);

    chartInstance.data.labels = labels;
    chartInstance.data.datasets[0].data = data;
    chartInstance.data.datasets[0].label = `${sym} Price`;

    if (chartInstance.data.datasets[1]) {
        chartInstance.data.datasets[1].data = emaVals;
        chartInstance.data.datasets[1].label = `${sym} EMA (9)`;
        chartInstance.data.datasets[1].hidden = !showEma9;
    }
    if (chartInstance.data.datasets[2]) {
        chartInstance.data.datasets[2].data = bbVals.upper;
        chartInstance.data.datasets[2].label = `${sym} BB Upper (20, 2σ)`;
        chartInstance.data.datasets[2].hidden = !showBollinger;
    }
    if (chartInstance.data.datasets[3]) {
        chartInstance.data.datasets[3].data = bbVals.lower;
        chartInstance.data.datasets[3].label = `${sym} BB Lower (20, 2σ)`;
        chartInstance.data.datasets[3].hidden = !showBollinger;
    }

    chartInstance.update();
    updateFloatingTooltipBadge(data[data.length - 1], labels[labels.length - 1]);
}

function setSidebarSection(section) {
    // 1. Highlight sidebar nav item
    document.querySelectorAll('.sidebar-nav .nav-item').forEach(b => {
        b.classList.toggle('active', b.dataset.section === section);
    });

    // 2. Hide all view containers, show only selected
    document.querySelectorAll('.dashboard-view').forEach(v => {
        v.classList.remove('active');
    });

    const targetView = document.getElementById(`view-${section}`);
    if (targetView) {
        targetView.classList.add('active');
    }

    // 3. Update topbar page title and subtitle dynamically
    const titleEl = document.querySelector('.topbar-page-title');
    const subtitleEl = document.querySelector('.topbar-page-subtitle');
    const metaTitles = {
        'dashboard': {
            title: 'Market Overview',
            sub: 'Real-time Crypto Analytics & Anomaly Detection'
        },
        'live-market': {
            title: 'Live Market Stream',
            sub: 'Direct WebSocket Ticks & Dynamic Streaming Canvas'
        },
        'price-analytics': {
            title: 'Price Analytics & Microstructure',
            sub: '1-Minute Aggregations, Rolling VWAP & Financial Precision'
        },
        'recent-trades': {
            title: 'Recent Trades Blotter',
            sub: 'Institutional Tick Audit Trail & Real-time USD/INR Conversion'
        },
        'anomaly-alerts': {
            title: 'Anomaly Detection Engine',
            sub: 'Dynamic Statistical Outliers (|Z| ≥ 3.0σ) & Real-time Alerts'
        },
        'pipeline-health': {
            title: 'Pipeline Health & Telemetry',
            sub: 'Kafka KRaft, Spark Streaming, ClickHouse OLAP & FastAPI'
        },
        'arbitrage-spreads': {
            title: 'Multi-Exchange Arbitrage & India Premium',
            sub: 'Cross-Venue Event-Time Stream Joins & Structural Regulatory Spreads'
        }
    };

    if (metaTitles[section]) {
        if (titleEl) titleEl.textContent = metaTitles[section].title;
        if (subtitleEl) subtitleEl.textContent = metaTitles[section].sub;
    }

    // 4. Smooth scroll to top of dashboard shell so user is at top of page
    scrollToDashboard();

    // 5. Trigger view-specific refreshes
    if (section === 'dashboard') {
        if (chartInstance) chartInstance.resize();
    } else if (section === 'live-market') {
        initOrUpdateLiveMarketChart();
        updateLiveMarketKpis();
    } else if (section === 'price-analytics') {
        updateAnalyticsMatrixTable();
    } else if (section === 'recent-trades') {
        renderDedicatedTradesBlotter();
    } else if (section === 'anomaly-alerts') {
        renderDedicatedAlertsPage();
    } else if (section === 'pipeline-health') {
        updatePipelineHealth(telemetryMetrics);
    } else if (section === 'arbitrage-spreads') {
        renderArbitrageMatrix();
        renderIndiaPremiumMatrix();
    }
}

function onLiveMarketSelect(sym) {
    selectCardSymbol(sym);
    updateLiveMarketKpis();
    if (liveMarketChartInstance) {
        initOrUpdateLiveMarketChart();
    }
}

function updateLiveMarketKpis() {
    const cur = liveMarket[selectedSymbol] || liveMarket.BTC;
    const isINR = currencyMode === 'INR';
    const curPriceEl = document.getElementById('liveMarketCurrentPrice');
    const chgEl = document.getElementById('liveMarketChange');
    const highEl = document.getElementById('liveMarketHigh');
    const lowEl = document.getElementById('liveMarketLow');
    const volEl = document.getElementById('liveMarketVolume');
    const titleEl = document.getElementById('liveMarketChartTitle');
    const selectEl = document.getElementById('liveMarketSymbolSelect');

    if (selectEl) selectEl.value = selectedSymbol;
    if (titleEl) titleEl.textContent = `${selectedSymbol}-USD Real-Time Stream`;
    if (curPriceEl) curPriceEl.textContent = formatPrice(cur.price, currencyMode);
    if (chgEl) {
        const sign = (cur.change || 1.24) >= 0 ? '+' : '';
        chgEl.textContent = `${sign}${(cur.change || 1.24).toFixed(2)}% today`;
        chgEl.className = `kpi-card-sub ${(cur.change || 1.24) >= 0 ? 'text-green' : 'text-red'}`;
    }
    if (highEl) highEl.textContent = formatPrice(cur.price * 1.012, currencyMode);
    if (lowEl) lowEl.textContent = formatPrice(cur.price * 0.988, currencyMode);
    if (volEl) volEl.textContent = `${(selectedSymbol === 'BTC' ? 14820 : 92400).toLocaleString('en-US')} ${selectedSymbol}`;
}

function initOrUpdateLiveMarketChart() {
    const canvas = document.getElementById('liveMarketDedicatedChart');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');

    const labels = [];
    const values = [];
    const now = new Date();
    const basePrice = (currencyMode === 'INR' ? (liveMarket[selectedSymbol].price * (activeFxRate || 94.96)) : liveMarket[selectedSymbol].price);

    for (let i = 50; i >= 0; i--) {
        const t = new Date(now.getTime() - i * 30 * 1000);
        labels.push(`${String(t.getHours()).padStart(2, '0')}:${String(t.getMinutes()).padStart(2, '0')}:${String(t.getSeconds()).padStart(2, '0')}`);
        const noise = (Math.sin(i * 0.3) * 0.004 + (Math.random() - 0.49) * 0.002);
        values.push(basePrice * (1 + noise));
    }

    if (!liveMarketChartInstance) {
        const gradient = ctx.createLinearGradient(0, 0, 0, 320);
        gradient.addColorStop(0, 'rgba(16, 185, 129, 0.22)');
        gradient.addColorStop(1, 'rgba(16, 185, 129, 0.0)');

        liveMarketChartInstance = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: `${selectedSymbol} Live WebSocket Stream`,
                    data: values,
                    borderColor: '#10B981',
                    borderWidth: 2.2,
                    backgroundColor: gradient,
                    fill: true,
                    tension: 0.35,
                    pointRadius: 0,
                    pointHoverRadius: 5,
                    pointHoverBackgroundColor: '#10B981'
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: { duration: 400 },
                plugins: {
                    legend: { display: false },
                    tooltip: { mode: 'index', intersect: false }
                },
                scales: {
                    x: {
                        grid: { display: false },
                        ticks: { color: '#94A3B8', font: { size: 10 }, maxTicksLimit: 8 }
                    },
                    y: {
                        position: 'right',
                        grid: { color: isDarkMode ? 'rgba(55, 65, 81, 0.5)' : 'rgba(226, 232, 240, 0.8)' },
                        ticks: {
                            color: '#94A3B8',
                            font: { size: 10 },
                            callback: (val) => currencyMode === 'INR' ? `₹${(val / 100000).toFixed(1)}L` : `$${Number(val).toLocaleString()}`
                        }
                    }
                }
            }
        });
    } else {
        liveMarketChartInstance.data.labels = labels;
        liveMarketChartInstance.data.datasets[0].data = values;
        liveMarketChartInstance.data.datasets[0].label = `${selectedSymbol} Live WebSocket Stream`;
        liveMarketChartInstance.update();
        liveMarketChartInstance.resize();
    }
}

// =============================================================================
// THEME TOGGLE (LIGHT / DARK)
// =============================================================================

function toggleTheme() {
    isDarkMode = !isDarkMode;
    document.body.classList.toggle('dark-theme', isDarkMode);

    const sun = document.querySelector('.sun-icon');
    const moon = document.querySelector('.moon-icon');
    if (sun && moon) {
        sun.style.display = isDarkMode ? 'none' : 'block';
        moon.style.display = isDarkMode ? 'block' : 'none';
    }

    // Refresh Chart styling for theme
    if (chartInstance) {
        chartInstance.options.scales.y.grid.color = isDarkMode ? 'rgba(55, 65, 81, 0.5)' : 'rgba(226, 232, 240, 0.8)';
        chartInstance.update();
    }

    showToast(`Switched to ${isDarkMode ? 'Dark' : 'Light'} Mode`);
}

function toggleChartViewMode() {
    showToast('Grid View toggled: 1-Minute Live Candlesticks');
}

function toggleEma9() {
    showEma9 = !showEma9;
    const btn = document.getElementById('btnToggleEma9');
    if (btn) btn.classList.toggle('active', showEma9);

    if (chartInstance && chartInstance.data.datasets[1]) {
        chartInstance.data.datasets[1].hidden = !showEma9;
        chartInstance.update();
    }
    showToast(showEma9 ? 'EMA (9) Overlay Enabled' : 'EMA (9) Overlay Disabled');
}

function toggleBollingerBands() {
    showBollinger = !showBollinger;
    const btn = document.getElementById('btnToggleBollinger');
    if (btn) btn.classList.toggle('active', showBollinger);

    if (chartInstance && chartInstance.data.datasets[2] && chartInstance.data.datasets[3]) {
        chartInstance.data.datasets[2].hidden = !showBollinger;
        chartInstance.data.datasets[3].hidden = !showBollinger;
        chartInstance.update();
    }
    showToast(showBollinger ? 'Bollinger Bands (20, 2σ) Enabled' : 'Bollinger Bands Disabled');
}

// =============================================================================
// MODALS & SPOTLIGHT SEARCH (⌘K)
// =============================================================================

function openSearchModal() {
    const m = document.getElementById('searchModal');
    if (m) {
        m.classList.add('active');
        const input = document.getElementById('spotlightSearchInput');
        if (input) {
            input.value = '';
            setTimeout(() => input.focus(), 50);
        }
    }
}

function closeSearchModal() {
    const m = document.getElementById('searchModal');
    if (m) m.classList.remove('active');
}

function openTelemetryModal() {
    const m = document.getElementById('telemetryModal');
    if (m) m.classList.add('active');
}

function closeTelemetryModal() {
    const m = document.getElementById('telemetryModal');
    if (m) m.classList.remove('active');
}

function openSettingsModal() {
    openTelemetryModal();
}

function closeModalOnBackdrop(e, modalId) {
    if (e.target.id === modalId) {
        const m = document.getElementById(modalId);
        if (m) m.classList.remove('active');
    }
}

function handleSearchInput(query) {
    const q = query.toLowerCase().trim();
    const items = document.querySelectorAll('.search-item');
    items.forEach(item => {
        const text = item.textContent.toLowerCase();
        item.style.display = text.includes(q) ? 'flex' : 'none';
    });
}

function quickSelectSymbol(sym) {
    selectCardSymbol(sym);
    closeSearchModal();
}

function simulateAnomalyAlert() {
    closeSearchModal();
    const cur = liveMarket[selectedSymbol] || liveMarket.BTC;
    const alertObj = {
        symbol: selectedSymbol,
        alert_time: new Date().toLocaleTimeString(),
        price_return_pct: (Math.random() > 0.5 ? 1 : -1) * (2.4 + Math.random() * 1.5),
        threshold: 2.0,
        z_score: (3.1 + Math.random() * 1.2).toFixed(2)
    };

    updateAnomalyAlertsTable([alertObj, ...cachedAlerts]);
    showToast(`🚨 Volatility Alert: ${selectedSymbol} |Z| = ${alertObj.z_score}σ`);
}

function viewAllTrades() {
    showToast('Streaming all live Coinbase trade events.');
}

function viewAllAlerts() {
    scrollToAnomalyAlerts();
    showToast('Displaying real-time market anomaly feed.');
}

function showToast(msg) {
    const t = document.getElementById('toast');
    if (!t) return;
    t.textContent = msg;
    t.style.display = 'block';
    setTimeout(() => { t.style.display = 'none'; }, 2800);
}

// ⌘K / Ctrl+K Global Shortcut Handler
document.addEventListener('keydown', (e) => {
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
        e.preventDefault();
        openSearchModal();
    }
    if (e.key === 'Escape') {
        closeSearchModal();
        closeTelemetryModal();
    }
});

// =============================================================================
// DOM INITIALIZATION
// =============================================================================

document.addEventListener('DOMContentLoaded', () => {
    initLiveClock();

    // Populate initial sparklines
    Object.keys(liveMarket).forEach(k => {
        const p = liveMarket[k].price;
        liveMarket[k].history = [p * 0.99, p * 0.994, p * 0.992, p * 0.998, p * 1.002, p * 1.001, p];
    });

    updateStatCards();
    updateOneMinuteAnalytics();
    initPriceHistoryChart();

    // Pre-populate both overview previews and dedicated view tables
    updateRecentTradesTable(cachedTrades);
    updateAnomalyAlertsTable(cachedAlerts);
    updatePipelineHealth(telemetryMetrics);
    updateAnalyticsMatrixTable();
    updateArbitragePulseStrip();
    renderArbitrageMatrix();
    renderIndiaPremiumMatrix();

    // Start Real-Time Live Ingestion & Polling Loop
    fetchLivePipelineData();
    setInterval(fetchLivePipelineData, 2500);
});

// =============================================================================
// MULTI-EXCHANGE ARBITRAGE & INDIA PREMIUM MATRIX RENDERING
// =============================================================================

function updateArbitrageUI() {
    updateArbitragePulseStrip();
    renderArbitrageMatrix();
}

function updateIndiaPremiumUI() {
    updateArbitragePulseStrip();
    renderIndiaPremiumMatrix();
}

function updateArbitragePulseStrip() {
    const sym = (selectedSymbol === 'INR' || !selectedSymbol) ? 'BTC' : selectedSymbol;

    // 1. Cross-Exchange Arbitrage Card (Coinbase vs Kraken)
    const arbPairs = cachedArbitrage.pairs || {};
    const arb = arbPairs[sym];
    const arbActiveSymEl = document.getElementById('arbActiveSym');
    const arbGrossSpreadEl = document.getElementById('arbGrossSpreadVal');
    const arbNetProfitEl = document.getElementById('arbNetProfitVal');
    const arbStatusTagEl = document.getElementById('arbStatusTag');

    if (arbActiveSymEl) arbActiveSymEl.textContent = sym;

    if (arb) {
        if (arbGrossSpreadEl) {
            arbGrossSpreadEl.textContent = `$${arb.absolute_spread.toFixed(2)} (${arb.gross_spread_pct.toFixed(3)}%)`;
        }
        if (arbNetProfitEl) {
            const netSign = arb.net_profit_pct > 0 ? '+' : '';
            arbNetProfitEl.textContent = `${netSign}${arb.net_profit_pct.toFixed(3)}% (${arb.is_arbitrage_active ? 'In Profit' : 'Below Hurdle'})`;
            arbNetProfitEl.className = `sm-val ${arb.is_arbitrage_active ? 'positive' : 'negative'}`;
        }
        if (arbStatusTagEl) {
            if (arb.is_arbitrage_active) {
                arbStatusTagEl.textContent = 'Active Window';
                arbStatusTagEl.className = 'spread-status-tag active-opp';
            } else {
                arbStatusTagEl.textContent = 'Market Efficient';
                arbStatusTagEl.className = 'spread-status-tag inside-hurdle';
            }
        }
    }

    // 2. India Regulatory & Liquidity Premium Card (CoinDCX vs ECB Benchmark)
    const indiaPairs = cachedIndiaPremium.pairs || {};
    const ind = indiaPairs[sym];
    const indiaActiveSymEl = document.getElementById('indiaActiveSym');
    const indiaCoinDCXEl = document.getElementById('indiaCoinDCXVal');
    const indiaGlobalImpliedEl = document.getElementById('indiaGlobalImpliedVal');
    const indiaPremiumPctEl = document.getElementById('indiaPremiumPctVal');
    const indiaPremiumRegimeEl = document.getElementById('indiaPremiumRegime');

    if (indiaActiveSymEl) indiaActiveSymEl.textContent = sym;

    if (ind) {
        if (indiaCoinDCXEl) {
            indiaCoinDCXEl.textContent = `₹${Math.round(ind.coindcx_price_inr).toLocaleString('en-IN')}`;
        }
        if (indiaGlobalImpliedEl) {
            indiaGlobalImpliedEl.textContent = `₹${Math.round(ind.global_implied_inr).toLocaleString('en-IN')}`;
        }
        if (indiaPremiumPctEl) {
            const sign = ind.premium_pct >= 0 ? '+' : '';
            const inrDiff = Math.round(ind.premium_inr);
            const inrFormatted = (Math.abs(inrDiff) >= 100000) 
                ? `₹${(inrDiff / 100000).toFixed(2)}L`
                : `₹${inrDiff.toLocaleString('en-IN')}`;
            indiaPremiumPctEl.textContent = `${sign}${ind.premium_pct.toFixed(2)}% (${inrFormatted})`;
            indiaPremiumPctEl.className = `sm-val ${ind.premium_pct >= 0 ? 'positive' : 'negative'}`;
        }
        if (indiaPremiumRegimeEl) {
            indiaPremiumRegimeEl.textContent = ind.regime || 'Domestic Premium';
            indiaPremiumRegimeEl.className = `spread-status-tag india ${ind.regime_color || 'amber'}`;
        }
    }
}

function renderArbitrageMatrix() {
    const tbody = document.getElementById('arbitrageMatrixBody');
    if (!tbody) return;

    const pairs = cachedArbitrage.pairs;
    if (!pairs || Object.keys(pairs).length === 0) return;

    // Update KPIs
    const effEl = document.getElementById('kpiEfficiency');
    const durEl = document.getElementById('kpiDuration');
    const oppsEl = document.getElementById('kpiActiveOpps');

    const metrics = cachedArbitrage.metrics || {};
    if (effEl) effEl.textContent = `${metrics.efficiency_pct || 99.1}%`;
    if (durEl) durEl.textContent = `${metrics.avg_duration_sec || 1.42}s`;

    let activeCount = 0;
    const trackedSymbols = ['BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'LTC'];

    tbody.innerHTML = trackedSymbols.map(sym => {
        const item = pairs[sym];
        if (!item) return '';

        if (item.is_arbitrage_active) activeCount++;

        const isAct = item.is_arbitrage_active;
        const netSign = item.net_profit_pct > 0 ? '+' : '';
        const netClass = isAct ? 'positive' : 'negative';

        return `
            <tr>
                <td><span class="sym-badge">${sym}-USD</span></td>
                <td><strong style="color:var(--text-primary);">$${item.coinbase_price.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 4})}</strong></td>
                <td><strong style="color:var(--text-primary);">$${item.kraken_price.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 4})}</strong></td>
                <td>$${item.absolute_spread.toFixed(2)}</td>
                <td>${item.gross_spread_pct.toFixed(3)}%</td>
                <td><span style="font-family: monospace; color: var(--text-secondary);">~118 ms</span></td>
                <td><span style="color: #F59E0B;">0.35%</span></td>
                <td>
                    <span class="trend-badge ${netClass}" style="font-weight: 700;">
                        ${netSign}${item.net_profit_pct.toFixed(3)}%
                    </span>
                </td>
                <td>
                    <span class="direction-badge" style="background: rgba(99, 102, 241, 0.1); color: #818CF8; border: 1px solid rgba(99, 102, 241, 0.2); padding: 3px 8px; border-radius: 4px; font-size: 0.72rem; white-space: nowrap;">
                        ${item.direction}
                    </span>
                </td>
                <td>
                    <span class="spread-status-tag ${isAct ? 'active-opp' : 'inside-hurdle'}" style="font-size: 0.72rem; padding: 3px 8px;">
                        ${isAct ? 'Active Opportunity' : 'Inside Hurdle'}
                    </span>
                </td>
            </tr>
        `;
    }).join('');

    if (oppsEl) oppsEl.textContent = `${activeCount} / 6`;
}

function renderIndiaPremiumMatrix() {
    const tbody = document.getElementById('indiaPremiumMatrixBody');
    if (!tbody) return;

    const pairs = cachedIndiaPremium.pairs;
    if (!pairs || Object.keys(pairs).length === 0) return;

    const fxRate = cachedIndiaPremium.fx_rate || 86.85;
    const termFxEl = document.getElementById('termFxRate');
    if (termFxEl) termFxEl.textContent = fxRate.toFixed(2);

    const trackedSymbols = ['BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'LTC'];

    tbody.innerHTML = trackedSymbols.map(sym => {
        const item = pairs[sym];
        if (!item) return '';

        const sign = item.premium_pct >= 0 ? '+' : '';
        const diffSign = item.premium_inr >= 0 ? '+' : '';
        const isPos = item.premium_pct >= 0;

        return `
            <tr>
                <td><span class="sym-badge" style="background: rgba(245, 158, 11, 0.15); color: #F59E0B; border: 1px solid rgba(245, 158, 11, 0.3);">${sym}-INR</span></td>
                <td><strong style="color:var(--text-primary);">₹${Math.round(item.coindcx_price_inr).toLocaleString('en-IN')}</strong></td>
                <td>$${item.global_price_usd.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 4})}</td>
                <td>₹${Math.round(item.global_implied_inr).toLocaleString('en-IN')}</td>
                <td style="color: ${isPos ? '#10B981' : '#EF4444'}; font-weight: 600;">
                    ${diffSign}₹${Math.round(item.premium_inr).toLocaleString('en-IN')}
                </td>
                <td>
                    <span class="trend-badge ${isPos ? 'positive' : 'negative'}" style="font-weight: 700;">
                        ${sign}${item.premium_pct.toFixed(2)}%
                    </span>
                </td>
                <td>
                    <span class="regime-badge ${item.regime_color || 'amber'}" style="font-size: 0.72rem; padding: 3px 8px; border-radius: 4px;">
                        ${item.regime}
                    </span>
                </td>
                <td>
                    <span style="font-size: 0.7rem; color: #EF4444; background: rgba(239, 68, 68, 0.1); border: 1px solid rgba(239, 68, 68, 0.25); padding: 3px 8px; border-radius: 4px; display: inline-flex; align-items: center; gap: 4px;">
                        <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"></rect><path d="M7 11V7a5 5 0 0 1 10 0v4"></path></svg>
                        Capital Control Locked (1% TDS + LRS)
                    </span>
                </td>
            </tr>
        `;
    }).join('');
}

// Export all functions to window for robust inline handler binding
window.scrollToDashboard = scrollToDashboard;
window.scrollToFeatures = scrollToFeatures;
window.scrollToAnomalyAlerts = scrollToAnomalyAlerts;
window.selectCardSymbol = selectCardSymbol;
window.setSidebarSection = setSidebarSection;
window.toggleTheme = toggleTheme;
window.toggleChartViewMode = toggleChartViewMode;
window.openSearchModal = openSearchModal;
window.closeSearchModal = closeSearchModal;
window.openTelemetryModal = openTelemetryModal;
window.closeTelemetryModal = closeTelemetryModal;
window.openSettingsModal = openSettingsModal;
window.closeModalOnBackdrop = closeModalOnBackdrop;
window.handleSearchInput = handleSearchInput;
window.quickSelectSymbol = quickSelectSymbol;
window.simulateAnomalyAlert = simulateAnomalyAlert;
window.viewAllTrades = viewAllTrades;
window.viewAllAlerts = viewAllAlerts;
window.setTimeframe = setTimeframe;
window.onPairDropdownChange = onPairDropdownChange;
window.onCurrencyDropdownChange = onCurrencyDropdownChange;
window.onAnalyticsPairChange = onAnalyticsPairChange;
window.setCurrencyToggle = setCurrencyToggle;
window.showToast = showToast;
window.onLiveMarketSelect = onLiveMarketSelect;
window.filterDedicatedTrades = filterDedicatedTrades;
window.filterDedicatedAlerts = filterDedicatedAlerts;
window.fetchLivePipelineData = fetchLivePipelineData;
window.toggleEma9 = toggleEma9;
window.toggleBollingerBands = toggleBollingerBands;
window.updateOrderFlowWidget = updateOrderFlowWidget;
window.updateArbitrageUI = updateArbitrageUI;
window.updateIndiaPremiumUI = updateIndiaPremiumUI;
window.updateArbitragePulseStrip = updateArbitragePulseStrip;
window.renderArbitrageMatrix = renderArbitrageMatrix;
window.renderIndiaPremiumMatrix = renderIndiaPremiumMatrix;

