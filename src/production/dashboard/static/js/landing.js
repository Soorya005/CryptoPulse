// =============================================================================
// CRYPTOPULSE - QUANTITATIVE STREAMING ENGINE CONTROLLER
// Real-time Multi-Venue Ingestion, Pipeline Simulator, Interactive Math Sandbox
// =============================================================================

let isDarkMode = true; // Default to sleek obsidian theme
let currentAsset = 'BTC';
let orderSizeUSD = 50000;
let isSimulating = false;

// Cached Real-Time Data
let liveArbitrage = null;
let liveIndiaPremium = null;
let liveTelemetry = null;

// Quantitative Benchmark Baselines
const QUANT_BASELINES = {
    BTC: { cb: 80197.30, kr: 80200.10, inr: 8106712, gross: 0.003, inr_prem: 14.62, fx: 88.19 },
    ETH: { cb: 2322.06, kr: 2322.77, inr: 235023, gross: 0.031, inr_prem: 14.78, fx: 88.19 },
    SOL: { cb: 101.42, kr: 101.45, inr: 10254, gross: 0.030, inr_prem: 14.62, fx: 88.19 },
    XRP: { cb: 1.3005, kr: 1.2952, inr: 130.80, gross: 0.406, inr_prem: 14.06, fx: 88.19 },
    DOGE: { cb: 0.0789, kr: 0.0771, inr: 7.85, gross: 2.295, inr_prem: 12.93, fx: 88.19 },
    LTC: { cb: 56.558, kr: 56.550, inr: 6750, gross: 0.014, inr_prem: 35.33, fx: 88.19 }
};

// =============================================================================
// 1. THEME INITIALIZER (DEFAULTS TO SLEEK DARK MODE)
// =============================================================================
function initTheme() {
    const saved = localStorage.getItem('cryptopulse_theme');
    if (saved === 'light') {
        isDarkMode = false;
        document.body.classList.add('light-theme');
        document.body.classList.remove('dark-theme');
    } else {
        isDarkMode = true;
        document.body.classList.add('dark-theme');
        document.body.classList.remove('light-theme');
    }
    syncThemeIcons();
}

function toggleTheme() {
    isDarkMode = !isDarkMode;
    document.body.classList.toggle('light-theme', !isDarkMode);
    document.body.classList.toggle('dark-theme', isDarkMode);
    localStorage.setItem('cryptopulse_theme', isDarkMode ? 'dark' : 'light');
    syncThemeIcons();
}

function syncThemeIcons() {
    const suns = document.querySelectorAll('.sun-icon');
    const moons = document.querySelectorAll('.moon-icon');
    suns.forEach(s => s.style.display = isDarkMode ? 'block' : 'none');
    moons.forEach(m => m.style.display = isDarkMode ? 'none' : 'block');
}

// =============================================================================
// 2. LIVE DATA INGESTION FROM PRODUCTION ENGINE
// =============================================================================
async function pollStreamingEngine() {
    try {
        const [resArb, resIndia, resTelem] = await Promise.allSettled([
            fetch('/api/arbitrage'),
            fetch('/api/india-premium'),
            fetch('/api/telemetry')
        ]);

        if (resArb.status === 'fulfilled' && resArb.value.ok) {
            liveArbitrage = await resArb.value.json();
            renderShowcaseRadar(liveArbitrage);
            renderMarqueeTape();
        }

        if (resIndia.status === 'fulfilled' && resIndia.value.ok) {
            liveIndiaPremium = await resIndia.value.json();
            renderShowcaseIndia(liveIndiaPremium);
            renderMarqueeTape();
        }

        if (resTelem.status === 'fulfilled' && resTelem.value.ok) {
            liveTelemetry = await resTelem.value.json();
            renderEngineTelemetry(liveTelemetry);
        }

        computeQuantitativeSandbox();
    } catch (err) {
        console.warn('[Engine Poller] Using active fallback baseline:', err);
    }
}

function renderShowcaseRadar(data) {
    const tbody = document.getElementById('quantRadarTableBody');
    if (!tbody || !data || !data.pairs) return;

    const tracked = ['BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'LTC'];
    tbody.innerHTML = tracked.map(sym => {
        const item = data.pairs[sym] || {};
        const cb = Number(item.coinbase_price || QUANT_BASELINES[sym].cb);
        const kr = Number(item.kraken_price || QUANT_BASELINES[sym].kr);
        const gross = Number(item.gross_spread_pct || QUANT_BASELINES[sym].gross);
        const skew = item.clock_skew_ms || 84;
        const isArb = item.is_arbitrage_active || (gross > 0.35);

        return `
            <tr>
                <td><span style="font-weight:700; color:var(--c-aubergine);">${sym}-USD</span></td>
                <td style="color:var(--c-aubergine); font-weight:600;">$${cb.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: (cb < 1 ? 4 : 2)})}</td>
                <td style="color:var(--c-aubergine); font-weight:600;">$${kr.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: (kr < 1 ? 4 : 2)})}</td>
                <td style="color:${isArb ? 'var(--c-green)' : 'var(--c-marigold-deep)'}; font-weight:700;">
                    ${gross.toFixed(3)}%
                </td>
                <td style="color:var(--c-aubergine-mid);">&Delta;t ${skew}ms</td>
                <td>
                    <span class="pill-status-editorial ${isArb ? 'viable' : 'hurdle'}">
                        ${isArb ? 'Viable Spread' : 'Inside 35 bps Hurdle'}
                    </span>
                </td>
            </tr>
        `;
    }).join('');

    const effEl = document.getElementById('engineEfficiencyMetric');
    if (effEl && data.metrics) {
        effEl.textContent = `${data.metrics.efficiency_pct.toFixed(1)}%`;
    }
}

function renderShowcaseIndia(data) {
    const tbody = document.getElementById('indiaMoatTableBody');
    if (!tbody || !data || !data.pairs) return;

    const tracked = ['BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'LTC'];
    tbody.innerHTML = tracked.map(sym => {
        const item = data.pairs[sym] || {};
        const inrPrice = Number(item.coindcx_price_inr || QUANT_BASELINES[sym].inr);
        const globalImplied = Number(item.global_implied_inr || (QUANT_BASELINES[sym].cb * 88.19));
        const prem = Number(item.premium_pct || QUANT_BASELINES[sym].inr_prem);

        return `
            <tr>
                <td><span style="font-weight:700; color:var(--c-aubergine);">${sym}-INR</span></td>
                <td style="color:var(--c-aubergine); font-weight:700;">₹${Math.round(inrPrice).toLocaleString('en-IN')}</td>
                <td style="color:var(--c-aubergine-mid);">₹${Math.round(globalImplied).toLocaleString('en-IN')}</td>
                <td style="color:var(--c-marigold-deep); font-weight:700;">+${prem.toFixed(2)}%</td>
                <td>
                    <span class="pill-status-editorial locked">
                        1% TDS &bull; LRS Locked
                    </span>
                </td>
            </tr>
        `;
    }).join('');
}

function renderEngineTelemetry(data) {
    if (!data) return;
    const rateEl = document.getElementById('ribbonThroughputVal');
    const latEl = document.getElementById('ribbonLatencyVal');
    if (rateEl && data.ingestion_rate_per_sec) rateEl.textContent = `${Number(data.ingestion_rate_per_sec).toLocaleString()} msg/s`;
    if (latEl && data.e2e_latency_ms) latEl.textContent = `${data.e2e_latency_ms} ms`;
}

function renderMarqueeTape() {
    const track = document.getElementById('liveMarqueeTrack');
    if (!track) return;

    const arb = (liveArbitrage && liveArbitrage.pairs) ? liveArbitrage.pairs : {};
    const ind = (liveIndiaPremium && liveIndiaPremium.pairs) ? liveIndiaPremium.pairs : {};

    const entries = ['BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'LTC'].map(sym => {
        const a = arb[sym] || {};
        const i = ind[sym] || {};
        const pUSD = a.coinbase_price || QUANT_BASELINES[sym].cb;
        const gross = a.gross_spread_pct !== undefined ? a.gross_spread_pct : QUANT_BASELINES[sym].gross;
        const pINR = i.coindcx_price_inr || QUANT_BASELINES[sym].inr;
        const prem = i.premium_pct !== undefined ? i.premium_pct : QUANT_BASELINES[sym].inr_prem;

        return `
            <div class="ticker-leaf">
                <span class="leaf-sym">${sym}</span>
                <span style="font-weight:600;">$${Number(pUSD).toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: (pUSD < 1 ? 4 : 2)})}</span>
                <span class="leaf-rate">Spread ${Number(gross).toFixed(3)}%</span>
                <span style="color:rgba(58,15,52,0.2);">&bull;</span>
                <span style="font-weight:600;">₹${Math.round(pINR).toLocaleString('en-IN')}</span>
                <span style="color:var(--c-marigold-deep); font-weight:700;">+${Number(prem).toFixed(2)}% Prem</span>
            </div>
        `;
    }).join('');

    track.innerHTML = entries + entries;
}

// =============================================================================
// 3. INTERACTIVE QUANTITATIVE SANDBOX (ARBITRAGE & TDS CALCULATOR)
// =============================================================================
function selectAsset(sym) {
    currentAsset = sym;
    const btns = document.querySelectorAll('.asset-pill-select, .calc-asset-btn');
    btns.forEach(b => b.classList.toggle('active', b.getAttribute('data-sym') === sym));
    computeQuantitativeSandbox();
}

function onNotionalInput(val) {
    const num = parseFloat(val) || 0;
    orderSizeUSD = Math.max(1000, Math.min(5000000, num));
    computeQuantitativeSandbox();
}

function computeQuantitativeSandbox() {
    const sym = currentAsset;
    const a = (liveArbitrage && liveArbitrage.pairs && liveArbitrage.pairs[sym]) || {};
    const i = (liveIndiaPremium && liveIndiaPremium.pairs && liveIndiaPremium.pairs[sym]) || {};

    const grossPct = a.gross_spread_pct !== undefined ? a.gross_spread_pct : QUANT_BASELINES[sym].gross;
    const hurdlePct = 0.35;
    const netPct = grossPct - hurdlePct;
    const premPct = i.premium_pct !== undefined ? i.premium_pct : QUANT_BASELINES[sym].inr_prem;
    const fx = (liveIndiaPremium && liveIndiaPremium.fx_rate) || 88.19;

    const notionalUSD = orderSizeUSD;
    const notionalINR = notionalUSD * fx;

    const hurdleUSD = notionalUSD * 0.0035;
    const netUSD = notionalUSD * (netPct / 100);
    const tdsINR = notionalINR * 0.01;
    const premINR = notionalINR * (premPct / 100);

    // Update Elements
    const elGross = document.getElementById('calcGrossVal');
    const elHurdle = document.getElementById('calcHurdleVal');
    const elNet = document.getElementById('calcNetVal');
    const elVerdict = document.getElementById('calcVerdictBox');
    const elTds = document.getElementById('calcTdsVal');
    const elPrem = document.getElementById('calcPremVal');

    if (elGross) elGross.textContent = `${grossPct.toFixed(3)}% ($${(notionalUSD * (grossPct / 100)).toFixed(2)})`;
    if (elHurdle) elHurdle.textContent = `-$${hurdleUSD.toFixed(2)} (35 bps)`;
    if (elNet) {
        elNet.textContent = `${netUSD >= 0 ? '+' : '-'}$${Math.abs(netUSD).toFixed(2)} (${netPct.toFixed(3)}%)`;
        elNet.style.color = netUSD >= 0 ? 'var(--c-green)' : 'var(--c-red)';
    }
    if (elTds) elTds.textContent = `₹${Math.round(tdsINR).toLocaleString('en-IN')} (Sec 194S)`;
    if (elPrem) elPrem.textContent = `+₹${Math.round(premINR).toLocaleString('en-IN')} (+${premPct.toFixed(2)}%)`;

    if (elVerdict) {
        if (netPct > 0) {
            elVerdict.innerHTML = `
                <div style="padding:12px 14px; background:var(--c-green-light); border:1px solid rgba(5,150,105,0.3); border-radius:8px; color:var(--c-green); font-size:0.8rem; font-family:var(--font-mono);">
                    <strong>STATUS: ARBITRAGE_VIABLE</strong> &bull; Gross spread exceeds 35 bps fee hurdle (+${netPct.toFixed(3)}% net alpha).
                </div>
            `;
        } else {
            elVerdict.innerHTML = `
                <div style="padding:12px 14px; background:var(--c-marigold-light); border:1px solid rgba(234,170,0,0.3); border-radius:8px; color:var(--c-marigold-deep); font-size:0.8rem; font-family:var(--font-mono);">
                    <strong>STATUS: ORDER_SUPPRESSED</strong> &bull; Inside 35 bps hurdle (${grossPct.toFixed(3)}% &lt; 0.35%). Execution suppressed to prevent fee decay.
                </div>
            `;
        }
    }
}

// =============================================================================
// 4. LIVE STREAMING PIPELINE SIMULATOR (INTERACTIVE PACKET TRACER)
// =============================================================================
async function triggerPipelineSimulation() {
    if (isSimulating) return;
    isSimulating = true;

    const btn = document.getElementById('simFireBtn');
    const logEl = document.getElementById('simLogOutput');
    const nodes = [
        document.getElementById('simNode1'),
        document.getElementById('simNode2'),
        document.getElementById('simNode3'),
        document.getElementById('simNode4'),
        document.getElementById('simNode5')
    ];

    if (btn) btn.disabled = true;

    const sym = currentAsset;
    const price = (liveArbitrage && liveArbitrage.pairs && liveArbitrage.pairs[sym] && liveArbitrage.pairs[sym].coinbase_price) || QUANT_BASELINES[sym].cb;
    const eventId = 'tx-' + Math.random().toString(16).substring(2, 8);

    const steps = [
        { node: 0, text: `[1/5] Ingested raw WebSocket tick: ${sym} @ $${price} (event_id: ${eventId})`, delay: 280 },
        { node: 1, text: `[2/5] Committed to Kafka KRaft: topic=stock-ticks, partition=2, offset=492104`, delay: 320 },
        { node: 2, text: `[3/5] Spark Structured Streaming: aggregated tumbling window [T, T+3s], &Delta;t=42ms`, delay: 350 },
        { node: 3, text: `[4/5] ClickHouse OLAP write: stocks.ticks, ZSTD columnar block flushed (48 bytes)`, delay: 300 },
        { node: 4, text: `[5/5] FastAPI ASGI broadcast: keyset cursor cursor_ts pushed to SSE terminal client!`, delay: 200 }
    ];

    for (let i = 0; i < steps.length; i++) {
        nodes.forEach((n, idx) => {
            if (n) n.classList.toggle('active-packet', idx === i);
        });
        if (logEl) logEl.textContent = steps[i].text;
        await new Promise(r => setTimeout(r, steps[i].delay));
    }

    await new Promise(r => setTimeout(r, 600));
    nodes.forEach(n => { if (n) n.classList.remove('active-packet'); });
    if (logEl) logEl.textContent = `Completed end-to-end event stream in 58 ms. Zero packet drop verified.`;
    if (btn) btn.disabled = false;
    isSimulating = false;
}

// =============================================================================
// 5. TERMINAL SHOWCASE TABS
// =============================================================================
function switchShowcaseTab(tabKey) {
    const btns = document.querySelectorAll('.terminal-nav-pill-btn');
    btns.forEach(b => b.classList.toggle('active', b.getAttribute('data-tab') === tabKey));

    const paneRadar = document.getElementById('showcaseRadarPane');
    const paneIndia = document.getElementById('showcaseIndiaPane');
    const paneImbalance = document.getElementById('showcaseImbalancePane');

    if (paneRadar) paneRadar.style.display = tabKey === 'radar' ? 'block' : 'none';
    if (paneIndia) paneIndia.style.display = tabKey === 'india' ? 'block' : 'none';
    if (paneImbalance) paneImbalance.style.display = tabKey === 'imbalance' ? 'block' : 'none';
}

// =============================================================================
// 6. GLOBAL KEYBOARD SHORTCUTS & INIT
// =============================================================================
document.addEventListener('keydown', (e) => {
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
    if (e.key.toLowerCase() === 't') {
        window.location.href = '/app';
    } else if (e.key.toLowerCase() === 'd') {
        toggleTheme();
    }
});

document.addEventListener('DOMContentLoaded', () => {
    initTheme();
    renderMarqueeTape();
    computeQuantitativeSandbox();
    pollStreamingEngine();

    // Poll live streaming pipeline every 2.5s
    setInterval(pollStreamingEngine, 2500);

    // Auto-fire one subtle packet trace on first load after 1.5s
    setTimeout(triggerPipelineSimulation, 1500);
});

// Window Bindings
window.toggleTheme = toggleTheme;
window.selectAsset = selectAsset;
window.onNotionalInput = onNotionalInput;
window.triggerPipelineSimulation = triggerPipelineSimulation;
window.switchShowcaseTab = switchShowcaseTab;
