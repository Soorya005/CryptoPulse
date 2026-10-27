// =============================================================================
// CRYPTOPULSE - INSTITUTIONAL LANDING PAGE CONTROLLER
// Real-time API Polling, Interactive Calculator, Tabbed Preview, and Dual Theme
// =============================================================================

let isDarkMode = false;
let currentCalcSymbol = 'BTC';
let calcOrderSizeUSD = 50000;

// Cached API data
let cachedArbData = null;
let cachedIndiaData = null;
let cachedTelemetry = null;

// Fallback baseline values
const BASELINE_DATA = {
    BTC: { cb: 80197.30, kr: 80200.10, inr: 8106712, gross: 0.003, inr_prem: 14.62 },
    ETH: { cb: 2322.06, kr: 2322.77, inr: 235023, gross: 0.031, inr_prem: 14.78 },
    SOL: { cb: 101.42, kr: 101.45, inr: 10254, gross: 0.030, inr_prem: 14.62 },
    XRP: { cb: 1.3005, kr: 1.2952, inr: 130.80, gross: 0.406, inr_prem: 14.06 },
    DOGE: { cb: 0.0789, kr: 0.0771, inr: 7.85, gross: 2.295, inr_prem: 12.93 },
    LTC: { cb: 56.558, kr: 56.550, inr: 6750, gross: 0.014, inr_prem: 35.33 }
};

// =============================================================================
// THEME CONTROLLER
// =============================================================================
function initTheme() {
    const saved = localStorage.getItem('cryptopulse_theme');
    if (saved === 'dark') {
        isDarkMode = true;
        document.body.classList.add('dark-theme');
    } else {
        isDarkMode = false;
        document.body.classList.remove('dark-theme');
    }
    updateThemeIcons();
}

function toggleTheme() {
    isDarkMode = !isDarkMode;
    document.body.classList.toggle('dark-theme', isDarkMode);
    localStorage.setItem('cryptopulse_theme', isDarkMode ? 'dark' : 'light');
    updateThemeIcons();
}

function updateThemeIcons() {
    const suns = document.querySelectorAll('.sun-icon');
    const moons = document.querySelectorAll('.moon-icon');
    suns.forEach(s => s.style.display = isDarkMode ? 'none' : 'block');
    moons.forEach(m => m.style.display = isDarkMode ? 'block' : 'none');
}

// =============================================================================
// REAL-TIME API INGESTION
// =============================================================================
async function fetchLandingLiveData() {
    try {
        const [resArb, resIndia, resTelem] = await Promise.allSettled([
            fetch('/api/arbitrage'),
            fetch('/api/india-premium'),
            fetch('/api/telemetry')
        ]);

        if (resArb.status === 'fulfilled' && resArb.value.ok) {
            cachedArbData = await resArb.value.json();
            updateArbitragePreview(cachedArbData);
            updateMarqueeTicker();
        }

        if (resIndia.status === 'fulfilled' && resIndia.value.ok) {
            cachedIndiaData = await resIndia.value.json();
            updateIndiaPreview(cachedIndiaData);
            updateMarqueeTicker();
        }

        if (resTelem.status === 'fulfilled' && resTelem.value.ok) {
            cachedTelemetry = await resTelem.value.json();
            updateTelemetryStats(cachedTelemetry);
        }

        updateCalculatorOutput();
    } catch (err) {
        console.warn('[Landing] Polling fallback active:', err);
    }
}

function updateArbitragePreview(data) {
    if (!data || !data.pairs) return;
    const pairs = data.pairs;
    const tbody = document.getElementById('previewArbTableBody');
    if (!tbody) return;

    const symbols = ['BTC', 'ETH', 'SOL', 'XRP'];
    tbody.innerHTML = symbols.map(sym => {
        const item = pairs[sym] || {};
        const cb = Number(item.coinbase_price || BASELINE_DATA[sym].cb);
        const kr = Number(item.kraken_price || BASELINE_DATA[sym].kr);
        const gross = Number(item.gross_spread_pct || BASELINE_DATA[sym].gross);
        const net = Number(item.net_profit_pct || (gross - 0.35));
        const isOpp = item.is_arbitrage_active || (net > 0);

        return `
            <tr>
                <td><strong style="color:var(--text-primary);">${sym}-USD</strong></td>
                <td>$${cb.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 4})}</td>
                <td>$${kr.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 4})}</td>
                <td style="color:${isOpp ? 'var(--color-green)' : 'var(--text-secondary)'}; font-weight:700;">
                    ${gross.toFixed(3)}%
                </td>
                <td>
                    <span class="badge-status-pill ${isOpp ? 'opportunity' : 'efficient'}" style="font-size:0.7rem; padding: 2px 7px;">
                        ${isOpp ? 'Opportunity' : 'Inside Hurdle'}
                    </span>
                </td>
            </tr>
        `;
    }).join('');

    // Update Hero Stat
    const effEl = document.getElementById('heroStatEfficiency');
    if (effEl && data.metrics) {
        effEl.textContent = `${data.metrics.efficiency_pct.toFixed(1)}%`;
    }
}

function updateIndiaPreview(data) {
    if (!data || !data.pairs) return;
    const pairs = data.pairs;
    const tbody = document.getElementById('previewIndiaTableBody');
    if (!tbody) return;

    const symbols = ['BTC', 'ETH', 'SOL', 'LTC'];
    tbody.innerHTML = symbols.map(sym => {
        const item = pairs[sym] || {};
        const inrPrice = Number(item.coindcx_price_inr || BASELINE_DATA[sym].inr);
        const prem = Number(item.premium_pct || BASELINE_DATA[sym].inr_prem);

        return `
            <tr>
                <td><strong style="color:var(--text-primary);">${sym}-INR</strong></td>
                <td>₹${Math.round(inrPrice).toLocaleString('en-IN')}</td>
                <td style="color:var(--color-amber); font-weight:700;">+${prem.toFixed(2)}%</td>
                <td>
                    <span class="capital-locked-badge" style="font-size:0.68rem; padding: 2px 6px;">
                        Locked (1% TDS)
                    </span>
                </td>
            </tr>
        `;
    }).join('');
}

function updateTelemetryStats(data) {
    if (!data) return;
    const rateEl = document.getElementById('heroStatThroughput');
    const latEl = document.getElementById('heroStatLatency');

    if (rateEl && data.ingestion_rate_per_sec) {
        rateEl.textContent = `${Number(data.ingestion_rate_per_sec).toLocaleString('en-US')} msg/s`;
    }
    if (latEl && data.e2e_latency_ms) {
        latEl.textContent = `< ${data.e2e_latency_ms} ms`;
    }
}

function updateMarqueeTicker() {
    const track = document.getElementById('marqueeTrack');
    if (!track) return;

    const pairs = (cachedArbData && cachedArbData.pairs) ? cachedArbData.pairs : {};
    const inrPairs = (cachedIndiaData && cachedIndiaData.pairs) ? cachedIndiaData.pairs : {};

    const items = ['BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'LTC'].map(sym => {
        const arb = pairs[sym] || {};
        const inr = inrPairs[sym] || {};
        const cbPrice = arb.coinbase_price || BASELINE_DATA[sym].cb;
        const gross = arb.gross_spread_pct !== undefined ? arb.gross_spread_pct : BASELINE_DATA[sym].gross;
        const inrPrice = inr.coindcx_price_inr || BASELINE_DATA[sym].inr;
        const prem = inr.premium_pct !== undefined ? inr.premium_pct : BASELINE_DATA[sym].inr_prem;

        return `
            <div class="marquee-item">
                <span class="marquee-sym">${sym}</span>
                <span class="marquee-val">$${Number(cbPrice).toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: (cbPrice < 1 ? 4 : 2)})}</span>
                <span class="marquee-badge arb">Spread ${Number(gross).toFixed(2)}%</span>
                <span style="color:var(--text-muted);">|</span>
                <span class="marquee-val">₹${Math.round(inrPrice).toLocaleString('en-IN')}</span>
                <span class="marquee-badge positive">+${Number(prem).toFixed(1)}% Prem</span>
            </div>
        `;
    }).join('');

    // Duplicate string for seamless infinite horizontal scroll
    track.innerHTML = items + items;
}

// =============================================================================
// INTERACTIVE SPREAD & TDS CALCULATOR LOGIC
// =============================================================================
function setCalcSymbol(sym) {
    currentCalcSymbol = sym;
    const buttons = document.querySelectorAll('.asset-pill-btn');
    buttons.forEach(btn => {
        btn.classList.toggle('active', btn.getAttribute('data-sym') === sym);
    });
    updateCalculatorOutput();
}

function onCalcAmountChange(val) {
    const clean = parseFloat(val) || 0;
    calcOrderSizeUSD = Math.max(100, Math.min(10000000, clean));
    updateCalculatorOutput();
}

function updateCalculatorOutput() {
    const sym = currentCalcSymbol;
    const arb = (cachedArbData && cachedArbData.pairs && cachedArbData.pairs[sym]) || {};
    const inr = (cachedIndiaData && cachedIndiaData.pairs && cachedIndiaData.pairs[sym]) || {};

    const grossSpreadPct = arb.gross_spread_pct !== undefined ? arb.gross_spread_pct : BASELINE_DATA[sym].gross;
    const feeHurdlePct = 0.35; // 0.15% maker + 0.20% taker
    const netArbPct = grossSpreadPct - feeHurdlePct;
    const indiaPremPct = inr.premium_pct !== undefined ? inr.premium_pct : BASELINE_DATA[sym].inr_prem;
    const fxRate = (cachedIndiaData && cachedIndiaData.fx_rate) || 88.19;

    const notionalUSD = calcOrderSizeUSD;
    const notionalINR = notionalUSD * fxRate;

    // Gross opportunity value
    const grossSpreadUSD = (notionalUSD * (grossSpreadPct / 100));
    const netSpreadUSD = (notionalUSD * (netArbPct / 100));
    const tds1PctINR = (notionalINR * 0.01);
    const grossIndiaDiffINR = (notionalINR * (indiaPremPct / 100));

    // Update DOM
    const elGrossPct = document.getElementById('calcGrossSpreadPct');
    const elNetPct = document.getElementById('calcNetProfitPct');
    const elHurdle = document.getElementById('calcHurdleUSD');
    const elNetUSD = document.getElementById('calcNetProfitUSD');
    const elTds = document.getElementById('calcTdsINR');
    const elIndiaPrem = document.getElementById('calcIndiaPremINR');
    const elStatus = document.getElementById('calcArbStatus');

    if (elGrossPct) elGrossPct.textContent = `${grossSpreadPct.toFixed(3)}%`;
    if (elNetPct) {
        elNetPct.textContent = `${netArbPct >= 0 ? '+' : ''}${netArbPct.toFixed(3)}%`;
        elNetPct.style.color = netArbPct >= 0 ? 'var(--color-green)' : 'var(--color-red)';
    }
    if (elHurdle) elHurdle.textContent = `$${(notionalUSD * 0.0035).toFixed(2)}`;
    if (elNetUSD) {
        elNetUSD.textContent = `${netSpreadUSD >= 0 ? '+' : '-'}$${Math.abs(netSpreadUSD).toFixed(2)}`;
        elNetUSD.style.color = netSpreadUSD >= 0 ? 'var(--color-green)' : 'var(--color-red)';
    }
    if (elTds) elTds.textContent = `₹${Math.round(tds1PctINR).toLocaleString('en-IN')}`;
    if (elIndiaPrem) elIndiaPrem.textContent = `+₹${Math.round(grossIndiaDiffINR).toLocaleString('en-IN')} (+${indiaPremPct.toFixed(1)}%)`;

    if (elStatus) {
        if (netArbPct > 0) {
            elStatus.innerHTML = `
                <div style="padding:10px 14px; background:var(--color-green-bg); border:1px solid var(--color-green-border); border-radius:8px; color:var(--color-green); font-size:0.82rem; font-weight:600;">
                    ✓ Arbitrage Opportunity Exceeds 0.35% Fee Hurdle. Net Profit Viable.
                </div>
            `;
        } else {
            elStatus.innerHTML = `
                <div style="padding:10px 14px; background:var(--bg-card-subtle); border:1px solid var(--border-card); border-radius:8px; color:var(--text-secondary); font-size:0.82rem;">
                    ● Inside Fee Hurdle (${grossSpreadPct.toFixed(3)}% &lt; 0.35%). HFT market efficiency active; execution suppressed.
                </div>
            `;
        }
    }
}

// =============================================================================
// TAB SWITCHER IN TERMINAL PREVIEW
// =============================================================================
function switchTerminalTab(tabId) {
    const tabs = document.querySelectorAll('.terminal-tab-btn');
    tabs.forEach(t => t.classList.toggle('active', t.getAttribute('data-tab') === tabId));

    const contentArb = document.getElementById('previewTabArbitrage');
    const contentIndia = document.getElementById('previewTabIndia');
    const contentPipeline = document.getElementById('previewTabPipeline');

    if (contentArb) contentArb.style.display = tabId === 'arb' ? 'grid' : 'none';
    if (contentIndia) contentIndia.style.display = tabId === 'india' ? 'block' : 'none';
    if (contentPipeline) contentPipeline.style.display = tabId === 'pipeline' ? 'block' : 'none';
}

// =============================================================================
// INITIALIZATION
// =============================================================================
document.addEventListener('DOMContentLoaded', () => {
    initTheme();
    updateMarqueeTicker();
    updateCalculatorOutput();
    fetchLandingLiveData();

    // Poll live data every 2.5 seconds
    setInterval(fetchLandingLiveData, 2500);
});

// Window Bindings
window.toggleTheme = toggleTheme;
window.setCalcSymbol = setCalcSymbol;
window.onCalcAmountChange = onCalcAmountChange;
window.switchTerminalTab = switchTerminalTab;
