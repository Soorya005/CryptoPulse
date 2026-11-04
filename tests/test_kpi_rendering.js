const fs = require('fs');

const html = fs.readFileSync('src/production/dashboard/static/crypto_index.html', 'utf8');

// Simple DOM Mock
const elements = {};
const idRegex = /id=["']([^"']+)["']/g;
let match;
while ((match = idRegex.exec(html)) !== null) {
    const id = match[1];
    elements[id] = {
        id: id,
        textContent: '',
        innerHTML: '',
        className: '',
        style: {},
        value: '',
        getContext: () => ({
            clearRect: () => {},
            beginPath: () => {},
            arc: () => {},
            fill: () => {},
            stroke: () => {},
            moveTo: () => {},
            lineTo: () => {},
            createLinearGradient: () => ({ addColorStop: () => {} }),
            fillRect: () => {},
            fillText: () => {},
            measureText: () => ({ width: 10 }),
            setLineDash: () => {},
        }),
        addEventListener: () => {},
        removeEventListener: () => {},
        setAttribute: () => {},
        getAttribute: () => null,
        classList: {
            add: () => {},
            remove: () => {},
            contains: () => false,
            toggle: () => {}
        }
    };
}

console.log('Mocked elements count:', Object.keys(elements).length);

global.window = global;
global.location = { protocol: 'http:', host: 'localhost:8502' };
global.window.location = global.location;
global.document = {
    getElementById: (id) => elements[id] || null,
    querySelectorAll: (sel) => [],
    querySelector: (sel) => null,
    addEventListener: (event, cb) => {
        if (event === 'DOMContentLoaded') {
            global.onDOMContentLoaded = cb;
        }
    },
    createElement: (tag) => ({
        style: {},
        classList: { add: () => {}, remove: () => {} },
        appendChild: () => {},
        setAttribute: () => {},
    }),
    body: {
        classList: { add: () => {}, remove: () => {}, contains: () => false }
    }
};

global.localStorage = {
    getItem: () => null,
    setItem: () => {}
};

global.EventSource = class {
    constructor() { this.readyState = 0; }
    close() {}
};

global.WebSocket = class {
    constructor() { this.readyState = 0; }
    close() {}
};

global.Chart = class {
    constructor() {
        this.data = { labels: [], datasets: [{ data: [] }] };
    }
    destroy() {}
    update() {}
};

// Real fetch to localhost:8502
const nativeFetch = global.fetch;
global.fetch = async (url) => {
    const fullUrl = url.startsWith('http') ? url : `http://localhost:8502${url}`;
    console.log('Real fetch to:', fullUrl);
    const res = await nativeFetch(fullUrl);
    return res;
};

try {
    const js = fs.readFileSync('src/production/dashboard/static/js/dashboard.js', 'utf8');
    eval(js);
    console.log('Eval dashboard.js successful!');

    if (global.onDOMContentLoaded) {
        console.log('Executing onDOMContentLoaded...');
        global.onDOMContentLoaded();
        console.log('onDOMContentLoaded finished successfully!');

        setTimeout(() => {
            console.log('--- KPI Values After Polling ---');
            console.log('dedicatedMaxZScore:', elements['dedicatedMaxZScore'].textContent);
            console.log('dedicatedMaxZScoreSub:', elements['dedicatedMaxZScoreSub'].textContent);
            console.log('dedicatedDetectionLatency:', elements['dedicatedDetectionLatency'].textContent);
            console.log('dedicatedAvgTradeSize:', elements['dedicatedAvgTradeSize'].textContent);
            console.log('dedicatedAvgTradeSub:', elements['dedicatedAvgTradeSub'].textContent);
            console.log('dedicatedTakerRatio:', elements['dedicatedTakerRatio'].textContent);
            console.log('dedicatedTakerSentiment:', elements['dedicatedTakerSentiment'].textContent);

            console.log('\n--- Testing filterDedicatedTrades("BTC") ---');
            global.filterDedicatedTrades('BTC');
            console.log('BTC dedicatedAvgTradeSize:', elements['dedicatedAvgTradeSize'].textContent);
            console.log('BTC dedicatedAvgTradeSub:', elements['dedicatedAvgTradeSub'].textContent);
            console.log('BTC dedicatedTakerRatio:', elements['dedicatedTakerRatio'].textContent);
            console.log('BTC dedicatedTakerSentiment:', elements['dedicatedTakerSentiment'].textContent);

            process.exit(0);
        }, 500);
    }
} catch (err) {
    console.error('Error during eval or DOMContentLoaded:', err);
}
