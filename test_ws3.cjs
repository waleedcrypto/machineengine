const WebSocket = require('ws');
const ws = new WebSocket('wss://stream.binance.com:9443/ws/btcusdt@aggTrade');
ws.on('open', () => console.log('connected spot'));
ws.on('message', (d) => { console.log('msg spot', d.toString()); ws.close(); });
setTimeout(() => ws.close(), 5000);
