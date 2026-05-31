const WebSocket = require('ws');
const ws = new WebSocket('wss://fstream.binance.com/ws/btcusdt@aggTrade');
ws.on('open', () => { console.log('ws opened'); });
ws.on('message', (d) => { console.log('msg', JSON.parse(d.toString())); ws.close(); });
