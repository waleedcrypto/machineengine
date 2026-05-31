const WebSocket = require('ws');
const ws = new WebSocket('wss://fstream.binance.com/stream?streams=btcusdt@aggTrade/btcusdt@markPrice');
ws.on('open', () => {
  console.log('connected to combined stream');
});
ws.on('message', (data) => {
  console.log('msg:', data.toString());
  ws.close();
});
setTimeout(() => { ws.close(); console.log('timeout'); }, 5000);
