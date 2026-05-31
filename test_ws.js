const WebSocket = require('ws');
const ws = new WebSocket('wss://fstream.binance.com/ws/btcusdt@aggTrade');
ws.on('open', () => {
  console.log('connected');
});
ws.on('message', (data) => {
  console.log('msg:', data.toString());
  ws.close();
});
ws.on('error', (err) => {
  console.log('err:', err);
});
