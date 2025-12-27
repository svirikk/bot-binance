// ============================================================================
// BINANCE FUTURES AGGRESSIVE FLOW MONITOR (VPN-Ready)
// Real-time detection via individual symbol streams (works globally)
// ============================================================================

if (process.env.NODE_ENV !== 'production') {
  require('dotenv').config();
}

const WebSocket = require('ws');
const TelegramBot = require('node-telegram-bot-api');
const axios = require('axios');

// ============================================================================
// CONFIGURATION
// ============================================================================

const CONFIG = {
  // Signal thresholds
  MIN_VOLUME_USD: parseFloat(process.env.MIN_VOLUME_USD) || 300_000,
  MIN_DOMINANCE: parseFloat(process.env.MIN_DOMINANCE) || 65.0,
  MIN_PRICE_CHANGE: parseFloat(process.env.MIN_PRICE_CHANGE) || 0.3,
  
  // Time windows
  WINDOW_SECONDS: parseInt(process.env.WINDOW_SECONDS) || 180,
  COOLDOWN_MINUTES: parseInt(process.env.COOLDOWN_MINUTES) || 15,
  
  // Symbols (manual list - no API needed)
  SYMBOLS: (process.env.SYMBOLS || 'BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT,ADAUSDT,DOGEUSDT,AVAXUSDT,DOTUSDT,MATICUSDT,LINKUSDT,LTCUSDT,UNIUSDT,ATOMUSDT,NEARUSDT,APTUSDT,ARBUSDT,OPUSDT,SUIUSDT,INJUSDT,SEIUSDT,TIAUSDT,PEPEUSDT,WLDUSDT,RENDERUSDT,FETUSDT,TAOUSDT,ORDIUSDT,STXUSDT,MANTAUSDT').split(','),
  
  // System
  STATS_LOG_INTERVAL: parseInt(process.env.STATS_LOG_INTERVAL) || 60,
  MAX_RECONNECTS: parseInt(process.env.MAX_RECONNECTS) || 10,
  
  // Binance - DIRECT connection (no /stream path)
  BINANCE_WS: 'wss://fstream.binance.com/ws',
  
  // Telegram
  TELEGRAM_TOKEN: process.env.TELEGRAM_BOT_TOKEN,
  TELEGRAM_CHAT_ID: process.env.TELEGRAM_CHAT_ID,
};

// ============================================================================
// SYMBOL STATE
// ============================================================================

class SymbolState {
  constructor(symbol, windowSeconds) {
    this.symbol = symbol;
    this.windowMs = windowSeconds * 1000;
    this.trades = [];
    this.firstPrice = null;
    this.lastPrice = null;
  }

  addTrade(timestamp, price, quantity, isBuyerMaker) {
    const volume = price * quantity;
    
    const trade = {
      timestamp,
      price,
      buyVol: isBuyerMaker ? 0 : volume,
      sellVol: isBuyerMaker ? volume : 0
    };

    this.trades.push(trade);
    this.lastPrice = price;
    
    if (this.firstPrice === null) {
      this.firstPrice = price;
    }

    this.cleanup(timestamp);
  }

  cleanup(currentTime) {
    const cutoff = currentTime - this.windowMs;
    this.trades = this.trades.filter(t => t.timestamp >= cutoff);

    if (this.trades.length > 0) {
      this.firstPrice = this.trades[0].price;
    } else {
      this.firstPrice = null;
    }
  }

  getStats() {
    if (this.trades.length === 0) return null;

    let buyVolume = 0;
    let sellVolume = 0;

    for (const trade of this.trades) {
      buyVolume += trade.buyVol;
      sellVolume += trade.sellVol;
    }

    const totalVolume = buyVolume + sellVolume;
    if (totalVolume === 0) return null;

    const buyDominance = (buyVolume / totalVolume) * 100;
    const sellDominance = (sellVolume / totalVolume) * 100;
    
    const dominantSide = buyVolume > sellVolume ? 'buy' : 'sell';
    const dominance = Math.max(buyDominance, sellDominance);

    const priceChange = this.firstPrice 
      ? ((this.lastPrice - this.firstPrice) / this.firstPrice) * 100
      : 0;

    const duration = (this.trades[this.trades.length - 1].timestamp - this.trades[0].timestamp) / 1000;

    return {
      buyVolume,
      sellVolume,
      totalVolume,
      dominantSide,
      dominance,
      priceChange,
      duration,
      tradeCount: this.trades.length,
      lastPrice: this.lastPrice
    };
  }

  reset() {
    this.trades = [];
    this.firstPrice = null;
    this.lastPrice = null;
  }
}

// ============================================================================
// TRADE AGGREGATOR
// ============================================================================

class TradeAggregator {
  constructor(windowSeconds) {
    this.windowSeconds = windowSeconds;
    this.states = new Map();
  }

  addTrade(symbol, timestamp, price, quantity, isBuyerMaker) {
    if (!this.states.has(symbol)) {
      this.states.set(symbol, new SymbolState(symbol, this.windowSeconds));
    }
    this.states.get(symbol).addTrade(timestamp, price, quantity, isBuyerMaker);
  }

  getStats(symbol) {
    const state = this.states.get(symbol);
    return state ? state.getStats() : null;
  }

  resetSymbol(symbol) {
    const state = this.states.get(symbol);
    if (state) state.reset();
  }

  getActiveCount() {
    return this.states.size;
  }

  getTotalTrades() {
    let total = 0;
    for (const state of this.states.values()) {
      total += state.trades.length;
    }
    return total;
  }
}

// ============================================================================
// SIGNAL ENGINE
// ============================================================================

class SignalEngine {
  shouldAlert(stats) {
    if (!stats) return false;
    if (stats.totalVolume < CONFIG.MIN_VOLUME_USD) return false;
    if (stats.dominance < CONFIG.MIN_DOMINANCE) return false;
    if (Math.abs(stats.priceChange) < CONFIG.MIN_PRICE_CHANGE) return false;
    
    // Direction alignment
    if (stats.dominantSide === 'buy' && stats.priceChange < 0) return false;
    if (stats.dominantSide === 'sell' && stats.priceChange > 0) return false;

    return true;
  }

  interpretSignal(stats) {
    if (stats.dominantSide === 'buy') {
      return {
        type: 'SHORT SQUEEZE',
        emoji: '🟢',
        direction: 'UP'
      };
    } else {
      return {
        type: 'LONG FLUSH',
        emoji: '🔴',
        direction: 'DOWN'
      };
    }
  }
}

// ============================================================================
// COOLDOWN MANAGER
// ============================================================================

class CooldownManager {
  constructor(cooldownMinutes) {
    this.cooldowns = new Map();
    this.cooldownMs = cooldownMinutes * 60 * 1000;
  }

  canAlert(symbol, stats) {
    if (!this.cooldowns.has(symbol)) return true;

    const last = this.cooldowns.get(symbol);
    const elapsed = Date.now() - last.timestamp;
    
    if (elapsed < this.cooldownMs) {
      const oppositeSide = stats.dominantSide !== last.side;
      const biggerVolume = stats.totalVolume / last.volume >= 2.0;
      return oppositeSide || biggerVolume;
    }

    return true;
  }

  recordAlert(symbol, stats) {
    this.cooldowns.set(symbol, {
      timestamp: Date.now(),
      side: stats.dominantSide,
      volume: stats.totalVolume
    });
  }
}

// ============================================================================
// ALERT MANAGER
// ============================================================================

class AlertManager {
  constructor(telegram) {
    this.telegram = telegram;
    this.alertCount = 0;
  }

  async sendAlert(symbol, stats, interpretation) {
    const lines = [];
    
    lines.push(`${interpretation.emoji} ${interpretation.type}`);
    lines.push(`💰 Volume: $${this.fmt(stats.totalVolume)} in ${stats.duration.toFixed(0)}s`);
    lines.push(`📊 Dominance: ${stats.dominance.toFixed(1)}% ${interpretation.direction}`);
    lines.push('━━━━━━━━━━━━━━━━━');
    
    const cleanSymbol = symbol.replace('USDT', '');
    lines.push(`🎯 ${symbol} #${cleanSymbol}`);
    
    const priceSign = stats.priceChange >= 0 ? '+' : '';
    lines.push(`📈 Price Δ: ${priceSign}${stats.priceChange.toFixed(2)}%`);
    lines.push(`💵 Last: $${stats.lastPrice.toFixed(4)}`);
    
    lines.push('━━━━━━━━━━━━━━━━━');
    lines.push(`🟢 Aggressive Buy: $${this.fmt(stats.buyVolume)}`);
    lines.push(`🔴 Aggressive Sell: $${this.fmt(stats.sellVolume)}`);
    
    const message = lines.join('\n');
    
    try {
      await this.telegram.sendMessage(CONFIG.TELEGRAM_CHAT_ID, message);
      this.alertCount++;
      console.log(`[ALERT] ${symbol} | ${interpretation.type} | $${this.fmt(stats.totalVolume)} | ${stats.dominance.toFixed(1)}% | Δ${stats.priceChange.toFixed(2)}%`);
    } catch (error) {
      console.error(`[ALERT] Error:`, error.message);
    }
  }

  fmt(num) {
    if (num >= 1_000_000) return (num / 1_000_000).toFixed(2) + 'M';
    if (num >= 1_000) return (num / 1_000).toFixed(0) + 'K';
    return num.toFixed(0);
  }

  getCount() {
    return this.alertCount;
  }
}

// ============================================================================
// MULTI-WEBSOCKET MANAGER
// ============================================================================

class MultiWebSocketManager {
  constructor(symbols, tradeAggregator, signalEngine, cooldownManager, alertManager) {
    this.symbols = symbols;
    this.tradeAggregator = tradeAggregator;
    this.signalEngine = signalEngine;
    this.cooldownManager = cooldownManager;
    this.alertManager = alertManager;
    
    this.connections = new Map();
    this.tradeCount = 0;
    this.lastStatsLog = Date.now();
    this.reconnectAttempts = new Map();
  }

  connectAll() {
    console.log(`[WS] Connecting to ${this.symbols.length} symbols...`);
    
    // Connect in batches to avoid overwhelming
    const batchSize = 10;
    for (let i = 0; i < this.symbols.length; i += batchSize) {
      setTimeout(() => {
        const batch = this.symbols.slice(i, i + batchSize);
        batch.forEach(symbol => this.connectSymbol(symbol));
      }, i * 100); // 100ms delay between batches
    }
  }

  connectSymbol(symbol) {
    const streamName = `${symbol.toLowerCase()}@aggTrade`;
    const url = `${CONFIG.BINANCE_WS}/${streamName}`;
    
    const ws = new WebSocket(url);

    ws.on('open', () => {
      console.log(`[WS] ${symbol} connected`);
      this.reconnectAttempts.set(symbol, 0);
    });

    ws.on('message', (data) => {
      this.handleMessage(symbol, data);
    });

    ws.on('error', (error) => {
      console.error(`[WS] ${symbol} error:`, error.message);
    });

    ws.on('close', () => {
      console.log(`[WS] ${symbol} closed`);
      this.reconnectSymbol(symbol);
    });

    this.connections.set(symbol, ws);
  }

  handleMessage(symbol, data) {
    try {
      const trade = JSON.parse(data);
      
      // Binance aggTrade format
      const price = parseFloat(trade.p);
      const quantity = parseFloat(trade.q);
      const timestamp = trade.T;
      const isBuyerMaker = trade.m;
      
      this.tradeAggregator.addTrade(symbol, timestamp, price, quantity, isBuyerMaker);
      this.tradeCount++;
      
      // Check for signal
      const stats = this.tradeAggregator.getStats(symbol);
      
      if (stats && stats.totalVolume >= CONFIG.MIN_VOLUME_USD * 0.5) {
        if (this.signalEngine.shouldAlert(stats)) {
          if (this.cooldownManager.canAlert(symbol, stats)) {
            const interpretation = this.signalEngine.interpretSignal(stats);
            this.alertManager.sendAlert(symbol, stats, interpretation);
            this.cooldownManager.recordAlert(symbol, stats);
            this.tradeAggregator.resetSymbol(symbol);
          }
        }
      }
      
      this.logStats();
      
    } catch (error) {
      console.error(`[WS] ${symbol} parse error:`, error.message);
    }
  }

  logStats() {
    const now = Date.now();
    if (now - this.lastStatsLog < CONFIG.STATS_LOG_INTERVAL * 1000) {
      return;
    }

    const activeSymbols = this.tradeAggregator.getActiveCount();
    const totalTrades = this.tradeAggregator.getTotalTrades();
    const alerts = this.alertManager.getCount();
    const connected = Array.from(this.connections.values()).filter(ws => ws.readyState === WebSocket.OPEN).length;
    
    console.log(`[STATS] Connected: ${connected}/${this.symbols.length} | Active: ${activeSymbols} | Trades: ${totalTrades} | Alerts: ${alerts} | Rate: ${(this.tradeCount / CONFIG.STATS_LOG_INTERVAL).toFixed(0)}/s`);
    
    this.tradeCount = 0;
    this.lastStatsLog = now;
  }

  reconnectSymbol(symbol) {
    const attempts = this.reconnectAttempts.get(symbol) || 0;
    
    if (attempts >= CONFIG.MAX_RECONNECTS) {
      console.error(`[WS] ${symbol} max reconnects reached`);
      return;
    }

    this.reconnectAttempts.set(symbol, attempts + 1);
    
    setTimeout(() => {
      console.log(`[WS] ${symbol} reconnecting (${attempts + 1}/${CONFIG.MAX_RECONNECTS})...`);
      this.connectSymbol(symbol);
    }, 5000 * (attempts + 1)); // Exponential backoff
  }

  closeAll() {
    for (const ws of this.connections.values()) {
      ws.close();
    }
    this.connections.clear();
  }
}

// ============================================================================
// MAIN APPLICATION
// ============================================================================

class BinanceFuturesFlowBot {
  constructor() {
    this.telegram = new TelegramBot(CONFIG.TELEGRAM_TOKEN, { polling: false });
    this.tradeAggregator = new TradeAggregator(CONFIG.WINDOW_SECONDS);
    this.signalEngine = new SignalEngine();
    this.cooldownManager = new CooldownManager(CONFIG.COOLDOWN_MINUTES);
    this.alertManager = new AlertManager(this.telegram);
    this.wsManager = null;
  }

  async start() {
    console.log('='.repeat(70));
    console.log('BINANCE FUTURES AGGRESSIVE FLOW MONITOR');
    console.log('='.repeat(70));
    console.log(`Symbols: ${CONFIG.SYMBOLS.length} | Window: ${CONFIG.WINDOW_SECONDS}s`);
    console.log(`Min Volume: $${(CONFIG.MIN_VOLUME_USD / 1e6).toFixed(1)}M | Dominance: ${CONFIG.MIN_DOMINANCE}%`);
    console.log(`Price Δ: ${CONFIG.MIN_PRICE_CHANGE}% | Cooldown: ${CONFIG.COOLDOWN_MINUTES}min`);
    console.log('='.repeat(70));
    console.log('Monitoring:', CONFIG.SYMBOLS.join(', '));
    console.log('='.repeat(70));

    // Test Telegram
    try {
      await this.telegram.sendMessage(
        CONFIG.TELEGRAM_CHAT_ID,
        '🚀 <b>Binance Futures Monitor Started</b>\n\n' +
        `📊 Watching ${CONFIG.SYMBOLS.length} symbols\n` +
        `⚡ Signal: $${(CONFIG.MIN_VOLUME_USD / 1e6).toFixed(1)}M | ${CONFIG.MIN_DOMINANCE}% | ${CONFIG.MIN_PRICE_CHANGE}%`,
        { parse_mode: 'HTML' }
      );
      console.log('[TELEGRAM] ✅ Connected\n');
    } catch (error) {
      console.error('[TELEGRAM] ❌ Error:', error.message);
      process.exit(1);
    }

    // Connect WebSockets
    this.wsManager = new MultiWebSocketManager(
      CONFIG.SYMBOLS,
      this.tradeAggregator,
      this.signalEngine,
      this.cooldownManager,
      this.alertManager
    );
    
    this.wsManager.connectAll();

    // Graceful shutdown
    process.on('SIGINT', () => this.shutdown());
    process.on('SIGTERM', () => this.shutdown());
  }

  async shutdown() {
    console.log('\n[SHUTDOWN] Stopping...');
    
    if (this.wsManager) {
      this.wsManager.closeAll();
    }
    
    await this.telegram.sendMessage(
      CONFIG.TELEGRAM_CHAT_ID,
      '⛔ Binance Futures Monitor Stopped'
    );
    
    process.exit(0);
  }
}

// ============================================================================
// STARTUP
// ============================================================================

if (require.main === module) {
  const bot = new BinanceFuturesFlowBot();
  bot.start().catch(error => {
    console.error('[FATAL]', error);
    process.exit(1);
  });
}

module.exports = { BinanceFuturesFlowBot };