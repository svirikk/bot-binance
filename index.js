// ============================================================================
// BINANCE FUTURES AGGRESSIVE FLOW MONITOR
// Real-time detection of forced market movements via aggressive trade flow
// ============================================================================
//
// ARCHITECTURE PRINCIPLES:
// 1. Single WebSocket stream (!aggTrade@arr) - all USDT perpetuals
// 2. Rolling window aggregation per symbol
// 3. Signal detection based on volume dominance + price impulse
// 4. Zero duplicate alerts via cooldown system
// 5. Memory-efficient per-symbol state management
//
// WHY THIS APPROACH:
// - Binance provides excellent global agg trade stream
// - No need for individual subscriptions (scales to 1000+ symbols)
// - Real aggressive flow = better signal than liquidation events
// - REST only for symbol initialization, not hot path
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
  MIN_DOMINANCE: parseFloat(process.env.MIN_DOMINANCE) || 70.0,
  MIN_PRICE_CHANGE: parseFloat(process.env.MIN_PRICE_CHANGE) || 0.3,
  
  // Time windows
  WINDOW_SECONDS: parseInt(process.env.WINDOW_SECONDS) || 180,
  COOLDOWN_MINUTES: parseInt(process.env.COOLDOWN_MINUTES) || 15,
  
  // Optional filters
  MIN_24H_VOLUME: parseFloat(process.env.MIN_24H_VOLUME) || 0, // 0 = disabled
  BLACKLIST: (process.env.BLACKLIST || '').split(',').filter(Boolean),
  
  // System
  STATS_LOG_INTERVAL: parseInt(process.env.STATS_LOG_INTERVAL) || 60, // seconds
  SYMBOL_REFRESH_HOURS: parseInt(process.env.SYMBOL_REFRESH_HOURS) || 4,
  
  // Binance
  BINANCE_WS: 'wss://fstream.binance.com/stream',
  BINANCE_API: 'https://fapi.binance.com',
  
  // Telegram
  TELEGRAM_TOKEN: process.env.TELEGRAM_BOT_TOKEN,
  TELEGRAM_CHAT_ID: process.env.TELEGRAM_CHAT_ID,
};

// ============================================================================
// SYMBOL MANAGER
// ============================================================================
// Manages active symbols and optional 24h volume filtering

class SymbolManager {
  constructor() {
    this.symbols = new Set();
    this.volumeData = new Map(); // symbol -> 24h volume
  }

  async initialize() {
    console.log('[SYMBOLS] Fetching Binance Futures symbols...');
    
    try {
      const response = await axios.get(`${CONFIG.BINANCE_API}/fapi/v1/ticker/24hr`, {
        timeout: 10000,
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
      });

      let count = 0;
      let filtered = 0;

      for (const ticker of response.data) {
        const symbol = ticker.symbol;
        
        // Only USDT perpetuals
        if (!symbol.endsWith('USDT')) continue;
        
        // Blacklist check
        if (CONFIG.BLACKLIST.includes(symbol)) {
          filtered++;
          continue;
        }
        
        const volume24h = parseFloat(ticker.quoteVolume) || 0;
        
        // Optional volume filter
        if (CONFIG.MIN_24H_VOLUME > 0 && volume24h < CONFIG.MIN_24H_VOLUME) {
          filtered++;
          continue;
        }
        
        this.symbols.add(symbol);
        this.volumeData.set(symbol, volume24h);
        count++;
      }

      console.log(`[SYMBOLS] Active: ${count} | Filtered: ${filtered}`);
      
      if (CONFIG.MIN_24H_VOLUME > 0) {
        console.log(`[SYMBOLS] Filter: 24h volume > $${(CONFIG.MIN_24H_VOLUME / 1e6).toFixed(1)}M`);
      }
      
      if (CONFIG.BLACKLIST.length > 0) {
        console.log(`[SYMBOLS] Blacklisted: ${CONFIG.BLACKLIST.join(', ')}`);
      }

      return Array.from(this.symbols);
      
    } catch (error) {
      console.error('[SYMBOLS] Error:', error.message);
      return [];
    }
  }

  isActive(symbol) {
    return this.symbols.has(symbol);
  }

  getVolume24h(symbol) {
    return this.volumeData.get(symbol) || 0;
  }

  getCount() {
    return this.symbols.size;
  }
}

// ============================================================================
// SYMBOL STATE
// ============================================================================
// Per-symbol rolling window state

class SymbolState {
  constructor(symbol, windowSeconds) {
    this.symbol = symbol;
    this.windowMs = windowSeconds * 1000;
    this.trades = []; // [{timestamp, price, buyVol, sellVol}]
    this.firstPrice = null;
    this.lastPrice = null;
  }

  addTrade(timestamp, price, quantity, isBuyerMaker) {
    const volume = price * quantity;
    
    const trade = {
      timestamp,
      price,
      buyVol: isBuyerMaker ? 0 : volume,  // buyer = taker buy (aggressive buy)
      sellVol: isBuyerMaker ? volume : 0   // seller = taker sell (aggressive sell)
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
    
    let firstValidIdx = 0;
    for (let i = 0; i < this.trades.length; i++) {
      if (this.trades[i].timestamp >= cutoff) {
        firstValidIdx = i;
        break;
      }
    }

    if (firstValidIdx > 0) {
      this.trades = this.trades.slice(firstValidIdx);
    }

    // Update first price
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

  getMemorySize() {
    return this.trades.length;
  }
}

// ============================================================================
// TRADE AGGREGATOR
// ============================================================================
// Manages all symbol states and provides aggregated stats

class TradeAggregator {
  constructor(windowSeconds) {
    this.windowSeconds = windowSeconds;
    this.states = new Map(); // symbol -> SymbolState
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
    if (state) {
      state.reset();
    }
  }

  getActiveSymbolCount() {
    return this.states.size;
  }

  getTotalTradeCount() {
    let total = 0;
    for (const state of this.states.values()) {
      total += state.getMemorySize();
    }
    return total;
  }

  cleanup() {
    // Remove empty states to prevent memory leak
    for (const [symbol, state] of this.states.entries()) {
      if (state.getMemorySize() === 0) {
        this.states.delete(symbol);
      }
    }
  }
}

// ============================================================================
// SIGNAL ENGINE
// ============================================================================
// Detects tradeable signals based on configurable criteria

class SignalEngine {
  shouldAlert(stats) {
    if (!stats) return false;

    // Volume threshold
    if (stats.totalVolume < CONFIG.MIN_VOLUME_USD) {
      return false;
    }

    // Dominance threshold
    if (stats.dominance < CONFIG.MIN_DOMINANCE) {
      return false;
    }

    // Price change threshold
    if (Math.abs(stats.priceChange) < CONFIG.MIN_PRICE_CHANGE) {
      return false;
    }

    // Direction alignment: buy dominance should = price up (and vice versa)
    if (stats.dominantSide === 'buy' && stats.priceChange < 0) {
      return false;
    }
    
    if (stats.dominantSide === 'sell' && stats.priceChange > 0) {
      return false;
    }

    return true;
  }

  interpretSignal(stats) {
    // Buy dominance = shorts forced to cover (aggressive buying)
    // Sell dominance = longs forced to close (aggressive selling)
    
    if (stats.dominantSide === 'buy') {
      return {
        type: 'SHORT SQUEEZE',
        emoji: '🟢',
        direction: 'UP',
        liquidatedSide: 'shorts'
      };
    } else {
      return {
        type: 'LONG FLUSH',
        emoji: '🔴',
        direction: 'DOWN',
        liquidatedSide: 'longs'
      };
    }
  }
}

// ============================================================================
// COOLDOWN MANAGER
// ============================================================================
// Prevents duplicate alerts for same symbol

class CooldownManager {
  constructor(cooldownMinutes) {
    this.cooldowns = new Map(); // symbol -> {timestamp, side, volume}
    this.cooldownMs = cooldownMinutes * 60 * 1000;
  }

  canAlert(symbol, stats) {
    if (!this.cooldowns.has(symbol)) {
      return true;
    }

    const lastAlert = this.cooldowns.get(symbol);
    const elapsed = Date.now() - lastAlert.timestamp;
    
    // Cooldown not expired
    if (elapsed < this.cooldownMs) {
      // Allow if opposite side or 2x volume
      const oppositeSide = stats.dominantSide !== lastAlert.side;
      const biggerVolume = stats.totalVolume / lastAlert.volume >= 2.0;
      
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

  cleanup() {
    const now = Date.now();
    for (const [symbol, data] of this.cooldowns.entries()) {
      if (now - data.timestamp > this.cooldownMs * 2) {
        this.cooldowns.delete(symbol);
      }
    }
  }
}

// ============================================================================
// ALERT MANAGER
// ============================================================================
// Formats and sends Telegram alerts

class AlertManager {
  constructor(telegram) {
    this.telegram = telegram;
    this.alertCount = 0;
  }

  async sendAlert(symbol, stats, interpretation, volume24h) {
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
    
    if (volume24h > 0) {
      lines.push(`📊 24h Volume: $${this.fmt(volume24h)}`);
    }
    
    const message = lines.join('\n');
    
    try {
      await this.telegram.sendMessage(CONFIG.TELEGRAM_CHAT_ID, message, {
        parse_mode: 'HTML',
        disable_web_page_preview: true
      });
      
      this.alertCount++;
      
      console.log(`[ALERT] ${symbol} | ${interpretation.type} | $${this.fmt(stats.totalVolume)} | ${stats.dominance.toFixed(1)}% | Δ${stats.priceChange.toFixed(2)}%`);
      
    } catch (error) {
      console.error(`[ALERT] Error sending for ${symbol}:`, error.message);
    }
  }

  fmt(num) {
    if (num >= 1_000_000) return (num / 1_000_000).toFixed(2) + 'M';
    if (num >= 1_000) return (num / 1_000).toFixed(0) + 'K';
    return num.toFixed(0);
  }

  getAlertCount() {
    return this.alertCount;
  }
}

// ============================================================================
// WEBSOCKET MANAGER
// ============================================================================
// Connects to Binance global aggTrade stream

class WebSocketManager {
  constructor(symbolManager, tradeAggregator, signalEngine, cooldownManager, alertManager) {
    this.symbolManager = symbolManager;
    this.tradeAggregator = tradeAggregator;
    this.signalEngine = signalEngine;
    this.cooldownManager = cooldownManager;
    this.alertManager = alertManager;
    
    this.ws = null;
    this.reconnectAttempts = 0;
    this.maxReconnects = 10;
    this.reconnectDelay = 5000;
    this.pingInterval = null;
    
    this.tradeCount = 0;
    this.lastStatsLog = Date.now();
  }

  connect() {
    console.log('[WS] Connecting to Binance...');
    
    // Global aggTrade stream for all symbols
    const streamName = '!aggTrade@arr';
    const url = `${CONFIG.BINANCE_WS}?streams=${streamName}`;
    
    this.ws = new WebSocket(url);

    this.ws.on('open', () => {
      console.log('[WS] Connected successfully');
      this.reconnectAttempts = 0;
      this.startPing();
      console.log(`[WS] Monitoring ${this.symbolManager.getCount()} symbols`);
      console.log(`[WS] Signal criteria: $${(CONFIG.MIN_VOLUME_USD / 1e6).toFixed(1)}M | ${CONFIG.MIN_DOMINANCE}% | ${CONFIG.MIN_PRICE_CHANGE}%`);
      console.log('[WS] Listening for aggressive trades...\n');
    });

    this.ws.on('message', (data) => {
      this.handleMessage(data);
    });

    this.ws.on('error', (error) => {
      console.error('[WS] Error:', error.message);
    });

    this.ws.on('close', () => {
      console.log('[WS] Connection closed');
      this.stopPing();
      this.reconnect();
    });

    this.ws.on('pong', () => {
      // Connection alive
    });
  }

  handleMessage(data) {
    try {
      const message = JSON.parse(data);
      
      // Global stream format: {stream: "...", data: {...}}
      if (!message.data) return;
      
      const trades = Array.isArray(message.data) ? message.data : [message.data];
      
      for (const trade of trades) {
        const symbol = trade.s;
        
        // Filter inactive symbols
        if (!this.symbolManager.isActive(symbol)) continue;
        
        const price = parseFloat(trade.p);
        const quantity = parseFloat(trade.q);
        const timestamp = trade.T;
        const isBuyerMaker = trade.m; // true = sell aggression, false = buy aggression
        
        // Add to aggregator
        this.tradeAggregator.addTrade(symbol, timestamp, price, quantity, isBuyerMaker);
        
        this.tradeCount++;
        
        // Check for signal
        const stats = this.tradeAggregator.getStats(symbol);
        
        if (stats && stats.totalVolume >= CONFIG.MIN_VOLUME_USD * 0.5) {
          // Check if alert conditions met
          if (this.signalEngine.shouldAlert(stats)) {
            if (this.cooldownManager.canAlert(symbol, stats)) {
              const interpretation = this.signalEngine.interpretSignal(stats);
              const volume24h = this.symbolManager.getVolume24h(symbol);
              
              this.alertManager.sendAlert(symbol, stats, interpretation, volume24h);
              this.cooldownManager.recordAlert(symbol, stats);
              this.tradeAggregator.resetSymbol(symbol);
            }
          }
        }
      }
      
      // Periodic stats logging
      this.logStats();
      
    } catch (error) {
      console.error('[WS] Parse error:', error.message);
    }
  }

  logStats() {
    const now = Date.now();
    if (now - this.lastStatsLog < CONFIG.STATS_LOG_INTERVAL * 1000) {
      return;
    }

    const activeSymbols = this.tradeAggregator.getActiveSymbolCount();
    const totalTrades = this.tradeAggregator.getTotalTradeCount();
    const alerts = this.alertManager.getAlertCount();
    
    console.log(`[STATS] Active: ${activeSymbols} symbols | Trades in memory: ${totalTrades} | Alerts sent: ${alerts} | Rate: ${(this.tradeCount / CONFIG.STATS_LOG_INTERVAL).toFixed(0)} trades/s`);
    
    this.tradeCount = 0;
    this.lastStatsLog = now;
    
    // Cleanup
    this.tradeAggregator.cleanup();
    this.cooldownManager.cleanup();
  }

  startPing() {
    this.pingInterval = setInterval(() => {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.ws.ping();
      }
    }, 30000);
  }

  stopPing() {
    if (this.pingInterval) {
      clearInterval(this.pingInterval);
      this.pingInterval = null;
    }
  }

  reconnect() {
    if (this.reconnectAttempts >= this.maxReconnects) {
      console.error('[WS] Max reconnect attempts reached');
      process.exit(1);
    }

    this.reconnectAttempts++;
    console.log(`[WS] Reconnecting in ${this.reconnectDelay / 1000}s... (attempt ${this.reconnectAttempts})`);
    
    setTimeout(() => {
      this.connect();
    }, this.reconnectDelay);
  }

  close() {
    this.stopPing();
    if (this.ws) {
      this.ws.close();
    }
  }
}

// ============================================================================
// MAIN APPLICATION
// ============================================================================

class BinanceFuturesFlowBot {
  constructor() {
    this.telegram = new TelegramBot(CONFIG.TELEGRAM_TOKEN, { polling: false });
    this.symbolManager = new SymbolManager();
    this.tradeAggregator = new TradeAggregator(CONFIG.WINDOW_SECONDS);
    this.signalEngine = new SignalEngine();
    this.cooldownManager = new CooldownManager(CONFIG.COOLDOWN_MINUTES);
    this.alertManager = new AlertManager(this.telegram);
    this.wsManager = null;
    this.refreshInterval = null;
  }

  async start() {
    console.log('='.repeat(70));
    console.log('BINANCE FUTURES AGGRESSIVE FLOW MONITOR');
    console.log('Real-time detection of forced liquidations via trade flow');
    console.log('='.repeat(70));
    console.log(`Window: ${CONFIG.WINDOW_SECONDS}s | Min Volume: $${(CONFIG.MIN_VOLUME_USD / 1e6).toFixed(1)}M`);
    console.log(`Min Dominance: ${CONFIG.MIN_DOMINANCE}% | Min Price Δ: ${CONFIG.MIN_PRICE_CHANGE}%`);
    console.log(`Cooldown: ${CONFIG.COOLDOWN_MINUTES} min | Symbol refresh: ${CONFIG.SYMBOL_REFRESH_HOURS}h`);
    console.log('='.repeat(70));

    // Test Telegram
    try {
      await this.telegram.sendMessage(
        CONFIG.TELEGRAM_CHAT_ID,
        '🚀 <b>Binance Futures Flow Monitor Started</b>\n\n✅ Monitoring aggressive trade flow',
        { parse_mode: 'HTML' }
      );
      console.log('[TELEGRAM] Connection OK\n');
    } catch (error) {
      console.error('[TELEGRAM] Error:', error.message);
      process.exit(1);
    }

    // Initialize symbols
    await this.symbolManager.initialize();

    // Connect WebSocket
    this.wsManager = new WebSocketManager(
      this.symbolManager,
      this.tradeAggregator,
      this.signalEngine,
      this.cooldownManager,
      this.alertManager
    );
    
    this.wsManager.connect();

    // Periodic symbol refresh
    this.startSymbolRefresh();

    // Graceful shutdown
    process.on('SIGINT', () => this.shutdown());
    process.on('SIGTERM', () => this.shutdown());
  }

  startSymbolRefresh() {
    this.refreshInterval = setInterval(async () => {
      console.log('\n[REFRESH] Updating symbol list...');
      await this.symbolManager.initialize();
    }, CONFIG.SYMBOL_REFRESH_HOURS * 60 * 60 * 1000);
  }

  async shutdown() {
    console.log('\n[SHUTDOWN] Stopping bot...');
    
    if (this.refreshInterval) {
      clearInterval(this.refreshInterval);
    }
    
    if (this.wsManager) {
      this.wsManager.close();
    }
    
    await this.telegram.sendMessage(
      CONFIG.TELEGRAM_CHAT_ID,
      '⛔ Binance Futures Flow Monitor Stopped',
      { parse_mode: 'HTML' }
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