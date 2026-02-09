// ============================================================================
// BINANCE FUTURES AGGRESSIVE FLOW MONITOR (Enhanced Version with OI)
// Individual symbol filters + Trading bot integration + Smart OI timing
// ============================================================================

if (process.env.NODE_ENV !== 'production') {
  require('dotenv').config();
}

const WebSocket = require('ws');
const TelegramBot = require('node-telegram-bot-api');
const axios = require('axios');

// ============================================================================
// CONFIGURATION WITH INDIVIDUAL SYMBOL SETTINGS + OI
// ============================================================================

const CONFIG = {
  // Individual symbol configurations
  SYMBOL_CONFIGS: {
    'ADAUSDT': {
      minVolumeUSD: 1_000_000,
      minDominance: 65.0,
      minPriceChange: 0.5,
      cooldownMinutes: 5,
      enabled: true
    },
    'TAOUSDT': {
      minVolumeUSD: 1_500_000,
      minDominance: 70.0,
      minPriceChange: 0.6,
      cooldownMinutes: 5,
      enabled: true
    },
    'HYPEUSDT': {
      minVolumeUSD: 2_000_000,
      minDominance: 70.0,
      minPriceChange: 1,
      cooldownMinutes: 5,
      enabled: true
    },
    'PEPEUSDT': {
      minVolumeUSD: 1_000_000,
      minDominance: 65.0,
      minPriceChange: 0.6,
      cooldownMinutes: 5,
      enabled: true
    },
    'WIFUSDT': {
      minVolumeUSD: 1_500_000,
      minDominance: 65.0,
      minPriceChange: 0.5,
      cooldownMinutes: 5,
      enabled: true
    },
    'BONKUSDT': {
      minVolumeUSD: 1_000_000,
      minDominance: 65.0,
      minPriceChange: 0.5,
      cooldownMinutes: 5,
      enabled: true
    },
    'DOGEUSDT': {
      minVolumeUSD: 5_000_000,
      minDominance: 70.0,
      minPriceChange: 0.75,
      cooldownMinutes: 5,
      enabled: true
    },
    'XRPUSDT': {
      minVolumeUSD: 5_000_000,
      minDominance: 70.0,
      minPriceChange: 1,
      cooldownMinutes: 5,
      enabled: true
    },
    'UNIUSDT': {
      minVolumeUSD: 1_000_000,
      minDominance: 65.0,
      minPriceChange: 0.5,
      cooldownMinutes: 5,
      enabled: true
    }
  },
  
  // Time window for aggregation
  WINDOW_SECONDS: parseInt(process.env.WINDOW_SECONDS) || 180,
  
  // Open Interest settings
  OI_ENABLED: process.env.OI_ENABLED === 'true' || true,
  OI_MODE: process.env.OI_MODE || 'FAST_MINUTE', // 'FAST_MINUTE' or 'CLASSIC_5MIN'
  OI_SAMPLE_INTERVAL: parseInt(process.env.OI_SAMPLE_INTERVAL) || 3, // секунди між замірами в FAST_MINUTE
  OI_MIN_CHANGE_THRESHOLD: parseFloat(process.env.OI_MIN_CHANGE_THRESHOLD) || 0.2, // мінімальна зміна OI для рішення
  OI_WINDOW_SECONDS: parseInt(process.env.OI_WINDOW_SECONDS) || parseInt(process.env.WINDOW_SECONDS) || 180, // для CLASSIC_5MIN
  OI_HISTORY_MINUTES: 10,
  
  // System
  STATS_LOG_INTERVAL: parseInt(process.env.STATS_LOG_INTERVAL) || 60,
  MAX_RECONNECTS: parseInt(process.env.MAX_RECONNECTS) || 10,
  
  // Binance API
  BINANCE_WS: 'wss://fstream.binance.com/ws',
  BINANCE_FUTURES_API: 'https://fapi.binance.com',
  
  // Telegram
  TELEGRAM_TOKEN: process.env.TELEGRAM_BOT_TOKEN,
  TELEGRAM_CHAT_ID: process.env.TELEGRAM_CHAT_ID,
  
  // Trading bot integration settings
  TRADING_BOT_ENABLED: process.env.TRADING_BOT_ENABLED === 'true' || false,
  ALERT_FORMAT: 'structured' // 'structured' for bot parsing or 'human' for readable
};

// Helper to get enabled symbols
CONFIG.getEnabledSymbols = () => {
  return Object.keys(CONFIG.SYMBOL_CONFIGS).filter(
    symbol => CONFIG.SYMBOL_CONFIGS[symbol].enabled
  );
};

// Helper to get config for symbol
CONFIG.getSymbolConfig = (symbol) => {
  return CONFIG.SYMBOL_CONFIGS[symbol] || null;
};

// ============================================================================
// OI EVALUATOR (FAST_MINUTE MODE)
// ============================================================================

class OIEvaluator {
  constructor() {
    this.activeSessions = new Map(); // symbol -> evaluation session
  }

  async startEvaluation(symbol, alertCreatedAt, initialPrice) {
    const now = Date.now();
    const currentMinute = Math.floor(now / 60000);
    const endOfMinute = (currentMinute + 1) * 60000 - 500; // -500ms запас
    const timeUntilEnd = endOfMinute - now;

    if (timeUntilEnd < 1000) {
      console.log(`[OI-EVAL] ${symbol} Замало часу до кінця хвилини (${timeUntilEnd}ms), пропускаємо OI`);
      return null;
    }

    console.log(`[OI-EVAL] ${symbol} Старт evaluation до ${new Date(endOfMinute).toISOString()}`);
    console.log(`[OI-EVAL] ${symbol} Доступний час: ${(timeUntilEnd / 1000).toFixed(1)}s`);

    const session = {
      symbol,
      alertCreatedAt,
      initialPrice,
      endOfMinute,
      samples: [],
      intervalHandle: null
    };

    this.activeSessions.set(symbol, session);

    // Перший замір одразу
    await this.collectSample(session);

    // Періодичні заміри
    session.intervalHandle = setInterval(async () => {
      if (Date.now() >= session.endOfMinute) {
        clearInterval(session.intervalHandle);
        return;
      }
      await this.collectSample(session);
    }, CONFIG.OI_SAMPLE_INTERVAL * 1000);

    // Фіналізуємо перед кінцем хвилини
    const finalizeTime = session.endOfMinute - now - 100; // за 100ms до deadline
    setTimeout(() => {
      this.finalizeEvaluation(symbol);
    }, finalizeTime);

    return session;
  }

  async collectSample(session) {
    try {
      const url = `${CONFIG.BINANCE_FUTURES_API}/fapi/v1/openInterest`;
      const response = await axios.get(url, {
        params: { symbol: session.symbol },
        timeout: 2000
      });

      if (response.data && response.data.openInterest) {
        const oi = parseFloat(response.data.openInterest);
        const timestamp = Date.now();
        
        session.samples.push({ timestamp, oi });
        
        console.log(`[OI-EVAL] ${session.symbol} Sample #${session.samples.length}: OI=${oi.toFixed(0)} @ ${new Date(timestamp).toISOString().substr(17, 5)}`);
      }
    } catch (error) {
      console.error(`[OI-EVAL] ${session.symbol} Помилка отримання OI:`, error.message);
    }
  }

  finalizeEvaluation(symbol) {
    const session = this.activeSessions.get(symbol);
    if (!session) return null;

    if (session.intervalHandle) {
      clearInterval(session.intervalHandle);
    }

    console.log(`[OI-EVAL] ${symbol} Фіналізація з ${session.samples.length} samples`);

    if (session.samples.length < 2) {
      console.log(`[OI-EVAL] ${symbol} Недостатньо samples, повертаємо null`);
      this.activeSessions.delete(symbol);
      return null;
    }

    const oiFirst = session.samples[0].oi;
    const oiLast = session.samples[session.samples.length - 1].oi;
    const oiDelta = oiLast - oiFirst;
    const oiDeltaPercent = (oiDelta / oiFirst) * 100;

    const result = {
      symbol,
      oiFirst,
      oiLast,
      oiDelta,
      oiDeltaPercent,
      sampleCount: session.samples.length,
      evaluationTime: session.samples[session.samples.length - 1].timestamp - session.samples[0].timestamp
    };

    console.log(`[OI-EVAL] ${symbol} РЕЗУЛЬТАТ: OI ${oiFirst.toFixed(0)} → ${oiLast.toFixed(0)} (${oiDeltaPercent > 0 ? '+' : ''}${oiDeltaPercent.toFixed(3)}%)`);

    this.activeSessions.delete(symbol);
    return result;
  }

  getResult(symbol) {
    return this.finalizeEvaluation(symbol);
  }

  cancelEvaluation(symbol) {
    const session = this.activeSessions.get(symbol);
    if (session && session.intervalHandle) {
      clearInterval(session.intervalHandle);
    }
    this.activeSessions.delete(symbol);
    console.log(`[OI-EVAL] ${symbol} Evaluation скасовано`);
  }
}

// ============================================================================
// CLASSIC OI TRACKER (for CLASSIC_5MIN mode)
// ============================================================================

class ClassicOITracker {
  constructor(symbols, windowSeconds, historyMinutes) {
    this.symbols = symbols;
    this.windowMs = windowSeconds * 1000;
    this.historyMs = historyMinutes * 60 * 1000;
    this.oiHistory = new Map();
    
    symbols.forEach(symbol => {
      this.oiHistory.set(symbol, []);
    });
    
    this.fetchInterval = null;
    this.isRunning = false;
    this.retryDelays = [1000, 2000, 5000, 10000];
    this.maxRetries = 4;
  }

  start(intervalSeconds) {
    if (this.isRunning) return;
    this.isRunning = true;
    console.log(`[OI-CLASSIC] Запуск для ${this.symbols.length} символів (кожні ${intervalSeconds}s)`);
    
    this.fetchAllSymbols();
    this.fetchInterval = setInterval(() => {
      this.fetchAllSymbols();
    }, intervalSeconds * 1000);
  }

  stop() {
    if (this.fetchInterval) {
      clearInterval(this.fetchInterval);
      this.fetchInterval = null;
    }
    this.isRunning = false;
  }

  async fetchAllSymbols() {
    const promises = this.symbols.map(symbol => this.fetchSymbolOI(symbol));
    await Promise.allSettled(promises);
  }

  async fetchSymbolOI(symbol, retryCount = 0) {
    try {
      const url = `${CONFIG.BINANCE_FUTURES_API}/fapi/v1/openInterest`;
      const response = await axios.get(url, {
        params: { symbol },
        timeout: 5000
      });

      if (response.data && response.data.openInterest) {
        const oi = parseFloat(response.data.openInterest);
        const timestamp = Date.now();
        this.addOIData(symbol, timestamp, oi);
      }
    } catch (error) {
      if (retryCount < this.maxRetries) {
        const delay = this.retryDelays[retryCount] || this.retryDelays[this.retryDelays.length - 1];
        await new Promise(resolve => setTimeout(resolve, delay));
        return this.fetchSymbolOI(symbol, retryCount + 1);
      }
    }
  }

  addOIData(symbol, timestamp, value) {
    const history = this.oiHistory.get(symbol);
    if (!history) return;

    history.push({ timestamp, value });
    const cutoff = timestamp - this.historyMs;
    const filtered = history.filter(item => item.timestamp >= cutoff);
    this.oiHistory.set(symbol, filtered);
  }

  getOIStats(symbol) {
    const history = this.oiHistory.get(symbol);
    if (!history || history.length === 0) return null;

    const now = Date.now();
    const windowAgoTime = now - this.windowMs;
    const latest = history[history.length - 1];
    const oiNow = latest.value;

    let oiWindowAgo = null;
    for (let i = history.length - 1; i >= 0; i--) {
      if (history[i].timestamp <= windowAgoTime) {
        oiWindowAgo = history[i].value;
        break;
      }
    }

    if (oiWindowAgo === null) {
      return { oiNow, oiWindowAgo: null, oiChangePercent: null, hasWindowData: false };
    }

    const oiChangeAbsolute = oiNow - oiWindowAgo;
    const oiChangePercent = (oiChangeAbsolute / oiWindowAgo) * 100;

    return {
      oiNow,
      oiWindowAgo,
      oiChangePercent,
      oiChangeAbsolute,
      hasWindowData: true
    };
  }
}

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
  shouldAlert(symbol, stats) {
    if (!stats) return false;
    
    const config = CONFIG.getSymbolConfig(symbol);
    if (!config || !config.enabled) return false;
    
    if (stats.totalVolume < config.minVolumeUSD) return false;
    if (stats.dominance < config.minDominance) return false;
    if (Math.abs(stats.priceChange) < config.minPriceChange) return false;
    
    if (stats.dominantSide === 'buy' && stats.priceChange < 0) return false;
    if (stats.dominantSide === 'sell' && stats.priceChange > 0) return false;

    return true;
  }

  interpretSignal(stats) {
    if (stats.dominantSide === 'buy') {
      return {
        type: 'SHORT_SQUEEZE',
        label: 'SHORT SQUEEZE',
        emoji: '🟢',
        baseDirection: 'LONG'
      };
    } else {
      return {
        type: 'LONG_LIQUIDATION',
        label: 'LONG LIQUIDATION',
        emoji: '🔴',
        baseDirection: 'SHORT'
      };
    }
  }

  // Визначення bounce vs continuation на основі OI
  determineDirection(stats, oiResult) {
    const base = this.interpretSignal(stats);
    
    // Якщо немає OI даних
    if (!oiResult || !oiResult.oiDeltaPercent) {
      return {
        ...base,
        finalDirection: 'BOUNCE',
        finalSide: base.baseDirection,
        strategy: 'BOUNCE',
        oiUsed: false,
        reason: 'OI дані недоступні'
      };
    }

    const { oiDeltaPercent } = oiResult;
    const priceChange = stats.priceChange;

    // Якщо OI змінився менше ніж поріг - залишаємо BOUNCE
    if (Math.abs(oiDeltaPercent) < CONFIG.OI_MIN_CHANGE_THRESHOLD) {
      return {
        ...base,
        finalDirection: 'BOUNCE',
        finalSide: base.baseDirection,
        strategy: 'BOUNCE',
        oiUsed: true,
        reason: `OI зміна мала (${oiDeltaPercent.toFixed(2)}% < ${CONFIG.OI_MIN_CHANGE_THRESHOLD}%)`
      };
    }

    let finalDirection, finalSide, strategy, reason;

    // Ціна впала
    if (priceChange < 0) {
      if (oiDeltaPercent > 0) {
        // OI росте = continuation SHORT
        finalDirection = 'CONTINUATION';
        finalSide = 'SHORT';
        strategy = 'CONTINUATION';
        reason = `Ціна↓ + OI↑ (${oiDeltaPercent.toFixed(2)}%) = продовження SHORT`;
      } else {
        // OI падає = bounce LONG
        finalDirection = 'BOUNCE';
        finalSide = 'LONG';
        strategy = 'BOUNCE';
        reason = `Ціна↓ + OI↓ (${oiDeltaPercent.toFixed(2)}%) = відскок LONG`;
      }
    } 
    // Ціна виросла
    else {
      if (oiDeltaPercent > 0) {
        // OI росте = continuation LONG
        finalDirection = 'CONTINUATION';
        finalSide = 'LONG';
        strategy = 'CONTINUATION';
        reason = `Ціна↑ + OI↑ (${oiDeltaPercent.toFixed(2)}%) = продовження LONG`;
      } else {
        // OI падає = bounce SHORT
        finalDirection = 'BOUNCE';
        finalSide = 'SHORT';
        strategy = 'BOUNCE';
        reason = `Ціна↑ + OI↓ (${oiDeltaPercent.toFixed(2)}%) = відскок SHORT`;
      }
    }

    return {
      ...base,
      finalDirection,
      finalSide,
      strategy,
      oiUsed: true,
      reason
    };
  }
}

// ============================================================================
// COOLDOWN MANAGER
// ============================================================================

class CooldownManager {
  constructor() {
    this.lastAlerts = new Map();
  }

  canAlert(symbol, stats) {
    const config = CONFIG.getSymbolConfig(symbol);
    if (!config) return false;

    const key = `${symbol}_${stats.dominantSide}`;
    const lastTime = this.lastAlerts.get(key);
    
    if (!lastTime) return true;

    const cooldownMs = config.cooldownMinutes * 60 * 1000;
    const elapsed = Date.now() - lastTime;
    
    return elapsed >= cooldownMs;
  }

  recordAlert(symbol, stats) {
    const key = `${symbol}_${stats.dominantSide}`;
    this.lastAlerts.set(key, Date.now());
  }
}

// ============================================================================
// ALERT MANAGER (оновлений)
// ============================================================================

class AlertManager {
  constructor(telegram, oiEvaluator, classicTracker) {
    this.telegram = telegram;
    this.oiEvaluator = oiEvaluator;
    this.classicTracker = classicTracker;
    this.pendingAlerts = new Map();
    this.alertCount = 0;
    this.minuteCheckInterval = null;
    this.startMinuteChecker();
  }

  startMinuteChecker() {
    this.minuteCheckInterval = setInterval(() => {
      const now = new Date();
      const seconds = now.getSeconds();
      
      if (seconds === 0 && this.pendingAlerts.size > 0) {
        this.flushPendingAlerts();
      }
    }, 1000);
  }

  async createAlert(symbol, stats, interpretation) {
    const key = `${symbol}_${stats.dominantSide}`;
    const alertCreatedAt = Date.now();

    console.log(`[ALERT] ${symbol} Створення pending alert @ ${new Date(alertCreatedAt).toISOString()}`);

    const alertData = {
      symbol,
      stats,
      interpretation,
      alertCreatedAt,
      oiResult: null,
      finalInterpretation: null
    };

    // Замінюємо якщо вже є pending alert для цього символа
    if (this.pendingAlerts.has(key)) {
      console.log(`[ALERT] ${symbol} Заміна існуючого pending alert`);
      this.oiEvaluator.cancelEvaluation(symbol);
    }

    this.pendingAlerts.set(key, alertData);

    // Запускаємо OI evaluation
    if (CONFIG.OI_ENABLED && CONFIG.OI_MODE === 'FAST_MINUTE') {
      console.log(`[ALERT] ${symbol} Запуск FAST_MINUTE OI evaluation`);
      await this.oiEvaluator.startEvaluation(symbol, alertCreatedAt, stats.lastPrice);
    } else if (CONFIG.OI_ENABLED && CONFIG.OI_MODE === 'CLASSIC_5MIN') {
      console.log(`[ALERT] ${symbol} Використання CLASSIC_5MIN режиму`);
      // OI вже збирається в фоні
    } else {
      console.log(`[ALERT] ${symbol} OI вимкнено, використання базового interpretation`);
    }
  }

  async flushPendingAlerts() {
    console.log(`\n[ALERT] ⏰ FLUSH: Відправка ${this.pendingAlerts.size} alert(s)...`);
    
    for (const [key, alertData] of this.pendingAlerts.entries()) {
      try {
        await this.finalizeAndSend(alertData);
        this.alertCount++;
      } catch (error) {
        console.error(`[ALERT] Помилка відправки ${alertData.symbol}:`, error.message);
      }
    }
    
    this.pendingAlerts.clear();
    console.log(`[ALERT] ✅ Flush завершено\n`);
  }

  async finalizeAndSend(alertData) {
    const { symbol, stats, interpretation } = alertData;

    let oiResult = null;
    let finalInterpretation = interpretation;

    // Отримуємо OI результат
    if (CONFIG.OI_ENABLED) {
      if (CONFIG.OI_MODE === 'FAST_MINUTE') {
        oiResult = this.oiEvaluator.getResult(symbol);
        if (oiResult) {
          console.log(`[ALERT] ${symbol} OI evaluation завершено: ${oiResult.sampleCount} samples`);
        } else {
          console.log(`[ALERT] ${symbol} OI evaluation не дав результату`);
        }
      } else if (CONFIG.OI_MODE === 'CLASSIC_5MIN') {
        const classicOI = this.classicTracker ? this.classicTracker.getOIStats(symbol) : null;
        if (classicOI && classicOI.hasWindowData) {
          oiResult = {
            oiFirst: classicOI.oiWindowAgo,
            oiLast: classicOI.oiNow,
            oiDelta: classicOI.oiChangeAbsolute,
            oiDeltaPercent: classicOI.oiChangePercent,
            sampleCount: 'classic',
            evaluationTime: CONFIG.OI_WINDOW_SECONDS * 1000
          };
          console.log(`[ALERT] ${symbol} CLASSIC OI: ${oiResult.oiDeltaPercent.toFixed(2)}%`);
        }
      }

      // Визначаємо фінальний напрямок з урахуванням OI
      const signalEngine = new SignalEngine();
      finalInterpretation = signalEngine.determineDirection(stats, oiResult);
    }

    console.log(`[ALERT] ${symbol} FINAL: ${finalInterpretation.strategy} ${finalInterpretation.finalSide} - ${finalInterpretation.reason}`);

    // Відправляємо повідомлення (БЕЗ API calls тут)
    await this.sendTelegramMessage({
      symbol,
      stats,
      interpretation: finalInterpretation,
      oiResult
    });
  }

  async sendTelegramMessage(data) {
    const { symbol, stats, interpretation, oiResult } = data;
    
    let message;
    if (CONFIG.ALERT_FORMAT === 'structured') {
      message = this.formatStructuredMessage(symbol, stats, interpretation, oiResult);
    } else {
      message = this.formatHumanMessage(symbol, stats, interpretation, oiResult);
    }

    await this.telegram.sendMessage(
      CONFIG.TELEGRAM_CHAT_ID,
      message,
      { parse_mode: 'HTML' }
    );
  }

  // Функція для екранування HTML символів
  escapeHtml(text) {
    if (typeof text !== 'string') {
      text = String(text);
    }
    return text
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  formatStructuredMessage(symbol, stats, interpretation, oiResult) {
    const lines = [];
    
    lines.push(`${interpretation.emoji} <b>${this.escapeHtml(interpretation.label)}</b>`);
    lines.push(`<code>───────────────────</code>`);
    
    const cleanSymbol = symbol.replace('USDT', '');
    lines.push(`🎯 <b>${this.escapeHtml(symbol)}</b> #${this.escapeHtml(cleanSymbol)}`);
    
    const priceSign = stats.priceChange >= 0 ? '+' : '';
    lines.push(`📈 Ціна: ${priceSign}${stats.priceChange.toFixed(2)}% | $${stats.lastPrice.toFixed(4)}`);
    lines.push(`💰 Об'єм: $${this.fmt(stats.totalVolume)} за ${stats.duration.toFixed(0)}с`);
    lines.push(`📊 Домінація: ${stats.dominance.toFixed(1)}% ${stats.dominantSide === 'buy' ? '🟢 BUY' : '🔴 SELL'}`);
    
    // OI Info
    if (oiResult) {
      lines.push(`<code>───────────────────</code>`);
      lines.push(`📊 <b>OPEN INTEREST</b>`);
      lines.push(`Режим: ${this.escapeHtml(CONFIG.OI_MODE)}`);
      
      if (oiResult.sampleCount !== 'classic') {
        lines.push(`Samples: ${oiResult.sampleCount} за ${(oiResult.evaluationTime / 1000).toFixed(1)}s`);
      }
      
      lines.push(`OI: ${this.fmtOI(oiResult.oiFirst)} → ${this.fmtOI(oiResult.oiLast)}`);
      
      const oiSign = oiResult.oiDeltaPercent >= 0 ? '+' : '';
      const oiEmoji = oiResult.oiDeltaPercent > 0 ? '📈' : oiResult.oiDeltaPercent < 0 ? '📉' : '➡️';
      lines.push(`Δ OI: ${oiEmoji} ${oiSign}${oiResult.oiDeltaPercent.toFixed(3)}%`);
    }
    
    // Strategy
    lines.push(`<code>───────────────────</code>`);
    lines.push(`🎯 <b>СТРАТЕГІЯ: ${this.escapeHtml(interpretation.strategy)}</b>`);
    lines.push(`📍 Напрямок: <b>${this.escapeHtml(interpretation.finalSide)}</b>`);
    
    if (interpretation.reason) {
      lines.push(`💡 ${this.escapeHtml(interpretation.reason)}`);
    }
    
    lines.push(`<code>───────────────────</code>`);
    
    // Machine-readable JSON
    const jsonData = {
      symbol,
      strategy: interpretation.strategy,
      direction: interpretation.finalSide,
      baseDirection: interpretation.baseDirection,
      type: interpretation.type,
      price: stats.lastPrice,
      priceChange: parseFloat(stats.priceChange.toFixed(4)),
      volume: parseFloat(stats.totalVolume.toFixed(2)),
      dominance: parseFloat(stats.dominance.toFixed(2)),
      dominantSide: stats.dominantSide,
      duration: parseFloat(stats.duration.toFixed(1)),
      timestamp: Date.now(),
      oiMode: CONFIG.OI_MODE,
      oiEnabled: CONFIG.OI_ENABLED,
      oiUsed: interpretation.oiUsed || false,
      oiDeltaPercent: oiResult ? parseFloat(oiResult.oiDeltaPercent.toFixed(4)) : null,
      oiFirst: oiResult ? oiResult.oiFirst : null,
      oiLast: oiResult ? oiResult.oiLast : null
    };
    
    // Екрануємо JSON для безпечного використання в HTML
    const jsonString = JSON.stringify(jsonData);
    lines.push(`<code>${this.escapeHtml(jsonString)}</code>`);
    
    return lines.join('\n');
  }

  formatHumanMessage(symbol, stats, interpretation, oiResult) {
    const lines = [];
    
    lines.push(`${interpretation.emoji} ${this.escapeHtml(interpretation.label)}`);
    lines.push(`🎯 Стратегія: ${this.escapeHtml(interpretation.strategy)} ${this.escapeHtml(interpretation.finalSide)}`);
    lines.push(`💰 Об'єм: $${this.fmt(stats.totalVolume)} за ${stats.duration.toFixed(0)}с`);
    lines.push('━━━━━━━━━━━━━━━━━');
    
    const cleanSymbol = symbol.replace('USDT', '');
    lines.push(`🎯 ${this.escapeHtml(symbol)} #${this.escapeHtml(cleanSymbol)}`);
    
    const priceSign = stats.priceChange >= 0 ? '+' : '';
    lines.push(`📈 Δ Ціни: ${priceSign}${stats.priceChange.toFixed(2)}%`);
    lines.push(`💵 Ціна: $${stats.lastPrice.toFixed(4)}`);
    
    if (oiResult) {
      lines.push('━━━━━━━━━━━━━━━━━');
      const oiSign = oiResult.oiDeltaPercent >= 0 ? '+' : '';
      lines.push(`📊 OI (${this.escapeHtml(CONFIG.OI_MODE)}): ${oiSign}${oiResult.oiDeltaPercent.toFixed(2)}%`);
      
      if (interpretation.reason) {
        lines.push(`💡 ${this.escapeHtml(interpretation.reason)}`);
      }
    }
    
    lines.push('━━━━━━━━━━━━━━━━━');
    lines.push(`🟢 Агресивний Buy: $${this.fmt(stats.buyVolume)}`);
    lines.push(`🔴 Агресивний Sell: $${this.fmt(stats.sellVolume)}`);
    
    return lines.join('\n');
  }

  fmt(num) {
    if (num >= 1_000_000) return (num / 1_000_000).toFixed(2) + 'M';
    if (num >= 1_000) return (num / 1_000).toFixed(0) + 'K';
    return num.toFixed(0);
  }

  fmtOI(num) {
    if (!num) return 'N/A';
    if (num >= 1_000_000) return (num / 1_000_000).toFixed(2) + 'M';
    if (num >= 1_000) return (num / 1_000).toFixed(1) + 'K';
    return num.toFixed(0);
  }

  getCount() {
    return this.alertCount;
  }

  getPendingCount() {
    return this.pendingAlerts.size;
  }

  stop() {
    if (this.minuteCheckInterval) {
      clearInterval(this.minuteCheckInterval);
    }
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
    console.log(`[WS] Підключення до ${this.symbols.length} символів...`);
    
    this.symbols.forEach((symbol, i) => {
      setTimeout(() => this.connectSymbol(symbol), i * 200);
    });
  }

  connectSymbol(symbol) {
    const streamName = `${symbol.toLowerCase()}@aggTrade`;
    const url = `${CONFIG.BINANCE_WS}/${streamName}`;
    
    const ws = new WebSocket(url);

    ws.on('open', () => {
      console.log(`[WS] ${symbol} підключено`);
      this.reconnectAttempts.set(symbol, 0);
    });

    ws.on('message', (data) => {
      this.handleMessage(symbol, data);
    });

    ws.on('error', (error) => {
      console.error(`[WS] ${symbol} помилка:`, error.message);
    });

    ws.on('close', () => {
      console.log(`[WS] ${symbol} закрито`);
      this.reconnectSymbol(symbol);
    });

    this.connections.set(symbol, ws);
  }

  handleMessage(symbol, data) {
    try {
      const trade = JSON.parse(data);
      
      const price = parseFloat(trade.p);
      const quantity = parseFloat(trade.q);
      const timestamp = trade.T;
      const isBuyerMaker = trade.m;
      
      this.tradeAggregator.addTrade(symbol, timestamp, price, quantity, isBuyerMaker);
      this.tradeCount++;
      
      const stats = this.tradeAggregator.getStats(symbol);
      const config = CONFIG.getSymbolConfig(symbol);
      
      if (stats && config && stats.totalVolume >= config.minVolumeUSD * 0.5) {
        if (this.signalEngine.shouldAlert(symbol, stats)) {
          if (this.cooldownManager.canAlert(symbol, stats)) {
            const interpretation = this.signalEngine.interpretSignal(stats);
            
            // Створюємо pending alert (БЕЗ блокування)
            this.alertManager.createAlert(symbol, stats, interpretation);
            
            this.cooldownManager.recordAlert(symbol, stats);
            this.tradeAggregator.resetSymbol(symbol);
          }
        }
      }
      
      this.logStats();
      
    } catch (error) {
      console.error(`[WS] ${symbol} помилка парсингу:`, error.message);
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
    const pendingAlerts = this.alertManager.getPendingCount();
    const connected = Array.from(this.connections.values()).filter(ws => ws.readyState === WebSocket.OPEN).length;
    
    console.log(`[STATS] Підключено: ${connected}/${this.symbols.length} | Активних: ${activeSymbols} | Трейдів: ${totalTrades} | Алертів: ${alerts} | Pending: ${pendingAlerts} | Rate: ${(this.tradeCount / CONFIG.STATS_LOG_INTERVAL).toFixed(0)}/s`);
    
    this.tradeCount = 0;
    this.lastStatsLog = now;
  }

  reconnectSymbol(symbol) {
    const attempts = this.reconnectAttempts.get(symbol) || 0;
    
    if (attempts >= CONFIG.MAX_RECONNECTS) {
      console.error(`[WS] ${symbol} досягнуто максимум переподключень`);
      return;
    }

    this.reconnectAttempts.set(symbol, attempts + 1);
    
    setTimeout(() => {
      console.log(`[WS] ${symbol} переподключення (${attempts + 1}/${CONFIG.MAX_RECONNECTS})...`);
      this.connectSymbol(symbol);
    }, 5000 * (attempts + 1));
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
    
    const symbols = CONFIG.getEnabledSymbols();
    
    // OI components
    this.oiEvaluator = new OIEvaluator();
    this.classicTracker = null;
    
    if (CONFIG.OI_ENABLED && CONFIG.OI_MODE === 'CLASSIC_5MIN') {
      this.classicTracker = new ClassicOITracker(
        symbols,
        CONFIG.OI_WINDOW_SECONDS,
        CONFIG.OI_HISTORY_MINUTES
      );
    }
    
    this.signalEngine = new SignalEngine();
    this.cooldownManager = new CooldownManager();
    this.alertManager = new AlertManager(this.telegram, this.oiEvaluator, this.classicTracker);
    this.wsManager = null;
  }

  async start() {
    const symbols = CONFIG.getEnabledSymbols();
    
    console.log('='.repeat(70));
    console.log('BINANCE FUTURES AGGRESSIVE FLOW MONITOR (Smart OI Timing)');
    console.log('='.repeat(70));
    console.log(`Символів: ${symbols.length} | Вікно: ${CONFIG.WINDOW_SECONDS}s`);
    
    if (CONFIG.OI_ENABLED) {
      console.log(`Open Interest: ✅ ${CONFIG.OI_MODE} mode`);
      if (CONFIG.OI_MODE === 'FAST_MINUTE') {
        console.log(`  - Samples кожні ${CONFIG.OI_SAMPLE_INTERVAL}s до кінця хвилини`);
        console.log(`  - Мін. зміна для рішення: ${CONFIG.OI_MIN_CHANGE_THRESHOLD}%`);
      } else {
        console.log(`  - Window: ${CONFIG.OI_WINDOW_SECONDS}s`);
      }
    } else {
      console.log(`Open Interest: ❌ Вимкнено`);
    }
    
    console.log('Налаштування символів:');
    symbols.forEach(symbol => {
      const config = CONFIG.getSymbolConfig(symbol);
      console.log(`  ${symbol}: Vol=$${(config.minVolumeUSD / 1e6).toFixed(1)}M | Dom=${config.minDominance}% | Δ=${config.minPriceChange}%`);
    });
    
    console.log('='.repeat(70));
    console.log(`Формат алертів: ${CONFIG.ALERT_FORMAT}`);
    console.log(`Інтеграція торгового бота: ${CONFIG.TRADING_BOT_ENABLED ? 'Увімкнено' : 'Вимкнено'}`);
    console.log('='.repeat(70));

    // Test Telegram
    try {
      const startMessage = symbols.map(s => {
        const c = CONFIG.getSymbolConfig(s);
        return `• ${s}: $${(c.minVolumeUSD / 1e6).toFixed(1)}M | ${c.minDominance}% | ${c.minPriceChange}%`;
      }).join('\n');
      
      await this.telegram.sendMessage(
        CONFIG.TELEGRAM_CHAT_ID,
        `🚀 <b>Binance Futures Monitor Запущено</b>\n\n` +
        `<b>📊 Моніторинг ${symbols.length} символів:</b>\n${startMessage}\n\n` +
        `⚙️ Формат: ${CONFIG.ALERT_FORMAT}\n` +
        `🤖 Торговий бот: ${CONFIG.TRADING_BOT_ENABLED ? 'ON' : 'OFF'}\n` +
        `📊 OI режим: ${CONFIG.OI_ENABLED ? CONFIG.OI_MODE : 'OFF'}`,
        { parse_mode: 'HTML' }
      );
      console.log('[TELEGRAM] ✅ Підключено\n');
    } catch (error) {
      console.error('[TELEGRAM] ❌ Помилка:', error.message);
      process.exit(1);
    }

    // Запуск Classic OI Tracker якщо потрібно
    if (this.classicTracker) {
      this.classicTracker.start(10); // кожні 10 секунд
    }

    // Connect WebSockets
    this.wsManager = new MultiWebSocketManager(
      symbols,
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
    console.log('\n[SHUTDOWN] Зупинка...');
    
    if (this.classicTracker) {
      this.classicTracker.stop();
    }
    
    if (this.wsManager) {
      this.wsManager.closeAll();
    }
    
    if (this.alertManager) {
      this.alertManager.stop();
    }
    
    await this.telegram.sendMessage(
      CONFIG.TELEGRAM_CHAT_ID,
      '⛔ Binance Futures Monitor Зупинено'
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
