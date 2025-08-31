import os
import sys
import time
import math
import requests
import pandas as pd
import yfinance as yf
import ta
from datetime import datetime
from zoneinfo import ZoneInfo
from io import StringIO

# -----------------------------
# 설정 (환경변수로 덮어쓰기 가능)
# -----------------------------
MIN_MARKET_CAP = float(os.getenv("MIN_MARKET_CAP", "20000000000"))  # 20B
MIN_AVG_VOLUME = float(os.getenv("MIN_AVG_VOLUME", "1000000"))      # 1M shares
PERIOD = os.getenv("HISTORY_PERIOD", "6mo")
INTERVAL = os.getenv("HISTORY_INTERVAL", "1d")
KST = ZoneInfo("Asia/Seoul")

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# -----------------------------
# 유틸
# -----------------------------
def yahoo_symbol_fix(ticker: str) -> str:
    # BRK.B -> BRK-B, BF.B -> BF-B 등
    return ticker.replace(".", "-").strip().upper()

def chunk_text(s: str, max_len: int = 3500):
    return [s[i:i+max_len] for i in range(0, len(s), max_len)]

def send_telegram_message(token: str, chat_id: str, text: str):
    if not token or not chat_id:
        print("⚠️ TELEGRAM_BOT_TOKEN/CHAT_ID가 비어있어 메시지 전송 생략")
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    for part in chunk_text(text):
        resp = requests.post(url, data={"chat_id": chat_id, "text": part})
        try:
            resp.raise_for_status()
        except Exception as e:
            print("텔레그램 전송 실패:", e, resp.text)

# -----------------------------
# 티커 수집 (위키백과)
# -----------------------------
def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"}
    html = requests.get(url, headers=headers).text
    tables = pd.read_html(StringIO(html))
    sp500_table = tables[0]
    return sp500_table['Symbol'].tolist()

def get_nasdaq100_tickers():
    url = "https://en.wikipedia.org/wiki/NASDAQ-100"
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"}
    html = requests.get(url, headers=headers).text
    tables = pd.read_html(StringIO(html))
    # NASDAQ100 표 찾기
    for table in tables:
        cols = [c.lower() for c in table.columns.astype(str)]
        if any("ticker" in c or "symbol" in c for c in cols):
            # 컬럼명 찾기
            for col in table.columns:
                if "Ticker" in col or "Symbol" in col:
                    return table[col].tolist()
    raise ValueError("NASDAQ-100 ticker table not found")
    
# -----------------------------
# 단일 티커 스크리닝
# -----------------------------
def screen_one(ticker: str):
    try:
        tk = yf.Ticker(ticker)

        # 시총, 거래량 (fast_info 우선, 없으면 info fallback)
        market_cap = None
        avg_volume = None

        try:
            fi = tk.fast_info
            market_cap = getattr(fi, "market_cap", None)
        except Exception:
            pass

        if market_cap is None:
            try:
                info = tk.info
                market_cap = info.get("marketCap")
                avg_volume = info.get("averageVolume") or info.get("averageDailyVolume10Day")
            except Exception:
                pass

        if avg_volume is None:
            # fast_info에선 평균 거래량이 잘 안 오므로 info로 재시도
            try:
                info = tk.info
                avg_volume = info.get("averageVolume") or info.get("averageDailyVolume10Day")
            except Exception:
                pass

        if not market_cap or market_cap < MIN_MARKET_CAP:
            return None
        if not avg_volume or avg_volume < MIN_AVG_VOLUME:
            return None

        # 시세 이력
        df = yf.download(ticker, period=PERIOD, interval=INTERVAL, auto_adjust=True, progress=False, threads=False)
        if df is None or df.empty:
            return None

        # 지표
        bb = ta.volatility.BollingerBands(close=df["Close"], window=20, window_dev=2)
        df["bb_low"] = bb.bollinger_lband()
        df["ma60"]   = df["Close"].rolling(60).mean()
        df["rsi"]    = ta.momentum.RSIIndicator(close=df["Close"], window=14).rsi()
        macd = ta.trend.MACD(close=df["Close"])
        df["macd"]   = macd.macd()
        df["signal"] = macd.macd_signal()

        last = df.iloc[-1]
        cond1 = last["Close"] <= last["bb_low"]          # 볼린저 하단
        cond2 = last["Close"] > last["ma60"]             # 60일선 위
        #cond3 = last["rsi"] < 35                         # 과매도
        #cond4 = (last["macd"] > last["signal"])          # MACD 골든

        if cond1 and cond2 and (cond3 or cond4):
            return {
                "Ticker": ticker,
                "Close": round(float(last["Close"]), 2),
                "RSI": round(float(last["rsi"]), 2) if not math.isnan(last["rsi"]) else None,
                "MACD": round(float(last["macd"]), 4) if not math.isnan(last["macd"]) else None,
                "Signal": round(float(last["signal"]), 4) if not math.isnan(last["signal"]) else None,
                "MarketCap": int(market_cap),
                "AvgVolume": int(avg_volume)
            }
        return None
    except Exception as e:
        print(f"[WARN] {ticker} 처리 중 오류: {e}")
        return None

# -----------------------------
# 메인
# -----------------------------
def main():
    kst_now = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    print(f"=== Stock Screener run @ KST {kst_now} ===")

    sp500 = get_sp500_tickers()
    ndx   = get_nasdaq100_tickers()
    tickers = sorted(set(sp500 + ndx))
    print(f"스캔 대상: {len(tickers)} 종목")

    hits = []
    for i, t in enumerate(tickers, 1):
        res = screen_one(t)
        if res:
            hits.append(res)
        # 너무 빠른 호출로 블록되는 걸 방지(안정용)
        time.sleep(0.05)
        if i % 100 == 0:
            print(f"진행률: {i}/{len(tickers)}")

    if not hits:
        msg = f"📉 [{kst_now} KST] 조건에 맞는 종목이 없습니다."
        print(msg)
        send_telegram_message(BOT_TOKEN, CHAT_ID, msg)
        return

    df = pd.DataFrame(hits).sort_values(["RSI", "Ticker"], ascending=[True, True])
    # 메시지 구성
    lines = [f"📈 [{kst_now} KST] 조건 충족 종목 ({len(df)}개)\n",
             "조건: BB 하단 터치 & 60MA 위 & (RSI<35 또는 MACD>Signal)\n",
             f"필터: 시총≥{int(MIN_MARKET_CAP):,}, 평균거래량≥{int(MIN_AVG_VOLUME):,}\n",
            ]
    for _, r in df.iterrows():
        lines.append(f"{r['Ticker']:>6}  Close:{r['Close']:<8} RSI:{r['RSI']:<5} MACD:{r['MACD']:<7} Sig:{r['Signal']:<7}")

    message = "\n".join(lines)
    print(message)
    send_telegram_message(BOT_TOKEN, CHAT_ID, message)

if __name__ == "__main__":
    sys.exit(main())
