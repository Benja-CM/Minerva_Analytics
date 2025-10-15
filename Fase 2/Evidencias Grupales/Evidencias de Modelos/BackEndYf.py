# main.py
import os
import firebase_admin
from firebase_admin import firestore
from firebase_functions import scheduler_fn
from firebase_functions.options import set_global_options
import functions_framework


# Initialize Firebase Admin SDK once globally.
firebase_admin.initialize_app()

# Lazily initialize Firestore client
_firestore_client = None
def get_firestore_client():
    global _firestore_client
    if _firestore_client is None:
        _firestore_client = firestore.client()
    return _firestore_client

TICKERS_CL = [
    "ENELCHILE.SN", "ENELAM.SN", "CHILE.SN", "BSANTANDER.SN", "COPEC.SN",
    "CENCOSUD.SN", "FALABELLA.SN", "PARAUCO.SN", "CMPC.SN", "AGUAS-A.SN",
    "AGUAS-C.SN", "CAP.SN", "CCU.SN", "VAPORES.SN", "BCI.SN", "ANDINA-B.SN",
    "ANDINA-A.SN", "IAM.SN", "SQM-A.SN", "SQM-B.SN", "ITAUCORP.SN",
    "ENTEL.SN", "SECURITY.SN", "COLBUN.SN", "ECL.SN", "AESGENER.SN",
    "FORUS.SN", "SALFACORP.SN", "VINA.SN", "HF.SN", "LTM.SN", "PAZ.SN",
    "ILC.SN", "CGE.SN", "SMU.SN", "VSPT.SN", "BESALCO.SN", "MELON.SN",
    "BLUMAR.SN", "NEXO.SN", "NAVIERA.SN", "MADECO.SN", "MULTIFOODS.SN"
]

@scheduler_fn.on_schedule(
    schedule="0 0 * * *", # Runs daily at midnight UTC
    region="us-central1", # Match your deployment region
    timeout_sec=540,      # Max 9 minutes (540 seconds) for event-driven functions
    memory=2048           # 2GB of memory, adjust as needed
)
def update_market_metrics_scheduled(event: scheduler_fn.ScheduledEvent):
    import yfinance as yf
    import numpy as np

    print(f"Scheduled function triggered at {event.schedule_time}")
    results = []
    db_client = get_firestore_client()

    for t in TICKERS_CL:
        try:
            df = yf.download(t, period="2y", interval="1d", progress=False)
            if df.empty:
                print(f"No data found for {t}. Skipping.")
                continue

            df["Return"] = df["Close"].pct_change()
            mean_daily = df["Return"].mean()
            vol_daily = df["Return"].std()

            annual_return = mean_daily * 252
            annual_vol = vol_daily * np.sqrt(252)
            sharpe_ratio = annual_return / annual_vol if annual_vol > 0 else np.nan

            info = yf.Ticker(t).info
            data = {
                "Ticker": t,
                "Company": info.get("longName"),
                "Sector": info.get("sector"),
                "MarketCap": info.get("marketCap"),
                "PE": info.get("trailingPE"),
                "PB": info.get("priceToBook"),
                "ROE": info.get("returnOnEquity"),
                "DebtToEquity": info.get("debtToEquity"),
                "DividendYield": info.get("dividendYield"),
                "AnnualReturn": annual_return,
                "AnnualVolatility": annual_vol,
                "SharpeRatio": sharpe_ratio,
                "Timestamp": firestore.SERVER_TIMESTAMP
            }

            db_client.collection("TK").document(t).set(data)
            results.append(data)

            print(f"Saved: {t}")

        except Exception as e:
            print(f"Error on {t}: {e}")

    print(f"Finished processing. Total tickers updated: {len(results)}")

@functions_framework.http
def get_historical_data_with_indicators(request):
    """
    Firebase Callable Function to fetch historical stock data and calculate technical indicators.
    It expects a JSON payload like:
    {
        "data": {
            "ticker": "GOOGL",
            "startDate": "2023-01-01",
            "endDate": "2023-01-31"
        }
    }
    """
    try:
        import yfinance as yf
        import pandas as pd
        import math
        request_json = request.get_json(silent=True)
        if not request_json or 'data' not in request_json:
            return {
                "data": {
                    "error": "Invalid request format. Missing 'data' field.",
                    "code": "invalid-argument"
                }
            }, 400 

        callable_data = request_json['data']
        ticker_symbol = callable_data.get('ticker')
        start_date = callable_data.get('startDate')
        end_date = callable_data.get('endDate')

        if not all([ticker_symbol, start_date, end_date]):
            return {
                "data": {
                    "error": "Missing ticker, startDate, or endDate parameters.",
                    "code": "invalid-argument"
                }
            }, 400

        ticker = yf.Ticker(ticker_symbol)
        # yfinance often requires a few extra days to accurately calculate indicators
        # Consider adjusting start_date to fetch more data for initial indicator calculations
        # For example, fetch 60-90 days prior to your requested start_date
        # For simplicity, we'll use the requested range here.
        df = ticker.history(start=start_date, end=end_date)

        if df.empty:
            return {
                "data": {
                    "error": f"No historical data found for {ticker_symbol} within the specified range.",
                    "code": "not-found"
                }
            }, 404

        # Ensure column names are clean (yfinance sometimes returns MultiIndex for 'history')
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]

        # Simple Moving Averages (SMAs)
        df["SMA_20"] = df["Close"].rolling(window=20, min_periods=1).mean()
        df["SMA_50"] = df["Close"].rolling(window=50, min_periods=1).mean()

        # Relative Strength Index (RSI)
        # Note: RSI calculation needs at least 14 periods. If data is shorter, it will be NaN.
        delta = df["Close"].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.ewm(span=14, adjust=False, min_periods=14).mean()
        avg_loss = loss.ewm(span=14, adjust=False, min_periods=14).mean()
        rs = avg_gain / avg_loss
        df["RSI"] = 100 - (100 / (1 + rs))

        # Bollinger Bands
        # Note: Bollinger Bands need at least 20 periods for calculation.
        rolling_mean_20 = df["Close"].rolling(window=20, min_periods=1).mean()
        rolling_std_20 = df["Close"].rolling(window=20, min_periods=1).std()
        df["BB_upper"] = rolling_mean_20 + (2 * rolling_std_20)
        df["BB_lower"] = rolling_mean_20 - (2 * rolling_std_20)

        # Moving Average Convergence Divergence (MACD)
        # Note: MACD needs at least 26 periods for full calculation.
        ema_12 = df["Close"].ewm(span=12, adjust=False, min_periods=1).mean()
        ema_26 = df["Close"].ewm(span=26, adjust=False, min_periods=1).mean()
        df["MACD"] = ema_12 - ema_26
        df["Signal"] = df["MACD"].ewm(span=9, adjust=False, min_periods=1).mean()
        df["MACD_Hist"] = df["MACD"] - df["Signal"] # Often useful to include

        # Reset index to turn 'Date' into a regular column, then convert to records
        # Fill NaN values with None (which becomes 'null' in JSON) as NaN is not JSON serializable.
        # This allows your client app to handle missing data points cleanly.
        df_cleaned = df.reset_index().fillna(value=math.nan) # Use math.nan which converts to null
        
        # Convert Timestamp objects to string format for JSON
        df_cleaned['Date'] = df_cleaned['Date'].dt.strftime('%Y-%m-%d')
        
        historical_data_list = df_cleaned.to_dict('records')

        return {
            "data": {
                "ticker": ticker_symbol,
                "historicalData": historical_data_list
            }
        }, 200 
              # The actual result is in the "data" field of the JSON body
              # Any "error" field or "code" in the "data" field indicates an error to the SDK.

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return {
            "data": {
                "error": f"Failed to fetch or process historical data: {str(e)}",
                "code": "internal"
            }
        }, 500 # Callable functions *always* return 200 OK for Firebase SDK
              # The actual result is in the "data" field of the JSON body
              # Any "error" field or "code" in the "data" field indicates an error to the SDK.