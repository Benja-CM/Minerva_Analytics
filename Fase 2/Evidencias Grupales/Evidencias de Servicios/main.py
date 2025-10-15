# main.py
import os
import firebase_admin
from firebase_admin import firestore
from firebase_functions import scheduler_fn, https_fn
from firebase_functions.options import set_global_options

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

@https_fn.on_request()
def get_historical_data_with_indicators(req: https_fn.Request):
    import yfinance as yf
    import pandas as pd
    import math

    try:
        # Parse JSON body
        request_json = req.get_json(silent=True)
        print(f"DEBUG: Request JSON received: {request_json}") 
        
        if not request_json or 'data' not in request_json:
            print("DEBUG: Invalid request format - 'data' field missing.") 
            return https_fn.Response(
                {
                    "error": "Invalid request format. Missing 'data' field.",
                    "code": "invalid-argument"
                },
                status=400,
                content_type="application/json" 
            )

        data = request_json['data']
        ticker_symbol = data.get('ticker')
        start_date = data.get('startDate')
        end_date = data.get('endDate')
        print(f"DEBUG: Ticker: {ticker_symbol}, Start: {start_date}, End: {end_date}") 

        if not all([ticker_symbol, start_date, end_date]):
            print("DEBUG: Missing ticker, startDate, or endDate parameters.") 
            return https_fn.Response(
                {
                    "error": "Missing ticker, startDate, or endDate parameters.",
                    "code": "invalid-argument"
                },
                status=400,
                content_type="application/json"
            )

        # Fetch data
        ticker = yf.Ticker(ticker_symbol)
        df = ticker.history(start=start_date, end=end_date)
        print(f"DEBUG: DataFrame fetched. Is empty: {df.empty}, Shape: {df.shape}") 

        if df.empty:
            print(f"DEBUG: No data found for {ticker_symbol} in range.") 
            return https_fn.Response(
                {
                    "error": f"No historical data found for {ticker_symbol} within the specified range.",
                    "code": "not-found"
                },
                status=404,
                content_type="application/json"
            )

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
            print("DEBUG: Flattened MultiIndex columns.") 

        # === Technical Indicators ===
        print("DEBUG: Technical indicators calculated.") 

        # === Clean and format output ===
        df_cleaned = df.reset_index().fillna(value=math.nan)
        df_cleaned['Date'] = df_cleaned['Date'].dt.strftime('%Y-%m-%d')
        print(f"DEBUG: Cleaned DataFrame head (first 5 rows):\n{df_cleaned.head().to_string()}")

        historical_data_list = df_cleaned.to_dict('records')
        print(f"DEBUG: Type of historical_data_list: {type(historical_data_list)}") 
        print(f"DEBUG: Length of historical_data_list: {len(historical_data_list)}") 
        if historical_data_list:
            print(f"DEBUG: First record in historical_data_list: {historical_data_list[0]}") 
        else:
            print("DEBUG: historical_data_list is empty.") 

        final_response_dict = {
            "ticker": ticker_symbol,
            "historicalData": historical_data_list
        }
        print(f"DEBUG: Final dictionary object attempting to be returned:\n{final_response_dict}")

        import json 
        json_output_string = json.dumps(final_response_dict)
        print(f"DEBUG: JSON string prepared for response (first 500 chars):\n{json_output_string[:500]}...")

        return https_fn.Response(
            json_output_string, 
            status=200,
            content_type="application/json"
        )

    except Exception as e:
        print(f"ERROR: An unexpected exception occurred: {e}")
        import traceback
        traceback.print_exc()

        error_response = {
            "error": f"Failed to fetch or process historical data: {str(e)}",
            "code": "internal"
        }
        return https_fn.Response(
            json.dumps(error_response),
            status=500,
            content_type="application/json"
        )