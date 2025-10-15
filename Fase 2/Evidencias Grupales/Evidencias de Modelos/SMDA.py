import yfinance as yf
import pandas as pd

# Descargar datos de una acción (ej. Apple)
ticker = yf.Ticker("AAPL")
df = ticker.history(period="2y")  # últimos 2 años

# Calcular indicadores técnicos
df["SMA_20"] = df["Close"].rolling(window=20).mean()
df["SMA_50"] = df["Close"].rolling(window=50).mean()

# RSI
delta = df["Close"].diff()
gain = delta.where(delta > 0, 0).rolling(14).mean()
loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
rs = gain / loss
df["RSI"] = 100 - (100 / (1 + rs))

# Bollinger Bands
df["BB_upper"] = df["SMA_20"] + 2 * df["Close"].rolling(20).std()
df["BB_lower"] = df["SMA_20"] - 2 * df["Close"].rolling(20).std()

print(df.tail())

info = ticker.info

print("Empresa:", info["longName"])
print("Sector:", info["sector"])
print("P/E Ratio:", info.get("trailingPE"))
print("P/B Ratio:", info.get("priceToBook"))
print("Dividend Yield:", info.get("dividendYield"))
print("ROE:", info.get("returnOnEquity"))
print("Debt to Equity:", info.get("debtToEquity"))

symbols = ["AAPL", "MSFT", "GOOG", "AMZN"]
data = yf.download(symbols, start="2020-01-01", end="2024-12-31")["Close"]
returns = data.pct_change().dropna()

corr_matrix = returns.corr()
print(corr_matrix)

import numpy as np

returns_daily = df["Close"].pct_change().dropna()
mean_return = returns_daily.mean()
volatility = returns_daily.std()
sharpe_ratio = (mean_return / volatility) * np.sqrt(252)  # anualizado

print(f"Retorno medio diario: {mean_return:.4f}")
print(f"Volatilidad: {volatility:.4f}")
print(f"Sharpe Ratio: {sharpe_ratio:.2f}")
