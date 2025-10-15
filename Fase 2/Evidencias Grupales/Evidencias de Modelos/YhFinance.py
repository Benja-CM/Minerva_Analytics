import yfinance as yf
ticker = yf.Ticker("AAPL")

info = ticker.info

print("Nombre:", info.get("longName"))
print("Precio actual:", info.get("currentPrice"))
print("Capitalización bursátil:", info.get("marketCap"))
print("P/E ratio:", info.get("trailingPE"))
print("Dividendo anual:", info.get("dividendYield"))

data = ticker.history(start="2025-01-01", end="2025-10-07")

print(data.head())

import matplotlib.pyplot as plt

data["Close"].plot(title="Precio de cierre AAPL 2025")
plt.show()
