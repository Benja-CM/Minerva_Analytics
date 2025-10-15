import yfinance as yf
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def get_data(symbol="BSANTANDER.SN", period="2y", interval="1d"):
    """
    Descarga datos históricos de un activo desde Yahoo Finance.
    """
    df = yf.download(symbol, period=period, interval=interval)
    df.dropna(inplace=True)
    df.to_csv('MAYF.csv')
    return df


def add_indicators(df):
    """
    Agrega indicadores técnicos al DataFrame.
    """
    # Forzar columnas simples (por si vienen de un MultiIndex)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]

    df["SMA_20"] = df["Close"].rolling(20).mean()
    df["SMA_50"] = df["Close"].rolling(50).mean()

    # RSI
    delta = df["Close"].diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss
    df["RSI"] = 100 - (100 / (1 + rs))

    # Bollinger Bands
    rolling_std = df["Close"].rolling(20).std()
    df["BB_upper"] = df["SMA_20"] + 2 * rolling_std
    df["BB_lower"] = df["SMA_20"] - 2 * rolling_std

    # MACD
    ema_12 = df["Close"].ewm(span=12, adjust=False).mean()
    ema_26 = df["Close"].ewm(span=26, adjust=False).mean()
    df["MACD"] = ema_12 - ema_26
    df["Signal"] = df["MACD"].ewm(span=9, adjust=False).mean()

    return df.dropna()


def performance_metrics(df):
    """
    Calcula métricas financieras clave.
    """
    returns = df["Close"].pct_change().dropna()
    mean_return = returns.mean()
    volatility = returns.std()
    sharpe = (mean_return / volatility) * np.sqrt(252)
    cumulative = (1 + returns).cumprod()

    drawdown = (cumulative / cumulative.cummax() - 1).min()

    metrics = {
        "Return (annualized)": mean_return * 252,
        "Volatility (annualized)": volatility * np.sqrt(252),
        "Sharpe Ratio": sharpe,
        "Max Drawdown": drawdown
    }
    return metrics


def fundamentals(symbol="BSANTANDER.SN"):
    """
    Extrae información fundamental del activo.
    """
    ticker = yf.Ticker(symbol)
    info = ticker.info
    fundamentals_data = {
        "Company": info.get("longName"),
        "Sector": info.get("sector"),
        "Market Cap": info.get("marketCap"),
        "P/E": info.get("trailingPE"),
        "P/B": info.get("priceToBook"),
        "ROE": info.get("returnOnEquity"),
        "Debt/Equity": info.get("debtToEquity"),
        "Dividend Yield": info.get("dividendYield")
    }
    return fundamentals_data


def plot_market(df, symbol="BSANTANDER.SN"):
    """
    Crea gráfico interactivo de precios con indicadores técnicos.
    """
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        row_heights=[0.7, 0.3],
                        vertical_spacing=0.05,
                        subplot_titles=(f"{symbol} Market Overview", "RSI Indicator"))

    # --- Candlestick ---
    fig.add_trace(go.Candlestick(
        x=df.index,
        open=df["Open"], high=df["High"],
        low=df["Low"], close=df["Close"],
        name="Precio"
    ), row=1, col=1)

    # --- SMA y Bollinger Bands ---
    fig.add_trace(go.Scatter(x=df.index, y=df["SMA_20"], name="SMA 20", line=dict(color="orange")), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df["SMA_50"], name="SMA 50", line=dict(color="blue")), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df["BB_upper"], name="Bollinger Upper", line=dict(color="gray", dash="dot")), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df["BB_lower"], name="Bollinger Lower", line=dict(color="gray", dash="dot")), row=1, col=1)

    # --- RSI ---
    fig.add_trace(go.Scatter(x=df.index, y=df["RSI"], name="RSI", line=dict(color="purple")), row=2, col=1)
    fig.add_hline(y=70, line_dash="dot", line_color="red", row=2, col=1)
    fig.add_hline(y=30, line_dash="dot", line_color="green", row=2, col=1)

    fig.update_layout(
        template="plotly_white",
        height=800,
        xaxis_rangeslider_visible=False,
        title=f"{symbol} Market Analysis"
    )
    fig.write_html('MAYD.html')


if __name__ == "__main__":
    symbol = "BSANTANDER.SN"
    df = get_data(symbol)
    df = add_indicators(df)

    metrics = performance_metrics(df)
    fundamentals_data = fundamentals(symbol)

    print("\n📊 MÉTRICAS FINANCIERAS:")
    for k, v in metrics.items():
        print(f"{k}: {v:.4f}")

    print("\n🏢 DATOS FUNDAMENTALES:")
    for k, v in fundamentals_data.items():
        print(f"{k}: {v}")

    plot_market(df, symbol)
