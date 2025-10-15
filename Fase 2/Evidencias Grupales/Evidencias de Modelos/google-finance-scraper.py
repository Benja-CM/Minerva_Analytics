# requirements:
# pip install selenium pandas
# necesitas Chrome o Chromium instalado; chromedriver se puede manejar automáticamente en Selenium 4.10+

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import pandas as pd
import time
import random

def make_driver(headless=True):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    # evita detección simple:
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    driver = webdriver.Chrome(options=opts)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined})
        """
    })
    return driver

def fetch_with_selenium(ticker: str, exchange: str = None, headless=True):
    ref = f"{ticker}:{exchange}" if exchange else ticker
    url = f"https://www.google.com/finance/quote/{ref}"

    driver = make_driver(headless=headless)
    try:
        driver.get(url)
        # espera implícita simple; para robustez usar WebDriverWait
        time.sleep(2 + random.random()*1.5)

        # ejemplos de localizadores (ajusta según la página actual)
        def safe_find(xpath_list):
            for xp in xpath_list:
                try:
                    el = driver.find_element(By.XPATH, xp)
                    txt = el.text.strip()
                    if txt:
                        return txt
                except Exception:
                    continue
            return None

        price = safe_find([
            "//div[contains(@class,'YMlKec')]",         # posible
            "//div[contains(@class,'IsqQVc')]"          # posible
        ])
        change = safe_find([
            "//div[contains(@class,'Jl2')]",            # ejemplo
            "//div[contains(@class,'WlRRw')]"           # ejemplo
        ])

        # ejemplo de captura de filas de estadisticas
        stats = {}
        rows = driver.find_elements(By.XPATH, "//div[contains(@class,'P6K39c')]")
        for r in rows:
            try:
                label = r.find_element(By.XPATH, ".//div[contains(@class,'eKzLze')]").text.strip()
                value = r.find_element(By.XPATH, ".//div[contains(@class,'TdYk6b')]").text.strip()
                stats[label] = value
            except Exception:
                continue

        return {"ticker": ref, "price": price, "change": change, "stats": stats, "url": url}
    finally:
        driver.quit()

if __name__ == "__main__":
    res = fetch_with_selenium("AAPL", "NASDAQ", headless=True)
    print(res)
    pd.DataFrame([{"ticker": res["ticker"], "price": res["price"], "change": res["change"]}]).to_csv("gf_selenium.csv", index=False)
