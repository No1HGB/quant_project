import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import yfinance as yf

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)

# S&P500 티커 리스트 불러오기 (위키피디아에서 크롤링)
wiki_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
tables = pd.read_html(wiki_url)
sp500 = tables[0]
tickers = sp500["Symbol"].tolist()

# 일별 OHLCV 데이터 다운로드
ohlcv = yf.download(
    tickers,
    period="3y",
    interval="1d",
    auto_adjust=True,  # 주식 분할/배당 보정
    group_by="ticker",
    threads=True,
)

# 저장
ohlcv_path = os.path.join(DATA_DIR, "sp500_ohlcv.csv")
ohlcv.to_csv(ohlcv_path)


def save_fundamentals(sym: str):
    """한 종목의 분기별 재무제표를 Excel로 저장"""
    t = yf.Ticker(sym)

    # info → DataFrame
    info_df = pd.DataFrame.from_dict(t.info, orient="index", columns=["value"])
    q_fin = t.quarterly_financials
    q_bs = t.quarterly_balance_sheet
    q_cf = t.quarterly_cashflow

    excel_path = os.path.join(DATA_DIR, f"{sym}.xlsx")
    with pd.ExcelWriter(excel_path, engine="xlsxwriter") as writer:
        info_df.to_excel(writer, sheet_name="info")
        if not q_fin.empty:
            q_fin.to_excel(writer, sheet_name="quarterly_financials")
        if not q_bs.empty:
            q_bs.to_excel(writer, sheet_name="quarterly_balance_sheet")
        if not q_cf.empty:
            q_cf.to_excel(writer, sheet_name="quarterly_cashflow")
    return sym


# 최대 10개 동시 실행
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(save_fundamentals, sym): sym for sym in tickers}
    for future in as_completed(futures):
        sym = futures[future]
        try:
            result = future.result()
            print(f"✔ {result} 저장 완료")
        except Exception as e:
            print(f"✖ {sym} 저장 실패: {e}")
