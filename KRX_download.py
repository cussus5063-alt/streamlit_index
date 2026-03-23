import requests, time
from datetime import timedelta, datetime
import pandas as pd
import streamlit as st

API_KEY = st.secrets["API_KEY"]
headers = {
    "AUTH_KEY": API_KEY,
    "User-Agent": "Mozilla/5.0"
}

info = {
    # 'KRX_IDX' : ["https://data-dbg.krx.co.kr/svc/apis/idx/krx_dd_trd", 'KRX_지수.parquet'],
    'DEVRIVATIVE_PRODUCT_IDX' : ['https://data-dbg.krx.co.kr/svc/apis/idx/drvprod_dd_trd', '파생상품_지수.parquet'],
    'FIXED_INCOME_IDX' : ['https://data-dbg.krx.co.kr/svc/apis/idx/bon_dd_trd', '채권_지수.parquet'],
    'KOSPI_IDX' : ['https://data-dbg.krx.co.kr/svc/apis/idx/kospi_dd_trd', 'KOSPI_지수.parquet'],
    'KOSDAQ_IDX' : ['https://data-dbg.krx.co.kr/svc/apis/idx/kosdaq_dd_trd', 'KOSDAQ_지수.parquet'],
    # 'KOSPI_STOCK' : ['https://data-dbg.krx.co.kr/svc/apis/sto/stk_bydd_trd', 'KOSPI_일별매매정보.parquet'],
    # 'KOSDAQ_STOCK' : ['https://data-dbg.krx.co.kr/svc/apis/sto/ksq_bydd_trd', 'KOSDAQ_일별매매정보.parquet'],
    # 'ETF' : ['https://data-dbg.krx.co.kr/svc/apis/etp/etf_bydd_trd', 'ETF_일별매매정보.parquet'],
    'OIL' : ['https://data-dbg.krx.co.kr/svc/apis/gen/oil_bydd_trd', '석유시장_일별매매정보.parquet'],
    'GOLD' : ['https://data-dbg.krx.co.kr/svc/apis/gen/gold_bydd_trd', '금시장_일별매매정보.parquet']
}

def fetch_index_data(url, start_date, end_date):
    all_data = []
    current = start_date
    while current <= end_date:
        print(current, 'will be fetched')
        date_str = current.strftime("%Y%m%d")
        params = {
            "basDd": date_str
        }
        try:
            r = requests.get(url, headers=headers, params=params, timeout=10)
            if r.status_code == 200:
                json_data = r.json()
                if 'OutBlock_1' in json_data:
                    df = pd.DataFrame(json_data['OutBlock_1'])
                    if not df.empty:
                        all_data.append(df)
            else:
                print(f"{date_str} error:", r.status_code)

        except Exception as e:
            print(f"{date_str} exception:", e)

        current += timedelta(days=1)
        time.sleep(0.2)

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        return pd.DataFrame()
    
def parse_df():
    for key, val in info.items():
        yield f'{key} will be parsed'
        url = val[0]
        file_nm = val[1]
        
        df_org = pd.read_parquet(file_nm)
        start_date = datetime.strptime(df_org['BAS_DD'].max(), '%Y%m%d')
        start_date += timedelta(days=1)
        end_date = datetime.now()
        
        df_tmp = fetch_index_data(url, start_date, end_date)
        print(df_tmp.tail())
        df_org = pd.concat([df_org, df_tmp]).reset_index(drop=True)
        df_org.to_parquet(file_nm, engine = 'pyarrow')
    yield 'Download completed'
