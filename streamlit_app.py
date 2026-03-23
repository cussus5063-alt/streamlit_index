import streamlit as st
import pandas as pd
import altair as alt
import numpy as np
from datetime import timedelta
import KRX_download

st.set_page_config(layout="wide")

# def display_df_trend(df, col_name, category, year):  
#   df['BAS_DD'] = pd.to_datetime(df['BAS_DD'], format='%Y%m%d').dt.date
#   from_date = df['BAS_DD'].max() - timedelta(days=365*year)
#   df = df.loc[df['BAS_DD']>from_date].reset_index(drop=True)
#   df[col_name[1]] = df[col_name[1]].replace('None', np.nan)
#   df[col_name[1]] = df[col_name[1]].replace('-', np.nan)
#   df = df.dropna(subset=col_name[1]).reset_index(drop=True)
#   name = df[col_name[0]].unique()

#   if category is None:
#     return
#   elif category == 'gold':
#     select = st.sidebar.selectbox('금 시세📀', name)
#     word = '금 시세'
#   elif category == 'oil':
#     select = st.sidebar.selectbox('원유 시세🛢', name)
#     word = '원유 시세'
#   elif category == 'kospi_idx':
#     select = st.sidebar.selectbox('KOSPI Index🔥', name)
#     word = 'KOSPI Index'
#   elif category == 'kosdaq_idx':
#     select = st.sidebar.selectbox('KOSDAQ Index💰', name)
#     word = 'KOSDAQ Index'

#   st.write("일자별 :red[{}]".format(word))
#   tmp = df.loc[df[col_name[0]]==select].reset_index(drop=True)
#   tmp[col_name[1]] = pd.to_numeric(tmp[col_name[1]], errors='coerce')
#   col_tmp = [col_name[0], 'BAS_DD', col_name[1]]
#   st.dataframe(tmp[col_tmp].sort_values(by='BAS_DD', ascending=False).reset_index(drop=True) ,hide_index=True)

#   st.write("최근 {year}년간 {name} Trend".format(year=year, name=select))
#   c = alt.Chart(tmp).mark_line(color='red').encode(x=alt.X('BAS_DD:T', axis=alt.Axis(format='%Y-%m', grid=True, gridDash=[4,6])), y=col_name[1])
#   st.altair_chart(c, use_container_width=True)

def display_df_trend(df, col_name, select, word, year):

    # df = df.copy()

    df['BAS_DD'] = pd.to_datetime(df['BAS_DD'], format='%Y%m%d').dt.date
    from_date = df['BAS_DD'].max() - timedelta(days=365 * year)
    df = df.loc[df['BAS_DD'] > from_date].reset_index(drop=True)

    df[col_name[1]] = df[col_name[1]].replace(['None', '-'], np.nan)
    df = df.dropna(subset=[col_name[1]]).reset_index(drop=True)

    tmp = df.loc[df[col_name[0]] == select].reset_index(drop=True)
    tmp[col_name[1]] = pd.to_numeric(tmp[col_name[1]], errors='coerce')

    st.markdown(f"##### {select} 시세")

    c = alt.Chart(tmp).mark_line(color='red').encode(
        x=alt.X('BAS_DD:T', axis=alt.Axis(format='%Y-%m', grid=True, gridDash=[4, 6])), y=col_name[1], tooltip=['BAS_DD', col_name[1]]
        )
    
    hover = alt.selection_point(fields=['BAS_DD'], nearest=True, on='mouseover', empty='none', clear='mouseout')
    points = c.mark_point(size=80, filled=True, color='green').encode(opacity=alt.condition(hover, alt.value(1), alt.value(0)))
    tooltips = alt.Chart(tmp).mark_rule(color='green').encode(
      x='BAS_DD:T',
      opacity=alt.condition(~hover, alt.value(0.5), alt.value(0)),
      tooltip=[alt.Tooltip('BAS_DD:T', title='BAS_DD'), alt.Tooltip(col_name[1], title=col_name[1])]
      ).add_params(hover)
    
    chart = (c + tooltips + points)
    st.altair_chart(chart, use_container_width=True)

st.title(":green[국내지수/금&원유] 거래가격 차트:sunglasses:")




# tab_gold, tab_oil, tab_kospi, tab_kosdaq= st.tabs(['금', '원유', 'KOSPI', 'KOSDAQ'])

# st.sidebar.title('항목 고르기')
# with tab_gold:
#   display_df_trend(gold_data, ['ISU_NM', 'TDD_CLSPRC'], 'gold', year)

# with tab_oil:
#   display_df_trend(oil_data, ['OIL_NM', 'WT_DIS_AVG_PRC'], 'oil', year)

# with tab_kospi:
#   display_df_trend(kospi_data, ['IDX_NM', 'CLSPRC_IDX'], 'kospi_idx', year)

# with tab_kosdaq:
#   display_df_trend(kosdaq_data, ['IDX_NM', 'CLSPRC_IDX'], 'kosdaq_idx', year)

@st.cache_data
def load_data():
    gold = pd.read_parquet("금시장_일별매매정보.parquet")
    oil = pd.read_parquet("석유시장_일별매매정보.parquet")
    kospi = pd.read_parquet("KOSPI_지수.parquet")
    kosdaq = pd.read_parquet("KOSDAQ_지수.parquet")
    drvprod = pd.read_parquet("파생상품_지수.parquet")
    # bond = pd.read_parquet('채권_지수.parquet')
    return gold, oil, kospi, kosdaq, drvprod

if st.sidebar.button('KRX Data Update', icon=":material/refresh:"):
  status_box = st.empty()

  for msg in KRX_download.parse_df():
      status_box.info(msg)

  st.cache_data.clear()
  st.success("모든 작업 완료!")
  st.rerun()

gold_data, oil_data, kospi_data, kosdaq_data, drvprod_data = load_data()

# gold_data = pd.read_parquet("금시장_일별매매정보.parquet")
# oil_data = pd.read_parquet("석유시장_일별매매정보.parquet")
# kospi_data = pd.read_parquet("KOSPI_지수.parquet")
# kosdaq_data = pd.read_parquet("KOSDAQ_지수.parquet")

category_map = {
    "금": (gold_data, ['ISU_NM', 'TDD_CLSPRC'], "금 시세"),
    "원유": (oil_data, ['OIL_NM', 'WT_DIS_AVG_PRC'], "원유 시세"),
    "KOSPI": (kospi_data, ['IDX_NM', 'CLSPRC_IDX'], "KOSPI Index"),
    "KOSDAQ": (kosdaq_data, ['IDX_NM', 'CLSPRC_IDX'], "KOSDAQ Index"),
    "Bond": (drvprod_data, ['IDX_NM', 'CLSPRC_IDX'], "파생상품")
}

with st.sidebar.container(border=True):    
    st.markdown("## 📊 :red[데이터 선택]")
    year = st.number_input(label=":red[조회년도 설정]", min_value=0.1, step=0.1, format="%.1f")
    category_label = st.selectbox(":red[카테고리]", list(category_map.keys()))
    print(category_label)

df, col_name, word = category_map[category_label]

items = sorted(df[col_name[0]].unique())
if category_label == 'Bond':
    choice_item = [False]*len(items)
else:
    choice_item = [True]*len(items)
options_df = pd.DataFrame({"선택": choice_item, "항목": items})
edited_df = st.sidebar.data_editor(options_df, use_container_width=True, height=300, hide_index=True)
selected_items = edited_df.loc[edited_df["선택"] == True, "항목"].tolist()

st.subheader(f"최근 {year}년 {word} Trend")

n_cols = 3

for i in range(0, len(selected_items), n_cols):
    cols = st.columns(n_cols)

    for j in range(n_cols):
        if i + j < len(selected_items):
            item = selected_items[i + j]

            with cols[j]:
                with st.container(border=True):
                    display_df_trend(df, col_name, item, word, year)