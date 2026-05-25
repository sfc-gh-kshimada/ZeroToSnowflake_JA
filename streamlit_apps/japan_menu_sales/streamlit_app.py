import streamlit as st
st.set_page_config(layout="wide", page_title="日本メニュー売上分析")

import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.title("🍽️ 2022年2月の日本のメニューアイテム売上")
st.caption("各メニューアイテムの日次売上（青エリア）と全体平均（赤ライン）を比較できます")
st.write('---')

@st.cache_data()
def load_data():
    japan_sales_df = session.table("tb_101.analytics.japan_menu_item_sales_feb_2022").to_pandas()
    return japan_sales_df

japan_sales = load_data()

daily_by_item = japan_sales.groupby(['DATE', 'MENU_ITEM_NAME'])['ORDER_TOTAL'].sum().reset_index()
daily_by_item['DATE'] = pd.to_datetime(daily_by_item['DATE'])
daily_by_item['ORDER_TOTAL'] = daily_by_item['ORDER_TOTAL'].astype(float)

overall_daily_avg = daily_by_item.groupby('DATE')['ORDER_TOTAL'].mean().reset_index()
overall_daily_avg.rename(columns={'ORDER_TOTAL': 'AVG_TOTAL'}, inplace=True)

chart_data = daily_by_item.merge(overall_daily_avg, on='DATE')

items = sorted(chart_data['MENU_ITEM_NAME'].unique())

with st.sidebar:
    st.header("⚙️ 表示設定")
    num_cols = st.slider("列数", min_value=2, max_value=6, value=4)
    chart_height = st.slider("チャートの高さ", min_value=80, max_value=250, value=120, step=10)
    selected_items = st.multiselect(
        "表示するメニューアイテム",
        options=items,
        default=items
    )

if not selected_items:
    st.warning("サイドバーからメニューアイテムを選択してください")
    st.stop()

filtered_items = [item for item in items if item in selected_items]

col_summary = st.columns(3)
total_sales = daily_by_item[daily_by_item['MENU_ITEM_NAME'].isin(filtered_items)]['ORDER_TOTAL'].sum()
avg_daily = overall_daily_avg['AVG_TOTAL'].mean()
col_summary[0].metric("📊 表示アイテム数", f"{len(filtered_items)} 品")
col_summary[1].metric("💰 合計売上", f"${total_sales:,.0f}")
col_summary[2].metric("📈 日次平均売上", f"${avg_daily:,.0f}")

st.write('---')

for row_start in range(0, len(filtered_items), num_cols):
    row_items = filtered_items[row_start:row_start + num_cols]
    cols = st.columns(num_cols, gap="small")
    for i, item in enumerate(row_items):
        item_data = chart_data[chart_data['MENU_ITEM_NAME'] == item].copy()

        item_avg = item_data['ORDER_TOTAL'].mean()
        overall_avg_val = item_data['AVG_TOTAL'].mean()
        diff_pct = ((item_avg - overall_avg_val) / overall_avg_val * 100) if overall_avg_val else 0
        badge = "🔴" if diff_pct < -10 else "🟢" if diff_pct > 10 else "🟡"

        tooltip_fields = [
            alt.Tooltip('DATE:T', title='日付', format='%m/%d'),
            alt.Tooltip('ORDER_TOTAL:Q', title='売上', format='$,.0f'),
            alt.Tooltip('AVG_TOTAL:Q', title='平均', format='$,.0f')
        ]

        area = alt.Chart(item_data).mark_area(
            opacity=0.3, color='#4C78A8',
            line={'color': '#4C78A8', 'strokeWidth': 1}
        ).encode(
            x=alt.X('DATE:T', axis=alt.Axis(format='%d', labelAngle=0, grid=False, tickCount=5), title=None),
            y=alt.Y('ORDER_TOTAL:Q', axis=alt.Axis(grid=True, gridColor='#eee', tickCount=3, format='~s'), title=None),
            tooltip=tooltip_fields
        )

        line = alt.Chart(item_data).mark_line(
            color='#e45756', strokeWidth=1.5, strokeDash=[4, 2]
        ).encode(
            x='DATE:T',
            y='AVG_TOTAL:Q',
            tooltip=tooltip_fields
        )

        c = (area + line).properties(
            title=alt.Title(text=f"{badge} {item}", subtitle=f"平均比: {diff_pct:+.1f}%", fontSize=11, anchor='start', subtitleFontSize=9, subtitleColor='#888'),
            height=chart_height
        ).configure_view(
            stroke='#ddd', strokeWidth=0.5
        ).configure(
            background='#ffffff',
            padding=5
        ).configure_axis(
            labelColor='#555', titleColor='#555',
            gridColor='#eee', domainColor='#999',
            labelFontSize=9
        )
        cols[i].altair_chart(c, use_container_width=True)

st.write('---')
with st.expander("📋 データテーブルを表示"):
    display_df = daily_by_item[daily_by_item['MENU_ITEM_NAME'].isin(filtered_items)].copy()
    display_df['DATE'] = display_df['DATE'].dt.strftime('%Y-%m-%d')
    st.dataframe(display_df, use_container_width=True, hide_index=True)
