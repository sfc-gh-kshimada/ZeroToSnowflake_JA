# Snowflake の Streamlit へようこそ！

# 必要なライブラリをインポートする
# streamlit はウェブアプリのインターフェース作成に使用します。
import streamlit as st
# pandas はデータの操作と分析に使用します。
import pandas as pd
# altair はインタラクティブなデータビジュアライゼーションの作成に使用します。
import altair as alt
# snowflake.snowpark.context は Snowflake への接続とアクティブセッションの取得に使用します。
from snowflake.snowpark.context import get_active_session

# --- アプリのセットアップとデータ読み込み ---

# Snowflake とやり取りするためのアクティブな Snowpark セッションを取得する。
session = get_active_session()

# ページ上部に表示される Streamlit アプリのタイトルを設定する。
st.title("2022年2月の日本のメニューアイテム売上")

st.write('---') # 区切り線を作成する

# Snowflake からデータを読み込む関数を定義する。
# @st.cache_data は、この関数の出力をキャッシュする Streamlit デコレーターです。
# これにより、データは Snowflake から一度だけ取得されます。
# キャッシュにより、後続の実行やユーザーがウィジェットを操作する際のパフォーマンスが向上します。
@st.cache_data()
def load_data():
    """
    Snowflake のテーブルに接続し、データを取得して Pandas DataFrame として返す。
    """
    # アクティブセッションを使用して Snowflake のテーブルを参照し、Pandas DataFrame に変換する。

    japan_sales_df = session.table("tb_101.analytics.japan_menu_item_sales_feb_2022").to_pandas()
    return japan_sales_df

# データを読み込む関数を呼び出す。キャッシュにより、初回実行後は高速になります。
japan_sales = load_data()


# --- ウィジェットによるユーザーインタラクション ---

# ドロップダウンを作成するために、DataFrame からメニューアイテム名の一意のリストを取得する。
menu_item_names = japan_sales['MENU_ITEM_NAME'].unique().tolist()

# Streamlit のサイドバーまたはメインページにドロップダウンメニュー（selectbox）を作成する。
# ユーザーの選択は 'selected_menu_item' 変数に格納される。
selected_menu_item = st.selectbox("メニューアイテムを選択してください", options=menu_item_names)


# --- データの準備 ---

# ユーザーが選択したメニューアイテムにマッチする行のみを含むように
# メイン DataFrame をフィルタリングする。
menu_item_sales = japan_sales[japan_sales['MENU_ITEM_NAME'] == selected_menu_item]

# フィルタリングされたデータを 'DATE' でグループ化し、各日の 'ORDER_TOTAL' の合計を計算する。
daily_totals = menu_item_sales.groupby('DATE')['ORDER_TOTAL'].sum().reset_index()


# --- チャートの設定 ---

# 動的な Y 軸スケールを設定するために、売上値の範囲を計算する。
min_value = daily_totals['ORDER_TOTAL'].min()
max_value = daily_totals['ORDER_TOTAL'].max()

# チャートの最小/最大値の上下に追加するマージンを計算する。
chart_margin = (max_value - min_value) / 2
y_margin_min = min_value - chart_margin
y_margin_max = max_value + chart_margin

# 折れ線グラフを作成する。
chart = alt.Chart(daily_totals).mark_line(
    point=True,     
    tooltip=True
).encode(
    x=alt.X('DATE:T',
            axis=alt.Axis(title='日付', format='%b %d'),
            title='日付'),
    y=alt.Y('ORDER_TOTAL:Q',
            axis=alt.Axis(title='総売上（$）'), 
            title='日次総売上',
# Y 軸のカスタムドメイン（範囲）を設定して、動的にパディングを追加する。
            scale=alt.Scale(domain=[y_margin_min, y_margin_max]))
).properties(
    title=f'メニューアイテムの日次総売上: {selected_menu_item}',
    height=500
)


# --- チャートの表示 ---

# Streamlit アプリに Altair チャートをレンダリングする。
# 'use_container_width=True' により、チャートがコンテナの全幅に広がります。
st.altair_chart(chart, use_container_width=True)
