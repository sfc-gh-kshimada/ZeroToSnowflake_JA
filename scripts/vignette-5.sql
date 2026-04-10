/***************************************************************************************************       
Asset:        Zero to Snowflake - アプリとコラボレーション
Version:      v2     
Copyright(c): 2025 Snowflake Inc. All rights reserved.
****************************************************************************************************

アプリとコラボレーション
1. Snowflake マーケットプレイスからの気象データの取得
2. アカウントデータと Weather Source データの統合
3. Safegraph POI データの探索
4. Snowflake における Streamlit の概要

****************************************************************************************************/

-- まず、セッションのクエリタグを設定します
ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"tb_zts","version":{"major":1, "minor":1},"attributes":{"is_quickstart":1, "source":"tastybytes", "vignette": "apps_and_collaboration"}}';

-- ワークシートのコンテキストを設定します
USE DATABASE tb_101;
USE ROLE accountadmin;
USE WAREHOUSE tb_de_wh;

/*  1. Snowflake マーケットプレイスからの気象データの取得
    ***********************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/user-guide/data-sharing-intro
    ***********************************************************
    ジュニアアナリストの Ben は、天気が米国のフードトラック売上に与える影響について
    より深いインサイトを得たいと考えています。
    そのために、Snowflake マーケットプレイスを使用して気象データをアカウントに追加し、
    TastyBytes の自社データと組み合わせてクエリを実行することで、
    全く新しいインサイトを発見します。
    
    Snowflake マーケットプレイスは、多様なサードパーティデータ、
    アプリケーション、AI 製品を発見・アクセスするための集中型ハブを提供します。
    このセキュアなデータ共有により、データを複製することなく
    ライブですぐにクエリ可能なデータにアクセスできます。
    
    Weather Source データを取得する手順:
    1. アカウントレベルで accountadmin を使用していることを確認します（左下隅を確認）。
    2. ナビゲーションメニューから「データ製品」ページに移動します。必要に応じて新しいブラウザタブで開けます。
    3. 検索バーに「Weather Source frostbyte」と入力します。
    4. 「Weather Source LLC: frostbyte」リストを選択し、「取得」をクリックします。
    5. 「オプション」をクリックしてオプションセクションを展開します。
    6. データベース名を「ZTS_WEATHERSOURCE」に変更します。
    7. 「PUBLIC」にアクセスを付与します。
    8. 「完了」をクリックします。
    
    このプロセスにより、Weather Source データにほぼ即座にアクセスできます。
    従来のデータ複製とパイプラインの必要性を排除することで、
    アナリストはビジネス上の質問から実行可能な分析へと直接進むことができます。
    
    気象データがアカウントに追加されたので、TastyBytes のアナリストは
    既存の場所データと即座に結合できます。
*/

-- アナリストロールに切り替える
USE ROLE tb_analyst;

/*  2. アカウントデータと Weather Source データの統合

    Weather Source のシェアから生の場所データを統合する前に、
    データシェアに直接クエリを実行して、扱うデータをより深く理解しましょう。
    まず、気象データで利用可能なすべての異なる都市のリストと、
    その都市の特定の気象指標を取得します。
*/
SELECT 
    DISTINCT city_name,
    AVG(max_wind_speed_100m_mph) AS avg_wind_speed_mph,
    AVG(avg_temperature_air_2m_f) AS avg_temp_f,
    AVG(tot_precipitation_in) AS avg_precipitation_in,
    MAX(tot_snowfall_in) AS max_snowfall_in
FROM zts_weathersource.onpoint_id.history_day
WHERE country = 'US'
GROUP BY city_name;

-- 次に、生の国データと Weather Source データシェアの過去の日次気象データを結合するビューを作成します。
CREATE OR REPLACE VIEW harmonized.daily_weather_v
COMMENT = 'Tasty Bytes がサービスを提供する都市に絞り込んだ Weather Source 日次過去データ'
    AS
SELECT
    hd.*,
    TO_VARCHAR(hd.date_valid_std, 'YYYY-MM') AS yyyy_mm,
    pc.city_name AS city,
    c.country AS country_desc
FROM zts_weathersource.onpoint_id.history_day hd
JOIN zts_weathersource.onpoint_id.postal_codes pc
    ON pc.postal_code = hd.postal_code
    AND pc.country = hd.country
JOIN raw_pos.country c
    ON c.iso_country = hd.country
    AND c.city = hd.city_name;

/*
    日次気象履歴ビューを使用して、Ben は2022年2月のハンブルクの
    平均日次気温を取得してラインチャートで可視化したいと思っています。

    結果ペインで「チャート」をクリックして、結果をグラフで可視化します。
    チャートビューの左側の「チャートタイプ」で以下を設定します:
    
        チャートタイプ: 折れ線グラフ | X 軸: DATE_VALID_STD | Y 軸: AVERAGE_TEMP_F
*/
SELECT
    dw.country_desc,
    dw.city_name,
    dw.date_valid_std,
    AVG(dw.avg_temperature_air_2m_f) AS average_temp_f
FROM harmonized.daily_weather_v dw
WHERE dw.country_desc = 'Germany'
    AND dw.city_name = 'Hamburg'
    AND YEAR(date_valid_std) = 2022
    AND MONTH(date_valid_std) = 2 -- 2月
GROUP BY dw.country_desc, dw.city_name, dw.date_valid_std
ORDER BY dw.date_valid_std DESC;

/*
    日次気象ビューが正常に機能しています！さらに一歩進めて、
    注文ビューと日次気象ビューを日次売上気象ビューとして組み合わせましょう。
    これにより、売上と天気の関係のトレンドや相関関係を発見できます。
*/
CREATE OR REPLACE VIEW analytics.daily_sales_by_weather_v
COMMENT = '日次気象指標と注文データ'
AS
WITH daily_orders_aggregated AS (
    SELECT
        DATE(o.order_ts) AS order_date,
        o.primary_city,
        o.country,
        o.menu_item_name,
        SUM(o.price) AS total_sales
    FROM
        harmonized.orders_v o
    GROUP BY ALL
)
SELECT
    dw.date_valid_std AS date,
    dw.city_name,
    dw.country_desc,
    ZEROIFNULL(doa.total_sales) AS daily_sales,
    doa.menu_item_name,
    ROUND(dw.avg_temperature_air_2m_f, 2) AS avg_temp_fahrenheit,
    ROUND(dw.tot_precipitation_in, 2) AS avg_precipitation_inches,
    ROUND(dw.tot_snowdepth_in, 2) AS avg_snowdepth_inches,
    dw.max_wind_speed_100m_mph AS max_wind_speed_mph
FROM
    harmonized.daily_weather_v dw
LEFT JOIN
    daily_orders_aggregated doa
    ON dw.date_valid_std = doa.order_date
    AND dw.city_name = doa.primary_city
    AND dw.country_desc = doa.country
ORDER BY 
    date ASC;

/*
    Ben はこの日次売上気象ビューを使用して、天気が売上に与える影響を解明できます。
    これまで未開拓だった関係性を探り、「シアトル市場において
    大雨は売上にどのような影響を与えるか？」などの質問に答えられるようになりました。

    チャートタイプ: 棒グラフ | X 軸: MENU_ITEM_NAME | Y 軸: DAILY_SALES
*/
SELECT * EXCLUDE (city_name, country_desc, avg_snowdepth_inches, max_wind_speed_mph)
FROM analytics.daily_sales_by_weather_v
WHERE 
    country_desc = 'United States'
    AND city_name = 'Seattle'
    AND avg_precipitation_inches >= 1.0
ORDER BY date ASC;

/*  3. Safegraph POI データの探索

    Ben はフードトラックの場所での天気状況についてより深いインサイトを得たいと思っています。
    幸い、Safegraph は Snowflake マーケットプレイスで無料の POI（ポイント・オブ・インタレスト）
    データを提供しています。
    
    このデータリストを使用するには、気象データと同様の手順に従います:
        1. アカウントレベルで accountadmin を使用していることを確認します（左下隅を確認）。
        2. ナビゲーションメニューから「データ製品」ページに移動します。必要に応じて新しいブラウザタブで開けます。
        3. 検索バーに「safegraph frostbyte」と入力します。
        4. 「Safegraph: frostbyte」リストを選択し、「取得」をクリックします。
        5. 「オプション」をクリックしてオプションセクションを展開します。
        6. データベース名を「ZTS_SAFEGRAPH」に設定します。
        7. 「PUBLIC」にアクセスを付与します。
        8. 「完了」をクリックします。
    
    Safegraph の POI データを Frostbyte のような気象データセットと自社の `orders_v` テーブルと
    結合することで、リスクの高い場所を特定し、外部要因による財務的影響を定量化できます。
*/
CREATE OR REPLACE VIEW harmonized.tastybytes_poi_v
AS 
SELECT 
    l.location_id,
    sg.postal_code,
    sg.country,
    sg.city,
    sg.iso_country_code,
    sg.location_name,
    sg.top_category,
    sg.category_tags,
    sg.includes_parking_lot,
    sg.open_hours
FROM raw_pos.location l
JOIN zts_safegraph.public.frostbyte_tb_safegraph_s sg 
    ON l.location_id = sg.location_id
    AND l.iso_country_code = sg.iso_country_code;

-- POI データと気象データを組み合わせて、2022年の米国における平均風速上位3位の場所を見つけます。
SELECT TOP 3
    p.location_id,
    p.city,
    p.postal_code,
    AVG(hd.max_wind_speed_100m_mph) AS average_wind_speed
FROM harmonized.tastybytes_poi_v AS p
JOIN
    zts_weathersource.onpoint_id.history_day AS hd
    ON p.postal_code = hd.postal_code
WHERE
    p.country = 'United States'
    AND YEAR(hd.date_valid_std) = 2022
GROUP BY p.location_id, p.city, p.postal_code
ORDER BY average_wind_speed DESC;

/*
    前のクエリの location_id を使用して、異なる天気条件下での売上パフォーマンスを
    直接比較したいと思います。共通テーブル式（CTE）を使用して、上のクエリをサブクエリとして
    利用し、平均風速が最も高い上位3か所の場所を見つけ、それらの特定の場所の
    売上データを分析します。CTE は複雑なクエリを読みやすく、パフォーマンスの良い
    異なる小さなクエリに分割するのに役立ちます。
    
    各トラックブランドの売上データを2つのバケットに分けます:
    最大風速が時速20マイル以下の「穏やか」な日と、20マイルを超える「風の強い」日です。

    このビジネス上の影響は、ブランドの天候耐性を特定することです。
    これらの売上数字を並べて比較することで、どのブランドが「天候に強い」か、
    どのブランドが強風で売上が大幅に落ちるかを即座に把握できます。
    これにより、弱いブランドへの「風の強い日」プロモーション実施、
    在庫調整、またはブランドのメニューを場所の典型的な天候に合わせるための
    将来のトラック展開戦略の改善など、より的確な運営上の意思決定が可能になります。
*/
WITH TopWindiestLocations AS (
    SELECT TOP 3
        p.location_id
    FROM harmonized.tastybytes_poi_v AS p
    JOIN
        zts_weathersource.onpoint_id.history_day AS hd
        ON p.postal_code = hd.postal_code
    WHERE
        p.country = 'United States'
        AND YEAR(hd.date_valid_std) = 2022
    GROUP BY p.location_id, p.city, p.postal_code
    ORDER BY AVG(hd.max_wind_speed_100m_mph) DESC
)
SELECT
    o.truck_brand_name,
    ROUND(
        AVG(CASE WHEN hd.max_wind_speed_100m_mph <= 20 THEN o.order_total END),
    2) AS avg_sales_calm_days,
    ZEROIFNULL(ROUND(
        AVG(CASE WHEN hd.max_wind_speed_100m_mph > 20 THEN o.order_total END),
    2)) AS avg_sales_windy_days
FROM analytics.orders_v AS o
JOIN
    zts_weathersource.onpoint_id.history_day AS hd
    ON o.primary_city = hd.city_name
    AND DATE(o.order_ts) = hd.date_valid_std
WHERE o.location_id IN (SELECT location_id FROM TopWindiestLocations)
GROUP BY o.truck_brand_name
ORDER BY o.truck_brand_name;

/*----------------------------------------------------------------------------------
 リセットスクリプト
----------------------------------------------------------------------------------*/
USE ROLE accountadmin;

-- ビューをドロップする
DROP VIEW IF EXISTS harmonized.daily_weather_v;
DROP VIEW IF EXISTS analytics.daily_sales_by_weather_v;
DROP VIEW IF EXISTS harmonized.tastybytes_poi_v;

-- クエリタグを解除する
ALTER SESSION UNSET query_tag;
-- ウェアハウスをサスペンドする
ALTER WAREHOUSE tb_de_wh SUSPEND;
