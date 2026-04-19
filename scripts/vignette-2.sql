/***************************************************************************************************       
Asset:        Zero to Snowflake - シンプルなデータパイプライン
Version:      v2     
Copyright(c): 2025 Snowflake Inc. All rights reserved.
****************************************************************************************************

シンプルなデータパイプライン
1. 外部ステージからのデータ取り込み
2. 半構造化データと VARIANT データ型
3. ダイナミックテーブル（Dynamic Tables）
4. ダイナミックテーブルによるシンプルなパイプライン
5. 有向非巡回グラフ（DAG）によるパイプラインの可視化

****************************************************************************************************/

ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"tb_zts","version":{"major":1, "minor":1},"attributes":{"is_quickstart":1, "source":"tastybytes", "vignette": "data_pipeline"}}';

/*
    ここでは TastyBytes のデータエンジニアの役割を担い、生のメニューデータを使ったデータパイプラインを
    作成することを目的として、コンテキストを適切に設定しましょう。
*/
USE DATABASE tb_101;
USE ROLE tb_data_engineer;
USE WAREHOUSE tb_de_wh;

/*  1. 外部ステージからのデータ取り込み
    ***************************************************************
    SQL リファレンス:
    https://docs.snowflake.com/ja/sql-reference/sql/copy-into-table
    ***************************************************************

    現在、データは CSV 形式で Amazon S3 バケットに保存されています。
    この生の CSV データをステージに読み込んでから、作業用のステージングテーブルへ
    COPY INTO する必要があります。
    
    Snowflake のステージとは、データファイルが保存されている場所を指定する
    名前付きデータベースオブジェクトです。テーブルへのデータのロードや
    テーブルからのアンロードを行うことができます。

    ステージを作成する際には以下を指定します:
                                - データを取得する S3 バケット
                                - データの解析に使用するファイルフォーマット（今回は CSV）
*/

-- メニューステージを作成する
CREATE OR REPLACE STAGE raw_pos.menu_stage
COMMENT = 'メニューデータ用ステージ'
URL = 's3://sfquickstarts/frostbyte_tastybytes/raw_pos/menu/'
FILE_FORMAT = public.csv_ff;

CREATE OR REPLACE TABLE raw_pos.menu_staging
(
    menu_id NUMBER(19,0),
    menu_type_id NUMBER(38,0),
    menu_type VARCHAR(16777216),
    truck_brand_name VARCHAR(16777216),
    menu_item_id NUMBER(38,0),
    menu_item_name VARCHAR(16777216),
    item_category VARCHAR(16777216),
    item_subcategory VARCHAR(16777216),
    cost_of_goods_usd NUMBER(38,4),
    sale_price_usd NUMBER(38,4),
    menu_item_health_metrics_obj VARIANT
);

-- ステージとテーブルの準備ができたら、ステージから新しい menu_staging テーブルにデータをロードしましょう。
COPY INTO raw_pos.menu_staging
FROM @raw_pos.menu_stage;

-- オプション: ロードが成功したことを確認する
SELECT * FROM raw_pos.menu_staging;

/*  2. Snowflake における半構造化データ
    *********************************************************************
    ユーザーガイド:
    https://docs.snowflake.com/ja/sql-reference/data-types-semistructured
    *********************************************************************
    
    Snowflake は VARIANT データ型を使用して JSON などの半構造化データの処理に優れています。
    データを自動的に解析・最適化・インデックス化し、標準 SQL と特殊な関数を使って
    クエリを実行し、簡単に抽出・分析できます。
    Snowflake は JSON、Avro、ORC、Parquet、XML などの半構造化データ型をサポートしています。
    
    menu_item_health_metrics_obj カラムの VARIANT オブジェクトには2つの主なキーと値のペアが含まれます:
        - menu_item_id: アイテムの一意の識別子を表す数値。
        - menu_item_health_metrics: 健康情報の詳細を持つオブジェクトを格納する配列。
        
    menu_item_health_metrics 配列内の各オブジェクトには以下があります:
        - ingredients: 文字列の配列。
        - 'Y' または 'N' の文字列値を持つ複数の食事制限フラグ。
*/
SELECT menu_item_health_metrics_obj FROM raw_pos.menu_staging;

/*
    このクエリは、データの内部 JSON 的な構造をナビゲートするための特殊な構文を使用しています。
    コロン演算子（:）はキー名でデータにアクセスし、角括弧（[]）は配列の要素を数値位置で選択します。
    これらの演算子を組み合わせて、ネストされたオブジェクトから材料リストを抽出できます。
    
    VARIANT オブジェクトから取得した要素は VARIANT 型のまま残ります。
    これらの要素を既知のデータ型にキャストすることで、クエリのパフォーマンスが向上し
    データ品質も向上します。
    キャストには2つの方法があります:
        - CAST 関数
        - 短縮構文: <ソース式> :: <ターゲットデータ型>

    以下は、これらのトピックをすべて組み合わせて、メニューアイテム名、メニューアイテム ID、
    必要な材料リストを取得するクエリです。
*/
SELECT
    menu_item_name,
    CAST(menu_item_health_metrics_obj:menu_item_id AS INTEGER) AS menu_item_id, -- 'AS' を使用したキャスト
    menu_item_health_metrics_obj:menu_item_health_metrics[0]:ingredients::ARRAY AS ingredients -- ダブルコロン（::）構文を使用したキャスト
FROM raw_pos.menu_staging;

/*
    半構造化データを扱う際に活用できるもう一つの強力な関数が FLATTEN です。
    FLATTEN を使用すると、JSON や配列などの半構造化データを展開し、
    指定されたオブジェクト内の要素ごとに1行を生成できます。

    これを使用して、トラックで使用されるすべてのメニューの全材料リストを取得できます。
*/
SELECT
    i.value::STRING AS ingredient_name,
    m.menu_item_health_metrics_obj:menu_item_id::INTEGER AS menu_item_id
FROM
    raw_pos.menu_staging m,
    LATERAL FLATTEN(INPUT => m.menu_item_health_metrics_obj:menu_item_health_metrics[0]:ingredients::ARRAY) i;

/*  3. ダイナミックテーブル（Dynamic Tables）
    **************************************************************
    ユーザーガイド:
    https://docs.snowflake.com/ja/user-guide/dynamic-tables-about
    **************************************************************
    
    すべての材料を構造化フォーマットで保存して、個別にクエリ・フィルタリング・
    分析できるようにしたいところです。しかし、フードトラックのフランチャイズは常に
    新しく魅力的なメニューアイテムを追加しており、その多くにはまだデータベースに
    ない独自の材料が使われています。
    
    そのため、データ変換パイプラインを簡素化するために設計された強力なツールである
    ダイナミックテーブルを使用できます。
    ダイナミックテーブルは以下の理由から今回のユースケースに最適です:
        - 宣言型の構文で作成され、データは指定されたクエリで定義されます。
        - 自動データ更新により、手動更新やカスタムスケジューリングなしに
          データが常に最新の状態に保たれます。
        - Snowflake ダイナミックテーブルによるデータ鮮度管理は、
          ダイナミックテーブル自体だけでなく、それに依存する下流のデータオブジェクトにも
          適用されます。

    これらの機能を実際に確認するために、シンプルなダイナミックテーブルパイプラインを作成し、
    ステージングテーブルに新しいメニューアイテムを追加して自動更新を実演します。

    まず、材料用のダイナミックテーブルを作成します。
*/
CREATE OR REPLACE DYNAMIC TABLE harmonized.ingredient
    LAG = '1 minute'
    WAREHOUSE = 'TB_DE_WH'
AS
    SELECT
    ingredient_name,
    menu_ids
FROM (
    SELECT DISTINCT
        i.value::STRING AS ingredient_name, -- 重複を排除した材料の値
        ARRAY_AGG(m.menu_item_id) AS menu_ids -- 材料が使用されているメニュー ID の配列
    FROM
        raw_pos.menu_staging m,
        LATERAL FLATTEN(INPUT => menu_item_health_metrics_obj:menu_item_health_metrics[0]:ingredients::ARRAY) i
    GROUP BY i.value::STRING
);

-- 材料のダイナミックテーブルが正常に作成されたことを確認しましょう
SELECT * FROM harmonized.ingredient;

/*
    サンドイッチトラック「Better Off Bread」が新しいメニューアイテム、
    バインミーサンドイッチを導入しました。
    このメニューアイテムには、フランスパン、マヨネーズ、ピクルスダイコンなど
    いくつかの新しい材料が含まれています。
    
    ダイナミックテーブルの自動更新機能により、menu_staging テーブルにこの新しいメニューアイテムを
    追加するだけで、ingredients テーブルに自動的に反映されます。
*/
INSERT INTO raw_pos.menu_staging 
SELECT 
    10101,
    15, -- トラック ID
    'Sandwiches',
    'Better Off Bread', -- トラックブランド名
    157, -- メニューアイテム ID
    'Banh Mi', -- メニューアイテム名
    'Main',
    'Cold Option',
    9.0,
    12.0,
    PARSE_JSON('{
      "menu_item_health_metrics": [
        {
          "ingredients": [
            "French Baguette",
            "Mayonnaise",
            "Pickled Daikon",
            "Cucumber",
            "Pork Belly"
          ],
          "is_dairy_free_flag": "N",
          "is_gluten_free_flag": "N",
          "is_healthy_flag": "Y",
          "is_nut_free_flag": "Y"
        }
      ],
      "menu_item_id": 157
    }'
);

/*
    French Baguette と Pickled Daikon が ingredients テーブルに表示されていることを確認します。
    「クエリが結果を返しませんでした」と表示される場合、ダイナミックテーブルがまだ更新されていません。
    ダイナミックテーブルのラグ設定に追いつくまで最大1分待ってください。
*/

SELECT * FROM harmonized.ingredient 
WHERE ingredient_name IN ('French Baguette', 'Pickled Daikon');

/* 4. ダイナミックテーブルによるシンプルなパイプライン

    次に、材料からメニューへの参照ダイナミックテーブルを作成しましょう。
    これにより、特定の材料を使用しているメニューアイテムを確認できます。
    そして、どのトラックにどの材料が何個必要かを判断できます。
    このテーブルもダイナミックテーブルなので、menu_staging テーブルに追加された
    メニューアイテムで新しい材料が使用された場合、自動的に更新されます。
*/
CREATE OR REPLACE DYNAMIC TABLE harmonized.ingredient_to_menu_lookup
    LAG = '1 minute'
    WAREHOUSE = 'TB_DE_WH'    
AS
SELECT
    i.ingredient_name,
    m.menu_item_health_metrics_obj:menu_item_id::INTEGER AS menu_item_id
FROM
    raw_pos.menu_staging m,
    LATERAL FLATTEN(INPUT => m.menu_item_health_metrics_obj:menu_item_health_metrics[0]:ingredients) f
JOIN harmonized.ingredient i ON f.value::STRING = i.ingredient_name;

-- ingredient_to_menu_lookup が正常に作成されたことを確認する
SELECT * 
FROM harmonized.ingredient_to_menu_lookup
ORDER BY menu_item_id;

/*
    次の2つのINSERTクエリを実行して、2022年1月27日にトラック #15 で
    バインミーサンドイッチ2個が注文された状況をシミュレートします。
    その後、トラックごとの材料使用量を示す別の下流ダイナミックテーブルを作成します。
*/
INSERT INTO raw_pos.order_header
SELECT 
    459520441, -- 注文 ID
    15, -- トラック ID
    1030, -- 場所 ID
    101565,
    null,
    200322900,
    TO_TIMESTAMP_NTZ('08:00:00', 'hh:mi:ss'),
    TO_TIMESTAMP_NTZ('14:00:00', 'hh:mi:ss'),
    null,
    TO_TIMESTAMP_NTZ('2022-01-27 08:21:08.000'), -- 注文タイムスタンプ
    null,
    'USD',
    14.00,
    null,
    null,
    14.00;
    
INSERT INTO raw_pos.order_detail
SELECT
    904745311, -- 注文詳細 ID
    459520441, -- 注文 ID
    157, -- メニューアイテム ID
    null,
    0,
    2, -- 注文数量
    14.00,
    28.00,
    null;

/*
    次に、米国内の個々のフードトラックごとに各材料の月間使用量を集計する
    別のダイナミックテーブルを作成します。
    これにより、材料の消費量を追跡でき、在庫の最適化、コスト管理、
    メニュー計画やサプライヤーとの関係についての意思決定に役立ちます。
    
    注文タイムスタンプから日付の部分を抽出するために使用する2つの異なる方法に注目してください:
      -> EXTRACT(<日付部分> FROM <日時>) は、指定されたタイムスタンプから指定した日付部分を
         切り出します。EXTRACT 関数で使用できる日時部分は多数あり、最も一般的なのは
         YEAR、MONTH、DAY、HOUR、MINUTE、SECOND です。
      -> MONTH(<日時>) は 1〜12 の月のインデックスを返します。YEAR(<日時>) と DAY(<日時>) も
         同様にそれぞれ年と日を返します。
*/

-- 次にテーブルを作成する
CREATE OR REPLACE DYNAMIC TABLE harmonized.ingredient_usage_by_truck 
    LAG = '2 minute'
    WAREHOUSE = 'TB_DE_WH'  
    AS 
    SELECT
        oh.truck_id,
        EXTRACT(YEAR FROM oh.order_ts) AS order_year,
        MONTH(oh.order_ts) AS order_month,
        i.ingredient_name,
        SUM(od.quantity) AS total_ingredients_used
    FROM
        raw_pos.order_detail od
        JOIN raw_pos.order_header oh ON od.order_id = oh.order_id
        JOIN harmonized.ingredient_to_menu_lookup iml ON od.menu_item_id = iml.menu_item_id
        JOIN harmonized.ingredient i ON iml.ingredient_name = i.ingredient_name
        JOIN raw_pos.location l ON l.location_id = oh.location_id
    WHERE l.country = 'United States'
    GROUP BY
        oh.truck_id,
        order_year,
        order_month,
        i.ingredient_name
    ORDER BY
        oh.truck_id,
        total_ingredients_used DESC;

/*
    新しく作成した ingredient_usage_by_truck ビューを使用して、
    2022年1月のトラック #15 の材料使用量を確認しましょう。
*/
SELECT
    truck_id,
    ingredient_name,
    SUM(total_ingredients_used) AS total_ingredients_used,
FROM
    harmonized.ingredient_usage_by_truck
WHERE
    order_month = 1 -- 月は数値 1〜12 で表されます
    AND truck_id = 15
GROUP BY truck_id, ingredient_name
ORDER BY total_ingredients_used DESC;

/*  5. 有向非巡回グラフ（DAG）によるパイプラインの可視化

    最後に、パイプラインの有向非巡回グラフ（DAG）を確認しましょう。
    DAG はデータパイプラインを可視化します。複雑なデータワークフローを視覚的に
    オーケストレーションし、タスクが正しい順序で実行されるようにするために使用できます。
    パイプライン内の各ダイナミックテーブルのラグメトリクスと設定を確認したり、
    必要に応じてテーブルを手動で更新したりすることもできます。

    DAG へのアクセス方法:
    - ナビゲーションメニューの「カタログ」ボタンをクリックしてデータベース画面を開く
    - 「TB_101」の横の矢印「>」をクリックしてデータベースを展開する
    - 「HARMONIZED」を展開し、「ダイナミックテーブル」を展開する
    - 「INGREDIENT」テーブルをクリックする
    - 「グラフ」タブをクリックする
*/

-------------------------------------------------------------------------
--RESET--
-------------------------------------------------------------------------
USE ROLE accountadmin;
-- ダイナミックテーブルをドロップする
DROP TABLE IF EXISTS raw_pos.menu_staging;
DROP TABLE IF EXISTS harmonized.ingredient;
DROP TABLE IF EXISTS harmonized.ingredient_to_menu_lookup;
DROP TABLE IF EXISTS harmonized.ingredient_usage_by_truck;

-- 挿入データを削除する
DELETE FROM raw_pos.order_detail
WHERE order_detail_id = 904745311;
DELETE FROM raw_pos.order_header
WHERE order_id = 459520441;

-- クエリタグを解除する
ALTER SESSION UNSET query_tag;
-- ウェアハウスをサスペンドする
ALTER WAREHOUSE tb_de_wh SUSPEND;
