/***************************************************************************************************       
Asset:        Zero to Snowflake - Snowflake 入門
Version:      v2     
Copyright(c): 2025 Snowflake Inc. All rights reserved.
****************************************************************************************************

Snowflake 入門
1. 仮想ウェアハウスと設定
2. クエリ結果キャッシュの活用
3. 基本的なデータ変換テクニック
4. UNDROP によるデータ復元
5. リソースモニター
6. 予算管理（Budgets）
7. ユニバーサルサーチ

****************************************************************************************************/

-- 開始前に、このクエリを実行してセッションのクエリタグを設定してください。
ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"tb_zts","version":{"major":1, "minor":1},"attributes":{"is_quickstart":1, "source":"tastybytes", "vignette": "getting_started_with_snowflake"}}';

-- ワークシートのコンテキストを設定します。データベース、スキーマ、ロールを設定します。

USE DATABASE tb_101;
USE ROLE accountadmin;

/*   1. 仮想ウェアハウスと設定
    **************************************************************
     ユーザーガイド:
     https://docs.snowflake.com/en/user-guide/warehouses-overview
    **************************************************************
    
    仮想ウェアハウスは、Snowflake データに対して分析を実行するための
    動的でスケーラブル、かつコスト効率に優れたコンピューティングリソースです。
    基盤となる技術的な詳細を意識することなく、すべてのデータ処理ニーズを
    処理することを目的としています。

    ウェアハウスのパラメータ:
      > WAREHOUSE_SIZE: 
            ウェアハウス内の1クラスターあたりの利用可能なコンピュートリソース量を指定します。
            利用可能なサイズは X-Small から 6X-Large の範囲です。
            デフォルト: 'XSmall'
      > WAREHOUSE_TYPE:
            仮想ウェアハウスのタイプを定義します。アーキテクチャと動作を決定します。
            タイプ:
                'STANDARD' - 汎用ワークロード向け
                'SNOWPARK_OPTIMIZED' - メモリ集約型ワークロード向け
            デフォルト: 'STANDARD'
      > AUTO_SUSPEND:
            ウェアハウスが自動的にサスペンドするまでの非アクティブ時間（秒）を指定します。
            デフォルト: 600秒
      > INITIALLY_SUSPENDED:
            ウェアハウスを作成直後にサスペンド状態で開始するかどうかを決定します。
            デフォルト: TRUE
      > AUTO_RESUME:
            クエリが送信されたときに、サスペンド状態のウェアハウスを自動的に再開するかどうかを決定します。
            デフォルト: TRUE

        それでは、最初のウェアハウスを作成してみましょう！
*/

-- まず、アクセス権限を持つアカウント上の既存ウェアハウスを確認しましょう
SHOW WAREHOUSES;

/*
    ウェアハウスとその属性（名前、状態（実行中またはサスペンド）、タイプ、サイズなど）の一覧が表示されます。
    
    Snowsight でもすべてのウェアハウスを表示・管理できます。ウェアハウスページにアクセスするには、
    ナビゲーションメニューの「管理」ボタンをクリックし、展開された管理カテゴリの「ウェアハウス」リンクをクリックします。
    
    ウェアハウスページには、アカウント上のウェアハウスとその属性の一覧が表示されます。
*/

-- シンプルな SQL コマンドでウェアハウスを簡単に作成できます
CREATE OR REPLACE WAREHOUSE my_wh
    COMMENT = 'TastyBytes 用マイウェアハウス'
    WAREHOUSE_TYPE = 'standard'
    WAREHOUSE_SIZE = 'xsmall'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'standard'
    AUTO_SUSPEND = 60
    INITIALLY_SUSPENDED = true
    AUTO_RESUME = false;

/*
    ウェアハウスを作成したら、このワークシートがそのウェアハウスを使用するように指定する必要があります。
    SQL コマンドまたは UI のどちらでも設定できます。
*/

-- ウェアハウスを使用する
USE WAREHOUSE my_wh;

/*
    簡単なクエリを実行してみましょう。しかし、結果ペインにエラーメッセージが表示され、
    ウェアハウス MY_WH がサスペンド中であることが通知されます。試してみてください。
*/
SELECT * FROM raw_pos.truck_details;

/*    
    クエリの実行やすべての DML 操作にはアクティブなウェアハウスが必要なため、
    データからインサイトを得るにはウェアハウスを再開する必要があります。
    
    エラーメッセージには、SQL コマンドの実行を提案するヒントも含まれています:
    'ALTER warehouse MY_WH resume'。早速実行しましょう！
*/
ALTER WAREHOUSE my_wh RESUME;

/* 
    また、再度サスペンドした場合に手動再開が不要となるよう、
    AUTO_RESUME を TRUE に設定します。
 */
ALTER WAREHOUSE my_wh SET AUTO_RESUME = TRUE;

-- ウェアハウスが起動したので、先ほどのクエリを再実行してみましょう
SELECT * FROM raw_pos.truck_details;

-- これでデータに対してクエリを実行できるようになりました

/* 
    次に、Snowflake のウェアハウスのスケーラビリティの力を体験してみましょう。
    
    Snowflake のウェアハウスはスケーラビリティと弾力性を考慮して設計されており、
    ワークロードのニーズに応じてコンピュートリソースを柔軟に増減できます。
    
    シンプルな ALTER WAREHOUSE 文でウェアハウスをオンザフライでスケールアップできます。
*/
ALTER WAREHOUSE my_wh SET warehouse_size = 'XLarge';

-- トラック別の売上を確認しましょう
SELECT
    o.truck_brand_name,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(o.price) AS total_sales
FROM analytics.orders_v o
GROUP BY o.truck_brand_name
ORDER BY total_sales DESC;

/*
    結果パネルを開いた状態で、右上のツールバーを確認してください。
    検索、カラム選択、クエリ詳細と実行時間統計の表示、カラム統計の表示、
    結果のダウンロードなどのオプションがあります。
    
    検索 - 検索語句で結果をフィルタリングする
    カラム選択 - 結果に表示するカラムを有効/無効にする
    クエリ詳細 - SQL テキスト、返された行数、クエリ ID、実行ロールと
                 ウェアハウスなど、クエリに関連する情報を表示する
    クエリ実行時間 - コンパイル、プロビジョニング、実行時間の内訳を表示する
    カラム統計 - 結果パネルのカラム分布に関連するデータを表示する
    結果のダウンロード - 結果を CSV としてエクスポート・ダウンロードする
*/

/*  2. クエリ結果キャッシュの活用
    *******************************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/user-guide/querying-persisted-results
    *******************************************************************
    
    次に進む前に、Snowflake のもう一つの強力な機能を紹介します:
    クエリ結果キャッシュです。
    
    先ほどの「トラック別売上」クエリは、XL ウェアハウスでも実行に数秒かかりました。

    同じ「トラック別売上」クエリを再実行し、クエリ実行時間ペインで合計実行時間を確認してください。
    初回実行では数秒かかったものが、次回実行ではほんの数百ミリ秒になっていることに気づくでしょう。
    これがクエリ結果キャッシュの効果です。

    クエリ履歴パネルを開き、初回と2回目の実行時間を比較してみてください。
    
    クエリ結果キャッシュの概要:
    - クエリの結果は24時間保持されますが、クエリが実行されるたびにタイマーがリセットされます。
    - 結果キャッシュを利用するにはほとんどコンピュートリソースが不要なため、
      頻繁に実行されるレポートやダッシュボード、およびクレジット消費の管理に最適です。
    - キャッシュはクラウドサービスレイヤーに存在し、個々のウェアハウスから論理的に分離されています。
      これにより、同一アカウント内のすべての仮想ウェアハウスとユーザーからグローバルにアクセス可能です。
*/

-- より小さなデータセットで作業するので、ウェアハウスをスケールダウンします
ALTER WAREHOUSE my_wh SET warehouse_size = 'XSmall';

/*  3. 基本的なデータ変換テクニック

    ウェアハウスが設定され稼働中になったので、トラックメーカーの分布を把握したいと思います。
    しかし、この情報は VARIANT データ型で年式、メーカー、モデルの情報が格納されている
    'truck_build' カラムに埋め込まれています。

    VARIANT データ型は半構造化データの一例です。OBJECT、ARRAY などあらゆるデータ型を格納できます。
    今回の場合、truck_build には year、make、model の3つの VARCHAR 値を持つ単一の OBJECT が格納されています。
    
    これら3つのプロパティをそれぞれ独立したカラムに分離することで、
    シンプルで使いやすい分析が可能になります。
*/
SELECT truck_build FROM raw_pos.truck_details;

/*  ゼロコピークローニング（Zero Copy Cloning）

    truck_build カラムのデータは常に同じフォーマットに従っています。
    'make' のデータ品質分析をより簡単に行うために、別途カラムが必要です。
    計画としては、truck テーブルの開発コピーを作成し、year、make、model の新しいカラムを追加してから、
    truck_build VARIANT オブジェクトから各プロパティを抽出してこれらの新しいカラムに格納します。
 
    Snowflake の強力なゼロコピークローニングにより、データベースオブジェクトの同一で完全に機能する
    独立したコピーを、追加のストレージスペースを一切使用せずに即座に作成できます。

    ゼロコピークローニングは Snowflake 独自のマイクロパーティションアーキテクチャを活用して、
    クローンオブジェクトと元のコピーの間でデータを共有します。
    どちらかのテーブルに変更を加えると、変更されたデータのみの新しいマイクロパーティションが作成されます。
    これらの新しいマイクロパーティションは、クローンまたは元のオブジェクトのいずれか変更した側が所有します。
    基本的に、一方のテーブルに加えられた変更は、もう一方に影響しません。
*/

-- truck テーブルのゼロコピークローンとして truck_dev テーブルを作成する
CREATE OR REPLACE TABLE raw_pos.truck_dev CLONE raw_pos.truck_details;

-- truck テーブルのクローンが truck_dev に正常に作成されたことを確認する
SELECT TOP 15 * 
FROM raw_pos.truck_dev
ORDER BY truck_id;

/*
    truck テーブルの開発コピーが作成できたので、新しいカラムを追加します。
    注: 3つの文をまとめて実行するには、それらを選択して画面右上の青い「実行」ボタンをクリックするか、
    キーボードを使用してください。
    
        Mac: command + return
        Windows: Ctrl + Enter
*/

ALTER TABLE raw_pos.truck_dev ADD COLUMN IF NOT EXISTS year NUMBER;
ALTER TABLE raw_pos.truck_dev ADD COLUMN IF NOT EXISTS make VARCHAR(255);
ALTER TABLE raw_pos.truck_dev ADD COLUMN IF NOT EXISTS model VARCHAR(255);

/*
    次に、truck_build カラムから抽出したデータで新しいカラムを更新します。
    コロン（:）演算子を使用して truck_build カラム内の各キーの値にアクセスし、
    その値をそれぞれのカラムに設定します。
*/
UPDATE raw_pos.truck_dev
SET 
    year = truck_build:year::NUMBER,
    make = truck_build:make::VARCHAR,
    model = truck_build:model::VARCHAR;

-- 3つのカラムが正常に追加され、truck_build から抽出されたデータが格納されたことを確認する
SELECT year, make, model FROM raw_pos.truck_dev;

-- 異なるメーカーの数を集計して、TastyBytes フードトラック車両の分布を把握しましょう
SELECT 
    make,
    COUNT(*) AS count
FROM raw_pos.truck_dev
GROUP BY make
ORDER BY make ASC;

/*
    上記クエリを実行すると、データセットに問題があることに気づきます。
    一部のトラックのメーカーが 'Ford' で、別の一部が 'Ford_' となっており、
    同じトラックメーカーに対して2つの異なるカウントが生じています。
*/

-- まず UPDATE を使用して 'Ford_' の出現箇所を 'Ford' に変更します
UPDATE raw_pos.truck_dev
    SET make = 'Ford'
    WHERE make = 'Ford_';

-- make カラムが正常に更新されたことを確認する
SELECT truck_id, make 
FROM raw_pos.truck_dev
ORDER BY truck_id;

/*
    make カラムが正常に修正されました。次に truck テーブルと truck_dev テーブルを SWAP します。
    このコマンドは2つのテーブル間でメタデータとデータをアトミックに交換し、
    truck_dev テーブルを新しい本番 truck テーブルとして即座に昇格させます。
*/
ALTER TABLE raw_pos.truck_details SWAP WITH raw_pos.truck_dev; 

-- 先ほどのクエリを再実行して正確なメーカー数を確認する
SELECT 
    make,
    COUNT(*) AS count
FROM raw_pos.truck_details
GROUP BY
    make
ORDER BY count DESC;

/*
    変更が正しく反映されています。まずデータを3つの別々のカラムに分割したので、
    truck_build カラムを本番データベースから削除してクリーンアップを行います。
    その後、truck_dev テーブルも不要になったのでドロップします。
*/

-- シンプルな ALTER TABLE ... DROP COLUMN コマンドで古い truck_build カラムを削除できます
ALTER TABLE raw_pos.truck_details DROP COLUMN truck_build;

-- truck_dev テーブルをドロップする
DROP TABLE raw_pos.truck_details;

/*  4. UNDROP によるデータ復元
  
    大変です！誤って本番の truck テーブルをドロップしてしまいました。😱

    幸い、UNDROP コマンドを使用してテーブルをドロップ前の状態に復元できます。
    UNDROP は Snowflake の強力なタイムトラベル機能の一部であり、
    設定されたデータ保持期間（デフォルト24時間）内にドロップされたデータベースオブジェクトを
    復元することができます。

    UNDROP を使って本番の 'truck' テーブルをすぐに復元しましょう！
*/

-- オプション: 'truck' テーブルが存在しないことを確認するクエリを実行します
    -- 注: 'Table TRUCK does not exist or not authorized.' というエラーは、テーブルがドロップされたことを意味します。
DESCRIBE TABLE raw_pos.truck_details;

-- 本番の 'truck' テーブルに UNDROP を実行して、ドロップ前の状態に復元する
UNDROP TABLE raw_pos.truck_details;

-- テーブルが正常に復元されたことを確認する
SELECT * from raw_pos.truck_details;

-- 今度は本物の truck_dev テーブルをドロップする
DROP TABLE raw_pos.truck_dev;

/*  5. リソースモニター
    ***********************************************************
    ユーザーガイド:                                   
    https://docs.snowflake.com/en/user-guide/resource-monitors
    ***********************************************************

    コンピュートの使用量と支出の監視は、クラウドベースのワークフローにとって重要です。
    Snowflake はリソースモニターを使用してウェアハウスのクレジット使用量を
    シンプルかつわかりやすく追跡する方法を提供しています。

    リソースモニターでは、クレジットクォータを定義し、定義した使用量のしきい値に達した際に
    関連するウェアハウスに対して特定のアクションをトリガーできます。

    リソースモニターが実行できるアクション:
    - NOTIFY: 指定したユーザーまたはロールにメール通知を送信します。
    - SUSPEND: しきい値に達した際に関連ウェアハウスをサスペンドします。
               注: 実行中のクエリは完了が許可されます。
    - SUSPEND_IMMEDIATE: しきい値に達した際に関連ウェアハウスをサスペンドし、
                         実行中のすべてのクエリをキャンセルします。

    それでは、ウェアハウス my_wh 用のリソースモニターを作成しましょう。

    まず Snowsight でアカウントレベルのロールを accountadmin に設定しましょう。
    手順:
    - 画面左下のユーザーアイコンをクリックする
    - 「ロールの切り替え」にカーソルを合わせる
    - ロールリストパネルから 'ACCOUNTADMIN' を選択する

    次に、ワークシートで accountadmin ロールを使用します。
*/
USE ROLE accountadmin;

-- SQL を使用してリソースモニターを作成するには、以下のクエリを実行します
CREATE OR REPLACE RESOURCE MONITOR my_resource_monitor
    WITH CREDIT_QUOTA = 100
    FREQUENCY = MONTHLY -- DAILY、WEEKLY、YEARLY、または NEVER（1回限りのクォータ）も指定可能
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 75 PERCENT DO NOTIFY
             ON 90 PERCENT DO SUSPEND
             ON 100 PERCENT DO SUSPEND_IMMEDIATE;

-- リソースモニターを作成したら、my_wh に適用します
ALTER WAREHOUSE my_wh 
    SET RESOURCE_MONITOR = my_resource_monitor;

/*  6. 予算管理（Budgets）
    ****************************************************
      ユーザーガイド:                                   
      https://docs.snowflake.com/en/user-guide/budgets 
    ****************************************************
      
    前のステップでは、ウェアハウスのクレジット使用量を監視するリソースモニターを設定しました。
    このステップでは、Snowflake のコスト管理をより包括的かつ柔軟に行うための
    予算管理（Budget）を作成します。
    
    リソースモニターがウェアハウスとコンピュートの使用量に特化しているのに対して、
    予算管理はあらゆる Snowflake オブジェクトやサービスのコストを追跡・制限し、
    ドル金額が指定されたしきい値に達した際にユーザーへ通知するために使用できます。
*/

-- まず予算を作成しましょう
CREATE OR REPLACE SNOWFLAKE.CORE.BUDGET my_budget()
    COMMENT = 'Tasty Bytes 用マイ予算';

/*
    予算を設定する前に、アカウントのメールアドレスを確認する必要があります。

    メールアドレスを確認するには:
    - 画面左下のユーザーアイコンをクリックする
    - 「設定」をクリックする
    - メールフィールドにメールアドレスを入力する
    - 「保存」をクリックする
    - メールを確認し、指示に従ってメールアドレスを確認する
        注: 数分経ってもメールが届かない場合は、「確認メールを再送信」をクリックしてください
     
    新しい予算の設定、メールの確認、アカウントレベルのロールを accountadmin に設定したら、
    Snowsight の予算ページに移動してリソースを予算に追加しましょう。

    Snowsight の予算ページへのアクセス方法:
    - ナビゲーションメニューの「管理」ボタンをクリックする
    - 最初の項目「コスト管理」をクリックする
    - 「予算」タブをクリックする
    
    ウェアハウスの選択を求められた場合は tb_dev_wh を選択します。
    または、画面右上のウェアハウスパネルでウェアハウスが tb_dev_wh に設定されていることを確認します。
    
    予算ページには現在の期間の支出に関する指標が表示されます。
    画面中央には予測支出とともに現在の支出のグラフが表示されます。
    画面下部には先ほど作成した 'MY_BUDGET' 予算が表示されます。
    クリックして予算ページを表示します。
    
    右上の「<- 予算の詳細」をクリックすると、予算の詳細パネルが表示されます。
    予算と関連するすべてのリソースに関する情報を確認できます。
    現在は監視中のリソースがないので、追加しましょう。
    「編集」ボタンをクリックして予算の編集パネルを開きます。
    
    - 予算名はそのまま
    - 支出限度額を 100 に設定する
    - 先ほど確認したメールアドレスを入力する
    - 「+ タグ & リソース」ボタンをクリックしてリソースを追加する
    - 「データベース」→「TB_101」を展開し、「ANALYTICS」スキーマのチェックボックスをオンにする
    - 下にスクロールして「ウェアハウス」を展開する
    - 「TB_DE_WH」のチェックボックスをオンにする
    - 「完了」をクリックする
    - 予算の編集メニューに戻り、「変更を保存」をクリックする
*/

/*  7. ユニバーサルサーチ
    **************************************************************************
      ユーザーガイド                                                             
      https://docs.snowflake.com/en/user-guide/ui-snowsight-universal-search  
    **************************************************************************

    ユニバーサルサーチを使用すると、アカウント内の任意のオブジェクトを簡単に検索できるほか、
    マーケットプレイスのデータ製品、関連する Snowflake ドキュメント、
    コミュニティナレッジベースの記事も探索できます。

    試してみましょう。
    - ユニバーサルサーチを使用するには、ナビゲーションメニューの「検索」をクリックします
    - ユニバーサルサーチの UI が表示されます。最初の検索語句を入力しましょう。
    - 検索バーに 'truck' と入力し、結果を観察します。上部のセクションには、
      データベース、テーブル、ビュー、ステージなど、アカウント上の関連オブジェクトのカテゴリが表示されます。
      データベースオブジェクトの下には、関連するマーケットプレイスのリストとドキュメントのセクションが表示されます。

    - 自然言語で探しているものを説明する検索語句も使用できます。
      どのトラックフランチャイズにリピーター顧客が最も多いかを調べたい場合、
      'どのトラックフランチャイズが最も忠実な顧客基盤を持っていますか？' のように検索できます。
      「テーブルとビュー」セクションの「すべてを表示 >」ボタンをクリックすると、
      クエリに関連するすべてのテーブルとビューを表示できます。

    ユニバーサルサーチは異なるスキーマから複数のテーブルとビューを返します。
    各オブジェクトに対して関連するカラムも一覧表示されていることに注目してください。
    これらはすべて、リピーター顧客についてのデータドリブンな回答を得るための
    優れた出発点となります。
*/

-------------------------------------------------------------------------
--RESET--
-------------------------------------------------------------------------
-- 作成したオブジェクトをドロップする
DROP RESOURCE MONITOR IF EXISTS my_resource_monitor;
DROP TABLE IF EXISTS raw_pos.truck_dev;

-- truck_details をリセットする
CREATE OR REPLACE TABLE raw_pos.truck_details
AS 
SELECT * EXCLUDE (year, make, model)
FROM raw_pos.truck;

DROP WAREHOUSE IF EXISTS my_wh;
-- クエリタグを解除する
ALTER SESSION UNSET query_tag;
