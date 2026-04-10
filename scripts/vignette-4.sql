/***************************************************************************************************       
Asset:        Zero to Snowflake - Horizon によるガバナンス
Version:      v2     
Copyright(c): 2025 Snowflake Inc. All rights reserved.
****************************************************************************************************

Horizon によるガバナンス
1. ロールとアクセス制御の概要
2. 自動タグ付けによるタグベースの分類
3. マスキングポリシーによるカラムレベルのセキュリティ
4. 行アクセスポリシーによる行レベルのセキュリティ
5. データメトリック関数によるデータ品質モニタリング
6. Trust Center によるアカウントセキュリティ監視

****************************************************************************************************/

-- セッションのクエリタグを設定する
ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"tb_zts","version":{"major":1, "minor":1},"attributes":{"is_quickstart":1, "source":"tastybytes", "vignette": "governance_with_horizon"}}';

-- まず、ワークシートのコンテキストを設定します
USE ROLE useradmin;
USE DATABASE tb_101;
USE WAREHOUSE tb_dev_wh;

/*  1. ロールとアクセス制御の概要
    *************************************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/user-guide/security-access-control-overview
    *************************************************************************
    
    Snowflake のアクセス制御フレームワークは以下に基づいています:
      - ロールベースのアクセス制御（RBAC）: アクセス権限はロールに割り当てられ、
        そのロールがユーザーに割り当てられます。
      - 裁量的アクセス制御（DAC）: 各オブジェクトにはオーナーがおり、
        オーナーはそのオブジェクトへのアクセスを許可できます。
    
    Snowflake のアクセス制御を理解するための主要な概念:
      - セキュリティ保護可能なオブジェクト: 誰が使用または参照できるかを制御できるすべてのもの。
        明示的に権限が与えられていない限り、アクセスできません。
        これらのオブジェクトは個人ではなくグループ（ロール）によって管理されます。
        データベース、テーブル、関数などはすべてセキュリティ保護可能なオブジェクトです。
      - ロール: 付与できる権限のセットのようなもの。
        個々のユーザーや他のロールにも付与でき、権限の連鎖を作成します。
      - 権限（Privilege）: オブジェクトに対して何かを行うための特定のアクセス許可。
        多くの小さな権限を組み合わせて、誰がどれだけのアクセスを持つかを正確に制御できます。
      - ユーザー: Snowflake が認識する単なるアイデンティティ（ユーザー名など）。
        実際の人やコンピュータープログラムが対象となります。
    
      Snowflake システム定義ロールの定義:
       - ORGADMIN: 組織レベルでの操作を管理するロール。
       - ACCOUNTADMIN: システムの最上位ロールであり、アカウント内の限られた/制御された
          数のユーザーにのみ付与する必要があります。
       - SECURITYADMIN: グローバルに任意のオブジェクト権限を管理し、ユーザーとロールを作成・
          監視・管理できるロール。
       - USERADMIN: ユーザーとロールの管理専用のロール。
       - SYSADMIN: アカウント内でウェアハウスとデータベースを作成する権限を持つロール。
       - PUBLIC: すべてのユーザーとロールに自動的に付与される擬似ロール。
          セキュリティ保護可能なオブジェクトを所有でき、所有するものはすべて
          アカウント内の他のすべてのユーザーとロールが利用できます。

                                +---------------+
                                | ACCOUNTADMIN  |
                                +---------------+
                                  ^    ^     ^
                                  |    |     |
                    +-------------+-+  |    ++-------------+
                    | SECURITYADMIN |  |    |   SYSADMIN   |<------------+
                    +---------------+  |    +--------------+             |
                            ^          |     ^        ^                  |
                            |          |     |        |                  |
                    +-------+-------+  |     |  +-----+-------+  +-------+-----+
                    |   USERADMIN   |  |     |  | CUSTOM ROLE |  | CUSTOM ROLE |
                    +---------------+  |     |  +-------------+  +-------------+
                            ^          |     |      ^              ^      ^
                            |          |     |      |              |      |
                            |          |     |      |              |    +-+-----------+
                            |          |     |      |              |    | CUSTOM ROLE |
                            |          |     |      |              |    +-------------+
                            |          |     |      |              |           ^
                            |          |     |      |              |           |
                            +----------+-----+---+--+--------------+-----------+
                                                 |
                                            +----+-----+
                                            |  PUBLIC  |
                                            +----------+

    このセクションでは、カスタムのデータスチュワードロールを作成し、権限を関連付ける方法を説明します。
*/
-- まず、アカウントに既に存在するロールを確認しましょう。
SHOW ROLES;

-- 次に、データスチュワードロールを作成します。
CREATE OR REPLACE ROLE tb_data_steward
    COMMENT = 'カスタムロール';
-- ロールを作成したら、SECURITYADMIN ロールに切り替えて新しいロールに権限を付与できます。

/*
    新しいロールを作成したら、クエリを実行するためにウェアハウスを使用できるようにする必要があります。
    次に進む前に、ウェアハウス権限についてより深く理解しましょう。
     
    - MODIFY: サイズの変更を含む、ウェアハウスのすべてのプロパティを変更する権限を有効にします。
    - MONITOR: ウェアハウスで実行された現在および過去のクエリ、
       ウェアハウスの使用統計を表示する権限を有効にします。
    - OPERATE: ウェアハウスの状態変更（停止、開始、サスペンド、再開）を可能にします。
       また、ウェアハウスで実行された現在および過去のクエリの表示と、
       実行中のクエリの中止も可能にします。
    - USAGE: 仮想ウェアハウスを使用し、その結果としてウェアハウスでクエリを実行する権限を有効にします。
       SQL 文が送信されたときにウェアハウスが自動再開するよう設定されている場合、
       ウェアハウスは自動的に再開して文を実行します。
    - ALL: OWNERSHIP を除くすべての権限をウェアハウスに付与します。

      ウェアハウス権限を理解したので、新しいロールに operate と usage の権限を付与できます。
      まず、SECURITYADMIN ロールに切り替えます。
*/
USE ROLE securityadmin;
-- まず、ロールにウェアハウス tb_dev_wh を使用する権限を付与します
GRANT OPERATE, USAGE ON WAREHOUSE tb_dev_wh TO ROLE tb_data_steward;

/*
     次に、Snowflake のデータベースとスキーマの権限について理解しましょう:
      - MODIFY: データベース設定を変更する権限を有効にします。
      - MONITOR: DESCRIBE コマンドの実行を有効にします。
      - USAGE: データベースを使用する権限（SHOW DATABASES コマンドの出力にデータベースの詳細を
         返すことを含む）を有効にします。データベース内のオブジェクトを表示したり
         アクションを実行するには追加の権限が必要です。
      - ALL: OWNERSHIP を除くすべての権限をデータベースに付与します。
*/

GRANT USAGE ON DATABASE tb_101 TO ROLE tb_data_steward;
GRANT USAGE ON ALL SCHEMAS IN DATABASE tb_101 TO ROLE tb_data_steward;

/*
    Snowflake のテーブルとビュー内のデータへのアクセスは以下の権限で管理されます:
        SELECT: データを取得する権限を付与します。
        INSERT: 新しい行の追加を許可します。
        UPDATE: 既存の行の変更を許可します。
        DELETE: 行の削除を許可します。
        TRUNCATE: テーブル内のすべての行の削除を許可します。

      次に、raw_customer スキーマ内のテーブルで SELECT クエリを実行できるようにします。
*/

-- RAW_CUSTOMER スキーマ内のすべてのテーブルに SELECT 権限を付与する
GRANT SELECT ON ALL TABLES IN SCHEMA raw_customer TO ROLE tb_data_steward;
-- governance スキーマとそのすべてのテーブルに ALL 権限を付与する
GRANT ALL ON SCHEMA governance TO ROLE tb_data_steward;
GRANT ALL ON ALL TABLES IN SCHEMA governance TO ROLE tb_data_steward;

/*
    新しいロールを使用するには、現在のユーザーにもロールを付与する必要があります。
    次の2つのクエリを実行して、現在のユーザーに新しいデータスチュワードロールを
    使用する権限を付与します。
*/
SET my_user = CURRENT_USER();
GRANT ROLE tb_data_steward TO USER IDENTIFIER($my_user);

/*
    最後に、以下のクエリを実行して新しく作成したロールを使用します！
    --> または、ワークシート UI の「ロールとウェアハウスを選択」ボタンをクリックし、
        'tb_data_steward' を選択してロールを切り替えることもできます。
*/
USE ROLE tb_data_steward;

-- 記念に、これから扱うデータの種類を確認してみましょう。
SELECT TOP 100 * FROM raw_customer.customer_loyalty;

/*
    顧客ロイヤルティデータが表示されています。
    しかし、よく見るとこのテーブルには個人識別情報（PII）が大量に含まれていることが明らかです。
    次のセクションでは、これを軽減する方法をさらに詳しく説明します。
*/

/*  2. 自動タグ付けによるタグベースの分類
    ******************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/user-guide/classify-auto
    ******************************************************

    前のクエリで、顧客ロイヤルティテーブルに相当量の個人識別情報（PII）が
    保存されていることに気づきました。Snowflake の自動タグ付け機能を
    タグベースのマスキングと組み合わせることで、クエリ結果の機密データを難読化できます。

    Snowflake は、データベーススキーマ内のカラムを継続的に監視することで、
    機密情報を自動的に検出してタグ付けできます。
    データエンジニアがスキーマに分類プロファイルを割り当てると、
    そのスキーマのテーブル内のすべての機密データがプロファイルのスケジュールに基づいて
    自動的に分類されます。
    
    次に、分類プロファイルを作成し、カラムのセマンティックカテゴリに基づいて
    自動的に割り当てられるタグを指定します。まず accountadmin ロールに切り替えましょう。
*/
USE ROLE accountadmin;

/*
    governance スキーマを作成し、その中に PII 用タグを作成してから、
    データベースオブジェクトにタグを適用する権限を新しいロールに付与します。
*/
CREATE OR REPLACE TAG governance.pii;
GRANT APPLY TAG ON ACCOUNT TO ROLE tb_data_steward;

/*
    まず、tb_data_steward ロールに raw_customer スキーマでのデータ分類を実行し、
    分類プロファイルを作成するための適切な権限を付与する必要があります。
*/
GRANT EXECUTE AUTO CLASSIFICATION ON SCHEMA raw_customer TO ROLE tb_data_steward;
GRANT DATABASE ROLE SNOWFLAKE.CLASSIFICATION_ADMIN TO ROLE tb_data_steward;
GRANT CREATE SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE ON SCHEMA governance TO ROLE tb_data_steward;

-- データスチュワードロールに戻る。
USE ROLE tb_data_steward;

/*
    分類プロファイルを作成します。スキーマに追加されたオブジェクトは即座に分類され、
    30日間有効で自動的にタグ付けされます。
*/
CREATE OR REPLACE SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE
  governance.tb_classification_profile(
    {
      'minimum_object_age_for_classification_days': 0,
      'maximum_classification_validity_days': 30,
      'auto_tag': true
    });

/*
    指定されたセマンティックカテゴリに基づいてカラムを自動タグ付けするタグマップを作成します。
    つまり、semantic_categories 配列内のいずれかの値で分類されたカラムには、
    自動的に PII タグが付与されます。
*/
CALL governance.tb_classification_profile!SET_TAG_MAP(
  {'column_tag_map':[
    {
      'tag_name':'tb_101.governance.pii',
      'tag_value':'pii',
      'semantic_categories':['NAME', 'PHONE_NUMBER', 'POSTAL_CODE', 'DATE_OF_BIRTH', 'CITY', 'EMAIL']
    }]});

-- SYSTEM$CLASSIFY を呼び出して、分類プロファイルで customer_loyalty テーブルを自動分類する。
CALL SYSTEM$CLASSIFY('tb_101.raw_customer.customer_loyalty', 'tb_101.governance.tb_classification_profile');

/*
    次のクエリを実行して、自動分類とタグ付けの結果を確認します。
    すべての Snowflake アカウントで利用可能な、自動生成された INFORMATION_SCHEMA から
    メタデータを取得します。各カラムのタグ付け方法と、先ほど作成した分類プロファイルとの
    関連性を確認してください。
    
    すべてのカラムは PRIVACY_CATEGORY と SEMANTIC_CATEGORY タグでそれぞれタグ付けされており、
    それぞれ異なる目的を持っています。
    PRIVACY_CATEGORY はカラム内の個人データの機密性レベルを示し、
    SEMANTIC_CATEGORY はデータが表す実際の概念を説明します。
    
    最後に、分類タグマップ配列で指定したセマンティックカテゴリでタグ付けされたカラムが、
    カスタムの 'PII' タグでタグ付けされていることに注目してください。
*/
SELECT 
    column_name,
    tag_database,
    tag_schema,
    tag_name,
    tag_value,
    apply_method
FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS('raw_customer.customer_loyalty', 'table'));

/*  3. マスキングポリシーによるカラムレベルのセキュリティ
    **************************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/user-guide/security-column-intro
    **************************************************************

    Snowflake のカラムレベルセキュリティでは、マスキングポリシーを使用して
    カラムのデータを保護できます。主に2つの機能があります:
    動的データマスキング（クエリ時に機密データを隠したり変換する）と
    外部トークン化（Snowflake に入る前にデータをトークン化し、クエリ時にデトークン化する）です。

    PII として機密カラムにタグ付けしたので、そのタグに関連付ける
    マスキングポリシーを2つ作成します。1つ目は氏名、メール、電話番号などの
    機密文字列データ用で、2つ目は誕生日などの機密 DATE 値用です。

    どちらのマスキングロジックも似ています: 現在のロールが PII タグ付きカラムをクエリする際に、
    アカウント管理者または TastyBytes 管理者でない場合、文字列値は 'MASKED' と表示されます。
    日付値は元の年のみが表示され、月日は 01-01 として表示されます。
*/

-- 機密文字列データ用マスキングポリシーを作成する
CREATE OR REPLACE MASKING POLICY governance.mask_string_pii AS (original_value STRING)
RETURNS STRING ->
  CASE WHEN
    -- 現在のロールが特権ロールのいずれでもない場合、カラムをマスクする。
    CURRENT_ROLE() NOT IN ('ACCOUNTADMIN', 'TB_ADMIN')
    THEN '****MASKED****'
    -- それ以外（タグが機密でないか、ロールが特権を持つ場合）は元の値を表示する。
    ELSE original_value
  END;

-- 機密 DATE データ用マスキングポリシーを作成する
CREATE OR REPLACE MASKING POLICY governance.mask_date_pii AS (original_value DATE)
RETURNS DATE ->
  CASE WHEN
    CURRENT_ROLE() NOT IN ('ACCOUNTADMIN', 'TB_ADMIN')
    THEN DATE_TRUNC('year', original_value) -- マスク時は年のみ変更されず、月日は 01-01 となる
    ELSE original_value
  END;

-- 自動的に customer_loyalty テーブルに適用されたタグに両方のマスキングポリシーを添付する
ALTER TAG governance.pii SET
    MASKING POLICY governance.mask_string_pii,
    MASKING POLICY governance.mask_date_pii;

/*
    public ロールに切り替え、customer_loyalty テーブルの最初の100行をクエリして、
    マスキングポリシーが機密データをどのように難読化するかを確認します。
*/
USE ROLE public;
SELECT TOP 100 * FROM raw_customer.customer_loyalty;

-- TB_ADMIN ロールに切り替えて、管理者ロールにはマスキングポリシーが適用されないことを確認する
USE ROLE tb_admin;
SELECT TOP 100 * FROM raw_customer.customer_loyalty;

/*  4. 行アクセスポリシーによる行レベルのセキュリティ
    ***********************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/user-guide/security-row-intro
    ***********************************************************

    Snowflake は行アクセスポリシーを使用した行レベルセキュリティをサポートしており、
    クエリ結果に返される行を決定します。ポリシーはテーブルに添付され、
    定義したルールに対して各行を評価することで機能します。
    これらのルールは多くの場合、クエリを実行するユーザーの現在のロールなど、
    ユーザーの属性を使用します。

    例えば、行アクセスポリシーを使用して、米国のユーザーには
    米国内の顧客のデータのみが表示されるようにできます。

    まず、データスチュワードロールに切り替えましょう。
*/
USE ROLE tb_data_steward;

-- 行アクセスポリシーを作成する前に、行ポリシーマップを作成します。
CREATE OR REPLACE TABLE governance.row_policy_map
    (role STRING, country_permission STRING);

/*
    行ポリシーマップは、ロールと許可されたアクセス行の値を関連付けます。
    例えば、ロール tb_data_engineer を国の値 'United States' に関連付けると、
    tb_data_engineer は country の値が 'United States' の行のみを参照できます。
*/
INSERT INTO governance.row_policy_map
    VALUES('tb_data_engineer', 'United States');

/*
    行ポリシーマップを作成したら、行アクセスポリシーを作成します。
    
    このポリシーでは、管理者は制限なしに行にアクセスできますが、
    ポリシーマップ内の他のロールは関連する国にマッチする行のみを参照できます。
*/
CREATE OR REPLACE ROW ACCESS POLICY governance.customer_loyalty_policy
    AS (country STRING) RETURNS BOOLEAN ->
        CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN') 
        OR EXISTS 
            (
            SELECT 1
                FROM governance.row_policy_map rp
            WHERE
                UPPER(rp.role) = CURRENT_ROLE()
                AND rp.country_permission = country
            );

-- customer_loyalty テーブルの 'country' カラムに行アクセスポリシーを適用する。
ALTER TABLE raw_customer.customer_loyalty
    ADD ROW ACCESS POLICY governance.customer_loyalty_policy ON (country);

/*
    行ポリシーマップで 'United States' に関連付けたロールに切り替え、
    行アクセスポリシーが設定されたテーブルをクエリした結果を確認します。
*/
USE ROLE tb_data_engineer;

-- 米国の顧客のみが表示されるはずです。
SELECT TOP 100 * FROM raw_customer.customer_loyalty;

/*
    おつかれさまでした！Snowflake のカラムおよび行レベルのセキュリティ戦略を使用して
    データを管理・保護する方法についての理解が深まりました。
    個人識別情報を含むカラムを保護するためにマスキングポリシーと組み合わせてタグを作成する方法と、
    ロールが特定のカラム値のデータのみにアクセスできるようにする行アクセスポリシーを学びました。
*/

/*  5. データメトリック関数によるデータ品質モニタリング
    ***********************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/user-guide/data-quality-intro
    ***********************************************************

    Snowflake は、データメトリック関数（DMF）を使用してデータの整合性と信頼性を維持します。
    これはプラットフォーム内で品質チェックを自動化する強力な機能です。
    任意のテーブルやビューにこれらのチェックをスケジュールすることで、
    データの整合性を明確に把握でき、より信頼性の高いデータドリブンな意思決定が可能になります。
    
    Snowflake は即時使用できる組み込みシステム DMF と、
    独自のビジネスロジック向けのカスタム DMF を柔軟に作成する機能の両方を提供し、
    包括的な品質モニタリングを実現します。

    システム DMF のいくつかを見てみましょう！
*/

-- DMF の使用を開始するために TastyBytes データスチュワードロールに切り替える
USE ROLE tb_data_steward;

-- order_header テーブルから null の customer_id のパーセンテージを返します。
SELECT SNOWFLAKE.CORE.NULL_PERCENT(SELECT customer_id FROM raw_pos.order_header);

-- DUPLICATE_COUNT を使用して重複した注文 ID を確認できます。
SELECT SNOWFLAKE.CORE.DUPLICATE_COUNT(SELECT order_id FROM raw_pos.order_header); 

-- すべての注文の平均注文合計金額
SELECT SNOWFLAKE.CORE.AVG(SELECT order_total FROM raw_pos.order_header);

/*
    独自のカスタムデータメトリック関数を作成して、特定のビジネスルールに従って
    データ品質を監視することもできます。
    注文合計が単価と数量の積と等しくない注文をチェックする
    カスタム DMF を作成します。
*/

-- カスタムデータメトリック関数を作成する
CREATE OR REPLACE DATA METRIC FUNCTION governance.invalid_order_total_count(
    order_prices_t table(
        order_total NUMBER,
        unit_price NUMBER,
        quantity INTEGER
    )
)
RETURNS NUMBER
AS
'SELECT COUNT(*)
 FROM order_prices_t
 WHERE order_total != unit_price * quantity';

-- 合計が単価 × 数量と等しくない新しい注文をシミュレートする
INSERT INTO raw_pos.order_detail
SELECT
    904745311,
    459520442,
    52,
    null,
    0,
    2, -- 数量
    5.0, -- 単価
    5.0, -- 合計金額（意図的に不正確）
    null;

-- カスタム DMF を order_detail テーブルに対して呼び出す。
SELECT governance.invalid_order_total_count(
    SELECT 
        price, 
        unit_price, 
        quantity 
    FROM raw_pos.order_detail
) AS num_orders_with_incorrect_price;

-- order_detail テーブルのデータメトリックスケジュールを変更時にトリガーするよう設定する
ALTER TABLE raw_pos.order_detail
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

-- カスタム DMF をテーブルに割り当てる
ALTER TABLE raw_pos.order_detail
    ADD DATA METRIC FUNCTION governance.invalid_order_total_count
    ON (price, unit_price, quantity);

/*  6. Trust Center によるアカウントセキュリティ監視
    **************************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/user-guide/trust-center/overview
    **************************************************************

    Trust Center は、スキャナーを使用してアカウントのセキュリティリスクを自動的に評価・監視します。
    スキャナーは、アカウントのセキュリティリスクや違反を確認し、
    その結果に基づいて推奨アクションを提供するスケジュールされたバックグラウンドプロセスです。
    多くの場合、スキャナーパッケージにグループ化されています。
    
    Trust Center の一般的なユースケース:
        - ユーザーの多要素認証（MFA）が有効になっているかの確認
        - 過剰な権限を持つロールの検出
        - 少なくとも90日間ログインしていない非アクティブユーザーの検出
        - リスクのあるユーザーの検出と軽減

    開始前に、Trust Center の管理者になるために必要な権限を管理者ロールに付与します。
*/
USE ROLE accountadmin;
GRANT APPLICATION ROLE SNOWFLAKE.TRUST_CENTER_ADMIN TO ROLE tb_admin;
USE ROLE tb_admin; -- TastyBytes 管理者ロールに切り替える

/*
    ナビゲーションメニューで「ガバナンス & セキュリティ」にカーソルを合わせ、
    「Trust Center」をクリックします。必要に応じて別のブラウザタブで開くこともできます。
    Trust Center を初めて読み込むと、いくつかのペインとセクションが表示されます:
        1. タブ: 検出事項、スキャナーパッケージ
        2. パスワード準備状況ペイン
        3. セキュリティ違反のオープン数
        4. フィルター付き違反リスト

    CIS ベンチマークスキャナーパッケージを有効にするよう促すメッセージが
    タブの下に表示される場合があります。次にそれを行います。

    「スキャナーパッケージ」タブをクリックします。スキャナーパッケージのリストが表示されます。
    これらはスキャナー（アカウントのセキュリティリスクを確認するスケジュールされた
    バックグラウンドプロセス）のグループです。各スキャナーパッケージには名前、
    プロバイダー、アクティブ/非アクティブなスキャナーの数、ステータスが表示されます。
    Security Essentials スキャナーパッケージ以外のすべてのスキャナーパッケージは
    デフォルトで無効になっています。
    
    「CIS ベンチマーク」をクリックして、スキャナーパッケージの詳細を確認します。
    スキャナーパッケージの名前と説明、およびパッケージを有効にするオプションが表示されます。
    その下にはスキャナーパッケージ内のスキャナーのリストがあります。
    各スキャナーをクリックすると、スケジュール、最終実行日時と曜日、説明など
    詳細情報を確認できます。

    「パッケージを有効にする」ボタンをクリックして有効化しましょう。
    「スキャナーパッケージを有効にする」モーダルが表示され、
    スキャナーパッケージのスケジュールを設定できます。
    このパッケージを月次スケジュールで実行するよう設定しましょう。

    「頻度」のドロップダウンをクリックして「毎月」を選択します。他の値はそのままにします。
    パッケージは有効化時と設定されたスケジュールで自動的に実行されることに注意してください。
    
    オプションで通知設定を構成できます。最小重大度トリガーレベルが「クリティカル」で、
    受信者として「管理者ユーザー」が選択されているデフォルト値のままにできます。
    「続行」をクリックします。
    スキャナーパッケージが完全に有効化されるまで少し時間がかかる場合があります。

    アカウントの「Threat Intelligence」スキャナーパッケージについても同じことを繰り返します。
    前のスキャナーパッケージと同じ設定を使用します。
    
    両方のパッケージが有効化されたら、「検出事項」タブに戻り、
    スキャナーパッケージが発見した違反を確認します。

    重大度レベルごとの違反数のグラフとともに、違反リストにはるかに多くのエントリが
    表示されるはずです。違反リストには、短い説明、重大度、スキャナーパッケージなど、
    すべての違反に関する詳細情報が表示されます。違反を解決済みとしてマークするオプションもあります。
    また、個々の違反をクリックすると、サマリーや修復オプションなど
    より詳細な情報を含む詳細ペインが表示されます。

    違反リストは、ドロップダウンオプションを使用してステータス、重大度、スキャナーパッケージで
    フィルタリングできます。違反グラフの重大度カテゴリをクリックすると、
    そのタイプのフィルターも適用されます。
    
    現在アクティブなフィルターカテゴリの横にある「X」をクリックしてフィルターをキャンセルします。
*/

-------------------------------------------------------------------------
--RESET--
-------------------------------------------------------------------------
USE ROLE accountadmin;

-- データスチュワードロールをドロップする
DROP ROLE IF EXISTS tb_data_steward;

-- マスキングポリシー
ALTER TAG IF EXISTS governance.pii UNSET
    MASKING POLICY governance.mask_string_pii,
    MASKING POLICY governance.mask_date_pii;
DROP MASKING POLICY IF EXISTS governance.mask_string_pii;
DROP MASKING POLICY IF EXISTS governance.mask_date_pii;

-- 自動分類
ALTER SCHEMA raw_customer UNSET CLASSIFICATION_PROFILE;
DROP SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE IF EXISTS tb_classification_profile;

-- 行アクセスポリシー
ALTER TABLE raw_customer.customer_loyalty 
    DROP ROW ACCESS POLICY governance.customer_loyalty_policy;
DROP ROW ACCESS POLICY IF EXISTS governance.customer_loyalty_policy;

-- データメトリック関数
DELETE FROM raw_pos.order_detail WHERE order_detail_id = 904745311;
ALTER TABLE raw_pos.order_detail
    DROP DATA METRIC FUNCTION governance.invalid_order_total_count ON (price, unit_price, quantity);
DROP FUNCTION governance.invalid_order_total_count(TABLE(NUMBER, NUMBER, INTEGER));
ALTER TABLE raw_pos.order_detail UNSET DATA_METRIC_SCHEDULE;

-- タグの解除
ALTER TABLE raw_customer.customer_loyalty
  MODIFY
    COLUMN first_name UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY,
    COLUMN last_name UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY,
    COLUMN e_mail UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY,
    COLUMN phone_number UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY,
    COLUMN postal_code UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY,
    COLUMN marital_status UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY,
    COLUMN gender UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY,
    COLUMN birthday_date UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY,
    COLUMN country UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY,
    COLUMN city UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY;

-- PII タグをドロップする
DROP TAG IF EXISTS governance.pii;
-- クエリタグを解除する
ALTER SESSION UNSET query_tag;
ALTER WAREHOUSE tb_dev_wh SUSPEND;
