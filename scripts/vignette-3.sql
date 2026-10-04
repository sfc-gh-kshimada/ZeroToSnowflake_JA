/***************************************************************************************************
Asset:        Zero to Snowflake - Horizon ガバナンス・ハンズオン (Vignette 3)
Version:      v1.1
Copyright(c): 2025 Snowflake Inc. All rights reserved.

このスクリプトでは Snowflake Horizon を使った PII データ保護を体験します:
  1. RBAC                — tb_data_steward / us_analyst / ja_analyst ロールを作成し最小権限を付与
  2. 自動分類 & PII タグ — 分類プロファイルで PII カラムを自動検出・タグ付け
  3. Dynamic Masking      — pii タグに紐付くマスキングポリシーで列値を難読化
  4. Row Access Policy    — ロールごとに参照可能な国を制限
  5. クリーンアップ      — 作成したオブジェクトを削除

前提条件:
  - setup.sql 実行済み

重要: セカンダリーロールについて
  - 本スクリプトは最初に USE SECONDARY ROLES NONE を実行し、各ロール単体の挙動を確認します

ポリシー設計方針:
  - ACCOUNTADMIN は緊急時アクセス用途として全ポリシーをバイパス（マスクなし・全行参照可）
  - Masking バイパス:    ACCOUNTADMIN, TB_ADMIN, TB_DATA_ENGINEER, TB_DATA_STEWARD
  - Row Access バイパス: ACCOUNTADMIN, TB_ADMIN, TB_DATA_ENGINEER, TB_DATA_STEWARD
  - 末尾の Section 5 で作成オブジェクトを削除するため、スクリプトは繰り返し実行可能です
****************************************************************************************************/

-- セッションの初期設定
ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"tb_zts","version":{"major":1, "minor":1},"attributes":{"is_quickstart":1, "source":"sql", "vignette": "governance_with_horizon"}}';

-- セカンダリーロールを無効化（各ロール単体の権限で動作確認するため）
USE SECONDARY ROLES NONE;


/*==================================================================================================
 1. ロールとアクセス制御 (RBAC)
   最小権限の原則に基づき、ガバナンス専任のカスタムロール tb_data_steward を作成する。
==================================================================================================*/

-- tb_data_steward ロールの作成
USE ROLE useradmin;
CREATE ROLE IF NOT EXISTS tb_data_steward
    COMMENT = 'カスタムロール: ガバナンスオブジェクトを管理するデータスチュワード';

-- tb_data_steward への権限付与
USE ROLE securityadmin;

-- ベストプラクティス: カスタムロールは SYSADMIN に継承させ、SYSADMIN 以上で管理できるようにする
GRANT ROLE tb_data_steward TO ROLE sysadmin;

-- ウェアハウスの使用権限
GRANT OPERATE, USAGE ON WAREHOUSE tb_dev_wh TO ROLE tb_data_steward;

-- データベース・スキーマへのアクセス権限
GRANT USAGE ON DATABASE tb_101 TO ROLE tb_data_steward;
GRANT USAGE ON SCHEMA tb_101.raw_customer TO ROLE tb_data_steward;
GRANT USAGE ON SCHEMA tb_101.governance TO ROLE tb_data_steward;

-- raw_customer テーブルの参照権限と governance スキーマの全権限
GRANT SELECT ON ALL TABLES IN SCHEMA tb_101.raw_customer TO ROLE tb_data_steward;
GRANT ALL ON SCHEMA tb_101.governance TO ROLE tb_data_steward;
GRANT ALL ON ALL TABLES IN SCHEMA tb_101.governance TO ROLE tb_data_steward;

-- 自動分類・分類プロファイル作成の権限（Section 2 で使用）
GRANT EXECUTE AUTO CLASSIFICATION ON SCHEMA tb_101.raw_customer TO ROLE tb_data_steward;
GRANT DATABASE ROLE SNOWFLAKE.CLASSIFICATION_ADMIN TO ROLE tb_data_steward;
GRANT CREATE SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE ON SCHEMA tb_101.governance TO ROLE tb_data_steward;

-- アカウントレベルの権限付与は ACCOUNTADMIN が必要
USE ROLE accountadmin;
GRANT APPLY TAG ON ACCOUNT TO ROLE tb_data_steward;             -- タグ適用（Section 2）
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE tb_data_steward;  -- タグへのマスキングポリシー関連付け（Section 3）
GRANT APPLY ROW ACCESS POLICY ON ACCOUNT TO ROLE tb_data_steward; -- テーブルへの行アクセスポリシー適用（Section 4）
USE ROLE securityadmin;

-- 現在のユーザーに tb_data_steward を付与
SET my_user = CURRENT_USER();
GRANT ROLE tb_data_steward TO USER IDENTIFIER($my_user);

-- 付与結果の確認
SHOW GRANTS TO ROLE tb_data_steward;

-- アナリストロールの作成: 国別に参照範囲を制限するロール
USE ROLE useradmin;
CREATE ROLE IF NOT EXISTS us_analyst
    COMMENT = 'Tasty Bytes 米国担当アナリスト';
CREATE ROLE IF NOT EXISTS ja_analyst
    COMMENT = 'Tasty Bytes 日本担当アナリスト';

-- アナリストロールへの権限付与
USE ROLE securityadmin;
GRANT ROLE us_analyst TO ROLE sysadmin;
GRANT ROLE ja_analyst TO ROLE sysadmin;
GRANT USAGE ON DATABASE tb_101 TO ROLE us_analyst;
GRANT USAGE ON DATABASE tb_101 TO ROLE ja_analyst;
GRANT USAGE ON SCHEMA tb_101.raw_customer TO ROLE us_analyst;
GRANT USAGE ON SCHEMA tb_101.raw_customer TO ROLE ja_analyst;
GRANT SELECT ON TABLE tb_101.raw_customer.customer_loyalty TO ROLE us_analyst;
GRANT SELECT ON TABLE tb_101.raw_customer.customer_loyalty TO ROLE ja_analyst;
GRANT OPERATE, USAGE ON WAREHOUSE tb_analyst_wh TO ROLE us_analyst;
GRANT OPERATE, USAGE ON WAREHOUSE tb_analyst_wh TO ROLE ja_analyst;
GRANT ROLE us_analyst TO USER IDENTIFIER($my_user);
GRANT ROLE ja_analyst TO USER IDENTIFIER($my_user);

-- PII データの確認
USE ROLE tb_data_steward;
USE WAREHOUSE tb_dev_wh;
USE DATABASE tb_101;
SELECT TOP 100 * FROM raw_customer.customer_loyalty;


/*==================================================================================================
 2. 自動タグ付けと PII 分類
   分類プロファイル で PII カラムを自動検出し pii タグを付与する。
==================================================================================================*/

USE ROLE tb_data_steward;

CREATE TAG IF NOT EXISTS governance.pii
    ALLOWED_VALUES 'TRUE', 'FALSE'
    PROPAGATE = ON_DEPENDENCY_AND_DATA_MOVEMENT;

-- 分類プロファイルの作成 (auto_tag を true にすることで PII カラムへ自動的にタグが付与される)
CREATE OR REPLACE SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE
  governance.tb_classification_profile(
    {
      'minimum_object_age_for_classification_days': 0,   -- 作成直後のテーブルでも分類対象にする
      'maximum_classification_validity_days': 30,        -- 分類結果の有効期間
      'auto_tag': true
    });

-- タグマップ: 検出された PII セマンティックカテゴリに pii タグを自動付与
CALL governance.tb_classification_profile!SET_TAG_MAP(
  {'column_tag_map':[
    {
      'tag_name':'tb_101.governance.pii',
      'tag_value':'TRUE',
      'semantic_categories':['NAME', 'PHONE_NUMBER', 'POSTAL_CODE', 'DATE_OF_BIRTH', 'CITY', 'EMAIL']
    }]});

-- スキーマまたはデータベースに適用する場合（オプション）:
-- ALTER DATABASE tb_101 SET CLASSIFICATION_PROFILE = 'tb_101.governance.tb_classification_profile';
-- ALTER SCHEMA tb_101.raw_customer SET CLASSIFICATION_PROFILE = 'tb_101.governance.tb_classification_profile';

-- customer_loyalty テーブルを自動分類 (実行に数秒かかります)
CALL SYSTEM$CLASSIFY('tb_101.raw_customer.customer_loyalty', 'tb_101.governance.tb_classification_profile');

-- タグ付け結果の確認 (apply_method = AUTO となっていれば自動タグ付け成功)
SELECT
    column_name,
    tag_database,
    tag_schema,
    tag_name,
    tag_value,
    apply_method
FROM TABLE(
    tb_101.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS('tb_101.raw_customer.customer_loyalty', 'TABLE')
)
ORDER BY column_name, tag_database, tag_name;

-- タグ付けされた列のデータ型を確認
SELECT c.column_name, c.data_type
FROM tb_101.INFORMATION_SCHEMA.COLUMNS c
JOIN TABLE(
    tb_101.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS('tb_101.raw_customer.customer_loyalty', 'TABLE')
) t ON t.column_name = c.column_name
WHERE c.table_schema = 'RAW_CUSTOMER'
  AND c.table_name = 'CUSTOMER_LOYALTY'
  AND t.tag_name = 'PII'
ORDER BY c.column_name;


/*==================================================================================================
 3. Dynamic Masking Policy (カラムレベルセキュリティ)
   pii タグに紐付くマスキングポリシーで、ACCOUNTADMIN / TB_ADMIN / TB_DATA_ENGINEER / TB_DATA_STEWARD 以外には PII を難読化する。
==================================================================================================*/

USE ROLE tb_data_steward;

-- 文字列型 PII 用 (TB 系ロールは生値を参照、その他は '****MASKED****' で表示)
CREATE OR REPLACE MASKING POLICY governance.mask_string_pii AS (original_value STRING)
RETURNS STRING ->
  CASE
    WHEN original_value IS NULL THEN NULL
    WHEN NOT (IS_ROLE_IN_SESSION('ACCOUNTADMIN') OR IS_ROLE_IN_SESSION('TB_ADMIN')
              OR IS_ROLE_IN_SESSION('TB_DATA_ENGINEER') OR IS_ROLE_IN_SESSION('TB_DATA_STEWARD'))
      THEN '****MASKED****'
    ELSE original_value
  END;

-- DATE 型 PII 用 (TB 系ロールは生値を参照、その他は年初日に丸めて表示)
CREATE OR REPLACE MASKING POLICY governance.mask_date_pii AS (original_value DATE)
RETURNS DATE ->
  CASE
    WHEN original_value IS NULL THEN NULL
    WHEN NOT (IS_ROLE_IN_SESSION('ACCOUNTADMIN') OR IS_ROLE_IN_SESSION('TB_ADMIN')
              OR IS_ROLE_IN_SESSION('TB_DATA_ENGINEER') OR IS_ROLE_IN_SESSION('TB_DATA_STEWARD'))
      THEN DATE_TRUNC('year', original_value)
    ELSE original_value
  END;

-- pii タグに両マスキングポリシーを関連付ける
ALTER TAG governance.pii SET
    MASKING POLICY governance.mask_string_pii,
    MASKING POLICY governance.mask_date_pii;

-- 動作確認 1: us_analyst ロール → PII カラムがマスクされる
USE ROLE us_analyst;
USE WAREHOUSE tb_analyst_wh;
SELECT TOP 100 * FROM tb_101.raw_customer.customer_loyalty;

-- 動作確認 2: TB_ADMIN ロール → 元の値がそのまま表示される
USE ROLE tb_admin;
USE WAREHOUSE tb_dev_wh;
SELECT TOP 100 * FROM tb_101.raw_customer.customer_loyalty;


/*==================================================================================================
 4. Row Access Policy (行レベルセキュリティ)
   us_analyst / ja_analyst の参照可能な行を国で制限する。
   ACCOUNTADMIN / TB_ADMIN / TB_DATA_ENGINEER / TB_DATA_STEWARD は全行参照可能。
==================================================================================================*/

USE ROLE tb_data_steward;
USE WAREHOUSE tb_dev_wh;

-- ポリシーマップテーブル: ロール ↔ 参照可能な国の対応表
CREATE OR REPLACE TABLE governance.row_policy_map
    (role STRING, country_permission STRING);

INSERT INTO governance.row_policy_map VALUES
    ('US_ANALYST', 'United States'),
    ('JA_ANALYST', 'Japan');

-- 行アクセスポリシーの作成
-- バイパス: ACCOUNTADMIN / TB_ADMIN / TB_DATA_ENGINEER / TB_DATA_STEWARD は全行参照可能
-- マップ登録済み（US_ANALYST / JA_ANALYST）: 許可された国のみ
-- マップ未登録のその他ロール: 0 件
CREATE OR REPLACE ROW ACCESS POLICY governance.customer_loyalty_policy
    AS (country STRING) RETURNS BOOLEAN ->
        (IS_ROLE_IN_SESSION('ACCOUNTADMIN') OR IS_ROLE_IN_SESSION('TB_ADMIN')
         OR IS_ROLE_IN_SESSION('TB_DATA_ENGINEER') OR IS_ROLE_IN_SESSION('TB_DATA_STEWARD'))
        OR EXISTS (
            SELECT 1
            FROM governance.row_policy_map rp
            WHERE rp.role = CURRENT_ROLE()
              AND rp.country_permission = country
        );

-- customer_loyalty テーブルの country カラムにポリシーを適用する
ALTER TABLE raw_customer.customer_loyalty
    ADD ROW ACCESS POLICY governance.customer_loyalty_policy ON (country);

-- 動作確認 1: US_ANALYST → 米国の顧客のみ表示される
USE ROLE us_analyst;
USE WAREHOUSE tb_analyst_wh;
SELECT TOP 100 * FROM tb_101.raw_customer.customer_loyalty;

-- 動作確認 2: JA_ANALYST → 日本の顧客のみ表示される
USE ROLE ja_analyst;
SELECT TOP 100 * FROM tb_101.raw_customer.customer_loyalty;

-- 動作確認 3: TB_DATA_ENGINEER → バイパスのため全行参照可能
USE ROLE tb_data_engineer;
USE WAREHOUSE tb_de_wh;
SELECT country, COUNT(*) AS cnt
FROM tb_101.raw_customer.customer_loyalty
GROUP BY country
ORDER BY cnt DESC;


/*==================================================================================================
 5. クリーンアップ
   本ハンズオンで作成したオブジェクトを削除し、環境を元の状態に戻す。
==================================================================================================*/
USE ROLE accountadmin;

-- Row Access Policy を解除して削除
ALTER TABLE tb_101.raw_customer.customer_loyalty
    DROP ROW ACCESS POLICY tb_101.governance.customer_loyalty_policy;
DROP ROW ACCESS POLICY IF EXISTS tb_101.governance.customer_loyalty_policy;
DROP TABLE IF EXISTS tb_101.governance.row_policy_map;

-- Masking Policy をタグから外して削除
ALTER TAG tb_101.governance.pii UNSET
    MASKING POLICY tb_101.governance.mask_string_pii,
    MASKING POLICY tb_101.governance.mask_date_pii;
DROP MASKING POLICY IF EXISTS tb_101.governance.mask_string_pii;
DROP MASKING POLICY IF EXISTS tb_101.governance.mask_date_pii;

-- 分類プロファイルとタグの削除
DROP SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE IF EXISTS tb_101.governance.tb_classification_profile;
DROP TAG IF EXISTS tb_101.governance.pii;

-- カスタムロールの削除
USE ROLE useradmin;
DROP ROLE IF EXISTS tb_data_steward;
DROP ROLE IF EXISTS us_analyst;
DROP ROLE IF EXISTS ja_analyst;

-- セカンダリーロールの設定を元に戻す（ユーザーのデフォルトは ALL）
USE SECONDARY ROLES ALL;
