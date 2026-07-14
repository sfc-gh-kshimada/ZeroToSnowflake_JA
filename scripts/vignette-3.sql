/***************************************************************************************************
Asset:        Zero to Snowflake - Horizon ガバナンス・ハンズオン (Vignette 3)
Version:      v1
Copyright(c): 2025 Snowflake Inc. All rights reserved.

このスクリプトでは Snowflake Horizon を使った PII データ保護を体験します:
  1. RBAC                — tb_data_steward カスタムロールを作成し最小権限を付与
  2. 自動分類 & PII タグ — 分類プロファイルで PII カラムを自動検出・タグ付け
  3. Dynamic Masking      — pii タグに紐付くマスキングポリシーで列値を難読化
  4. Row Access Policy    — ロールごとに参照可能な国を制限

前提条件:
  - setup.sql 実行済み（tb_101 DB, raw_customer/governance スキーマ, tb_admin/tb_data_engineer 等のロール, tb_dev_wh ウェアハウス）
  - 実行ユーザーは ACCOUNTADMIN / SECURITYADMIN / USERADMIN を利用可能であること
  - 対象テーブル: tb_101.raw_customer.customer_loyalty

ポリシー設計方針:
  - ACCOUNTADMIN は緊急時アクセス用途として全ポリシーをバイパス（マスクなし・全行参照可）
  - Masking バイパス:    ACCOUNTADMIN, TB_ADMIN, TB_DATA_ENGINEER, TB_DATA_STEWARD
  - Row Access バイパス: ACCOUNTADMIN, TB_ADMIN, TB_DATA_ENGINEER, TB_DATA_STEWARD
****************************************************************************************************/

-- セッションの初期設定
ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"tb_zts","version":{"major":1, "minor":1},"attributes":{"is_quickstart":1, "source":"sql", "vignette": "governance_with_horizon"}}';

USE ROLE useradmin;
USE DATABASE tb_101;
USE WAREHOUSE tb_dev_wh;


/*==================================================================================================
 1. ロールとアクセス制御 (RBAC)
   最小権限の原則に基づき、ガバナンス専任のカスタムロール tb_data_steward を作成する。
==================================================================================================*/

-- 既存ロールの一覧確認
SHOW ROLES;

-- tb_data_steward ロールの作成
USE ROLE useradmin;
CREATE OR REPLACE ROLE tb_data_steward
    COMMENT = 'カスタムロール: ガバナンスオブジェクトを管理するデータスチュワード';

-- tb_data_steward への権限付与
USE ROLE securityadmin;

-- ウェアハウスの使用権限
GRANT OPERATE, USAGE ON WAREHOUSE tb_dev_wh TO ROLE tb_data_steward;

-- データベース・スキーマへのアクセス権限
GRANT USAGE ON DATABASE tb_101 TO ROLE tb_data_steward;
GRANT USAGE ON ALL SCHEMAS IN DATABASE tb_101 TO ROLE tb_data_steward;

-- raw_customer テーブルの参照権限と governance スキーマの全権限
GRANT SELECT ON ALL TABLES IN SCHEMA raw_customer TO ROLE tb_data_steward;
GRANT ALL ON SCHEMA governance TO ROLE tb_data_steward;
GRANT ALL ON ALL TABLES IN SCHEMA governance TO ROLE tb_data_steward;

-- タグ適用・自動分類・分類プロファイル作成の権限（Section 2 で使用）
GRANT APPLY TAG ON ACCOUNT TO ROLE tb_data_steward;
GRANT EXECUTE AUTO CLASSIFICATION ON SCHEMA raw_customer TO ROLE tb_data_steward;
GRANT DATABASE ROLE SNOWFLAKE.CLASSIFICATION_ADMIN TO ROLE tb_data_steward;
GRANT CREATE SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE ON SCHEMA governance TO ROLE tb_data_steward;

-- 現在のユーザーに tb_data_steward を付与
SET my_user = CURRENT_USER();
GRANT ROLE tb_data_steward TO USER IDENTIFIER($my_user);

-- 付与結果の確認
SHOW GRANTS TO ROLE tb_data_steward;

-- PII データの確認 (tb_data_steward は raw_customer.customer_loyalty を参照可能)
USE ROLE tb_data_steward;
SELECT TOP 100 * FROM raw_customer.customer_loyalty;


/*==================================================================================================
 2. 自動タグ付けと PII 分類
   分類プロファイル (auto_tag=true) で PII カラムを自動検出し pii タグを付与する。
==================================================================================================*/

-- Section 1 で必要な権限は付与済みのため、tb_data_steward で直接タグ・プロファイルを作成できる
USE ROLE tb_data_steward;

CREATE OR REPLACE TAG governance.pii
    ALLOWED_VALUES 'TRUE', 'FALSE'
    PROPAGATE = ON_DEPENDENCY_AND_DATA_MOVEMENT;

-- 分類プロファイルの作成 (auto_tag を true にすることで PII カラムへ自動的にタグが付与される)

CREATE OR REPLACE SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE
  governance.tb_classification_profile(
    {
      'minimum_object_age_for_classification_days': 0,
      'maximum_classification_validity_days': 30,
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


-- スキーマまたはデータベースに適用
-- データベース全体に適用する場合：
-- ALTER DATABASE tb_101 SET CLASSIFICATION_PROFILE = 'tb_101.governance.tb_classification_profile';
-- 特定スキーマに適用する場合：
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

/*==================================================================================================
 3. Dynamic Masking Policy (カラムレベルセキュリティ)
   pii タグに紐付くマスキングポリシーで、ACCOUNTADMIN / TB_ADMIN / TB_DATA_ENGINEER / TB_DATA_STEWARD 以外には PII を難読化する。
==================================================================================================*/

USE ROLE tb_data_steward;

-- 文字列型 PII 用 (TB 系ロールは生値を参照、その他は '****MASKED****' で表示)
CREATE OR REPLACE MASKING POLICY governance.mask_string_pii AS (original_value STRING)
RETURNS STRING ->
  CASE WHEN
    CURRENT_ROLE() NOT IN ('ACCOUNTADMIN', 'TB_ADMIN', 'TB_DATA_ENGINEER', 'TB_DATA_STEWARD')
    THEN '****MASKED****'
    ELSE original_value
  END;

-- DATE 型 PII 用 (TB 系ロールは生値を参照、その他は年初日に丸めて表示)
CREATE OR REPLACE MASKING POLICY governance.mask_date_pii AS (original_value DATE)
RETURNS DATE ->
  CASE WHEN
    CURRENT_ROLE() NOT IN ('ACCOUNTADMIN', 'TB_ADMIN', 'TB_DATA_ENGINEER', 'TB_DATA_STEWARD')
    THEN DATE_TRUNC('year', original_value)
    ELSE original_value
  END;

-- pii タグに両マスキングポリシーを関連付ける
ALTER TAG governance.pii SET
    MASKING POLICY governance.mask_string_pii,
    MASKING POLICY governance.mask_date_pii;

-- 動作確認 1: PUBLIC ロール → PII カラムがマスクされる
USE ROLE public;
SELECT TOP 100 * FROM raw_customer.customer_loyalty;

-- 動作確認 2: TB_ADMIN ロール → 元の値がそのまま表示される
USE ROLE tb_admin;
SELECT TOP 100 * FROM raw_customer.customer_loyalty;


/*==================================================================================================
 4. Row Access Policy (行レベルセキュリティ)
   us_analyst / ja_analyst の参照可能な行を国で制限する。
   TB 系ロール（TB_ADMIN / TB_DATA_ENGINEER / TB_DEV / TB_DATA_STEWARD）は全行参照可能。
==================================================================================================*/

USE ROLE tb_data_steward;

-- ポリシーマップテーブル: ロール ↔ 参照可能な国の対応表
-- マップに登録したロールのみ行が絞られる（TB系ロールはポリシー側でバイパス）
CREATE OR REPLACE TABLE governance.row_policy_map
    (role STRING, country_permission STRING);

-- us_analyst は 'United States' の行のみ参照可能
INSERT INTO governance.row_policy_map
    VALUES('us_analyst', 'United States');

-- ja_analyst は 'Japan' の行のみ参照可能
INSERT INTO governance.row_policy_map
    VALUES('ja_analyst', 'Japan');

-- 行アクセスポリシーの作成
-- バイパス: ACCOUNTADMIN / TB_ADMIN / TB_DATA_ENGINEER / TB_DATA_STEWARD は全行参照可能
-- マップ登録済み（us_analyst / ja_analyst）: 許可された国のみ
-- マップ未登録のその他ロール: 0 件
CREATE OR REPLACE ROW ACCESS POLICY governance.customer_loyalty_policy
    AS (country STRING) RETURNS BOOLEAN ->
        CURRENT_ROLE() IN ('ACCOUNTADMIN', 'TB_ADMIN', 'TB_DATA_ENGINEER', 'TB_DATA_STEWARD')
        OR EXISTS (
            SELECT 1
            FROM governance.row_policy_map rp
            WHERE UPPER(rp.role) = CURRENT_ROLE()
              AND rp.country_permission = country
        );

-- customer_loyalty テーブルの country カラムにポリシーを適用する
ALTER TABLE raw_customer.customer_loyalty
    ADD ROW ACCESS POLICY governance.customer_loyalty_policy ON (country);

-- 動作確認 1: US_ANALYST → 米国の顧客のみ表示される
USE ROLE us_analyst;
SELECT TOP 100 * FROM raw_customer.customer_loyalty;

-- 動作確認 2: JA_ANALYST → 日本の顧客のみ表示される
USE ROLE ja_analyst;
SELECT TOP 100 * FROM raw_customer.customer_loyalty;

-- 動作確認 3: TB_DATA_ENGINEER → バイパスのため全行参照可能
USE ROLE tb_data_engineer;
SELECT country, COUNT(*) AS cnt
FROM tb_101.raw_customer.customer_loyalty
GROUP BY country
ORDER BY cnt DESC;


/*==================================================================================================
 (オプション) クリーンアップ
   ハンズオン後に作成オブジェクトを削除する場合は以下のブロックを実行してください。
==================================================================================================*/
/*
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
*/
