# AGENTS.md — Zero to Snowflake JA ハンズオン用ガードレール

このリポジトリは Snowflake の "Zero to Snowflake" 日本語ハンズオンの教材です。
`tb_101` データベースには `setup.sql` および `scripts/vignette-*.sql` / `scripts/vignette-*.md` が構築した
デモデータ・ダイナミックテーブル・ロール・ウェアハウス・ガバナンスポリシーが
含まれており、これらはハンズオン全体を通じて一貫性が必要です。

CoCo (Cortex Code) を使ってこのハンズオンを試す参加者の作業を壊さないよう、
以下のルールを必ず守ってください。

## 保護対象（変更・削除禁止）

以下のオブジェクトは、ユーザーから明示的かつ具体的な指示（対象のフル修飾名を
含む）がない限り、`CREATE OR REPLACE` / `ALTER` / `DROP` / `TRUNCATE` /
`DELETE` / `UPDATE` などの破壊的操作を **絶対に行わないこと**。

- データベース `tb_101` 自体（`DROP DATABASE` / `CREATE OR REPLACE DATABASE` 禁止）
- 既存スキーマ: `tb_101.raw_pos`, `tb_101.raw_customer`, `tb_101.raw_support`,
  `tb_101.harmonized`, `tb_101.analytics`, `tb_101.governance`,
  `tb_101.semantic_layer` およびその配下の全テーブル・ビュー・ダイナミックテーブル
- ロール: `tb_admin`, `tb_data_engineer`, `tb_dev`, `tb_analyst` とその権限設定
- ウェアハウス: `tb_de_wh`, `tb_dev_wh`, `tb_analyst_wh`, `tb_cortex_wh`
- コンピュートプール `tb_compute_pool`
- `governance` スキーマ内のタグ（`pii` 等）・マスキングポリシー・行アクセスポリシー
- `snowflake_intelligence` データベースおよびそこに登録された Agent
- `scripts/` 配下のファイル・`setup.sql`・`streamlit_apps/` 配下のコード
  （ユーザーから明示的な編集依頼があった場合のみ変更可）

## 自由に操作してよい領域

- スキーマ `tb_101.coco_handson`
  - 存在しない場合は作成してよい（`tb_data_engineer` ロールには
    `CREATE SCHEMA ON DATABASE tb_101` 権限が付与済み）
  - このスキーマ内のテーブル・ビュー・ダイナミックテーブルは自由に
    `CREATE` / `ALTER` / `DROP` してよい
  - 「練習用に〜を作って」「新しいパイプラインを試したい」といった依頼は、
    特に指示がない限りこのスキーマ内に作成すること

## 破壊的操作を求められた場合

保護対象への `CREATE OR REPLACE` / `DROP` / `TRUNCATE` / `DELETE` / `UPDATE` が
必要な場合は、実行前に対象のフル修飾オブジェクト名を明示してユーザーに確認を
取ること。確認が取れない場合は実行しない。

## その他

- コンテキスト設定は既存のハンズオンパターンに合わせ、
  `USE ROLE tb_data_engineer; USE WAREHOUSE tb_de_wh;` を基本とする。
- README.md / README_EN.md の内容と矛盾する提案をしないこと。
