<!--
Asset:        Zero to Snowflake - シンプルなデータパイプライン (Part 2, オプション)
Version:      v1
Copyright(c): 2025 Snowflake Inc. All rights reserved.

前提条件:
  - setup.sql 実行済み（tb_data_engineer ロールに CREATE SCHEMA ON DATABASE tb_101 権限が付与済み）
  - vignette-2-1.sql（シンプルなデータパイプライン Part 1）実行済み
  - リポジトリルートの AGENTS.md（CoCo のガードレール）
-->

# CoCo in Snowsight でパイプラインを作ってみる（任意）

## 概要

Snowflake には Snowsight に統合された AI エージェント **CoCo (Cortex Code)** があります。自然言語のプロンプトから SQL の作成・実行までを行えます。このセクションは任意ですが、Part 1（`vignette-2-1.sql`）で体験したダイナミックテーブルのパイプラインを、今度は自分でゼロから CoCo に作らせてみましょう。

このハンズオン専用に、既存のデータや設定に影響を与えない隔離スキーマ `tb_101.coco_handson` を用意しています。CoCo にはこのスキーマの中だけで自由に作業してもらうので、他の Vignette の内容を壊す心配はありません。

> **安全に試せる理由：** CoCo は `CREATE`・`INSERT`・`DROP` などの書き込み系 SQL を実行する前に必ず確認ダイアログを表示します（「今回のみ許可」「このチャットでは常に許可」などを選択可能）。またこのリポジトリには `AGENTS.md` というガードレールファイルが含まれており、CoCo に対して「`tb_101.coco_handson` 以外のオブジェクトは変更・削除しない」ことを指示しています。Git 連携済みのワークスペースとしてこのリポジトリを開くと（**Projects** » **Workspaces** » **+ Add New** » **Git Repository** から `https://github.com/sfc-gh-kshimada/ZeroToSnowflake_JA` を指定）、`AGENTS.md` が自動的に読み込まれます。通常のワークシートで試す場合でも、上記の確認ダイアログにより誤操作は防止されます。

## 学習内容
- Snowsight で CoCo (Cortex Code) パネルを開き、対話形式で SQL を生成・実行する方法。
- CoCo に対してロール・ウェアハウスのコンテキストを指定する方法。
- 自然言語の指示からダイナミックテーブルを使った集計パイプラインを構築する方法。

## 構築するもの
- 隔離スキーマ `tb_101.coco_handson` 内に、トラック別・日別の売上を集計するダイナミックテーブル（CoCo が生成）。

## ステップ 1 - CoCo パネルを開く

Snowsight の画面右下にある **CoCo (Cortex Code)** アイコンをクリックしてパネルを開きます。（Workspaces 内の SQL ファイルを開いている状態だと、そのファイルの内容もコンテキストとして利用されます。）

## ステップ 2 - コンテキストを指定してプロンプトを送信

CoCo はデフォルトロールでセッションを開始するため、最初のプロンプトでロールとウェアハウスを明示しましょう。以下のようなプロンプトをチャット欄に入力してください（そのままコピーして構いません）：

```
tb_data_engineer ロールと tb_de_wh ウェアハウスを使ってください。
tb_101 データベースに coco_handson という新しいスキーマがなければ作成し、
その中に daily_truck_sales という名前のダイナミックテーブルを作成してください。
要件:
- raw_pos.order_header と raw_pos.order_detail を JOIN して、
  トラックID (truck_id) と注文日 (order_ts の日付部分) ごとに
  売上合計 (price の合計) と注文件数を集計する
- LAG は '5 minutes' にする
- 作成後、上位10件を売上合計の降順で SELECT して結果を見せてください
```

## ステップ 3 - 提案されたSQLを確認して実行

CoCo が SQL を提案すると、書き込み操作（`CREATE SCHEMA` や `CREATE DYNAMIC TABLE` など）の実行前に確認ダイアログが表示されます。内容を確認し、問題なければ許可してください。

## ステップ 4 - 結果を確認する

CoCo が最後に実行した `SELECT` の結果を確認しましょう。同じ内容を通常の SQL ファイルからも確認できます：

```sql
SELECT * FROM tb_101.coco_handson.daily_truck_sales
ORDER BY total_sales DESC
LIMIT 10;
```

## ステップ 5（任意）- CoCo に改良を依頼する

生成された内容に対して、そのまま会話を続けて改良を依頼できます。例：

```
truck_brand_name も一緒に表示するように daily_truck_sales を修正してください。
```

> **ヒント：** チャット欄で `/` と入力すると、利用可能なスキル一覧が表示されます。組み込みスキルが利用可能であれば、それを使うことでより最適化された提案が得られる場合があります（利用可能なスキルはアカウントの設定によって異なります）。

## まとめ

CoCo (Cortex Code) を使うことで、SQL を一から書かずに自然言語からダイナミックテーブルのパイプラインを構築できることを体験しました。隔離スキーマと確認ダイアログの仕組みにより、安全に試行錯誤できます。次のセクションでは、Snowflake Horizon による本格的なガバナンス機能を見ていきましょう。
