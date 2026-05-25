import streamlit as st
import os
import json
import base64
import pandas as pd
from datetime import datetime

st.set_page_config(page_title="Cortex AI Playground", layout="wide")

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    .stRadio > label {
        font-weight: 500;
    }
    [data-testid="stSidebar"] {
        background-color: #1B6B8A !important;
    }
    [data-testid="stSidebar"] > div {
        background-color: #1B6B8A !important;
    }
    [data-testid="stSidebar"] * {
        color: #ffffff !important;
    }
    [data-testid="stSidebar"] [data-baseweb="select"] span,
    [data-testid="stSidebar"] [data-baseweb="select"] input,
    [data-testid="stSidebar"] [data-baseweb="select"] div {
        color: #1a1a2e !important;
    }
    [data-testid="stSidebar"] button {
        color: #1B6B8A !important;
        background-color: #ffffff !important;
        border: 2px solid #ffffff !important;
    }
    [data-testid="stSidebar"] button span,
    [data-testid="stSidebar"] button p {
        color: #1B6B8A !important;
    }
    [data-testid="stSidebar"] button:hover {
        background-color: #1B6B8A !important;
        color: #ffffff !important;
        border: 2px solid #ffffff !important;
    }
    [data-testid="stSidebar"] button:hover span,
    [data-testid="stSidebar"] button:hover p {
        color: #ffffff !important;
    }
    [data-testid="stSidebar"] .stCaption, [data-testid="stSidebar"] small {
        color: #ffffff !important;
        opacity: 1 !important;
    }
    [data-testid="stSidebar"] .stRadio label span {
        font-size: 0.9rem;
    }
    .function-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%) !important;
        -webkit-background-clip: text !important;
        -webkit-text-fill-color: transparent !important;
        font-size: 3.5rem !important;
        font-weight: 700 !important;
        margin-bottom: 0.5rem;
        letter-spacing: -0.5px;
    }
    .card {
        background: #f8f9fa;
        border-radius: 12px;
        padding: 1.5rem;
        border: 1px solid #e9ecef;
        margin-bottom: 1rem;
    }
    .result-card {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
        border-radius: 12px;
        padding: 1.5rem;
        border: none;
        margin-top: 1rem;
    }
    .sql-usage {
        background: #1e1e2e;
        border-radius: 8px;
        padding: 1rem;
        margin-top: 0.5rem;
    }
    div[data-testid="stExpander"] {
        border: 1px solid #e9ecef;
        border-radius: 8px;
    }
    .stButton > button {
        border-radius: 8px;
        font-weight: 500;
        transition: all 0.2s ease;
    }
    .stButton > button:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    }
    </style>
    """,
    unsafe_allow_html=True,
)

conn = st.connection("snowflake", ttl=os.getenv("SNOWFLAKE_CONNECTION_TTL"))
session = conn.session()

DB = "TB_101"
SCHEMA = "PUBLIC"
STAGE = f"@{DB}.{SCHEMA}.AI_PLAYGROUND_FILES"
METADATA_TABLE = f"{DB}.{SCHEMA}.AI_PLAYGROUND_FILE_METADATA"

FUNCTIONS = [
    "AI_CLASSIFY",
    "AI_FILTER",
    "AI_EXTRACT",
    "AI_SENTIMENT",
    "AI_SUMMARIZE_AGG",
    "AI_TRANSLATE",
    "AI_COMPLETE",
    "AI_PARSE_DOCUMENT",
    "AI_REDACT",
    "AI_EMBED",
]

FUNCTION_DOCS = {
    "AI_CLASSIFY": {
        "description": "テキストや画像をユーザー定義のラベルに分類します。",
        "doc_url": "https://docs.snowflake.com/en/sql-reference/functions/ai_classify",
        "usage": "SELECT AI_CLASSIFY('<text>', ['label1', 'label2', ...]) AS result\n-- マルチラベル:\nSELECT AI_CLASSIFY('<text>', ['l1','l2'], {'output_mode':'multi'}):labels AS result",
        "examples": [
            {
                "name": "レビュー感情分類",
                "input": "この商品は本当に最高です！今まで買った中で一番気に入っています。",
                "params": "ポジティブ, ネガティブ, 中立",
            },
            {
                "name": "サポートチケット分類",
                "input": "アカウントがロックされてしまい、パスワードのリセットができません。",
                "params": "請求関連, アカウントアクセス, 技術的問題, 機能リクエスト",
            },
        ],
    },
    "AI_FILTER": {
        "description": "自然言語の条件でテキストや画像をTRUE/FALSEでフィルタリングします。",
        "doc_url": "https://docs.snowflake.com/en/sql-reference/functions/ai_filter",
        "usage": "SELECT AI_FILTER(PROMPT('<condition>: {0}', '<text>')) AS result",
        "examples": [
            {
                "name": "技術的な内容かフィルタ",
                "input": "新しい Transformer アーキテクチャはマルチヘッドアテンション機構を使用しています。",
                "params": "このテキストはテクノロジーやコンピュータサイエンスに関する内容ですか？",
            },
            {
                "name": "緊急対応が必要なレビュー検出",
                "input": "商品が破損して届きました。すぐに返金してほしいです。",
                "params": "このテキストは緊急対応が必要な顧客クレームですか？",
            },
        ],
    },
    "AI_EXTRACT": {
        "description": "テキストやファイルから構造化データ（名前、日付、金額等）を抽出します。",
        "doc_url": "https://docs.snowflake.com/en/sql-reference/functions/ai_extract",
        "usage": "SELECT AI_EXTRACT('<text_or_file>', ['field1', 'field2', ...]) AS result",
        "examples": [
            {
                "name": "連絡先抽出",
                "input": "こんにちは、Snowflake 株式会社の山田太郎と申します。ご連絡は taro@example.com もしくは 03-1234-5678 までお願いします。",
                "params": "氏名, 会社名, メールアドレス, 電話番号",
            },
            {
                "name": "イベント情報抽出",
                "input": "2025年3月15日に東京ビッグサイトで開催される AI サミットへぜひご参加ください。チケット料金は5万円です。",
                "params": "イベント名, 開催日, 開催場所, 価格",
            },
        ],
    },
    "AI_SENTIMENT": {
        "description": "テキストの感情を分析します（positive/negative/neutral/mixed/unknown）。カテゴリ指定で側面ごとの評価も可能。",
        "doc_url": "https://docs.snowflake.com/en/sql-reference/functions/ai_sentiment",
        "usage": "SELECT AI_SENTIMENT('<text>') AS result\n-- カテゴリ指定:\nSELECT AI_SENTIMENT('<text>', ['cost', 'quality', 'service']) AS result",
        "examples": [
            {
                "name": "ポジティブレビュー",
                "input": "このサービスは本当に素晴らしいです！スタッフの対応も丁寧で迅速、心からおすすめできます。",
            },
            {
                "name": "ネガティブフィードバック",
                "input": "最悪の体験でした。商品は1日で壊れ、カスタマーサポートも全く役に立ちませんでした。",
            },
        ],
    },
    "AI_SUMMARIZE_AGG": {
        "description": "テキスト列やまとまった長文を集約して要約します（モデルのコンテキストウィンドウより大きいデータにも対応）。",
        "doc_url": "https://docs.snowflake.com/en/sql-reference/functions/ai_summarize_agg",
        "usage": "SELECT AI_SUMMARIZE_AGG('<text>') AS result\n-- 列に対する集約:\nSELECT AI_SUMMARIZE_AGG(review_text) FROM reviews;",
        "examples": [
            {
                "name": "長文ニュース記事要約",
                "input": """Snowflake は本日、データクラウド上で動作する Cortex AI 機能の大規模な拡張を発表しました。今回のアップデートでは、開発者がデータプラットフォーム内で直接 AI 駆動型アプリケーションを構築できるよう、複数の新しい AI 関数が追加されています。具体的には、PDF や画像から構造化データを抽出する高度なドキュメント処理機能、テキストと画像を統合的に扱えるマルチモーダル分類機能、そして日本語を含む多言語に対応した自然言語理解の精度向上などが含まれています。

また、Cortex Analyst によるテキストから SQL への変換機能、Cortex Search を用いた高速なセマンティック検索、Cortex Agents による複数ツール連携型のエージェント構築機能なども強化されました。これらの機能は、データガバナンスとセキュリティを Snowflake のプラットフォーム上で一貫して維持したまま AI を活用したいエンタープライズ顧客のニーズに応えるものです。

CEO の Sridhar Ramaswamy 氏は今回の発表に際し、「これらの新機能は、すべてのデータプロフェッショナルが AI を業務に取り込めるようにする上で大きな前進となるものであり、Snowflake はデータと AI の境界を取り払うことを目指している」と述べました。さらに同社は、2026 年を通じて AI 研究領域における主要組織との連携を継続的に強化し、プラットフォームの推論性能・コスト効率・モデル選択肢の拡大に注力する方針を明らかにしました。Snowflake によれば、これらの取り組みはすでに金融、小売、製造、ヘルスケアといった主要業界のグローバル顧客で本番稼働しており、今後数四半期にわたりさらに多くのリージョンで一般提供される予定です。""",
            },
        ],
    },
    "AI_TRANSLATE": {
        "description": "テキストを指定した言語に翻訳します。",
        "doc_url": "https://docs.snowflake.com/en/sql-reference/functions/ai_translate",
        "usage": "SELECT AI_TRANSLATE('<text>', '<from_lang>', '<to_lang>') AS result",
        "examples": [
            {
                "name": "英語→日本語",
                "input": "Welcome to Snowflake's AI Playground. Here you can experiment with various Cortex AI functions.",
                "from_lang": "en",
                "to_lang": "ja",
            },
            {
                "name": "日本語→英語",
                "input": "本日はご来場いただきありがとうございます。素晴らしい一日をお過ごしください。",
                "from_lang": "ja",
                "to_lang": "en",
            },
            {
                "name": "日本語→韓国語",
                "input": "こちらの会議室は午後3時から利用可能です。ご確認をお願いします。",
                "from_lang": "ja",
                "to_lang": "ko",
            },
        ],
    },
    "AI_COMPLETE": {
        "description": "LLMに自由なプロンプトを送信してテキスト生成を行います。構造化出力にも対応。",
        "doc_url": "https://docs.snowflake.com/en/sql-reference/functions/ai_complete",
        "usage": "SELECT AI_COMPLETE('<model>', '<prompt>') AS result\n-- 構造化出力:\nSELECT AI_COMPLETE('<model>', '<prompt>', {'response_format': {'type': 'json', 'schema': {...}}}) AS result\n-- 画像入力:\nSELECT AI_COMPLETE('<model>', PROMPT('<text> {0}', TO_FILE('@stage','img.png'))) AS result",
        "examples": [
            {
                "name": "コード説明",
                "input": "以下の SQL がどのような処理を行うか、初心者にもわかるように日本語で簡潔に説明してください:\nSELECT customer_id, SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date) AS running_total FROM orders;",
                "model": "claude-sonnet-4-6",
            },
            {
                "name": "メール文面生成",
                "input": "2 時間にわたるサービス障害についてお客様にお詫びする、プロフェッショナルかつ簡潔な日本語のメール文面を書いてください。件名と本文を含めてください。",
                "model": "claude-sonnet-4-6",
            },
            {
                "name": "構造化抽出",
                "input": "次のレビューから商品名・価格・評価を抽出してください: 『Sony WH-1000XM5 ヘッドフォンは 49,800 円ですが本当に素晴らしい音質です。5 段階評価で 4.5 をつけます。』",
                "model": "claude-sonnet-4-6",
                "structured": True,
            },
        ],
    },
    "AI_PARSE_DOCUMENT": {
        "description": "PDF・画像ファイルからテキストや構造化データを抽出します。OCR（スキャン文書向け）とLAYOUT（構造保持・Markdown出力）の2モード対応。ファイルは内部ステージに配置する必要があります。",
        "doc_url": "https://docs.snowflake.com/en/sql-reference/functions/ai_parse_document",
        "usage": f"SELECT AI_PARSE_DOCUMENT(TO_FILE('{STAGE}', '<filename>'), {{'mode': '<MODE>'}}) AS result",
        "examples": [
            {
                "name": "OCRモード",
                "params": "OCR",
            },
            {
                "name": "LAYOUTモード",
                "params": "LAYOUT",
            },
        ],
    },
    "AI_REDACT": {
        "description": "テキスト中の個人情報（PII）を自動的にマスキングします。",
        "doc_url": "https://docs.snowflake.com/en/sql-reference/functions/ai_redact",
        "usage": "SELECT AI_REDACT('<text>') AS result",
        "examples": [
            {
                "name": "個人情報マスキング",
                "input": "私の名前は John Smith です。社会保障番号は 123-45-6789、メールは john.smith@company.com、住所は東京都港区六本木 1-2-3 です。",
            },
            {
                "name": "日本語PII",
                "input": "山田太郎と申します。電話番号は 090-1234-5678、住所は東京都渋谷区神宮前 1-2-3、メールアドレスは taro.yamada@example.co.jp です。",
            },
        ],
    },
    "AI_EMBED": {
        "description": "テキストをベクトル（数値配列）に変換します。類似度検索やクラスタリングに利用できます。",
        "doc_url": "https://docs.snowflake.com/en/sql-reference/functions/ai_embed",
        "usage": "SELECT AI_EMBED('<model>', '<text>') AS result",
        "examples": [
            {
                "name": "文章ベクトル化",
                "input": "Snowflake はクラウドデータプラットフォームです。",
                "model": "snowflake-arctic-embed-l-v2.0",
            },
            {
                "name": "類似度比較",
                "input": "機械学習と人工知能",
                "model": "snowflake-arctic-embed-l-v2.0",
            },
        ],
    },
}

AVAILABLE_MODELS = [
    "claude-sonnet-4-6",
    "claude-opus-4-7",
    "claude-sonnet-4-5",
    "claude-haiku-4-5",
    "openai-gpt-5.2",
    "openai-gpt-5.1",
    "openai-gpt-4.1",
    "gemini-3.1-pro",
    "deepseek-r1",
    "llama4-maverick",
    "llama4-scout",
    "llama3.1-405b",
    "llama3.1-70b",
    "llama3.1-8b",
    "llama3.3-70b",
    "mistral-large2",
    "mixtral-8x7b",
    "snowflake-llama-3.3-70b",
]

EMBED_MODELS = [
    "snowflake-arctic-embed-l-v2.0",
    "snowflake-arctic-embed-m-v2.0",
]


def run_sql(sql):
    try:
        result = session.sql(sql).collect()
        return result, None
    except Exception as e:
        return None, str(e)


def run_ai_sql(sql):
    try:
        if debug_mode:
            st.caption("実行SQL")
            st.code(sql, language="sql")
        result = session.sql(sql).collect()
        return result, None
    except Exception as e:
        return None, str(e)


def upload_file_to_stage(uploaded_file, description="", source_url=""):
    file_name = uploaded_file.name.replace(" ", "_")
    session.file.put_stream(
        input_stream=uploaded_file,
        stage_location=f"{STAGE}/{file_name}",
        auto_compress=False,
        overwrite=True,
    )
    escaped_desc = description.replace("'", "''")
    escaped_url = source_url.replace("'", "''")
    session.sql(f"""
        MERGE INTO {METADATA_TABLE} t
        USING (SELECT '{file_name}' AS filename) s
        ON t.filename = s.filename
        WHEN MATCHED THEN UPDATE SET
            description = '{escaped_desc}',
            source_url = '{escaped_url}',
            updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (filename, description, source_url, uploaded_at, updated_at)
            VALUES ('{file_name}', '{escaped_desc}', '{escaped_url}', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    """).collect()
    return file_name


def ensure_metadata_table():
    try:
        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {METADATA_TABLE} (
                filename VARCHAR,
                description VARCHAR,
                source_url VARCHAR,
                uploaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
                updated_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """).collect()
    except Exception:
        pass


def get_file_metadata():
    try:
        rows = session.sql(f"SELECT filename, description, source_url, uploaded_at FROM {METADATA_TABLE} ORDER BY uploaded_at DESC").collect()
        return {row["FILENAME"]: {"description": row["DESCRIPTION"], "source_url": row["SOURCE_URL"], "uploaded_at": row["UPLOADED_AT"]} for row in rows}
    except Exception:
        return {}


ensure_metadata_table()


@st.cache_data(ttl=60)
def list_stage_files():
    try:
        rows = session.sql(f"LIST {STAGE}").collect()
        files = [row["name"].split("/")[-1] for row in rows if row["name"]]
        return [f for f in files if f]
    except Exception:
        return []


def render_image_input(key_prefix):
    input_mode = st.radio(
        "画像入力方法",
        ["ステージから選択", "テストファイル登録へ"],
        horizontal=True,
        key=f"{key_prefix}_img_mode",
    )
    file_name = None
    if input_mode == "ステージから選択":
        files = list_stage_files()
        image_files = [f for f in files if f.lower().endswith((".png", ".jpg", ".jpeg"))]
        if image_files:
            metadata = get_file_metadata()
            display_names = [f"{f} ({metadata.get(f, {}).get('description', '')})" if metadata.get(f, {}).get('description') else f for f in image_files]
            selected_idx = st.selectbox("ステージ上の画像", range(len(image_files)), format_func=lambda i: display_names[i], key=f"{key_prefix}_img_select")
            file_name = image_files[selected_idx]
            try:
                img_bytes = session.file.get_stream(f"{STAGE}/{file_name}", decompress=False).read()
                st.image(img_bytes, caption=file_name, width=300)
            except Exception:
                pass
        else:
            st.info("ステージに画像がありません。テストファイル登録からアップロードしてください。")
    else:
        st.info("サイドバーの「テストファイル登録」からファイルをアップロードしてください。")
        if st.button("テストファイル登録を開く", key=f"{key_prefix}_goto_stage"):
            st.session_state.show_stage_browser = True
            st.rerun()
    return file_name


def render_example_buttons(examples, callback, fn_key):
    if not examples:
        return
    cols = st.columns(len(examples))
    for i, ex in enumerate(examples):
        if cols[i].button(f"{ex['name']}", key=f"{fn_key}_ex_{i}", use_container_width=True):
            callback(ex)
            st.rerun()


def render_function_header(func_name):
    info = FUNCTION_DOCS[func_name]
    st.markdown(f'<p class="function-header">{func_name}</p>', unsafe_allow_html=True)
    st.markdown(f"{info['description']}")
    st.markdown(f"[Documentation]({info['doc_url']})")
    with st.expander("SQL Usage", expanded=False):
        st.code(info["usage"], language="sql")


def render_ai_classify():
    render_function_header("AI_CLASSIFY")
    info = FUNCTION_DOCS["AI_CLASSIFY"]

    def apply_example(ex):
        st.session_state.classify_text = ex["input"]
        st.session_state.classify_labels = ex["params"]
        st.session_state.classify_use_image = False

    render_example_buttons(info["examples"], apply_example, "classify")

    with st.container():
        use_image = st.toggle("画像を分類する", key="classify_use_image")
        multi_label = st.toggle("マルチラベル（複数ラベル付与）", key="classify_multi")

        image_file = None
        text = ""
        if use_image:
            image_file = render_image_input("classify")
        else:
            text = st.text_area("テキスト", height=100, key="classify_text")

        labels = st.text_input("ラベル（カンマ区切り）", key="classify_labels")

        if st.button("実行", key="classify_run", type="primary"):
            if labels and (text or image_file):
                with st.spinner("実行中..."):
                    label_list = ", ".join([f"'{l.strip()}'" for l in labels.split(",")])
                    config = ""
                    if multi_label:
                        config = ", {'output_mode': 'multi'}"

                    if use_image and image_file:
                        sql = f"SELECT AI_CLASSIFY(TO_FILE('{STAGE}', '{image_file}'), [{label_list}]{config}) AS result"
                    else:
                        escaped_text = text.replace("'", "''")
                        sql = f"SELECT AI_CLASSIFY('{escaped_text}', [{label_list}]{config}) AS result"

                    result, err = run_ai_sql(sql)
                    if err:
                        st.error(f"エラー: {err}")
                    else:
                        st.json(result[0]["RESULT"])
            else:
                st.warning("テキスト（または画像）とラベルを入力してください")


def render_ai_filter():
    render_function_header("AI_FILTER")
    info = FUNCTION_DOCS["AI_FILTER"]

    def apply_example(ex):
        st.session_state.filter_text = ex["input"]
        st.session_state.filter_condition = ex["params"]
        st.session_state.filter_use_image = False

    render_example_buttons(info["examples"], apply_example, "filter")

    with st.container():
        use_image = st.toggle("画像をフィルタする", key="filter_use_image")

        image_file = None
        text = ""
        if use_image:
            image_file = render_image_input("filter")
        else:
            text = st.text_area("テキスト", height=100, key="filter_text")

        condition = st.text_input("条件（自然言語）", key="filter_condition")

        if st.button("実行", key="filter_run", type="primary"):
            if condition and (text or image_file):
                with st.spinner("実行中..."):
                    escaped_cond = condition.replace("'", "''")
                    if use_image and image_file:
                        sql = f"SELECT AI_FILTER('{escaped_cond}', TO_FILE('{STAGE}', '{image_file}')) AS result"
                    else:
                        escaped_text = text.replace("'", "''")
                        sql = f"SELECT AI_FILTER(PROMPT('{escaped_cond}: {{0}}', '{escaped_text}')) AS result"

                    result, err = run_ai_sql(sql)
                    if err:
                        st.error(f"エラー: {err}")
                    else:
                        val = result[0]["RESULT"]
                        if val:
                            st.success("結果: **TRUE**")
                        else:
                            st.error("結果: **FALSE**")
            else:
                st.warning("テキスト（または画像）と条件を入力してください")


def render_ai_extract():
    render_function_header("AI_EXTRACT")
    info = FUNCTION_DOCS["AI_EXTRACT"]

    def apply_example(ex):
        st.session_state.extract_text = ex["input"]
        st.session_state.extract_fields = ex["params"]
        st.session_state.extract_input_mode = "テキスト入力"

    render_example_buttons(info["examples"], apply_example, "extract")

    with st.container():
        input_mode = st.radio(
            "入力方法",
            ["テキスト入力", "ステージから選択", "テストファイル登録へ"],
            horizontal=True,
            key="extract_input_mode",
        )

        text = ""
        file_name = None
        use_file = False

        if input_mode == "テキスト入力":
            text = st.text_area("テキスト", height=100, key="extract_text")
        elif input_mode == "テストファイル登録へ":
            st.info("サイドバーの「テストファイル登録」からファイルをアップロードしてください。")
            if st.button("テストファイル登録を開く", key="extract_goto_stage"):
                st.session_state.show_stage_browser = True
                st.rerun()
        else:
            files = list_stage_files()
            if files:
                metadata = get_file_metadata()
                display_names = [f"{f} ({metadata.get(f, {}).get('description', '')})" if metadata.get(f, {}).get('description') else f for f in files]
                selected_idx = st.selectbox("ステージ上のファイル", range(len(files)), format_func=lambda i: display_names[i], key="extract_stage_select")
                file_name = files[selected_idx]
                use_file = True
                if file_name.lower().endswith((".png", ".jpg", ".jpeg")):
                    try:
                        img_bytes = session.file.get_stream(f"{STAGE}/{file_name}", decompress=False).read()
                        st.image(img_bytes, caption=file_name, width=300)
                    except Exception:
                        pass
            else:
                st.info("ステージにファイルがありません")

        format_mode = st.radio(
            "responseFormat 形式",
            ["シンプル（カンマ区切り）", "JSON Schema（自由記述）"],
            horizontal=True,
            key="extract_format_mode",
        )

        if format_mode == "シンプル（カンマ区切り）":
            fields = st.text_input("抽出フィールド（カンマ区切り）", key="extract_fields")
        else:
            default_schema = '''{
  "schema": {
    "type": "object",
    "properties": {
      "store_name": {
        "description": "店舗名",
        "type": "string"
      },
      "items_table": {
        "description": "購入した商品の明細",
        "type": "object",
        "column_ordering": ["item_name", "unit_price", "quantity"],
        "properties": {
          "item_name": {
            "description": "商品名",
            "type": "array"
          },
          "unit_price": {
            "description": "単価",
            "type": "array"
          },
          "quantity": {
            "description": "数量",
            "type": "array"
          }
        }
      },
      "total": {
        "description": "合計金額",
        "type": "string"
      }
    }
  }
}'''
            fields = None
            schema_text = st.text_area(
                "responseFormat (JSON)",
                value=default_schema,
                height=300,
                key="extract_schema",
            )

        if st.button("実行", key="extract_run", type="primary"):
            if format_mode == "シンプル（カンマ区切り）":
                if fields:
                    with st.spinner("実行中..."):
                        field_list = ", ".join([f"'{f.strip()}'" for f in fields.split(",")])
                        if use_file and file_name:
                            sql = f"SELECT AI_EXTRACT(TO_FILE('{STAGE}', '{file_name}'), [{field_list}]) AS result"
                        else:
                            escaped_text = text.replace("'", "''")
                            sql = f"SELECT AI_EXTRACT('{escaped_text}', [{field_list}]) AS result"
                        result, err = run_ai_sql(sql)
                        if err:
                            st.error(f"エラー: {err}")
                        else:
                            st.json(result[0]["RESULT"])
                else:
                    st.warning("抽出フィールドを入力してください")
            else:
                if schema_text and schema_text.strip():
                    import json as _json
                    try:
                        _json.loads(schema_text)
                    except _json.JSONDecodeError as e:
                        st.error(f"JSONが不正です: {e}")
                        return
                    with st.spinner("実行中..."):
                        escaped_schema = schema_text.replace("'", "''")
                        if use_file and file_name:
                            sql = f"SELECT AI_EXTRACT(file => TO_FILE('{STAGE}', '{file_name}'), responseFormat => PARSE_JSON('{escaped_schema}')) AS result"
                        else:
                            escaped_text = text.replace("'", "''")
                            sql = f"SELECT AI_EXTRACT(text => '{escaped_text}', responseFormat => PARSE_JSON('{escaped_schema}')) AS result"
                        result, err = run_ai_sql(sql)
                        if err:
                            st.error(f"エラー: {err}")
                        else:
                            st.json(result[0]["RESULT"])
                else:
                    st.warning("responseFormat を入力してください")


def render_ai_sentiment():
    render_function_header("AI_SENTIMENT")
    info = FUNCTION_DOCS["AI_SENTIMENT"]

    def apply_example(ex):
        st.session_state.sentiment_text = ex["input"]

    render_example_buttons(info["examples"], apply_example, "sentiment")

    with st.container():
        text = st.text_area("テキスト", height=100, key="sentiment_text")
        categories_input = st.text_input("カテゴリ（カンマ区切り、任意）", key="sentiment_categories", placeholder="例: cost, quality, service, wait time")

        if st.button("実行", key="sentiment_run", type="primary"):
            if text:
                with st.spinner("実行中..."):
                    escaped_text = text.replace("'", "''")
                    if categories_input.strip():
                        cat_list = ", ".join([f"'{c.strip()}'" for c in categories_input.split(",")])
                        sql = f"SELECT AI_SENTIMENT('{escaped_text}', [{cat_list}]) AS result"
                    else:
                        sql = f"SELECT AI_SENTIMENT('{escaped_text}') AS result"
                    result, err = run_ai_sql(sql)
                    if err:
                        st.error(f"エラー: {err}")
                    else:
                        raw = result[0]["RESULT"]
                        if isinstance(raw, str):
                            import json as _json
                            parsed = _json.loads(raw)
                        else:
                            parsed = raw
                        categories = parsed.get("categories", [])
                        for cat in categories:
                            name = cat.get("name", "")
                            sentiment = cat.get("sentiment", "")
                            col1, col2 = st.columns([1, 2])
                            with col1:
                                st.markdown(f"**{name}**")
                            with col2:
                                if sentiment == "positive":
                                    st.success(f"😊 {sentiment}")
                                elif sentiment == "negative":
                                    st.error(f"😞 {sentiment}")
                                elif sentiment == "mixed":
                                    st.warning(f"🤔 {sentiment}")
                                elif sentiment == "unknown":
                                    st.caption(f"❓ {sentiment}")
                                else:
                                    st.info(f"😐 {sentiment}")
            else:
                st.warning("テキストを入力してください")


def render_ai_summarize():
    render_function_header("AI_SUMMARIZE_AGG")
    info = FUNCTION_DOCS["AI_SUMMARIZE_AGG"]

    def apply_example(ex):
        st.session_state.summarize_text = ex["input"]

    render_example_buttons(info["examples"], apply_example, "summarize")

    with st.container():
        text = st.text_area("テキスト", height=200, key="summarize_text")

        if st.button("実行", key="summarize_run", type="primary"):
            if text:
                with st.spinner("実行中..."):
                    escaped_text = text.replace("'", "''")
                    sql = f"SELECT AI_SUMMARIZE_AGG('{escaped_text}') AS result"
                    result, err = run_ai_sql(sql)
                    if err:
                        st.error(f"エラー: {err}")
                    else:
                        st.markdown(result[0]["RESULT"])
            else:
                st.warning("テキストを入力してください")


def render_ai_translate():
    render_function_header("AI_TRANSLATE")
    info = FUNCTION_DOCS["AI_TRANSLATE"]

    def apply_example(ex):
        st.session_state.translate_text = ex["input"]
        st.session_state.translate_from = ex["from_lang"]
        st.session_state.translate_to = ex["to_lang"]

    render_example_buttons(info["examples"], apply_example, "translate")

    with st.container():
        text = st.text_area("テキスト", height=100, key="translate_text")
        col1, col2 = st.columns(2)
        with col1:
            if "translate_from" not in st.session_state:
                st.session_state.translate_from = "en"
            from_lang = st.text_input("翻訳元言語", key="translate_from")
        with col2:
            if "translate_to" not in st.session_state:
                st.session_state.translate_to = "ja"
            to_lang = st.text_input("翻訳先言語", key="translate_to")

        if st.button("実行", key="translate_run", type="primary"):
            if text:
                with st.spinner("実行中..."):
                    escaped_text = text.replace("'", "''")
                    sql = f"SELECT AI_TRANSLATE('{escaped_text}', '{from_lang}', '{to_lang}') AS result"
                    result, err = run_ai_sql(sql)
                    if err:
                        st.error(f"エラー: {err}")
                    else:
                        st.markdown(result[0]["RESULT"])
            else:
                st.warning("テキストを入力してください")


def render_ai_complete():
    render_function_header("AI_COMPLETE")
    info = FUNCTION_DOCS["AI_COMPLETE"]

    def apply_example(ex):
        st.session_state.complete_prompt = ex["input"]
        st.session_state.complete_model = ex.get("model", "claude-sonnet-4-6")
        st.session_state.complete_use_image = False
        st.session_state.complete_structured = ex.get("structured", False)

    render_example_buttons(info["examples"], apply_example, "complete")

    with st.container():
        if "complete_model" not in st.session_state:
            st.session_state.complete_model = "claude-sonnet-4-6"
        model = st.selectbox("モデル", AVAILABLE_MODELS, key="complete_model")

        col_toggle1, col_toggle2 = st.columns(2)
        with col_toggle1:
            use_image = st.toggle("画像を入力に含める", key="complete_use_image")
        with col_toggle2:
            use_structured = st.toggle("構造化出力 (JSON)", key="complete_structured")

        image_file = None
        if use_image:
            image_file = render_image_input("complete")

        prompt = st.text_area("プロンプト", height=200, key="complete_prompt")

        schema_json = ""
        if use_structured:
            st.caption("出力のJSONスキーマを定義してください")
            default_schema = json.dumps({
                "type": "object",
                "properties": {
                    "product_name": {"type": "string"},
                    "price": {"type": "number"},
                    "rating": {"type": "number"}
                },
                "required": ["product_name", "price", "rating"]
            }, indent=2, ensure_ascii=False)
            schema_json = st.text_area("JSON Schema", value=default_schema, height=150, key="complete_schema")

        if st.button("実行", key="complete_run", type="primary"):
            if prompt:
                if use_image and use_structured:
                    st.error("画像入力と構造化出力（JSON Schema）は AI_COMPLETE で同時に利用できません。どちらか一方のトグルを OFF にしてください。")
                    return
                if use_image and not image_file:
                    st.warning("画像を選択してください")
                    return
                with st.spinner("実行中..."):
                    escaped_prompt = prompt.replace("'", "''")
                    if use_structured and schema_json:
                        try:
                            schema_obj = json.loads(schema_json)
                            opts = json.dumps({"response_format": {"type": "json", "schema": schema_obj}}, ensure_ascii=False)
                            escaped_opts = opts.replace("'", "''")
                            sql = f"SELECT AI_COMPLETE('{model}', '{escaped_prompt}', PARSE_JSON('{escaped_opts}')) AS result"
                        except json.JSONDecodeError:
                            st.error("無効なJSONスキーマです")
                            return
                    elif use_image and image_file:
                        sql = (
                            f"SELECT AI_COMPLETE('{model}', "
                            f"PROMPT('{escaped_prompt} {{0}}', TO_FILE('{STAGE}', '{image_file}'))) AS result"
                        )
                    else:
                        sql = f"SELECT AI_COMPLETE('{model}', '{escaped_prompt}') AS result"
                    result, err = run_ai_sql(sql)
                    if err:
                        st.error(f"エラー: {err}")
                    else:
                        output = result[0]["RESULT"]
                        if use_structured:
                            st.json(output)
                        else:
                            if isinstance(output, str):
                                text = output.strip().strip('"')
                                text = text.replace("\\n", "\n").replace("\\t", "\t")
                                st.markdown(text)
                            else:
                                st.markdown(str(output))
            else:
                st.warning("プロンプトを入力してください")


def render_ai_parse_document():
    render_function_header("AI_PARSE_DOCUMENT")
    info = FUNCTION_DOCS["AI_PARSE_DOCUMENT"]

    with st.container():
        input_mode = st.radio(
            "入力方法",
            ["ステージから選択", "テストファイル登録へ"],
            horizontal=True,
            key="parse_input_mode",
        )

        file_name = None
        if input_mode == "テストファイル登録へ":
            st.info("サイドバーの「テストファイル登録」からファイルをアップロードしてください。")
            if st.button("テストファイル登録を開く", key="parse_goto_stage"):
                st.session_state.show_stage_browser = True
                st.rerun()
        else:
            files = list_stage_files()
            if files:
                metadata = get_file_metadata()
                display_names = [f"{f} ({metadata.get(f, {}).get('description', '')})" if metadata.get(f, {}).get('description') else f for f in files]
                selected_idx = st.selectbox("ステージ上のファイル", range(len(files)), format_func=lambda i: display_names[i], key="parse_stage_select")
                file_name = files[selected_idx]
            else:
                st.info("ステージにファイルがありません")

        mode = st.radio("解析モード", ["OCR", "LAYOUT"], horizontal=True, key="parse_mode")
        if mode == "OCR":
            st.caption("スキャン文書・手書き・シンプルなテキスト抽出向け（低コスト: 0.5 credits/1000ページ）")
        else:
            st.caption("テーブル・フォーム・構造保持が必要なデジタルPDF向け。Markdown形式で出力（3.33 credits/1000ページ）")
        extract_images = False
        show_markdown = False
        if mode == "LAYOUT":
            col_opt1, col_opt2 = st.columns(2)
            with col_opt1:
                extract_images = st.toggle("画像を抽出する", key="parse_extract_images")
            with col_opt2:
                show_markdown = st.toggle("Markdown表示", key="parse_show_md")

        if file_name:
            if st.button("解析実行", key="parse_run", type="primary"):
                with st.spinner("実行中..."):
                    opts = f"'mode': '{mode}'"
                    if extract_images:
                        opts += ", 'extract_images': true"
                    sql = f"SELECT AI_PARSE_DOCUMENT(TO_FILE('{STAGE}', '{file_name}'), {{{opts}}}) AS result"
                    result, err = run_ai_sql(sql)
                    if err:
                        st.error(f"エラー: {err}")
                    else:
                        raw = result[0]["RESULT"]
                        parsed = json.loads(raw) if isinstance(raw, str) else raw
                        if show_markdown and mode == "LAYOUT":
                            try:
                                content = parsed.get("content", "")
                                if content:
                                    st.markdown(content)
                                else:
                                    st.json(raw)
                            except Exception:
                                st.json(raw)
                        else:
                            st.json(raw)
                        if extract_images and isinstance(parsed, dict):
                            images = parsed.get("images", [])
                            if images:
                                st.markdown(f"**抽出画像: {len(images)}件**")
                                for i, img in enumerate(images):
                                    img_b64 = img.get("image_base64", "")
                                    if img_b64:
                                        img_b64_clean = img_b64.split(",")[-1] if "," in img_b64 else img_b64
                                        img_html = f'<img src="data:image/png;base64,{img_b64_clean}" style="max-width:100%;" />'
                                        st.markdown(f"**Image {i+1}** (ID: {img.get('id', 'N/A')})")
                                        st.markdown(img_html, unsafe_allow_html=True)
                            else:
                                st.info("画像は検出されませんでした")
        else:
            st.info("ファイルを選択またはアップロードしてください")


def render_ai_redact():
    render_function_header("AI_REDACT")
    info = FUNCTION_DOCS["AI_REDACT"]

    def apply_example(ex):
        st.session_state.redact_text = ex["input"]

    render_example_buttons(info["examples"], apply_example, "redact")

    with st.container():
        text = st.text_area("テキスト", height=100, key="redact_text")

        if st.button("実行", key="redact_run", type="primary"):
            if text:
                with st.spinner("実行中..."):
                    escaped_text = text.replace("'", "''")
                    sql = f"SELECT AI_REDACT('{escaped_text}') AS result"
                    result, err = run_ai_sql(sql)
                    if err:
                        st.error(f"エラー: {err}")
                    else:
                        col1, col2 = st.columns(2)
                        with col1:
                            st.markdown("**入力テキスト**")
                            st.text(text)
                        with col2:
                            st.markdown("**マスキング結果**")
                            st.text(result[0]["RESULT"])
            else:
                st.warning("テキストを入力してください")


def render_ai_embed():
    render_function_header("AI_EMBED")
    info = FUNCTION_DOCS["AI_EMBED"]

    def apply_example(ex):
        st.session_state.embed_text = ex["input"]
        st.session_state.embed_model = ex.get("model", "snowflake-arctic-embed-l-v2.0")

    render_example_buttons(info["examples"], apply_example, "embed")

    with st.container():
        if "embed_model" not in st.session_state:
            st.session_state.embed_model = "snowflake-arctic-embed-l-v2.0"
        model = st.selectbox("モデル", EMBED_MODELS, key="embed_model")
        text = st.text_area("テキスト", height=100, key="embed_text")

        if st.button("実行", key="embed_run", type="primary"):
            if text:
                with st.spinner("実行中..."):
                    escaped_text = text.replace("'", "''")
                    sql = f"SELECT AI_EMBED('{model}', '{escaped_text}') AS result"
                    result, err = run_ai_sql(sql)
                    if err:
                        st.error(f"エラー: {err}")
                    else:
                        vec = result[0]["RESULT"]
                        st.metric("次元数", f"{len(vec) if isinstance(vec, (list, str)) else 'N/A'}")
                        with st.expander("全ベクトル表示"):
                            st.write(vec)
            else:
                st.warning("テキストを入力してください")


with st.sidebar:
    st.markdown("## ❄️ Cortex AI Playground")
    st.caption("Snowflake Cortex AI 関数をインタラクティブに試せるツール")
    selected_page = st.selectbox("関数を選択", FUNCTIONS, key="nav_function")
    if st.button("テストファイル登録", key="show_stage", use_container_width=True):
        st.session_state.show_stage_browser = not st.session_state.get("show_stage_browser", False)
    debug_mode = st.toggle("デバッグモード", value=False)
    st.markdown("<small>Powered by Cortex Code</small>", unsafe_allow_html=True)

if "prev_function" not in st.session_state:
    st.session_state.prev_function = selected_page
if st.session_state.prev_function != selected_page:
    st.session_state.show_stage_browser = False
    st.session_state.prev_function = selected_page

RENDER_MAP = {
    "AI_CLASSIFY": render_ai_classify,
    "AI_FILTER": render_ai_filter,
    "AI_EXTRACT": render_ai_extract,
    "AI_SENTIMENT": render_ai_sentiment,
    "AI_SUMMARIZE_AGG": render_ai_summarize,
    "AI_TRANSLATE": render_ai_translate,
    "AI_COMPLETE": render_ai_complete,
    "AI_PARSE_DOCUMENT": render_ai_parse_document,
    "AI_REDACT": render_ai_redact,
    "AI_EMBED": render_ai_embed,
}

if st.session_state.get("show_stage_browser", False):
    col_title, col_close = st.columns([6, 1])
    with col_title:
        st.markdown('<p class="function-header">テストファイル登録</p>', unsafe_allow_html=True)
    with col_close:
        if st.button("<<", key="close_stage"):
            st.session_state.show_stage_browser = False
            st.rerun()
    st.markdown(f"`{STAGE}`")
    with st.spinner("ファイル一覧を取得中..."):
        try:
            rows = session.sql(f"LIST {STAGE}").collect()
            metadata = get_file_metadata()
            if rows:
                data = []
                for row in rows:
                    name = row["name"].split("/")[-1] if row["name"] else ""
                    size_val = row["size"] if "size" in row.asDict() else 0
                    size_kb = round(size_val / 1024, 1) if size_val else 0
                    meta = metadata.get(name, {})
                    data.append({
                        "filename": name,
                        "size_kb": size_kb,
                        "description": meta.get("description", ""),
                        "source_url": meta.get("source_url", ""),
                    })
                df = pd.DataFrame(data)
                st.dataframe(df, use_container_width=True)
                st.caption(f"合計: {len(rows)} ファイル")

                del_files = [d["filename"] for d in data if d["filename"]]
                del_file = st.selectbox("削除するファイル", ["（選択してください）"] + del_files, key="stage_del_select")
                if del_file and del_file != "（選択してください）":
                    if st.button("削除", key="do_delete", type="secondary"):
                        try:
                            session.sql(f"REMOVE '{STAGE}/{del_file}'").collect()
                            session.sql(f"DELETE FROM {METADATA_TABLE} WHERE filename = '{del_file}'").collect()
                            st.success(f"'{del_file}' を削除しました")
                            st.rerun()
                        except Exception as e:
                            st.error(f"削除エラー: {e}")
            else:
                st.info("ステージにファイルがありません")
        except Exception as e:
            st.error(f"エラー: {e}")

    st.markdown("---")
    st.subheader("ファイル登録")
    if "upload_counter" not in st.session_state:
        st.session_state.upload_counter = 0
    uc = st.session_state.upload_counter
    uploaded = st.file_uploader("ファイルをアップロード", type=["pdf", "png", "jpg", "jpeg", "docx", "pptx", "tiff"], key=f"stage_upload_{uc}")
    upload_desc = st.text_input("説明（必須）", key=f"upload_desc_{uc}", placeholder="例: 2024年売上レポート")
    upload_url = st.text_input("ソースURL（任意）", key=f"upload_url_{uc}", placeholder="例: https://example.com/report.pdf")
    if uploaded:
        if not upload_desc:
            st.warning("説明を入力してください")
        elif st.button("登録", key="do_upload", type="primary"):
            with st.spinner("ファイルを登録中..."):
                fname = upload_file_to_stage(uploaded, description=upload_desc, source_url=upload_url)
            st.success(f"'{fname}' を登録しました")
            st.session_state.upload_counter += 1
            st.rerun()
else:
    RENDER_MAP[selected_page]()
