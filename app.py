"""
SOEL アナリティクスダッシュボード
店サポ用 - SNS→来店アトリビューション可視化

Usage:
    streamlit run app.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
import google.auth
from datetime import datetime

# ──────────────────────────────────────────────
# 設定
# ──────────────────────────────────────────────
PROJECT_ID = "braided-storm-479305-j8"
DATASET_GA4 = "analytics_513983029"
DATASET_META = "temp_meta_data"

# ──────────────────────────────────────────────
# 認証
# ──────────────────────────────────────────────
@st.cache_resource
def get_bq_client():
    from google.oauth2 import service_account
    import os

    # Streamlit Cloud: st.secrets にサービスアカウント情報がある場合
    try:
        if "gcp_service_account" in st.secrets:
            creds = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"],
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            return bigquery.Client(project=PROJECT_ID, credentials=creds)
    except Exception:
        pass

    # ローカル: サービスアカウントJSONファイル
    sa_path = os.path.join(os.path.dirname(__file__), "service_account.json")
    if os.path.exists(sa_path):
        creds = service_account.Credentials.from_service_account_file(
            sa_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return bigquery.Client(project=PROJECT_ID, credentials=creds)

    # フォールバック: gcloud ADC
    creds, project = google.auth.default()
    return bigquery.Client(project=PROJECT_ID, credentials=creds)


# ──────────────────────────────────────────────
# データ取得
# ──────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_cv_data() -> pd.DataFrame:
    """GA4 CVイベント（v_click_parsed全件）"""
    client = get_bq_client()
    query = f"""
    SELECT
        event_date,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        creative_name,
        link_domain,
        user_pseudo_id,
        ga_session_id
    FROM `{PROJECT_ID}.{DATASET_GA4}.v_click_parsed`
    WHERE link_url IS NOT NULL
    ORDER BY event_date DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=3600)
def load_meta_daily() -> pd.DataFrame:
    """Meta広告 日別パフォーマンス"""
    client = get_bq_client()
    query = f"""
    SELECT date, content_name, impressions, clicks, cv_count
    FROM `{PROJECT_ID}.{DATASET_META}.view1_daily_performance`
    ORDER BY date DESC
    """
    try:
        return client.query(query).to_dataframe()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_creatives_master() -> pd.DataFrame:
    """Metaクリエイティブマスター"""
    client = get_bq_client()
    query = f"""
    SELECT creative_name, campaign, ad_set, utm_content, utm_campaign, post_url
    FROM `{PROJECT_ID}.{DATASET_GA4}.m_meta_creatives`
    """
    try:
        return client.query(query).to_dataframe()
    except Exception:
        return pd.DataFrame()


# ──────────────────────────────────────────────
# ページ設定
# ──────────────────────────────────────────────
st.set_page_config(
    page_title="SOEL Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    [data-testid="stMetric"] {
        background: #f8f9fa;
        border-radius: 8px;
        padding: 12px 16px;
        border-left: 4px solid #1a73e8;
    }
</style>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
# サイドバー
# ──────────────────────────────────────────────
with st.sidebar:
    st.title("📊 SOEL Analytics")
    st.caption("一家ダイニング 採用LP アトリビューション")
    st.divider()

    page = st.radio(
        "ページ",
        ["🏠 概要", "🎬 クリエイティブ別", "📡 Meta広告", "🔀 ファネル", "🗂 生データ"],
        label_visibility="collapsed",
    )


# ──────────────────────────────────────────────
# データロード
# ──────────────────────────────────────────────
with st.spinner("BigQueryからデータを取得中..."):
    try:
        df_raw = load_cv_data()
        meta_df = load_meta_daily()
        creatives_df = load_creatives_master()
    except Exception as e:
        st.error(f"BigQuery接続エラー: {e}")
        st.info("gcloud auth application-default login を実行してください。")
        st.stop()

if df_raw.empty:
    st.warning("データが見つかりませんでした。")
    st.stop()

# 日付変換・前処理
df_raw["date"] = pd.to_datetime(df_raw["event_date"], format="%Y%m%d")
df_raw["creative_label"] = df_raw["creative_name"].fillna("(オーガニック/UTM未設定)")
df_raw.loc[df_raw["creative_label"] == "N/A", "creative_label"] = "(オーガニック/UTM未設定)"

# Meta日付変換
if not meta_df.empty:
    meta_df["date"] = pd.to_datetime(meta_df["date"])


# ──────────────────────────────────────────────
# ページ: 概要
# ──────────────────────────────────────────────
if page == "🏠 概要":
    st.title("概要ダッシュボード")
    st.caption("一家ダイニング採用LP / GA4 CVデータ + Meta広告データ")

    # KPIカード
    total_cv = len(df_raw)
    total_users = df_raw["user_pseudo_id"].nunique()
    total_sessions = df_raw["ga_session_id"].nunique()
    utm_rate = (df_raw["utm_source"].notna().sum() / total_cv * 100) if total_cv > 0 else 0

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("総CV数", f"{total_cv:,}")
    with col2:
        st.metric("ユニークユーザー", f"{total_users:,}")
    with col3:
        st.metric("セッション数", f"{total_sessions:,}")
    with col4:
        st.metric("UTM付与率", f"{utm_rate:.0f}%")

    # Meta広告KPI
    if not meta_df.empty:
        st.divider()
        total_imp = meta_df["impressions"].sum()
        total_clicks = meta_df["clicks"].sum()
        ctr = (total_clicks / total_imp * 100) if total_imp > 0 else 0
        # 広告クリック→CV率（GA4 CV / Meta clicks）
        ad_cvr = (total_cv / total_clicks * 100) if total_clicks > 0 else 0

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Meta 総インプレッション", f"{total_imp:,}")
        with col2:
            st.metric("Meta 総クリック", f"{total_clicks:,}")
        with col3:
            st.metric("CTR", f"{ctr:.2f}%")
        with col4:
            st.metric("クリック→CV率", f"{ad_cvr:.2f}%")

    st.divider()

    # 日別CV推移
    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown("#### 日別CV推移（GA4）")
        daily = df_raw.groupby("date").size().reset_index(name="CV数")
        daily.columns = ["日付", "CV数"]
        fig = px.bar(daily, x="日付", y="CV数", color_discrete_sequence=["#1a73e8"])
        fig.update_layout(margin=dict(t=10, b=10), height=300)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("#### 遷移先内訳")
        domain_df = df_raw.groupby("link_domain").size().reset_index(name="CV数")
        domain_df = domain_df.sort_values("CV数", ascending=False)
        fig2 = px.pie(domain_df, values="CV数", names="link_domain",
                      color_discrete_sequence=px.colors.qualitative.Set2)
        fig2.update_layout(margin=dict(t=10, b=10), height=300, showlegend=True)
        st.plotly_chart(fig2, use_container_width=True)

    # 広告 vs オーガニック
    st.markdown("#### 広告 vs オーガニック")
    df_raw["流入タイプ"] = df_raw["utm_source"].apply(lambda x: "Meta広告" if x == "meta" else ("オーガニック/不明" if pd.isna(x) else str(x)))
    type_df = df_raw.groupby("流入タイプ").size().reset_index(name="CV数")
    fig3 = px.pie(type_df, values="CV数", names="流入タイプ",
                  color_discrete_map={"Meta広告": "#1877F2", "オーガニック/不明": "#e0e0e0"})
    fig3.update_layout(margin=dict(t=10, b=10), height=250)
    st.plotly_chart(fig3, use_container_width=True)


# ──────────────────────────────────────────────
# ページ: クリエイティブ別
# ──────────────────────────────────────────────
elif page == "🎬 クリエイティブ別":
    st.title("クリエイティブ（動画）別パフォーマンス")
    st.caption("GA4 CVデータ / どの動画コンテンツが採用サイト遷移に貢献したか")

    # クリエイティブ別集計
    content_df = (
        df_raw.groupby("creative_label")
        .agg(
            CV数=("event_date", "size"),
            ユニークユーザー=("user_pseudo_id", "nunique"),
            セッション=("ga_session_id", "nunique"),
        )
        .reset_index()
        .sort_values("CV数", ascending=False)
    )
    content_df["CV割合(%)"] = (content_df["CV数"] / content_df["CV数"].sum() * 100).round(1)

    # 広告経由のみ（UTMあり）
    ad_only = content_df[content_df["creative_label"] != "(オーガニック/UTM未設定)"]

    if not ad_only.empty:
        st.markdown("#### 広告クリエイティブ別CV数")
        fig = px.bar(
            ad_only.sort_values("CV数"),
            x="CV数", y="creative_label",
            orientation="h",
            labels={"creative_label": "クリエイティブ名"},
            color="CV数",
            color_continuous_scale="Blues",
        )
        fig.update_layout(height=max(300, len(ad_only) * 50), margin=dict(t=10))
        st.plotly_chart(fig, use_container_width=True)

    # クリエイティブ × 日付ヒートマップ
    st.markdown("#### クリエイティブ × 日付（CV発生状況）")
    ad_raw = df_raw[df_raw["creative_label"] != "(オーガニック/UTM未設定)"]
    if not ad_raw.empty:
        heatmap_data = ad_raw.groupby(["creative_label", "date"]).size().reset_index(name="CV数")
        heatmap_pivot = heatmap_data.pivot(index="creative_label", columns="date", values="CV数").fillna(0)
        fig_heat = px.imshow(
            heatmap_pivot,
            labels=dict(x="日付", y="クリエイティブ", color="CV数"),
            color_continuous_scale="Blues",
            aspect="auto",
        )
        fig_heat.update_layout(height=max(250, len(heatmap_pivot) * 40), margin=dict(t=10))
        st.plotly_chart(fig_heat, use_container_width=True)

    # テーブル
    st.markdown("#### 全クリエイティブ一覧")
    content_df.columns = ["クリエイティブ名", "CV数", "ユニークユーザー", "セッション", "CV割合(%)"]
    st.dataframe(content_df, use_container_width=True, hide_index=True)

    # クリエイティブマスター（Instagram投稿リンク付き）
    if not creatives_df.empty:
        st.divider()
        st.markdown("#### クリエイティブマスター（Meta設定）")
        st.dataframe(creatives_df, use_container_width=True, hide_index=True)


# ──────────────────────────────────────────────
# ページ: Meta広告
# ──────────────────────────────────────────────
elif page == "📡 Meta広告":
    st.title("Meta広告パフォーマンス")
    st.caption("temp_meta_data.view1_daily_performance / キャンペーン別パフォーマンス")

    if meta_df.empty:
        st.warning("Meta広告データがありません。")
        st.stop()

    # キャンペーン別集計
    camp_summary = (
        meta_df.groupby("content_name")
        .agg(
            日数=("date", "nunique"),
            インプレッション=("impressions", "sum"),
            クリック=("clicks", "sum"),
        )
        .reset_index()
    )
    camp_summary["CTR(%)"] = (camp_summary["クリック"] / camp_summary["インプレッション"] * 100).round(2)
    camp_summary = camp_summary.sort_values("インプレッション", ascending=False)
    # テスト・ダミーデータを除外
    camp_summary = camp_summary[~camp_summary["content_name"].str.contains("Test|沙盒|ダミー", na=False)]

    st.markdown("#### キャンペーン別サマリー")
    st.dataframe(camp_summary, use_container_width=True, hide_index=True)

    st.divider()

    # 日別推移（インプレッション vs クリック）
    st.markdown("#### 日別推移")
    daily_meta = meta_df.groupby("date").agg(
        インプレッション=("impressions", "sum"),
        クリック=("clicks", "sum"),
    ).reset_index()

    fig = go.Figure()
    fig.add_trace(go.Bar(x=daily_meta["date"], y=daily_meta["インプレッション"],
                         name="インプレッション", marker_color="#1877F2", opacity=0.6))
    fig.add_trace(go.Scatter(x=daily_meta["date"], y=daily_meta["クリック"],
                             name="クリック", line=dict(color="#ea4335", width=2), yaxis="y2"))
    fig.update_layout(
        yaxis=dict(title="インプレッション"),
        yaxis2=dict(title="クリック", overlaying="y", side="right"),
        margin=dict(t=10, b=10), height=350,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    st.plotly_chart(fig, use_container_width=True)

    # キャンペーン別日別推移
    st.markdown("#### キャンペーン別 クリック推移")
    # 主要キャンペーンのみ
    main_camps = camp_summary.head(4)["content_name"].tolist()
    camp_daily = meta_df[meta_df["content_name"].isin(main_camps)]
    fig2 = px.line(camp_daily, x="date", y="clicks", color="content_name",
                   labels={"clicks": "クリック", "date": "日付", "content_name": "キャンペーン"})
    fig2.update_layout(margin=dict(t=10, b=10), height=350)
    st.plotly_chart(fig2, use_container_width=True)


# ──────────────────────────────────────────────
# ページ: ファネル
# ──────────────────────────────────────────────
elif page == "🔀 ファネル":
    st.title("ファネル分析")
    st.caption("Meta広告 インプレッション → クリック → 採用サイトCV")

    if meta_df.empty:
        st.warning("Meta広告データが必要です。")
        st.stop()

    total_imp = meta_df["impressions"].sum()
    total_clicks = meta_df["clicks"].sum()
    total_cv = len(df_raw[df_raw["utm_source"] == "meta"])  # 広告経由CVのみ

    # ファネルチャート
    fig = go.Figure(go.Funnel(
        y=["インプレッション", "クリック（LP訪問）", "CV（採用サイト遷移）"],
        x=[total_imp, total_clicks, total_cv],
        textinfo="value+percent initial",
        marker=dict(color=["#1877F2", "#34a853", "#ea4335"]),
    ))
    fig.update_layout(margin=dict(t=30, b=10), height=400)
    st.plotly_chart(fig, use_container_width=True)

    # ファネル数値
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("インプレッション→クリック率", f"{total_clicks/total_imp*100:.2f}%" if total_imp else "N/A")
    with col2:
        st.metric("クリック→CV率", f"{total_cv/total_clicks*100:.2f}%" if total_clicks else "N/A")
    with col3:
        st.metric("インプレッション→CV率", f"{total_cv/total_imp*100:.4f}%" if total_imp else "N/A")

    st.divider()

    # クリエイティブ別ファネル（広告経由CVのみ）
    st.markdown("#### クリエイティブ別 CV貢献度")
    ad_cv = (
        df_raw[df_raw["utm_source"] == "meta"]
        .groupby("creative_label")
        .agg(CV数=("event_date", "size"), UU=("user_pseudo_id", "nunique"))
        .reset_index()
        .sort_values("CV数", ascending=False)
    )
    if not ad_cv.empty:
        ad_cv["CV占有率(%)"] = (ad_cv["CV数"] / ad_cv["CV数"].sum() * 100).round(1)
        st.dataframe(ad_cv, use_container_width=True, hide_index=True)

        fig2 = px.pie(ad_cv, values="CV数", names="creative_label",
                      color_discrete_sequence=px.colors.qualitative.Set2)
        fig2.update_layout(margin=dict(t=10, b=10), height=300)
        st.plotly_chart(fig2, use_container_width=True)


# ──────────────────────────────────────────────
# ページ: 生データ
# ──────────────────────────────────────────────
elif page == "🗂 生データ":
    st.title("生データ")

    tab1, tab2, tab3 = st.tabs(["GA4 CVデータ", "Meta広告データ", "クリエイティブマスター"])

    with tab1:
        st.caption(f"v_click_parsed / {len(df_raw):,} 件")

        # フィルター
        col1, col2, col3 = st.columns(3)
        with col1:
            sources = ["すべて"] + sorted(df_raw["utm_source"].dropna().unique().tolist())
            sel_source = st.selectbox("UTMソース", sources)
        with col2:
            domains = ["すべて"] + sorted(df_raw["link_domain"].dropna().unique().tolist())
            sel_domain = st.selectbox("遷移先ドメイン", domains)
        with col3:
            creatives = ["すべて"] + sorted(df_raw["creative_label"].unique().tolist())
            sel_creative = st.selectbox("クリエイティブ", creatives)

        filtered = df_raw.copy()
        if sel_source != "すべて":
            filtered = filtered[filtered["utm_source"] == sel_source]
        if sel_domain != "すべて":
            filtered = filtered[filtered["link_domain"] == sel_domain]
        if sel_creative != "すべて":
            filtered = filtered[filtered["creative_label"] == sel_creative]

        st.dataframe(filtered, use_container_width=True, hide_index=True)
        st.download_button(
            "CSVダウンロード",
            filtered.to_csv(index=False).encode("utf-8"),
            file_name=f"ga4_cv_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )

    with tab2:
        if not meta_df.empty:
            st.caption(f"view1_daily_performance / {len(meta_df):,} 件")
            st.dataframe(meta_df, use_container_width=True, hide_index=True)
            st.download_button(
                "CSVダウンロード",
                meta_df.to_csv(index=False).encode("utf-8"),
                file_name=f"meta_daily_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
                key="meta_csv",
            )
        else:
            st.info("Meta広告データがありません。")

    with tab3:
        if not creatives_df.empty:
            st.caption(f"m_meta_creatives / {len(creatives_df):,} 件")
            st.dataframe(creatives_df, use_container_width=True, hide_index=True)
        else:
            st.info("クリエイティブマスターがありません。")
