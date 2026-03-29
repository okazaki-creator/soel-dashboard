#!/bin/bash
# SOEL Analytics Dashboard セットアップ

echo "📦 パッケージをインストール中..."
pip3 install google-cloud-bigquery google-auth-oauthlib streamlit plotly pandas --quiet

echo ""
echo "📋 credentials.json のコピー..."
if [ ! -f credentials.json ]; then
    cp ../soel-ai-secretary/credentials.json . 2>/dev/null || echo "⚠️  credentials.json を手動でコピーしてください"
fi

echo ""
echo "✅ セットアップ完了！"
echo ""
echo "起動コマンド:"
echo "  streamlit run app.py"
echo ""
echo "初回起動時にブラウザでGoogle認証が求められます。"
echo "okazaki@soel-tokyo.jp でログインしてください。"
