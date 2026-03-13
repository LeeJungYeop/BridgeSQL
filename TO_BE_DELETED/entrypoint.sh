#!/bin/bash
set -e

CATALOG_DIR="/app/data/catalog"

# 카탈로그 없으면 자동 프로파일링
if [ -z "$(ls -A $CATALOG_DIR 2>/dev/null)" ]; then
    echo "카탈로그가 없습니다. 프로파일링을 시작합니다..."
    python -m semantic_engine.cli profile
    echo "프로파일링 완료."
else
    echo "기존 카탈로그를 불러옵니다."
fi

# MCP 서버 실행
exec python -m semantic_engine.mcp.server
