"""
Slack Bot for Semantic Query Engine
Slack에서 자연어 쿼리를 처리하는 봇입니다.
"""

import os
import asyncio
from slack_bolt.async_app import AsyncApp
from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler

from semantic_engine.config import get_settings
from semantic_engine.profiler.connector import DatabaseConnector
from semantic_engine.rag.retriever import SemanticRetriever
from semantic_engine.engine.intent_guard import IntentGuard
from semantic_engine.engine.sql_generator import SQLGenerator, format_schema_for_prompt
from semantic_engine.engine.validator import SQLValidator
from semantic_engine.semantic.catalog import SemanticCatalog


# Slack App 초기화
app = AsyncApp(
    token=os.environ.get("SLACK_BOT_TOKEN"),
    signing_secret=os.environ.get("SLACK_SIGNING_SECRET")
)


# 전역 컴포넌트
connector = None
retriever = None
schema = None


def init_components():
    """컴포넌트 초기화"""
    global connector, retriever, schema
    
    connector = DatabaseConnector()
    retriever = SemanticRetriever()
    
    catalog = SemanticCatalog()
    db_name = connector.get_database_name()
    schema = catalog.load(db_name)


@app.event("app_mention")
async def handle_mention(event, say):
    """멘션 처리 - 자연어 쿼리"""
    
    if schema is None:
        init_components()
    
    # 멘션 텍스트에서 봇 ID 제거
    text = event.get("text", "")
    question = text.split(">", 1)[-1].strip() if ">" in text else text
    
    if not question:
        await say("질문을 입력해주세요. 예: `@봇 지난 달 매출 TOP 10`")
        return
    
    await say(f"🔍 분석 중: _{question}_")
    
    try:
        # 1. 컨텍스트 검색
        context = retriever.get_context_for_query(question)
        
        # 2. 모호성 판단
        guard = IntentGuard()
        analysis = await guard.analyze(question, context)
        
        if not analysis.is_clear:
            await say(guard.format_clarification(analysis))
            return
        
        # 3. SQL 생성
        generator = SQLGenerator()
        schema_str = format_schema_for_prompt(schema)
        result = await generator.generate(question, schema_str, context)
        
        # 4. SQL 검증
        validator = SQLValidator()
        validation = validator.validate(result.sql)
        
        if not validation.is_valid:
            await say(f"❌ SQL 검증 실패: {validation.error_message}")
            return
        
        # 5. 실행
        from sqlalchemy import text
        with connector.engine.connect() as conn:
            rs = conn.execute(text(validation.sanitized_sql))
            rows = rs.fetchall()
            columns = list(rs.keys())
        
        # 결과 포맷팅
        response = f"📝 *SQL:*\n```sql\n{validation.sanitized_sql}\n```\n"
        
        if rows:
            # 간단한 텍스트 테이블
            header = " | ".join(str(c) for c in columns)
            separator = "-" * len(header)
            data_rows = [" | ".join(str(v) for v in row) for row in rows[:10]]
            
            response += f"\n📊 *결과 ({len(rows)}건):*\n```\n{header}\n{separator}\n"
            response += "\n".join(data_rows)
            response += "\n```"
            
            if len(rows) > 10:
                response += f"\n_... 외 {len(rows) - 10}건_"
        else:
            response += "\n⚠️ 결과가 없습니다."
        
        await say(response)
        
    except Exception as e:
        await say(f"❌ 오류 발생: {e}")


@app.command("/sqe")
async def handle_slash_command(ack, respond, command):
    """슬래시 커맨드 처리"""
    await ack()
    
    if schema is None:
        init_components()
    
    text = command.get("text", "").strip()
    
    if not text:
        await respond("사용법: `/sqe 자연어 질문`\n예: `/sqe 이번 주 신규 가입자 수`")
        return
    
    # 멘션 핸들러와 동일한 로직 재사용
    await respond(f"🔍 분석 중: _{text}_")
    
    try:
        context = retriever.get_context_for_query(text)
        
        guard = IntentGuard()
        analysis = await guard.analyze(text, context)
        
        if not analysis.is_clear:
            await respond(guard.format_clarification(analysis))
            return
        
        generator = SQLGenerator()
        schema_str = format_schema_for_prompt(schema)
        result = await generator.generate(text, schema_str, context)
        
        validator = SQLValidator()
        validation = validator.validate(result.sql)
        
        if not validation.is_valid:
            await respond(f"❌ SQL 검증 실패: {validation.error_message}")
            return
        
        from sqlalchemy import text as sql_text
        with connector.engine.connect() as conn:
            rs = conn.execute(sql_text(validation.sanitized_sql))
            rows = rs.fetchall()
            columns = list(rs.keys())
        
        response = f"📝 *SQL:*\n```sql\n{validation.sanitized_sql}\n```\n"
        
        if rows:
            header = " | ".join(str(c) for c in columns)
            data_rows = [" | ".join(str(v) for v in row) for row in rows[:10]]
            response += f"\n📊 *결과 ({len(rows)}건):*\n```\n{header}\n{'-' * len(header)}\n"
            response += "\n".join(data_rows) + "\n```"
        else:
            response += "\n⚠️ 결과가 없습니다."
        
        await respond(response)
        
    except Exception as e:
        await respond(f"❌ 오류 발생: {e}")


async def main():
    """Slack 봇 실행"""
    handler = AsyncSocketModeHandler(
        app, 
        os.environ.get("SLACK_APP_TOKEN")
    )
    await handler.start_async()


if __name__ == "__main__":
    asyncio.run(main())
