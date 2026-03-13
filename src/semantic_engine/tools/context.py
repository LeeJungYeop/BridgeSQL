"""retrieve_context tool."""
from typing import Any

from mcp.types import TextContent, Tool

from semantic_engine.state import _ensure_imports, get_retriever

TOOLS = [
    Tool(
        name='retrieve_context',
        description=(
            'RAG로 질문과 관련된 테이블/컬럼 컨텍스트를 가져옵니다.\n'
            'generate_sql 호출 전에 사용해 SQL 생성 품질을 높입니다.\n'
            '스키마 전체 구조는 bridgesql://schema 리소스를 참조하세요.'
        ),
        inputSchema={
            'type': 'object',
            'properties': {
                'question': {'type': 'string', 'description': '컨텍스트를 가져올 자연어 질문'},
                'top_k': {'type': 'integer', 'description': '참조할 컬럼/테이블 수 (기본값: 10)', 'default': 10},
            },
            'required': ['question'],
        },
    ),
]


async def handle(name: str, args: dict[str, Any]) -> list[TextContent] | None:
    if name != 'retrieve_context':
        return None
    _ensure_imports()

    question = args['question']
    top_k = args.get('top_k', 10)

    try:
        retriever = get_retriever()
        context = retriever.get_context_for_query(question, top_k=top_k)
    except Exception as error:
        return [TextContent(type='text', text=f'컨텍스트 검색 실패: {error}')]

    return [TextContent(type='text', text=context)]
