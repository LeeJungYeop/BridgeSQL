"""MCP Resources — schema overview and table detail."""
from mcp.types import Resource, ResourceTemplate

from semantic_engine.formatters import format_schema_overview, format_table_detail
from semantic_engine.state import _ensure_imports, get_schema

RESOURCES = [
    Resource(
        uri='bridgesql://schema',
        name='Database Schema',
        description='데이터베이스 전체 테이블 목록과 요약. 어떤 테이블이 있는지 파악할 때 참조.',
        mimeType='text/plain',
    ),
]

RESOURCE_TEMPLATES = [
    ResourceTemplate(
        uriTemplate='bridgesql://table/{table_name}',
        name='Table Detail',
        description='특정 테이블의 컬럼, 타입, 비즈니스명, 샘플값. {table_name}에 테이블명 입력.',
        mimeType='text/plain',
    ),
]


async def read(uri_str: str) -> str:
    _ensure_imports()
    schema = get_schema()
    if not schema:
        return '카탈로그가 없습니다. 먼저 `sqe profile`을 실행하세요.'

    if uri_str == 'bridgesql://schema':
        return format_schema_overview(schema)

    if uri_str.startswith('bridgesql://table/'):
        table_name = uri_str.removeprefix('bridgesql://table/')
        table = schema.get_table(table_name)
        if not table:
            return f'테이블 `{table_name}`을 찾을 수 없습니다.'
        return format_table_detail(table)

    return f'Unknown resource: {uri_str}'
