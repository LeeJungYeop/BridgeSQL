"""catalog_edit tool."""
from typing import Any

from mcp.types import TextContent, Tool

from bridgesql.state import _ensure_imports, get_connector, invalidate_schema

TOOLS = [
    Tool(
        name='catalog_edit',
        description=(
            '카탈로그에서 테이블 또는 컬럼의 비즈니스명/설명을 수정합니다.\n'
            '사용자가 수정을 명령하면 즉시 실행하세요.\n'
            '컬럼 수정 시 column_name도 함께 전달하세요.'
        ),
        inputSchema={
            'type': 'object',
            'properties': {
                'table_name': {'type': 'string', 'description': '수정할 테이블명'},
                'business_name': {'type': 'string', 'description': '새 비즈니스명 (테이블)'},
                'description': {'type': 'string', 'description': '새 설명 (테이블)'},
                'column_name': {'type': 'string', 'description': '수정할 컬럼명 (컬럼 수정 시)'},
                'column_business_name': {'type': 'string', 'description': '새 비즈니스명 (컬럼)'},
                'column_description': {'type': 'string', 'description': '새 설명 (컬럼)'},
            },
            'required': ['table_name'],
        },
    ),
]


async def handle(name: str, args: dict[str, Any]) -> list[TextContent] | None:
    if name != 'catalog_edit':
        return None
    _ensure_imports()

    from bridgesql.semantic.catalog import SemanticCatalog

    table_name = args['table_name']
    business_name = args.get('business_name')
    description = args.get('description')
    column_name = args.get('column_name')
    column_business_name = args.get('column_business_name')
    column_description = args.get('column_description')

    if not any([business_name, description, column_business_name, column_description]):
        return [TextContent(
            type='text',
            text='수정할 내용이 없습니다. business_name, description, column_business_name, column_description 중 하나 이상을 전달하세요.',
        )]

    try:
        connector = get_connector()
        database_name = connector.get_database_name()
        catalog = SemanticCatalog()
        ok = catalog.edit_table(
            database_name=database_name,
            table_name=table_name,
            business_name=business_name,
            description=description,
            column_name=column_name,
            column_business_name=column_business_name,
            column_description=column_description,
        )
    except Exception as error:
        return [TextContent(type='text', text=f'수정 실패: {error}')]

    if not ok:
        return [TextContent(
            type='text',
            text=f'테이블 `{table_name}`을 찾을 수 없습니다. 카탈로그가 없거나 테이블명이 틀렸을 수 있습니다.',
        )]

    invalidate_schema()
    target = f'`{table_name}`' if not column_name else f'`{table_name}.{column_name}`'
    return [TextContent(type='text', text=f'수정 완료: {target}')]
