"""search_examples, learn, examples_delete tools."""
import uuid
from typing import Any

from mcp.types import TextContent, Tool

from semantic_engine.formatters import safe_join
from semantic_engine.state import _ensure_imports, get_retriever

TOOLS = [
    Tool(
        name='search_examples',
        description=(
            '저장된 성공 사례(질문-SQL 쌍)를 검색합니다.\n'
            '유사한 질문이 이전에 해결된 적 있는지 확인할 때 사용.\n'
            '좋은 예시가 있으면 execute_sql로 재활용 가능.'
        ),
        inputSchema={
            'type': 'object',
            'properties': {
                'question': {'type': 'string', 'description': '검색할 질문'},
                'top_k': {'type': 'integer', 'description': '반환할 결과 수 (기본값: 3)', 'default': 3},
            },
            'required': ['question'],
        },
    ),
    Tool(
        name='learn',
        description=(
            '사용자가 확인한 질문-SQL 쌍을 example pool에 저장합니다.\n'
            '사용자가 결과가 맞다고 확인했을 때만 호출하세요. 자동으로 호출하지 마세요.\n'
            '저장된 예시는 이후 유사 질문에서 few-shot으로 재사용됩니다.'
        ),
        inputSchema={
            'type': 'object',
            'properties': {
                'question': {'type': 'string', 'description': '저장할 자연어 질문'},
                'sql': {'type': 'string', 'description': '검증된 SQL 쿼리'},
            },
            'required': ['question', 'sql'],
        },
    ),
    Tool(
        name='examples_delete',
        description=(
            'example pool에서 특정 예제를 삭제합니다.\n'
            '사용자가 삭제를 명령하면 즉시 실행하세요.\n'
            '예제 ID는 search_examples 결과의 id 필드에서 확인하세요.'
        ),
        inputSchema={
            'type': 'object',
            'properties': {
                'id': {'type': 'string', 'description': '삭제할 예제의 ID'},
            },
            'required': ['id'],
        },
    ),
]


async def handle(name: str, args: dict[str, Any]) -> list[TextContent] | None:
    if name == 'search_examples':
        return await _search(args)
    if name == 'learn':
        return await _learn(args)
    if name == 'examples_delete':
        return await _delete(args)
    return None


async def _search(args: dict) -> list[TextContent]:
    _ensure_imports()
    question = args['question']
    top_k = args.get('top_k', 3)

    try:
        retriever = get_retriever()
        examples = retriever.search_few_shot_examples(question, top_k=top_k)
    except Exception as error:
        return [TextContent(type='text', text=f'검색 실패: {error}')]

    if not examples:
        return [TextContent(type='text', text='저장된 유사 사례가 없습니다.')]

    lines = [f'## Stored Examples: "{question}"', '']
    for i, ex in enumerate(examples, 1):
        lines.extend([
            f'### {i}. (score {ex["score"]:.2f})',
            f'**질문:** {ex["question"]}',
            '```sql', ex['sql'], '```', '',
        ])
    lines.append('_재사용하려면 `execute_sql`로 직접 실행_')
    return [TextContent(type='text', text=safe_join(lines))]


async def _learn(args: dict) -> list[TextContent]:
    _ensure_imports()
    question = args['question']
    sql = args['sql']

    try:
        retriever = get_retriever()
        retriever.index_few_shot_example(log_id=str(uuid.uuid4()), question=question, sql=sql)
    except Exception as error:
        return [TextContent(type='text', text=f'저장 실패: {error}')]

    return [TextContent(type='text', text=f'저장 완료: "{question}"')]


async def _delete(args: dict) -> list[TextContent]:
    _ensure_imports()
    example_id = args['id']

    try:
        retriever = get_retriever()
        deleted = retriever.delete_example(example_id)
    except Exception as error:
        return [TextContent(type='text', text=f'삭제 실패: {error}')]

    if not deleted:
        return [TextContent(type='text', text=f'예제를 찾을 수 없습니다: {example_id}')]
    return [TextContent(type='text', text=f'삭제 완료: {example_id}')]
