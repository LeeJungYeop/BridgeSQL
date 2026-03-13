"""generate_sql and execute_sql tools."""
from typing import Any

from mcp.types import TextContent, Tool

from semantic_engine.formatters import format_markdown_table, safe_join, truncate
from semantic_engine.state import _ensure_imports, get_connector, get_schema

TOOLS = [
    Tool(
        name='generate_sql',
        description=(
            'SQL을 생성만 하고 실행하지 않습니다.\n'
            'retrieve_context와 search_examples 결과를 context/few_shots에 주입하면 품질이 높아집니다.\n'
            'execute_sql 실패 후 재시도할 때는 previous_sql과 error_context를 함께 넘기세요. '
            '실패 원인을 분석해 보정된 SQL을 반환합니다.\n'
            '생성된 SQL은 execute_sql로 실행하세요.'
        ),
        inputSchema={
            'type': 'object',
            'properties': {
                'question': {'type': 'string', 'description': '자연어 데이터 질문'},
                'context': {'type': 'string', 'description': 'retrieve_context 결과 (선택)'},
                'few_shots': {
                    'type': 'array',
                    'description': 'search_examples 결과에서 선택한 예시 [{question, sql}, ...] (선택)',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'question': {'type': 'string'},
                            'sql': {'type': 'string'},
                        },
                    },
                },
                'previous_sql': {'type': 'string', 'description': '실패한 이전 SQL (재시도 시 사용)'},
                'error_context': {'type': 'string', 'description': 'execute_sql 에러 메시지 (재시도 시 사용)'},
            },
            'required': ['question'],
        },
    ),
    Tool(
        name='execute_sql',
        description=(
            'SQL을 직접 실행합니다.\n'
            'generate_sql로 받은 SQL을 검토 후 실행하거나, 직접 작성한 SQL을 실행할 때 사용.'
        ),
        inputSchema={
            'type': 'object',
            'properties': {
                'sql': {'type': 'string', 'description': '실행할 SQL 쿼리'},
            },
            'required': ['sql'],
        },
    ),
]


async def handle(name: str, args: dict[str, Any]) -> list[TextContent] | None:
    if name == 'generate_sql':
        return await _generate(args)
    if name == 'execute_sql':
        return await _execute(args)
    return None


async def _generate(args: dict) -> list[TextContent]:
    _ensure_imports()
    from semantic_engine.engine.sql_generator import SQLGenerator, format_schema_for_prompt
    from semantic_engine.engine.validator import SQLValidator

    question = args['question']
    context = args.get('context', '')
    few_shots = args.get('few_shots') or None
    previous_sql = args.get('previous_sql')
    error_context = args.get('error_context')

    schema = get_schema()
    if not schema:
        return [TextContent(type='text', text='카탈로그가 없습니다. 먼저 `sqe profile`을 실행하세요.')]

    schema_text = format_schema_for_prompt(schema)
    generator = SQLGenerator()
    validator = SQLValidator()

    if previous_sql and error_context:
        try:
            result = await generator.correct(previous_sql, error_context, schema_text)
        except ValueError as e:
            return [TextContent(type='text', text=f'보정 실패: {e}')]
        correction_label = f'**보정 원인:** {error_context[:200]}'
    else:
        result = await generator.generate(question, schema_text, context, few_shots)
        correction_label = None

    validation = validator.validate(result.sql)
    if not validation.is_valid:
        return [TextContent(type='text', text=f'SQL 검증 실패: {validation.error_message}')]

    lines = [
        '## Generated SQL',
        f'**질문:** {question}',
        f'**해석:** {result.explanation}',
        f'**신뢰도:** {result.confidence:.2f}',
        f'**사용 테이블:** {", ".join(result.tables_used)}',
    ]
    if correction_label:
        lines.append(correction_label)
    lines += ['', '```sql', validation.sanitized_sql, '```', '', '_실행하려면 `execute_sql` 호출_']
    return [TextContent(type='text', text=safe_join(lines))]


async def _execute(args: dict) -> list[TextContent]:
    _ensure_imports()
    from sqlalchemy import text as sa_text
    from semantic_engine.engine.validator import SQLValidator

    sql = args['sql']
    validator = SQLValidator()
    validation = validator.validate(sql)
    if not validation.is_valid:
        return [TextContent(type='text', text=f'SQL 검증 실패: {validation.error_message}')]

    try:
        connector = get_connector()
        with connector.engine.connect() as connection:
            result = connection.execute(sa_text(validation.sanitized_sql))
            rows = result.fetchall()
            columns = list(result.keys())
    except Exception as error:
        return [TextContent(type='text', text=f'실행 오류: {truncate(str(error), 300)}')]

    lines = ['## Execute Result', f'**조회 건수:** {len(rows)}건', '']
    if rows:
        lines.append(format_markdown_table(columns, rows))
        if len(rows) > 20:
            lines.append(f'_... 추가 {len(rows) - 20}건 생략_')
    else:
        lines.append('조회 결과가 없습니다.')

    return [TextContent(type='text', text=safe_join(lines))]
