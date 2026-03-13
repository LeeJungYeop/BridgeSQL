'''MCP server for BridgeSQL.'''

import asyncio
import threading
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import (
    TextContent, Tool, Prompt, PromptMessage, GetPromptResult,
    Resource, ResourceTemplate,
)

DatabaseConnector = None
SemanticRetriever = None
SemanticCatalog = None


def _ensure_imports() -> None:
    global DatabaseConnector, SemanticRetriever, SemanticCatalog
    if DatabaseConnector is None:
        from semantic_engine.profiler.connector import DatabaseConnector as _database_connector
        from semantic_engine.rag.retriever import SemanticRetriever as _semantic_retriever
        from semantic_engine.semantic.catalog import SemanticCatalog as _semantic_catalog

        DatabaseConnector = _database_connector
        SemanticRetriever = _semantic_retriever
        SemanticCatalog = _semantic_catalog


app = Server('semantic-query-engine')

_connector = None
_retriever = None
_catalog = None
_schema = None
_retriever_lock = threading.Lock()


def _safe_join(lines: list[str]) -> str:
    return '\n'.join(line for line in lines if line)


def _truncate(value: Any, limit: int = 120) -> str:
    text = str(value).replace('\n', ' ').strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3] + '...'


def _format_markdown_table(columns: list[str], rows: list[tuple], max_rows: int = 20) -> str:
    header = '| ' + ' | '.join(str(column) for column in columns) + ' |'
    separator = '| ' + ' | '.join('---' for _ in columns) + ' |'
    body = []
    for row in rows[:max_rows]:
        body.append('| ' + ' | '.join(_truncate(value, 80) for value in row) + ' |')
    return '\n'.join([header, separator] + body)


def _extract_evidence(question: str, top_k: int = 3) -> list[dict[str, Any]]:
    try:
        retriever = get_retriever()
        results = retriever.search(question, top_k=top_k)
    except Exception:
        return []

    evidence = []
    for item in results:
        metadata = item.get('metadata', {})
        if metadata.get('type') != 'column':
            continue
        label = str(metadata.get('table', '?')) + '.' + str(metadata.get('column', '?'))
        evidence.append(
            {
                'label': label,
                'business_name': metadata.get('business_name') or metadata.get('column') or '-',
                'score': float(item.get('score', 0.0)),
            }
        )
    return evidence


def _format_query_response(
    *,
    question: str,
    sql: str,
    explanation: str,
    tables_used: list[str],
    confidence: float,
    evidence: list[dict[str, Any]],
    rows: list[tuple] | None,
    columns: list[str] | None,
    correction_note: str | None = None,
    execution_error: str | None = None,
) -> str:
    lines = ['## BridgeSQL Query Result', f'**질문:** {question}']

    summary_bits = []
    if tables_used:
        summary_bits.append('사용 테이블: ' + ', '.join('`' + name + '`' for name in tables_used))
    if confidence:
        summary_bits.append(f'모델 신뢰도: {confidence:.2f}')
    if rows is not None:
        summary_bits.append(f'조회 건수: {len(rows)}건')
    if summary_bits:
        lines.extend(['', '**요약:** ' + ' | '.join(summary_bits)])

    if explanation:
        lines.extend(['', f'**해석:** {explanation}'])

    if evidence:
        lines.extend(['', '**선택 근거:**'])
        for item in evidence:
            lines.append(
                '- `' + item['label'] + '`: ' + item['business_name'] + f" (score {item['score']:.2f})"
            )

    if correction_note:
        lines.extend(['', f'**보정:** {correction_note}'])

    lines.extend(['', '**SQL:**', '```sql', sql, '```'])

    if rows is not None and columns is not None:
        lines.append('')
        if rows:
            lines.append('**결과 미리보기:**')
            lines.append(_format_markdown_table(columns, rows))
            if len(rows) > 20:
                lines.append(f'_... 추가 {len(rows) - 20}건 생략_')
        else:
            lines.append('**결과:** 조회 결과가 없습니다.')

    if execution_error:
        lines.extend(['', f'**실행 오류:** {_truncate(execution_error, 240)}'])

    return _safe_join(lines)


def _format_schema_overview(schema) -> str:
    lines = [
        '## Schema Overview',
        f'**데이터베이스:** {schema.database_name}',
        '',
        '| 테이블 | 비즈니스명 | 컬럼 수 | 행 수 |',
        '| --- | --- | --- | --- |',
    ]
    for table in schema.tables:
        row_count = f'{table.row_count:,}' if table.row_count else '-'
        lines.append(
            '| `' + table.name + '` | ' + (table.business_name or '-') + f' | {len(table.columns)} | {row_count} |'
        )
    return '\n'.join(lines)


def _format_table_detail(table) -> str:
    lines = [
        '## `' + table.name + '`',
        '**비즈니스명:** ' + (table.business_name or '-'),
        '**설명:** ' + (table.description or '-'),
        f'**행 수:** {table.row_count:,}' if table.row_count else '',
        '',
        '**컬럼:**',
    ]
    for column in table.columns:
        flags = []
        if column.is_primary_key:
            flags.append('PK')
        if column.is_foreign_key:
            flags.append('FK')
        suffix = ' [' + ', '.join(flags) + ']' if flags else ''
        lines.append(
            '- `' + column.name + '` (' + column.data_type + ')' + suffix + ': ' + (column.business_name or '-')
        )
        if column.description:
            lines.append('  - ' + column.description)
        if column.keywords:
            lines.append('  - 키워드: ' + ', '.join(column.keywords[:6]))
        if column.sample_values:
            samples = ', '.join(_truncate(value, 30) for value in column.sample_values[:5])
            lines.append('  - 샘플값: ' + samples)
    return _safe_join(lines)


def get_connector():
    global _connector
    if _connector is None:
        _connector = DatabaseConnector()
    return _connector


def get_retriever():
    global _retriever
    if _retriever is None:
        with _retriever_lock:
            if _retriever is None:
                _retriever = SemanticRetriever()
    return _retriever


def get_schema():
    global _schema, _catalog
    if _schema is None:
        if _catalog is None:
            _catalog = SemanticCatalog()
        connector = get_connector()
        database_name = connector.get_database_name()
        _schema = _catalog.load(database_name)
    return _schema

@app.list_resources()
async def list_resources() -> list[Resource]:
    return [
        Resource(
            uri='bridgesql://schema',
            name='Database Schema',
            description='데이터베이스 전체 테이블 목록과 요약. 어떤 테이블이 있는지 파악할 때 참조.',
            mimeType='text/plain',
        ),
    ]


@app.list_resource_templates()
async def list_resource_templates() -> list[ResourceTemplate]:
    return [
        ResourceTemplate(
            uriTemplate='bridgesql://table/{table_name}',
            name='Table Detail',
            description='특정 테이블의 컬럼, 타입, 비즈니스명, 샘플값. {table_name}에 테이블명 입력.',
            mimeType='text/plain',
        ),
    ]


@app.read_resource()
async def read_resource(uri) -> str:
    _ensure_imports()

    uri_str = str(uri)
    schema = get_schema()
    if not schema:
        return '카탈로그가 없습니다. 먼저 `sqe profile`을 실행하세요.'

    if uri_str == 'bridgesql://schema':
        return _format_schema_overview(schema)

    if uri_str.startswith('bridgesql://table/'):
        table_name = uri_str.removeprefix('bridgesql://table/')
        table = schema.get_table(table_name)
        if not table:
            return f'테이블 `{table_name}`을 찾을 수 없습니다.'
        return _format_table_detail(table)

    return f'Unknown resource: {uri_str}'


@app.list_prompts()
async def list_prompts() -> list[Prompt]:
    return [
        Prompt(
            name='bridgesql-instructions',
            description='BridgeSQL 에이전트 행동 지침',
        )
    ]


@app.get_prompt()
async def get_prompt(name: str, arguments: dict | None = None) -> GetPromptResult:
    instructions = """당신은 BridgeSQL 데이터 분석 에이전트입니다.

<!--
## 툴 사용 순서
1. search_examples — 유사한 과거 사례 먼저 확인
2. 사례 있으면: execute_sql로 바로 재활용
3. 사례 없으면: retrieve_context → generate_sql → execute_sql
-->

## 자기 검증 규칙
- 숫자가 포함된 답변을 하기 전에 반드시 내부적으로 확인하세요:
  - 언급한 숫자들이 서로 모순되지 않는가?
  - 부분 합계가 전체 합계와 일치하는가?
  - 모순이 있으면 데이터베이스 결과를 다시 확인한 후 답변하세요.

## SQL 실패 시
generate_sql에 previous_sql과 error_context를 넘겨 보정 후 재시도하세요.

## learn 툴
사용자가 "맞아", "맞는 거 같네", "ㅇㅇ", "좋아" 등 결과를 긍정적으로 확인하면
"이 쿼리를 저장해둘까요?" 라고 능동적으로 제안하세요.
사용자가 저장에 동의하면 learn 툴을 호출하세요. 동의 없이 자동으로 저장하지 마세요.
"""
    return GetPromptResult(
        messages=[
            PromptMessage(
                role='user',
                content=TextContent(type='text', text=instructions),
            )
        ]
    )


@app.list_tools()
async def list_tools() -> list[Tool]:
    return [
        # query 툴은 비활성화 — 에이전트가 개별 툴을 조합해서 사용
        # search_schema 제거 — retrieve_context로 통합
        # describe_schema, describe_table → Resource로 이동 (bridgesql://schema, bridgesql://table/{name})
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
                    'question': {
                        'type': 'string',
                        'description': '컨텍스트를 가져올 자연어 질문',
                    },
                    'top_k': {
                        'type': 'integer',
                        'description': '참조할 컬럼/테이블 수 (기본값: 10)',
                        'default': 10,
                    },
                },
                'required': ['question'],
            },
        ),
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
                    'question': {
                        'type': 'string',
                        'description': '자연어 데이터 질문',
                    },
                    'context': {
                        'type': 'string',
                        'description': 'retrieve_context 결과 (선택)',
                    },
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
                    'previous_sql': {
                        'type': 'string',
                        'description': '실패한 이전 SQL (재시도 시 사용)',
                    },
                    'error_context': {
                        'type': 'string',
                        'description': 'execute_sql에서 반환된 에러 메시지 (재시도 시 사용)',
                    },
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
                    'sql': {
                        'type': 'string',
                        'description': '실행할 SQL 쿼리',
                    },
                },
                'required': ['sql'],
            },
        ),
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
                    'question': {
                        'type': 'string',
                        'description': '검색할 질문',
                    },
                    'top_k': {
                        'type': 'integer',
                        'description': '반환할 결과 수 (기본값: 3)',
                        'default': 3,
                    },
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
                    'question': {
                        'type': 'string',
                        'description': '저장할 자연어 질문',
                    },
                    'sql': {
                        'type': 'string',
                        'description': '검증된 SQL 쿼리',
                    },
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
                    'id': {
                        'type': 'string',
                        'description': '삭제할 예제의 ID',
                    },
                },
                'required': ['id'],
            },
        ),
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
                    'table_name': {
                        'type': 'string',
                        'description': '수정할 테이블명',
                    },
                    'business_name': {
                        'type': 'string',
                        'description': '새 비즈니스명 (테이블)',
                    },
                    'description': {
                        'type': 'string',
                        'description': '새 설명 (테이블)',
                    },
                    'column_name': {
                        'type': 'string',
                        'description': '수정할 컬럼명 (컬럼 수정 시)',
                    },
                    'column_business_name': {
                        'type': 'string',
                        'description': '새 비즈니스명 (컬럼)',
                    },
                    'column_description': {
                        'type': 'string',
                        'description': '새 설명 (컬럼)',
                    },
                },
                'required': ['table_name'],
            },
        ),
    ]


@app.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    try:
        if name == 'query':
            return await handle_query(arguments)
        if name == 'retrieve_context':
            return await handle_retrieve_context(arguments)
        if name == 'generate_sql':
            return await handle_generate_sql(arguments)
        if name == 'execute_sql':
            return await handle_execute_sql(arguments)
        if name == 'search_examples':
            return await handle_search_examples(arguments)
        if name == 'learn':
            return await handle_learn(arguments)
        if name == 'examples_delete':
            return await handle_examples_delete(arguments)
        if name == 'catalog_edit':
            return await handle_catalog_edit(arguments)
        return [TextContent(type='text', text=f'Unknown tool: {name}')]
    except Exception as error:
        import sys
        import traceback

        traceback.print_exc(file=sys.stderr)
        return [TextContent(type='text', text=f'오류 발생: {type(error).__name__}: {error}')]

async def handle_query(args: dict) -> list[TextContent]:
    _ensure_imports()

    from sqlalchemy import text as sa_text
    from semantic_engine.engine.sql_generator import SQLGenerator, format_schema_for_prompt
    from semantic_engine.engine.validator import SQLValidator

    question = args['question']
    execute = args.get('execute', True)

    schema = get_schema()
    if not schema:
        return [TextContent(type='text', text='카탈로그가 없습니다. 먼저 `sqe profile`을 실행하세요.')]

    schema_text = format_schema_for_prompt(schema)

    context = ''
    few_shots = None
    evidence = []
    ambiguity = []
    try:
        retriever = get_retriever()
        context = retriever.get_context_for_query(question, top_k=10)
        evidence = _extract_evidence(question, top_k=3)
        ambiguity = retriever.detect_ambiguity(question)
        examples = retriever.search_few_shot_examples(question, top_k=3)
        if examples:
            few_shots = [{'question': item['question'], 'sql': item['sql']} for item in examples]
    except Exception:
        retriever = None

    if ambiguity:
        lines = [
            '## Clarification Needed',
            f'**질문:** {question}',
            '',
            '비슷한 의미로 해석될 수 있는 후보가 여러 개 있습니다. 아래 기준 중 어떤 의미인지 먼저 확인해 주세요.',
            '',
        ]
        for index, item in enumerate(ambiguity[:4], start=1):
            label = str(item['table']) + '.' + str(item['column'])
            lines.append(f'{index}. `{label}` (score {item["score"]:.2f})')
        lines.extend([
            '',
            '예: `1번 기준으로 다시 조회해줘` 또는 `정상여부 기준이야`처럼 답하면 됩니다.',
        ])
        return [TextContent(type='text', text='\n'.join(lines))]

    generator = SQLGenerator()
    validator = SQLValidator()

    generation_result = await generator.generate(question, schema_text, context, few_shots)
    validation = validator.validate(generation_result.sql)
    if not validation.is_valid:
        return [TextContent(type='text', text=f'SQL 검증 실패: {validation.error_message}')]

    current_sql = validation.sanitized_sql
    correction_note = None
    execution_error = None
    rows = None
    columns = None

    if execute:
        try:
            connector = get_connector()
            with connector.engine.connect() as connection:
                result = connection.execute(sa_text(current_sql))
                rows = result.fetchall()
                columns = list(result.keys())
        except Exception as error:
            execution_error = str(error)
            try:
                corrected = await generator.correct(current_sql, execution_error, schema_text)
                corrected_validation = validator.validate(corrected.sql)
                if corrected_validation.is_valid:
                    current_sql = corrected_validation.sanitized_sql
                    correction_note = corrected.explanation or '실행 오류를 기준으로 SQL을 자동 보정했습니다.'
                    generation_result = corrected
                    connector = get_connector()
                    with connector.engine.connect() as connection:
                        result = connection.execute(sa_text(current_sql))
                        rows = result.fetchall()
                        columns = list(result.keys())
                    execution_error = None
            except Exception:
                pass

    response_text = _format_query_response(
        question=question,
        sql=current_sql,
        explanation=generation_result.explanation,
        tables_used=generation_result.tables_used,
        confidence=generation_result.confidence,
        evidence=evidence,
        rows=rows,
        columns=columns,
        correction_note=correction_note,
        execution_error=execution_error,
    )
    return [TextContent(type='text', text=response_text)]

async def handle_describe_schema(args: dict) -> list[TextContent]:
    _ensure_imports()

    schema = get_schema()
    if not schema:
        return [TextContent(type='text', text='카탈로그가 없습니다.')]

    return [TextContent(type='text', text=_format_schema_overview(schema))]


async def handle_describe_table(args: dict) -> list[TextContent]:
    _ensure_imports()

    table_name = args['table_name']
    schema = get_schema()
    if not schema:
        return [TextContent(type='text', text='카탈로그가 없습니다.')]

    table = schema.get_table(table_name)
    if not table:
        return [TextContent(type='text', text=f'테이블 `{table_name}`을 찾을 수 없습니다.')]

    return [TextContent(type='text', text=_format_table_detail(table))]



async def handle_retrieve_context(args: dict) -> list[TextContent]:
    _ensure_imports()

    question = args['question']
    top_k = args.get('top_k', 10)

    try:
        retriever = get_retriever()
        context = retriever.get_context_for_query(question, top_k=top_k)
    except Exception as error:
        return [TextContent(type='text', text=f'컨텍스트 검색 실패: {error}')]

    return [TextContent(type='text', text=context)]


async def handle_generate_sql(args: dict) -> list[TextContent]:
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

    # 에러 컨텍스트가 있으면 correction 모드
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
    lines += [
        '',
        '```sql',
        validation.sanitized_sql,
        '```',
        '',
        '_실행하려면 `execute_sql` 호출_',
    ]
    return [TextContent(type='text', text=_safe_join(lines))]


async def handle_execute_sql(args: dict) -> list[TextContent]:
    _ensure_imports()

    from sqlalchemy import text as sa_text

    sql = args['sql']
    validator_mod = __import__('semantic_engine.engine.validator', fromlist=['SQLValidator'])
    validator = validator_mod.SQLValidator()
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
        return [TextContent(type='text', text=f'실행 오류: {_truncate(str(error), 300)}')]

    lines = ['## Execute Result', f'**조회 건수:** {len(rows)}건', '']
    if rows:
        lines.append(_format_markdown_table(columns, rows))
        if len(rows) > 20:
            lines.append(f'_... 추가 {len(rows) - 20}건 생략_')
    else:
        lines.append('조회 결과가 없습니다.')

    return [TextContent(type='text', text=_safe_join(lines))]


async def handle_search_examples(args: dict) -> list[TextContent]:
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
            '```sql',
            ex['sql'],
            '```',
            '',
        ])

    lines.append('_재사용하려면 `execute_sql`로 직접 실행_')
    return [TextContent(type='text', text=_safe_join(lines))]


async def handle_learn(args: dict) -> list[TextContent]:
    _ensure_imports()

    import uuid

    question = args['question']
    sql = args['sql']

    try:
        retriever = get_retriever()
        retriever.index_few_shot_example(log_id=str(uuid.uuid4()), question=question, sql=sql)
    except Exception as error:
        return [TextContent(type='text', text=f'저장 실패: {error}')]

    return [TextContent(type='text', text=f'저장 완료: "{question}"')]


async def handle_examples_delete(args: dict) -> list[TextContent]:
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


async def handle_catalog_edit(args: dict) -> list[TextContent]:
    _ensure_imports()

    table_name = args['table_name']
    business_name = args.get('business_name')
    description = args.get('description')
    column_name = args.get('column_name')
    column_business_name = args.get('column_business_name')
    column_description = args.get('column_description')

    if not any([business_name, description, column_business_name, column_description]):
        return [TextContent(type='text', text='수정할 내용이 없습니다. business_name, description, column_business_name, column_description 중 하나 이상을 전달하세요.')]

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
        return [TextContent(type='text', text=f'테이블 `{table_name}`을 찾을 수 없습니다. 카탈로그가 없거나 테이블명이 틀렸을 수 있습니다.')]

    # 캐시 무효화 — 다음 요청에서 최신 카탈로그 로드
    global _schema
    _schema = None

    target = f'`{table_name}`' if not column_name else f'`{table_name}.{column_name}`'
    return [TextContent(type='text', text=f'수정 완료: {target}')]


def _preload_in_background() -> None:
    def _load() -> None:
        try:
            _ensure_imports()
            get_retriever()
            get_schema()
        except Exception:
            pass

    thread = threading.Thread(target=_load, daemon=True)
    thread.start()


async def main() -> None:
    import sys

    print('[BridgeSQL] MCP server starting (preloading in background)', file=sys.stderr)
    _preload_in_background()
    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())


if __name__ == '__main__':
    asyncio.run(main())
