"""Text formatting helpers shared across tools and resources."""
from typing import Any


def safe_join(lines: list[str]) -> str:
    return '\n'.join(line for line in lines if line)


def truncate(value: Any, limit: int = 120) -> str:
    text = str(value).replace('\n', ' ').strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3] + '...'


def format_markdown_table(columns: list[str], rows: list[tuple], max_rows: int = 20) -> str:
    header = '| ' + ' | '.join(str(c) for c in columns) + ' |'
    separator = '| ' + ' | '.join('---' for _ in columns) + ' |'
    body = [
        '| ' + ' | '.join(truncate(v, 80) for v in row) + ' |'
        for row in rows[:max_rows]
    ]
    return '\n'.join([header, separator] + body)


def format_schema_overview(schema) -> str:
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
            '| `' + table.name + '` | ' + (table.business_name or '-') +
            f' | {len(table.columns)} | {row_count} |'
        )
    return '\n'.join(lines)


def format_table_detail(table) -> str:
    lines = [
        '## `' + table.name + '`',
        '**비즈니스명:** ' + (table.business_name or '-'),
        '**설명:** ' + (table.description or '-'),
        f'**행 수:** {table.row_count:,}' if table.row_count else '',
        '',
        '**컬럼:**',
    ]
    for col in table.columns:
        flags = []
        if col.is_primary_key:
            flags.append('PK')
        if col.is_foreign_key:
            flags.append('FK')
        suffix = ' [' + ', '.join(flags) + ']' if flags else ''
        lines.append(
            '- `' + col.name + '` (' + col.data_type + ')' + suffix +
            ': ' + (col.business_name or '-')
        )
        if col.description:
            lines.append('  - ' + col.description)
        if col.keywords:
            lines.append('  - 키워드: ' + ', '.join(col.keywords[:6]))
        if col.sample_values:
            samples = ', '.join(truncate(v, 30) for v in col.sample_values[:5])
            lines.append('  - 샘플값: ' + samples)
    return safe_join(lines)
