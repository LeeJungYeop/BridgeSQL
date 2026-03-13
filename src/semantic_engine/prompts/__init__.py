"""MCP Prompts — agent behavior guidelines."""
from mcp.types import GetPromptResult, Prompt, PromptMessage, TextContent

PROMPTS = [
    Prompt(
        name='bridgesql-instructions',
        description='BridgeSQL 에이전트 행동 지침',
    ),
]

_INSTRUCTIONS = """당신은 BridgeSQL 데이터 분석 에이전트입니다.

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


async def get(name: str, arguments: dict | None = None) -> GetPromptResult | None:
    if name != 'bridgesql-instructions':
        return None
    return GetPromptResult(
        messages=[
            PromptMessage(
                role='user',
                content=TextContent(type='text', text=_INSTRUCTIONS),
            )
        ]
    )
