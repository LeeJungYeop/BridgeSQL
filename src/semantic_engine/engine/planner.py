"""
2단계 쿼리 플래너
1단계: 관련 컬럼 후보 식별 및 사용자 확인
2단계: 확인된 컬럼으로 SQL 생성
"""

import json
from dataclasses import dataclass

from semantic_engine.config import get_settings
import google.generativeai as genai


@dataclass
class ColumnCandidate:
    """컬럼 후보"""
    table: str
    column: str
    business_name: str
    reason: str  # 왜 이 컬럼이 관련 있는지
    confidence: float


@dataclass
class QueryPlan:
    """쿼리 계획"""
    question: str
    intent: str  # 해석된 의도
    intent_type: str  # 의도 분류 (data_query, schema_explore)
    candidates: list[ColumnCandidate]
    recommended_columns: list[str]  # 추천 컬럼 (table.column 형식)
    needs_confirmation: bool
    is_cached: bool = False  # 이전 성공 사례 재사용 여부


class QueryPlanner:
    """2단계 쿼리 플래너"""
    
    PLANNING_PROMPT = """당신은 자연어 질문을 분석하여 관련 컬럼을 식별하는 전문가입니다.

## [중요] 전체 테이블 및 컬럼 목록
{global_schema_summary}

## 검색된 관련 상세 정보 (RAG)
{schema_context}

## 사용자 질문
"{question}"

## 작업 지침
1. 질문에 답하기 위해 필요한 컬럼들을 식별하세요. **특히 전역 목록을 보고 RAG 검색에서 누락되었을 수 있는 핵심 컬럼(예: 정상/불량 상태 컬럼 등)도 검토하세요.**
2. 각 컬럼이 왜 관련 있는지 설명하세요
3. **컬럼명이 질문에 직접 언급된 경우 최우선 순위로 선정하세요**
   - 예: "비파괴 검사자" 질문 → "비파괴검사_검사자" 컬럼 우선

## 출력 형식 (JSON)
{{
    "intent": "질문의 의도를 쉬운 말로 설명",
    "intent_type": "data_query 또는 schema_explore (단순 스키마 질문인 경우)",
    "candidates": [
        {{
            "table": "테이블명",
            "column": "컬럼명", 
            "business_name": "비즈니스 용어로 설명",
            "reason": "왜 이 컬럼이 필요한지 (쉬운 말로)",
            "confidence": 0.0-1.0
        }}
    ],
    "recommended_columns": ["table.column", ...],
    "needs_confirmation": true/false
}}

- needs_confirmation: 후보가 여러 개이거나 확실하지 않으면 true
- candidates는 confidence 내림차순으로 정렬
- **반드시 3~8개 후보를 제시하세요**
- confidence가 낮은 컬럼도 포함하세요 (사용자가 선택할 수 있도록)
- 컨텍스트에 있는 모든 컬럼을 후보로 고려하세요

JSON만 출력하세요:"""

    def __init__(self):
        settings = get_settings()
        genai.configure(api_key=settings.gemini_api_key.get_secret_value())
        self.model = genai.GenerativeModel(settings.gemini_model)
    
    async def plan(
        self, 
        question: str, 
        schema_context: str, 
        global_schema_summary: str = "",
        history: list[dict] | None = None
    ) -> QueryPlan:
        """질문 분석 및 컬럼 후보 식별"""
        
        norm_q = self._normalize_question(question)
        
        # 0. 이전 성공 사례(History)에서 동일/유사 질문 확인
        import difflib
        
        if history:
            for log in history:
                prev_q = log["question"]
                score = log.get("score", 0.0)
                is_match = False
                match_reason = ""
                
                # 1. Semantic Score가 높은 경우 (0.65 이상) -> 즉시 재사용
                if score >= 0.65 and log.get("selected_columns"):
                    is_match = True
                    match_reason = f"의미적으로 매우 유사한 질문('{prev_q}', 유사도 {score:.2f})"
                
                # 2. 문자열 유사도가 높은 경우 (0.8 이상) -> 재사용
                else:
                    norm_prev = self._normalize_question(prev_q)
                    str_sim = difflib.SequenceMatcher(None, norm_prev, norm_q).ratio()
                    
                    if (str_sim >= 0.8 or norm_prev == norm_q) and log.get("selected_columns"):
                        is_match = True
                        match_reason = f"문장 구조가 유사한 질문('{prev_q}', 유사도 {str_sim:.2f})"
                
                if is_match:
                    candidates = []
                    for col_path in log["selected_columns"]:
                        if "." in col_path:
                            table, column = col_path.split(".", 1)
                            candidates.append(ColumnCandidate(
                                table=table,
                                column=column,
                                business_name=column.replace("_", " "),
                                reason=f"{match_reason} -> 과거 선택 재사용",
                                confidence=1.0
                            ))
                    
                    if candidates:
                        return QueryPlan(
                            question=question,
                            intent=f"이전 유사 질문('{prev_q}')에 대한 성공적인 플랜을 재사용합니다.",
                            intent_type="data_query",
                            candidates=candidates,
                            recommended_columns=log["selected_columns"],
                            needs_confirmation=False,
                            is_cached=True
                        )
        
        # 1. 먼저 직접 컬럼 검색 (키워드 매칭)
        direct_matches = self._find_direct_column_matches(question, schema_context + global_schema_summary)
        
        # 2. 직접 매칭 정보를 컨텍스트에 추가
        if direct_matches:
            match_hint = "\n\n## 🎯 직접 매칭된 컬럼 (우선 사용)\n"
            for m in direct_matches:
                match_hint += f"- {m['table']}.{m['column']} ('{m['keyword']}' 포함)\n"
            schema_context = schema_context + match_hint
        
        prompt = self.PLANNING_PROMPT.format(
            global_schema_summary=global_schema_summary,
            schema_context=schema_context,
            question=question,
        )
        
        response = await self.model.generate_content_async(prompt)
        plan = self._parse_response(question, response.text)
        
        # 3. 직접 매칭된 컬럼이 후보에 없으면 추가
        for m in direct_matches:
            exists = any(c.table == m['table'] and c.column == m['column'] 
                        for c in plan.candidates)
            if not exists:
                plan.candidates.insert(0, ColumnCandidate(
                    table=m['table'],
                    column=m['column'],
                    business_name=m.get('business_name', m['column']),
                    reason=f"컬럼명에 '{m['keyword']}' 직접 포함",
                    confidence=0.95,
                ))
        
        # 4. 직접 매칭 컬럼을 추천으로 설정
        if direct_matches and not plan.recommended_columns:
            plan.recommended_columns = [f"{m['table']}.{m['column']}" for m in direct_matches]
        
        return plan
    
    def _find_direct_column_matches(self, question: str, schema_context: str) -> list[dict]:
        """질문의 키워드가 컬럼명에 직접 포함된 경우 찾기"""
        import re
        
        matches = []
        
        # 질문에서 주요 키워드 추출 (2글자 이상 한글/영문)
        keywords = re.findall(r'[가-힣a-zA-Z]{2,}', question)
        
        # 컨텍스트에서 테이블.컬럼 패턴 추출
        # 예: "product_info.비파괴검사_검사자" 또는 "컬럼명: 비파괴검사_검사자"
        column_patterns = re.findall(r'[`"]?(\w+)[`"]?\s*\.\s*[`"]?(\w+)[`"]?', schema_context)
        
        for table, column in column_patterns:
            column_lower = column.lower().replace('_', '')
            for kw in keywords:
                kw_clean = kw.lower().replace(' ', '').replace('_', '')
                # 키워드가 컬럼명에 포함되어 있으면 매칭
                if len(kw_clean) >= 2 and kw_clean in column_lower:
                    matches.append({
                        'table': table,
                        'column': column,
                        'keyword': kw,
                        'business_name': column.replace('_', ' '),
                    })
                    break  # 같은 컬럼 중복 방지
        
        # 중복 제거
        seen = set()
        unique_matches = []
        for m in matches:
            key = f"{m['table']}.{m['column']}"
            if key not in seen:
                seen.add(key)
                unique_matches.append(m)
        
        return unique_matches
    
    def _parse_response(self, question: str, text: str) -> QueryPlan:
        """LLM 응답 파싱"""
        text = text.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:-1])
        
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return QueryPlan(
                question=question,
                intent="분석 실패",
                intent_type="data_query",
                candidates=[],
                recommended_columns=[],
                needs_confirmation=True,
            )
        
        intent_type = data.get("intent_type", "data_query")

        candidates = [
            ColumnCandidate(
                table=c["table"],
                column=c["column"],
                business_name=c.get("business_name", c["column"]),
                reason=c.get("reason", ""),
                confidence=c.get("confidence", 0.5),
            )
            for c in data.get("candidates", [])
        ]

        # 키워드 기반 보완: LLM이 data_query로 분류했지만 순수 스키마 탐색인 경우만 보정
        # 조건: 스키마 키워드 포함 + 후보 컬럼이 있음 + 집계/필터 의도가 없음
        schema_only_keywords = ["테이블", "컬럼", "필드", "스키마", "데이터 구조"]
        data_intent_keywords = ["보여줘", "알려줘", "조회", "목록", "리스트", "찾아", "검색"]
        has_schema_keyword = any(kw in question for kw in schema_only_keywords)
        has_data_intent = any(kw in question for kw in data_intent_keywords)
        if has_schema_keyword and not has_data_intent and len(candidates) > 0:
            if intent_type == "data_query":
                intent_type = "schema_explore"
        
        # confidence 내림차순 정렬
        candidates.sort(key=lambda x: x.confidence, reverse=True)
        
        return QueryPlan(
            question=question,
            intent=data.get("intent", ""),
            intent_type=intent_type,
            candidates=candidates,
            recommended_columns=data.get("recommended_columns", []),
            needs_confirmation=data.get("needs_confirmation", True),
        )
    
    def format_plan_for_user(self, plan: QueryPlan) -> str:
        """사용자에게 보여줄 형식으로 포맷"""
        
        lines = [f"📋 **질문 분석 결과**\n"]
        lines.append(f"💡 **해석된 의도:** {plan.intent}\n")
        
        if not plan.candidates:
            lines.append("❌ 관련 컬럼을 찾지 못했습니다.")
            return "\n".join(lines)
        
        lines.append("📊 **관련 데이터 후보:**\n")
        
        # 관련성 있는 후보만 표시 (confidence > 0.3)
        relevant_candidates = [c for c in plan.candidates if c.confidence >= 0.3]
        if not relevant_candidates:
            # 최소 1개는 표시
            relevant_candidates = plan.candidates[:1]
        
        for i, c in enumerate(relevant_candidates, 1):
            confidence_bar = "●" * int(c.confidence * 5) + "○" * (5 - int(c.confidence * 5))
            lines.append(f"  **{i}. {c.business_name}** [{confidence_bar}]")
            lines.append(f"     └ {c.reason}")
        
        if plan.recommended_columns:
            rec_names = [c.business_name for c in plan.candidates 
                        if f"{c.table}.{c.column}" in plan.recommended_columns]
            if rec_names:
                lines.append(f"\n✅ **추천:** {', '.join(rec_names[:3])}")
        
        if plan.needs_confirmation and len(plan.candidates) > 1:
            lines.append("\n💬 위 후보 중 어떤 데이터를 사용할까요? (번호 또는 '추천')")
        
        return "\n".join(lines)
    
    def get_selected_columns(self, plan: QueryPlan, selection: str | int | None = None) -> list[str]:
        """사용자 선택에 따른 컬럼 목록 반환"""
        
        if selection is None or selection == "추천":
            return plan.recommended_columns
        
        if isinstance(selection, int) and 1 <= selection <= len(plan.candidates):
            c = plan.candidates[selection - 1]
            return [f"{c.table}.{c.column}"]
        
        if isinstance(selection, str):
            # "1, 2" 또는 "1,2" 형식 처리
            try:
                nums = [int(x.strip()) for x in selection.replace(",", " ").split()]
                columns = []
                for n in nums:
                    if 1 <= n <= len(plan.candidates):
                        c = plan.candidates[n - 1]
                        columns.append(f"{c.table}.{c.column}")
                return columns
            except ValueError:
                pass
        
        return plan.recommended_columns

    def _normalize_question(self, text: str) -> str:
        """질문 정규화 (비교용)"""
        import re
        # 1. 소문자화 (영어 포함시)
        text = text.lower().strip()
        # 2. 특수문자 제거
        text = re.sub(r'[^\w\s가-힣]', '', text)
        # 3. 공백 제거
        text = re.sub(r'\s+', '', text)
        # 4. 한국어 어미/조사 일부 제거 (실험적)
        text = re.sub(r'(인가요|인가|인가요?|였나요|였더라|입니까|있나요|있어|있니|있으신가요|있는가|있음)$', '', text)
        text = re.sub(r'(개|개수|개수는|갯수|갯수는|개였나|개였지)$', '', text)
        return text
