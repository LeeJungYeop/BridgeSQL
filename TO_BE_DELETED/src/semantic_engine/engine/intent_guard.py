"""
모호성 판단 및 역질의 생성기 (Intent Guard)
Chain-of-Thought 기반으로 질문의 명확성을 평가하고 필요시 역질의를 생성합니다.
"""

import json
from dataclasses import dataclass
from enum import Enum

import google.generativeai as genai

from semantic_engine.config import get_settings


class AmbiguityType(str, Enum):
    """모호성 유형"""
    COLUMN_AMBIGUOUS = "column_ambiguous"       # 같은 이름의 컬럼이 여러 테이블에 존재
    TABLE_AMBIGUOUS = "table_ambiguous"         # 어떤 테이블을 사용할지 불명확
    TIME_RANGE_MISSING = "time_range_missing"   # 기간 조건 누락
    AGGREGATION_UNCLEAR = "aggregation_unclear" # 집계 방식 불명확
    FILTER_AMBIGUOUS = "filter_ambiguous"       # 필터 조건 불명확
    INTENT_UNCLEAR = "intent_unclear"           # 전반적인 의도 불명확


@dataclass
class Ambiguity:
    """단일 모호성 항목"""
    type: AmbiguityType
    reason: str
    options: list[str]


@dataclass
class IntentAnalysis:
    """질문 분석 결과"""
    is_clear: bool
    confidence: float  # 0.0 ~ 1.0
    ambiguities: list[Ambiguity]
    clarification_question: str | None
    interpreted_intent: str | None  # 명확한 경우 해석된 의도


class IntentGuard:
    """질문 모호성 판단 및 역질의 생성"""
    
    ANALYSIS_PROMPT = """당신은 자연어를 SQL로 변환하는 시스템의 일부입니다.
사용자 질문을 분석하여 SQL을 생성하기에 충분히 명확한지 판단하세요.

## 데이터베이스 컨텍스트
{context}

## 사용자 질문
"{question}"

## 분석 지침
1. 질문이 명확하면 is_clear=true, confidence를 높게 설정
2. 다음과 같은 모호성이 있으면 식별하세요:
   - column_ambiguous: 같은 의미의 데이터가 여러 곳에 존재
   - table_ambiguous: 어떤 데이터 영역을 사용할지 불명확
   - time_range_missing: "최근", "올해" 등 기간이 모호하거나 누락
   - aggregation_unclear: 합계/평균/개수 등 집계 방식 불명확
   - filter_ambiguous: 필터 조건이 모호
   - intent_unclear: 전반적인 의도 이해 불가

3. 모호성이 있으면 **비즈니스 용어로** 선택지를 제시하세요
   - ❌ 잘못된 예: "product_info 테이블" "비파괴검사_판정 컬럼"
   - ✅ 올바른 예: "제품 검사 기록에서" "불합격 판정된 제품만"
   
4. 선택지는 사용자가 바로 이해할 수 있는 일상 언어로 작성하세요
   - 전문 용어, 테이블명, 컬럼명 등 기술 용어는 절대 사용하지 마세요
   - 사용자가 "1번", "2번" 이라고만 답해도 선택할 수 있게 구체적으로 작성

5. 질문이 명확하면 해석된 의도를 설명하세요

## 출력 형식 (JSON)
{{
    "is_clear": true/false,
    "confidence": 0.0-1.0,
    "ambiguities": [
        {{
            "type": "모호성_유형",
            "reason": "왜 모호한지 쉬운 말로 설명",
            "options": ["1번 선택지 (구체적)", "2번 선택지 (구체적)"]
        }}
    ],
    "clarification_question": "사용자에게 물어볼 질문 (쉬운 말로)",
    "interpreted_intent": "해석된 의도 (명확할 때만)"
}}

JSON만 출력하세요:"""
    
    def __init__(self, confidence_threshold: float = 0.7):
        """
        Args:
            confidence_threshold: 이 값 이상이면 명확한 것으로 판단
        """
        settings = get_settings()
        genai.configure(api_key=settings.gemini_api_key.get_secret_value())
        self.model = genai.GenerativeModel(settings.gemini_model)
        self.confidence_threshold = confidence_threshold
    
    async def analyze(self, question: str, context: str) -> IntentAnalysis:
        """질문 분석 및 모호성 판단"""
        
        prompt = self.ANALYSIS_PROMPT.format(
            context=context,
            question=question,
        )
        
        response = await self.model.generate_content_async(prompt)
        result = self._parse_response(response.text)
        
        # confidence가 threshold 미만이면 명확하지 않은 것으로 처리
        if result.confidence < self.confidence_threshold:
            result.is_clear = False
        
        return result
    
    def _parse_response(self, text: str) -> IntentAnalysis:
        """LLM 응답 파싱"""
        text = text.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:-1])
        
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            # 파싱 실패 시 불명확으로 처리
            return IntentAnalysis(
                is_clear=False,
                confidence=0.0,
                ambiguities=[Ambiguity(
                    type=AmbiguityType.INTENT_UNCLEAR,
                    reason="질문 분석 실패",
                    options=[],
                )],
                clarification_question="질문을 다시 구체적으로 말씀해주세요.",
                interpreted_intent=None,
            )
        
        ambiguities = [
            Ambiguity(
                type=AmbiguityType(a["type"]),
                reason=a["reason"],
                options=a.get("options", []),
            )
            for a in data.get("ambiguities", [])
        ]
        
        return IntentAnalysis(
            is_clear=data.get("is_clear", False),
            confidence=data.get("confidence", 0.0),
            ambiguities=ambiguities,
            clarification_question=data.get("clarification_question"),
            interpreted_intent=data.get("interpreted_intent"),
        )
    
    def format_clarification(self, analysis: IntentAnalysis) -> str:
        """역질의를 사용자 친화적 형식으로 포맷"""
        
        if analysis.is_clear:
            return f"✅ 질문을 이해했습니다: {analysis.interpreted_intent}"
        
        parts = ["❓ 질문을 더 정확히 답하기 위해 확인이 필요합니다:\n"]

        for i, amb in enumerate(analysis.ambiguities, 1):
            parts.append(f"\n**질문 {i}**: {amb.reason}")
            if amb.options:
                parts.append("선택지:")
                for j, opt in enumerate(amb.options, 1):
                    parts.append(f"  {j}. {opt}")
        
        if analysis.clarification_question:
            parts.append(f"\n💬 {analysis.clarification_question}")
        
        return "\n".join(parts)
