"""
LLM 기반 세만틱 설명 생성기
Gemini 2.0 Flash를 사용하여 컬럼/테이블의 비즈니스 설명을 자동 생성합니다.
"""

import json
from dataclasses import dataclass

from semantic_engine.config import get_settings
import google.generativeai as genai

from semantic_engine.db.schema_extractor import SchemaInfo, TableInfo, ColumnInfo


@dataclass
class ColumnSemantic:
    """컬럼 세만틱 정보"""
    business_name: str
    description: str
    data_type_hint: str
    keywords: list[str]


@dataclass  
class TableSemantic:
    """테이블 세만틱 정보"""
    business_name: str
    description: str
    purpose: str


class SemanticGenerator:
    """LLM 기반 세만틱 설명 생성기"""
    
    COLUMN_PROMPT_TEMPLATE = """당신은 데이터베이스 스키마 분석 전문가입니다.
다음 컬럼 정보를 분석하여 비즈니스 관점의 설명을 JSON으로 생성하세요.

## 테이블 정보
- 테이블명: {table_name}
- 테이블 설명: {table_comment}

## 컬럼 정보
- 컬럼명: {column_name}
- 데이터 타입: {data_type}
- NULL 허용: {nullable}
- Primary Key: {is_pk}
- Foreign Key: {fk_ref}
- 기존 코멘트: {column_comment}
- 고유값 개수: {unique_count}

## 샘플 데이터
{sample_values}

## 작성 지침
- unique_count가 10 이하인 컬럼은 각 값의 의미와 SQL 사용법을 description에 명시하세요.
  예: "값 목록: 0=정상(합격), 1=불량(불합격). 불량 조회: WHERE {column_name} = 1"
  예: "값 목록: '합격'=통과, '부적합'=불량. 불량 조회: WHERE {column_name} = '부적합'"
- 여러 테이블에 유사한 개념이 있을 때, 이 컬럼이 어떤 단위/관점의 데이터인지 명시하세요.
  예: "이미지 단위 불량 여부 (제품 단위 최종 판정은 별도 테이블 참조)"

## 출력 형식 (JSON)
{{
    "business_name": "비즈니스 관점의 한글 컬럼명",
    "description": "이 컬럼의 용도와 의미를 상세히 설명. unique_count <= 10이면 각 값의 의미와 SQL WHERE 예시 포함.",
    "data_type_hint": "실제 저장되는 데이터 형식 힌트 (예: 이메일, 전화번호, 금액 등)",
    "keywords": ["관련", "검색", "키워드", "리스트"]
}}

JSON만 출력하세요:"""

    TABLE_PROMPT_TEMPLATE = """당신은 데이터베이스 스키마 분석 전문가입니다.
다음 테이블 정보를 분석하여 비즈니스 관점의 설명을 JSON으로 생성하세요.

## 테이블 정보
- 테이블명: {table_name}
- 기존 코멘트: {table_comment}
- 예상 행 수: {row_count}

## 컬럼 목록
{column_list}

## 출력 형식 (JSON)
{{
    "business_name": "비즈니스 관점의 한글 테이블명",
    "description": "이 테이블의 용도와 저장 데이터를 상세히 설명",
    "purpose": "어떤 비즈니스 요구사항을 해결하는지"
}}

JSON만 출력하세요:"""
    
    def __init__(self):
        settings = get_settings()
        genai.configure(api_key=settings.gemini_api_key.get_secret_value())
        self.model = genai.GenerativeModel(settings.gemini_model)
    
    async def generate_column_semantic(
        self, 
        column: ColumnInfo, 
        table: TableInfo
    ) -> ColumnSemantic:
        """단일 컬럼의 세만틱 정보 생성"""
        
        unique_count_str = str(column.unique_count) if column.unique_count is not None else "알 수 없음"
        prompt = self.COLUMN_PROMPT_TEMPLATE.format(
            table_name=table.name,
            table_comment=table.comment or "없음",
            column_name=column.name,
            data_type=column.data_type,
            nullable=column.nullable,
            is_pk=column.is_primary_key,
            fk_ref=column.foreign_key_ref or "없음",
            column_comment=column.comment or "없음",
            unique_count=unique_count_str,
            sample_values=json.dumps(column.sample_values[:10], ensure_ascii=False, default=str),
        )
        
        response = await self.model.generate_content_async(prompt)
        result = self._parse_json_response(response.text)
        
        return ColumnSemantic(
            business_name=result.get("business_name", column.name),
            description=result.get("description", ""),
            data_type_hint=result.get("data_type_hint", column.data_type),
            keywords=result.get("keywords", []),
        )
    
    async def generate_table_semantic(self, table: TableInfo) -> TableSemantic:
        """단일 테이블의 세만틱 정보 생성"""
        
        column_list = "\n".join([
            f"- {c.name} ({c.data_type}): PK={c.is_primary_key}, FK={c.foreign_key_ref or 'N/A'}"
            for c in table.columns
        ])
        
        prompt = self.TABLE_PROMPT_TEMPLATE.format(
            table_name=table.name,
            table_comment=table.comment or "없음",
            row_count=table.row_count or "알 수 없음",
            column_list=column_list,
        )
        
        response = await self.model.generate_content_async(prompt)
        result = self._parse_json_response(response.text)
        
        return TableSemantic(
            business_name=result.get("business_name", table.name),
            description=result.get("description", ""),
            purpose=result.get("purpose", ""),
        )
    
    async def enrich_schema(self, schema: SchemaInfo) -> SchemaInfo:
        """전체 스키마에 세만틱 정보 추가"""
        
        for table in schema.tables:
            # 테이블 세만틱 생성
            table_semantic = await self.generate_table_semantic(table)
            table.business_name = table_semantic.business_name
            table.description = table_semantic.description
            
            # 컬럼 세만틱 생성
            for column in table.columns:
                try:
                    col_semantic = await self.generate_column_semantic(column, table)
                    column.business_name = col_semantic.business_name
                    column.description = col_semantic.description
                    column.keywords = col_semantic.keywords
                except Exception as e:
                    print(f"Warning: Failed to generate semantic for {table.name}.{column.name}: {e}")
        
        return schema
    
    @staticmethod
    def _parse_json_response(text: str) -> dict:
        """LLM 응답에서 JSON 추출"""
        # 마크다운 코드 블록 제거
        text = text.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:-1])
        
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            # JSON 파싱 실패 시 빈 dict 반환
            return {}
