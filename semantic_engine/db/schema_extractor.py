"""
스키마 메타데이터 추출기
테이블, 컬럼, 관계 정보를 자동으로 수집합니다.
"""

from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine


@dataclass
class ColumnInfo:
    """컬럼 메타데이터"""
    name: str
    data_type: str
    nullable: bool
    default: Any
    is_primary_key: bool
    is_foreign_key: bool
    foreign_key_ref: str | None = None  # "table.column" 형식
    comment: str | None = None
    
    # 통계 정보 (샘플링 후 채워짐)
    sample_values: list[Any] = field(default_factory=list)
    null_ratio: float | None = None
    unique_count: int | None = None
    
    # 세만틱 정보 (LLM 생성 후 채워짐)
    business_name: str | None = None
    description: str | None = None
    keywords: list[str] = field(default_factory=list)


@dataclass
class TableInfo:
    """테이블 메타데이터"""
    name: str
    schema: str | None
    columns: list[ColumnInfo]
    primary_keys: list[str]
    row_count: int | None = None
    comment: str | None = None
    
    # 세만틱 정보
    business_name: str | None = None
    description: str | None = None


@dataclass
class SchemaInfo:
    """전체 스키마 메타데이터"""
    database_name: str
    tables: list[TableInfo]
    
    def get_table(self, name: str) -> TableInfo | None:
        """테이블명으로 조회"""
        for table in self.tables:
            if table.name == name:
                return table
        return None
    
    def to_dict(self) -> dict:
        """딕셔너리 변환"""
        return {
            "database_name": self.database_name,
            "tables": [
                {
                    "name": t.name,
                    "schema": t.schema,
                    "row_count": t.row_count,
                    "comment": t.comment,
                    "business_name": t.business_name,
                    "description": t.description,
                    "columns": [
                        {
                            "name": c.name,
                            "data_type": c.data_type,
                            "nullable": c.nullable,
                            "is_primary_key": c.is_primary_key,
                            "is_foreign_key": c.is_foreign_key,
                            "foreign_key_ref": c.foreign_key_ref,
                            "comment": c.comment,
                            "sample_values": c.sample_values,
                            "null_ratio": c.null_ratio,
                            "unique_count": c.unique_count,
                            "business_name": c.business_name,
                            "description": c.description,
                            "keywords": c.keywords,
                        }
                        for c in t.columns
                    ],
                }
                for t in self.tables
            ],
        }
    
    def get_schema_summary(self) -> str:
        """LLM용 전체 스키마 요약 (테이블 및 컬럼 목록)"""
        if not self.tables:
            return "No tables found."
        
        lines = ["## [DB 전역 스키마 개요]"]
        for t in self.tables:
            col_names = [f"{c.name}({c.business_name or ''})" for c in t.columns]
            lines.append(f"- {t.name} ({t.business_name or ''}): {', '.join(col_names)}")
        
        return "\n".join(lines)


class SchemaExtractor:
    """데이터베이스 스키마 추출기"""
    
    def __init__(self, engine: Engine):
        self.engine = engine
        self._inspector = inspect(engine)
    
    def extract_full_schema(self, include_views: bool = False) -> SchemaInfo:
        """전체 스키마 추출"""
        # 데이터베이스 이름
        db_name = self._get_database_name()
        
        # 테이블 목록
        table_names = self._inspector.get_table_names()
        if include_views:
            table_names.extend(self._inspector.get_view_names())
        
        tables = []
        for table_name in table_names:
            table_info = self._extract_table_info(table_name)
            tables.append(table_info)
        
        return SchemaInfo(database_name=db_name, tables=tables)
    
    def _get_database_name(self) -> str:
        """데이터베이스 이름 조회"""
        url = str(self.engine.url)
        if "mysql" in url:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT DATABASE()"))
                row = result.fetchone()
                return row[0] if row else "unknown"
        elif "postgresql" in url:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT current_database()"))
                row = result.fetchone()
                return row[0] if row else "unknown"
        elif "sqlite" in url:
            return url.split("/")[-1].replace(".db", "")
        return "unknown"
    
    def _extract_table_info(self, table_name: str, schema: str | None = None) -> TableInfo:
        """단일 테이블 정보 추출"""
        # 컬럼 정보
        columns_raw = self._inspector.get_columns(table_name, schema=schema)
        pk_constraint = self._inspector.get_pk_constraint(table_name, schema=schema)
        fk_constraints = self._inspector.get_foreign_keys(table_name, schema=schema)
        
        primary_keys = pk_constraint.get("constrained_columns", []) if pk_constraint else []
        
        # FK 매핑 생성
        fk_map: dict[str, str] = {}
        for fk in fk_constraints:
            for local_col, ref_col in zip(
                fk["constrained_columns"], 
                fk["referred_columns"]
            ):
                fk_map[local_col] = f"{fk['referred_table']}.{ref_col}"
        
        columns = []
        for col in columns_raw:
            column_info = ColumnInfo(
                name=col["name"],
                data_type=str(col["type"]),
                nullable=col.get("nullable", True),
                default=col.get("default"),
                is_primary_key=col["name"] in primary_keys,
                is_foreign_key=col["name"] in fk_map,
                foreign_key_ref=fk_map.get(col["name"]),
                comment=col.get("comment"),
            )
            columns.append(column_info)
        
        # 테이블 코멘트 (MySQL만 지원)
        table_comment = self._get_table_comment(table_name, schema)
        
        # Row count 추정
        row_count = self._estimate_row_count(table_name, schema)
        
        return TableInfo(
            name=table_name,
            schema=schema,
            columns=columns,
            primary_keys=primary_keys,
            row_count=row_count,
            comment=table_comment,
        )
    
    def _get_table_comment(self, table_name: str, schema: str | None) -> str | None:
        """테이블 코멘트 조회 (MySQL)"""
        url = str(self.engine.url)
        if "mysql" not in url:
            return None
        
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT TABLE_COMMENT 
                    FROM information_schema.TABLES 
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name
                """)
                result = conn.execute(query, {"table_name": table_name})
                row = result.fetchone()
                return row[0] if row and row[0] else None
        except Exception:
            return None
    
    def _estimate_row_count(self, table_name: str, schema: str | None) -> int | None:
        """테이블 row count 추정 (빠른 방식)"""
        url = str(self.engine.url)
        
        try:
            with self.engine.connect() as conn:
                if "mysql" in url:
                    # MySQL은 information_schema에서 빠르게 추정
                    query = text("""
                        SELECT TABLE_ROWS 
                        FROM information_schema.TABLES 
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name
                    """)
                    result = conn.execute(query, {"table_name": table_name})
                    row = result.fetchone()
                    return row[0] if row else None
                else:
                    # 다른 DB는 COUNT(*) 사용 (느릴 수 있음)
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                    row = result.fetchone()
                    return row[0] if row else None
        except Exception:
            return None
