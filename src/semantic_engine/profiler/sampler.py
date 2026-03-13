"""
샘플 데이터 수집기
각 컬럼의 샘플 값과 통계 정보를 수집합니다.
"""

from sqlalchemy import text
from sqlalchemy.engine import Engine

from semantic_engine.config import get_settings
from semantic_engine.profiler.schema_extractor import SchemaInfo, TableInfo


class DataSampler:
    """컬럼별 샘플 데이터 및 통계 수집"""
    
    def __init__(self, engine: Engine, sample_limit: int | None = None):
        """
        Args:
            engine: SQLAlchemy 엔진
            sample_limit: 컬럼별 샘플 개수 (기본값: 설정에서 로드)
        """
        self.engine = engine
        settings = get_settings()
        self.sample_limit = sample_limit or settings.sample_data_limit
    
    def enrich_schema_with_samples(self, schema_info: SchemaInfo) -> SchemaInfo:
        """스키마 정보에 샘플 데이터 추가"""
        for table in schema_info.tables:
            self._sample_table(table)
        return schema_info
    
    def _sample_table(self, table: TableInfo) -> None:
        """단일 테이블의 모든 컬럼 샘플링"""
        column_names = [c.name for c in table.columns]
        
        if not column_names:
            return
        
        # 샘플 데이터 조회 (유니크한 값을 충분히 확보하기 위해 더 많은 행 조회)
        columns_str = ", ".join(f"`{c}`" for c in column_names)
        fetch_limit = self.sample_limit * 10
        query = text(f"SELECT {columns_str} FROM `{table.name}` LIMIT :limit")
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, {"limit": fetch_limit})
                rows = result.fetchall()
                
                # 컬럼별 샘플 값 수집 (중복 제거 및 순서 유지)
                for col_idx, column in enumerate(table.columns):
                    seen = set()
                    unique_samples = []
                    for row in rows:
                        val = self._serialize_value(row[col_idx])
                        if val is not None and val not in seen:
                            unique_samples.append(val)
                            seen.add(val)
                            if len(unique_samples) >= self.sample_limit:
                                break
                    column.sample_values = unique_samples
                
                # 통계 정보 수집
                self._collect_column_stats(table, conn)
                
        except Exception as e:
            print(f"Warning: Failed to sample table {table.name}: {e}")
    
    def _collect_column_stats(self, table: TableInfo, conn) -> None:
        """컬럼별 통계 정보 수집"""
        for column in table.columns:
            try:
                # NULL 비율
                null_query = text(f"""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN `{column.name}` IS NULL THEN 1 ELSE 0 END) as nulls
                    FROM `{table.name}`
                """)
                result = conn.execute(null_query)
                row = result.fetchone()
                if row and row[0] > 0:
                    # Decimal 타입 처리
                    total = int(row[0]) if row[0] else 0
                    nulls = int(row[1]) if row[1] else 0
                    column.null_ratio = round(nulls / total, 4) if total > 0 else 0.0
                
                # 유니크 값 개수 (대용량 테이블은 추정치)
                unique_query = text(f"""
                    SELECT COUNT(DISTINCT `{column.name}`) FROM `{table.name}`
                """)
                result = conn.execute(unique_query)
                row = result.fetchone()
                column.unique_count = int(row[0]) if row and row[0] else None
                
            except Exception:
                # 일부 컬럼 타입은 통계 수집 불가 (BLOB 등)
                pass
    
    @staticmethod
    def _serialize_value(value) -> str | int | float | bool | None:
        """값을 JSON 직렬화 가능한 형태로 변환"""
        if value is None:
            return None
        if isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, bytes):
            return f"<bytes:{len(value)}>"
        # datetime, date, decimal 등
        return str(value)
