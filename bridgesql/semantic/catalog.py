"""
세만틱 카탈로그 저장/조회 관리자
스키마 메타데이터를 JSON 파일로 저장하고 관리합니다.
"""

import json
from pathlib import Path
from datetime import datetime

from bridgesql.db.schema_extractor import SchemaInfo

_DEFAULT_CATALOG_DIR = Path.home() / ".bridgesql" / "catalog"


class SemanticCatalog:
    """세만틱 카탈로그 저장소"""

    def __init__(self, storage_dir: Path | str | None = None):
        self.storage_dir = Path(storage_dir) if storage_dir else _DEFAULT_CATALOG_DIR
        self.storage_dir.mkdir(parents=True, exist_ok=True)
    
    def save(self, schema: SchemaInfo) -> Path:
        """스키마 정보를 JSON 파일로 저장"""
        
        catalog_data = {
            "version": "1.0",
            "generated_at": datetime.now().isoformat(),
            "database": schema.database_name,
            "schema": schema.to_dict(),
        }
        
        filename = f"{schema.database_name}_catalog.json"
        filepath = self.storage_dir / filename
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(catalog_data, f, ensure_ascii=False, indent=2)
        
        return filepath
    
    def load(self, database_name: str) -> SchemaInfo | None:
        """저장된 카탈로그 로드"""
        
        filename = f"{database_name}_catalog.json"
        filepath = self.storage_dir / filename
        
        if not filepath.exists():
            return None
        
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        return self._dict_to_schema(data["schema"])
    
    def exists(self, database_name: str) -> bool:
        """카탈로그 존재 여부 확인"""
        filename = f"{database_name}_catalog.json"
        return (self.storage_dir / filename).exists()
    
    def edit_table(
        self,
        database_name: str,
        table_name: str,
        business_name: str | None = None,
        description: str | None = None,
        column_name: str | None = None,
        column_business_name: str | None = None,
        column_description: str | None = None,
    ) -> bool:
        """카탈로그 JSON에서 테이블/컬럼 메타데이터를 직접 수정. 성공하면 True."""
        filename = f"{database_name}_catalog.json"
        filepath = self.storage_dir / filename
        if not filepath.exists():
            return False

        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        schema = data["schema"]
        table = next((t for t in schema["tables"] if t["name"] == table_name), None)
        if table is None:
            return False

        if business_name is not None:
            table["business_name"] = business_name
        if description is not None:
            table["description"] = description

        if column_name is not None:
            col = next((c for c in table["columns"] if c["name"] == column_name), None)
            if col is not None:
                if column_business_name is not None:
                    col["business_name"] = column_business_name
                if column_description is not None:
                    col["description"] = column_description

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        return True

    def list_catalogs(self) -> list[str]:
        """저장된 모든 카탈로그 목록"""
        return [
            f.stem.replace("_catalog", "") 
            for f in self.storage_dir.glob("*_catalog.json")
        ]
    
    def _dict_to_schema(self, data: dict) -> SchemaInfo:
        """딕셔너리를 SchemaInfo 객체로 변환"""
        from bridgesql.db.schema_extractor import (
            SchemaInfo, TableInfo, ColumnInfo
        )
        
        tables = []
        for t in data["tables"]:
            columns = [
                ColumnInfo(
                    name=c["name"],
                    data_type=c["data_type"],
                    nullable=c["nullable"],
                    default=None,
                    is_primary_key=c["is_primary_key"],
                    is_foreign_key=c["is_foreign_key"],
                    foreign_key_ref=c.get("foreign_key_ref"),
                    comment=c.get("comment"),
                    sample_values=c.get("sample_values", []),
                    null_ratio=c.get("null_ratio"),
                    unique_count=c.get("unique_count"),
                    business_name=c.get("business_name"),
                    description=c.get("description"),
                    keywords=c.get("keywords", []),
                )
                for c in t["columns"]
            ]
            
            table = TableInfo(
                name=t["name"],
                schema=t.get("schema"),
                columns=columns,
                primary_keys=[c["name"] for c in t["columns"] if c["is_primary_key"]],
                row_count=t.get("row_count"),
                comment=t.get("comment"),
                business_name=t.get("business_name"),
                description=t.get("description"),
            )
            tables.append(table)
        
        return SchemaInfo(
            database_name=data["database_name"],
            tables=tables,
        )
