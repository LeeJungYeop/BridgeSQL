"""
ChromaDB 기반 벡터 임베딩 및 검색 모듈
세만틱 카탈로그를 벡터화하여 자연어 검색을 지원합니다.
"""

from pathlib import Path
from typing import Any

import chromadb
from chromadb.config import Settings as ChromaSettings
from chromadb.utils import embedding_functions

from bridgesql.config import get_settings
from bridgesql.db.schema_extractor import SchemaInfo


# 한국어 특화 임베딩 모델
KOREAN_EMBEDDING_MODEL = "jhgan/ko-sroberta-multitask"


class SemanticRetriever:
    """세만틱 카탈로그 벡터 검색기 (한국어 최적화)"""
    
    def __init__(self, persist_dir: Path | str | None = None, collection_name: str | None = None):
        settings = get_settings()
        persist_dir = Path(persist_dir or settings.chroma_persist_dir)
        persist_dir.mkdir(parents=True, exist_ok=True)
        
        self._client = chromadb.PersistentClient(
            path=str(persist_dir),
            settings=ChromaSettings(anonymized_telemetry=False),
        )
        
        # 한국어 임베딩 함수 설정
        self._embedding_fn = embedding_functions.SentenceTransformerEmbeddingFunction(
            model_name=KOREAN_EMBEDDING_MODEL
        )
        
        self._collection_name = collection_name or settings.chroma_collection_name
        
        # 기존 컬렉션과 임베딩 함수 충돌 방지 - 삭제 후 재생성
        try:
            self._collection = self._client.get_or_create_collection(
                name=self._collection_name,
                metadata={"hnsw:space": "cosine"},
                embedding_function=self._embedding_fn,
            )
        except ValueError as e:
            if "conflict" in str(e).lower():
                # 임베딩 충돌 시 컬렉션 삭제 후 재생성
                self._client.delete_collection(self._collection_name)
                self._collection = self._client.create_collection(
                    name=self._collection_name,
                    metadata={"hnsw:space": "cosine"},
                    embedding_function=self._embedding_fn,
                )
            else:
                raise
    
    def index_schema(self, schema: SchemaInfo) -> int:
        """스키마를 벡터 인덱스에 저장"""
        
        documents = []
        metadatas = []
        ids = []
        
        for table in schema.tables:
            # 테이블 레벨 문서
            table_doc = self._create_table_document(table)
            documents.append(table_doc)
            metadatas.append({
                "type": "table",
                "database": schema.database_name,
                "table": table.name,
                "business_name": table.business_name or table.name,
            })
            ids.append(f"{schema.database_name}.{table.name}")
            
            # 컬럼 레벨 문서
            for column in table.columns:
                col_doc = self._create_column_document(column, table)
                documents.append(col_doc)
                metadatas.append({
                    "type": "column",
                    "database": schema.database_name,
                    "table": table.name,
                    "column": column.name,
                    "data_type": column.data_type,
                    "business_name": column.business_name or column.name,
                })
                ids.append(f"{schema.database_name}.{table.name}.{column.name}")
        
        # 기존 데이터 삭제 후 재색인
        try:
            existing_ids = self._collection.get(
                where={"database": schema.database_name}
            )["ids"]
            if existing_ids:
                self._collection.delete(ids=existing_ids)
        except Exception:
            pass
        
        # 새 데이터 추가
        self._collection.add(
            documents=documents,
            metadatas=metadatas,
            ids=ids,
        )
        
        return len(documents)
    
    def search(
        self, 
        query: str, 
        top_k: int = 10,
        filter_type: str | None = None,
        filter_table: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        자연어 질문으로 관련 테이블/컬럼 검색 (Score Fusion 적용)
        테이블의 유사도와 컬럼의 유사도를 합산하여 더 정확한 맥락의 데이터를 찾습니다.
        """
        
        # 1. 넓게 검색하여 부모 테이블 점수 확보 (n=40)
        # 필터가 있더라도 내부적으로는 테이블 점수를 알아야 하므로 전체 검색
        raw_results = self._collection.query(
            query_texts=[query],
            n_results=40,
            include=["documents", "metadatas", "distances"],
        )
        
        # 2. 테이블 점수 맵 구축
        table_scores = {}
        for i in range(len(raw_results["ids"][0])):
            meta = raw_results["metadatas"][0][i]
            if meta["type"] == "table":
                score = 1 - raw_results["distances"][0][i]
                table_scores[meta["table"]] = max(score, 0)
        
        # 3. 모든 결과 아이템 구성 및 점수 합산
        all_items = []
        for i in range(len(raw_results["ids"][0])):
            meta = raw_results["metadatas"][0][i]
            item_id = raw_results["ids"][0][i]
            doc = raw_results["documents"][0][i]
            orig_score = max(1 - raw_results["distances"][0][i], 0)
            
            # 필터 적용
            if filter_type and meta["type"] != filter_type:
                continue
            if filter_table and meta["table"] != filter_table:
                continue
                
            final_score = orig_score
            table_boost = 0
            
            # 컬럼인 경우 부모 테이블 점수 합산 (Score Fusion)
            if meta["type"] == "column":
                table_name = meta["table"]
                table_boost = table_scores.get(table_name, 0)
                # 단순 합산 (가중치 조절 가능: 예 1.0 * col + 0.5 * table)
                final_score = orig_score + table_boost
                
            all_items.append({
                "id": item_id,
                "document": doc,
                "metadata": meta,
                "score": final_score,
                "original_score": orig_score,
                "table_boost": table_boost,
            })
        
        # 4. 점수순 정렬 및 최상위 k개 반환
        all_items.sort(key=lambda x: x["score"], reverse=True)
        return all_items[:top_k]
    
    def get_context_for_query(
        self, 
        query: str, 
        top_k: int = 5
    ) -> str:
        """SQL 생성에 필요한 컨텍스트 문자열 반환"""
        
        results = self.search(query, top_k=top_k)
        
        if not results:
            return "관련 테이블/컬럼을 찾을 수 없습니다."
        
        context_parts = []
        seen_tables = set()
        
        for item in results:
            meta = item["metadata"]
            
            if meta["type"] == "table":
                if meta["table"] not in seen_tables:
                    context_parts.append(
                        f"📋 테이블: {meta['table']} ({meta.get('business_name', '')})"
                    )
                    seen_tables.add(meta["table"])
            else:
                table_prefix = f"  └─ " if meta["table"] in seen_tables else f"📋 {meta['table']}."
                context_parts.append(
                    f"{table_prefix}{meta['column']} ({meta['data_type']}): {meta.get('business_name', '')}"
                )
                seen_tables.add(meta["table"])
        
        return "\n".join(context_parts)
    
    def _create_table_document(self, table) -> str:
        """테이블 검색용 문서 생성"""
        parts = [
            f"테이블: {table.name}",
            f"비즈니스명: {table.business_name or ''}",
            f"설명: {table.description or ''}",
            f"컬럼: {', '.join(c.name for c in table.columns)}",
        ]
        if table.comment:
            parts.append(f"코멘트: {table.comment}")
        return " | ".join(parts)
    
    def _create_column_document(self, column, table) -> str:
        """컬럼 검색용 문서 생성 - 키워드 변형어 포함"""
        parts = [
            f"테이블: {table.name}",
            f"컬럼: {column.name}",
            f"타입: {column.data_type}",
            f"비즈니스명: {column.business_name or ''}",
            f"설명: {column.description or ''}",
        ]
        
        # 컬럼명에서 키워드 변형어 생성
        name_variations = self._generate_name_variations(column.name)
        if name_variations:
            parts.append(f"키워드변형: {', '.join(name_variations)}")
        
        if column.keywords:
            parts.append(f"키워드: {', '.join(column.keywords)}")
        if column.sample_values:
            samples = [str(v) for v in column.sample_values[:5]]
            parts.append(f"샘플: {', '.join(samples)}")
        
        return " | ".join(parts)
    
    def _generate_name_variations(self, name: str) -> list[str]:
        """컬럼명에서 키워드 변형어 생성"""
        variations = set()
        
        # 원본 추가
        variations.add(name)
        
        # 언더스코어 제거 버전
        no_underscore = name.replace('_', '')
        variations.add(no_underscore)
        
        # 언더스코어를 공백으로
        spaced = name.replace('_', ' ')
        variations.add(spaced)
        
        # 각 단어 분리 (언더스코어 기준)
        words = name.split('_')
        variations.update(words)
        
        # 한글 2글자 이상 단어만 추출
        import re
        korean_words = re.findall(r'[가-힣]{2,}', name)
        variations.update(korean_words)
        
        # 영문 단어 추출
        english_words = re.findall(r'[a-zA-Z]{2,}', name)
        variations.update(w.lower() for w in english_words)
        
        # 원본 및 빈 문자열 제외
        variations.discard('')
        
        return list(variations)
    
    def detect_ambiguity(
        self,
        query: str,
        min_score: float = 0.45,
        gap_tolerance: float = 0.15,
        top_k: int = 20,
    ) -> list[dict[str, Any]]:
        """임베딩 유사도 기반 모호성 감지.

        여러 테이블의 컬럼이 비슷한 score로 매칭되면 모호한 질문으로 판단.

        Returns:
            모호한 경우 후보 컬럼 리스트 (table, column, score, metadata).
            명확한 경우 빈 리스트.
        """
        raw = self._collection.query(
            query_texts=[query],
            n_results=top_k,
            include=["metadatas", "distances"],
        )

        # 컬럼 결과만, Score Fusion 없이 순수 cosine 유사도 사용
        col_scores: dict[str, dict] = {}  # "table.column" → best entry
        for i in range(len(raw["ids"][0])):
            meta = raw["metadatas"][0][i]
            if meta["type"] != "column":
                continue
            score = max(1.0 - raw["distances"][0][i], 0.0)
            key = f"{meta['table']}.{meta['column']}"
            if key not in col_scores or score > col_scores[key]["score"]:
                col_scores[key] = {"score": score, "metadata": meta}

        # 테이블별 최고 score 컬럼
        table_best: dict[str, dict] = {}
        for key, entry in col_scores.items():
            table = entry["metadata"]["table"]
            if table not in table_best or entry["score"] > table_best[table]["score"]:
                table_best[table] = {
                    "table": table,
                    "column": entry["metadata"]["column"],
                    "score": entry["score"],
                    "metadata": entry["metadata"],
                }

        # min_score 이상인 테이블만 추림
        candidates = [v for v in table_best.values() if v["score"] >= min_score]
        candidates.sort(key=lambda x: x["score"], reverse=True)

        # 상위 2개 테이블 score 차이가 gap_tolerance 이내면 모호
        if len(candidates) >= 2:
            if candidates[0]["score"] - candidates[1]["score"] <= gap_tolerance:
                return candidates
        return []

    def clear(self) -> None:
        """컬렉션 초기화"""
        self._client.delete_collection(self._collection_name)
        self._collection = self._client.get_or_create_collection(
            name=self._collection_name,
            metadata={"hnsw:space": "cosine"},
            embedding_function=self._embedding_fn,
        )

    # ---------------------------------------------------------
    # Few-shot 예제 관리 (Query History)
    # ---------------------------------------------------------
    
    def _get_history_collection(self):
        """쿼리 이력 저장용 컬렉션 반환 (Lazy Init)"""
        return self._client.get_or_create_collection(
            name="query_history",
            metadata={"hnsw:space": "cosine"},
            embedding_function=self._embedding_fn,
        )

    def index_few_shot_example(self, log_id: str, question: str, sql: str, selected_columns: list[str] | None = None):
        """성공한 쿼리 예제 인덱싱"""
        import json
        collection = self._get_history_collection()
        
        metadata = {
            "log_id": log_id,
            "question": question,
            "sql": sql,
            "type": "few_shot"
        }
        
        if selected_columns:
            metadata["selected_columns"] = json.dumps(selected_columns, ensure_ascii=False)
        
        collection.add(
            documents=[question],  # 질문을 임베딩
            metadatas=[metadata],
            ids=[log_id]
        )
        
    def search_few_shot_examples(self, query: str, top_k: int = 5) -> list[dict]:
        """질문과 유사한 성공 사례 검색"""
        import json
        collection = self._get_history_collection()
        
        results = collection.query(
            query_texts=[query],
            n_results=top_k,
            include=["metadatas", "distances"]
        )
        
        items = []
        if results["ids"]:
            for i in range(len(results["ids"][0])):
                meta = results["metadatas"][0][i]
                
                # JSON 문자열로 저장된 selected_columns 복원
                if "selected_columns" in meta:
                    try:
                        meta["selected_columns"] = json.loads(meta["selected_columns"])
                    except:
                        meta["selected_columns"] = []
                
                items.append({
                    "id": results["ids"][0][i],
                    "metadata": meta,  # 여기에 selected_columns 포함됨
                    "question": meta["question"],
                    "sql": meta["sql"],
                    "selected_columns": meta.get("selected_columns", []),
                    "score": 1 - results["distances"][0][i],
                })
        
        return items

    def delete_example(self, example_id: str) -> bool:
        """예제 삭제. 성공하면 True, 없으면 False."""
        collection = self._get_history_collection()
        existing = collection.get(ids=[example_id])
        if not existing["ids"]:
            return False
        collection.delete(ids=[example_id])
        return True

    def list_examples(self, limit: int = 50) -> list[dict]:
        """저장된 모든 예제 목록 반환"""
        import json
        collection = self._get_history_collection()
        results = collection.get(include=["metadatas"], limit=limit)
        items = []
        for i, doc_id in enumerate(results["ids"]):
            meta = results["metadatas"][i]
            items.append({
                "id": doc_id,
                "question": meta.get("question", ""),
                "sql": meta.get("sql", ""),
            })
        return items
