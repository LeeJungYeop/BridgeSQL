"""
피드백 시스템
쿼리 실행 로그 및 사용자 평가를 수집합니다.
"""

import json
import uuid
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, asdict, field
from enum import Enum


class FeedbackType(str, Enum):
    """피드백 유형"""
    POSITIVE = "positive"   # 👍
    NEGATIVE = "negative"   # 👎
    CLARIFY = "clarify"     # 추가 질문 요청


@dataclass
class QueryLog:
    """쿼리 실행 로그"""
    id: str
    timestamp: str
    question: str
    generated_sql: str
    executed: bool
    row_count: int | None
    execution_time_ms: int | None
    error: str | None
    selected_columns: list[str] = field(default_factory=list)  # 분석에서 선택된 컬럼들
    feedback: FeedbackType | None = None
    follow_up: str | None = None  # 사용자 추가 설명
    
    def to_dict(self) -> dict:
        data = asdict(self)
        if self.feedback:
            data["feedback"] = str(self.feedback.value) if hasattr(self.feedback, "value") else str(self.feedback)
        return data


class FeedbackCollector:
    """피드백 수집 및 저장"""
    
    def __init__(self, storage_dir: Path | str = "./data/feedback", retriever=None):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self._logs_file = self.storage_dir / "query_logs.jsonl"
        self._feedback_file = self.storage_dir / "feedback.jsonl"
        self.retriever = retriever  # SemanticRetriever 인스턴스 (선택적)
    
    def log_query(
        self,
        question: str,
        generated_sql: str,
        executed: bool = False,
        row_count: int | None = None,
        execution_time_ms: int | None = None,
        error: str | None = None,
        selected_columns: list[str] | None = None,
    ) -> str:
        """쿼리 실행 로그 저장, 로그 ID 반환"""
        
        log = QueryLog(
            id=str(uuid.uuid4())[:8],
            timestamp=datetime.now().isoformat(),
            question=question,
            generated_sql=generated_sql,
            executed=executed,
            row_count=row_count,
            execution_time_ms=execution_time_ms,
            error=error,
            selected_columns=selected_columns or [],
        )
        
        with open(self._logs_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(log.to_dict(), ensure_ascii=False) + "\n")
        
        return log.id
    
    def add_feedback(
        self,
        log_id: str,
        feedback: FeedbackType,
        follow_up: str | None = None,
    ) -> bool:
        """기존 로그에 피드백 추가 및 벡터 인덱싱"""
        
        feedback_entry = {
            "log_id": log_id,
            "timestamp": datetime.now().isoformat(),
            "feedback": feedback.value,
            "follow_up": follow_up,
        }
        
        with open(self._feedback_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(feedback_entry, ensure_ascii=False) + "\n")
            
        # 긍정 피드백인 경우 벡터 DB에도 인덱싱 (Semantic Retrieval용)
        if feedback == FeedbackType.POSITIVE and self.retriever:
            try:
                # 로그 정보 가져오기
                question = ""
                sql = ""
                with open(self._logs_file, "r", encoding="utf-8") as f:
                    for line in f:
                        log = json.loads(line)
                        if log["id"] == log_id:
                            question = log["question"]
                            sql = log["generated_sql"]
                            break
                
                if question and sql:
                    # 선택된 컬럼 정보도 함께 인덱싱 (Plan 재사용용)
                    selected_columns = log.get("selected_columns", [])
                    self.retriever.index_few_shot_example(log_id, question, sql, selected_columns)
            except Exception as e:
                print(f"Failed to index feedback: {e}")
        
        return True
    
    def get_positive_examples(self, current_question: str | None = None, limit: int = 5) -> list[dict]:
        """
        긍정 피드백 받은 질문-SQL 쌍 반환 (Few-shot용)
        current_question이 있으면 의미적으로 유사한 예제를 검색합니다.
        """
        
        examples = []
        
        # 1. Semantic Search (유사도 기반)
        if current_question and self.retriever:
            try:
                similar_docs = self.retriever.search_few_shot_examples(current_question, top_k=limit)
                for doc in similar_docs:
                    meta = doc["metadata"]
                    # 점수가 너무 낮으면 제외 (0.3 미만)
                    if doc["score"] < 0.3:
                        continue
                        
                    examples.append({
                        "question": meta["question"],
                        "sql": meta["sql"],
                        "source": "semantic_search",
                        "score": doc["score"],
                        "selected_columns": doc.get("selected_columns", [])
                    })
                
                if examples:
                    return examples
            except Exception:
                pass  # 검색 실패 시 Fallback
        
        # 2. Fallback: 최근 긍정 피드백 (파일 기반)
        if not self._feedback_file.exists():
            return []

        positive_ids = set()
        with open(self._feedback_file, "r", encoding="utf-8") as f:
            for line in f:
                entry = json.loads(line)
                if entry.get("feedback") == FeedbackType.POSITIVE.value:
                    positive_ids.add(entry["log_id"])

        if not positive_ids or not self._logs_file.exists():
            return []

        results = []
        with open(self._logs_file, "r", encoding="utf-8") as f:
            for line in f:
                log = json.loads(line)
                if log["id"] in positive_ids:
                    results.append({
                        "question": log["question"],
                        "sql": log["generated_sql"],
                        "source": "file_fallback",
                        "score": 0.0,  # 실제 유사도 미계산 → 캐시 재사용 방지
                        "selected_columns": log.get("selected_columns", []),
                    })

        return results[-limit:]
    
    def get_negative_examples(self, limit: int = 10) -> list[dict]:
        """부정 피드백 받은 로그 반환 (검토용)"""
        
        if not self._feedback_file.exists():
            return []
        
        negative_entries = []
        with open(self._feedback_file, "r", encoding="utf-8") as f:
            for line in f:
                entry = json.loads(line)
                if entry.get("feedback") == FeedbackType.NEGATIVE.value:
                    negative_entries.append(entry)
        
        # 해당 로그와 매칭
        results = []
        if self._logs_file.exists() and negative_entries:
            log_map = {}
            with open(self._logs_file, "r", encoding="utf-8") as f:
                for line in f:
                    log = json.loads(line)
                    log_map[log["id"]] = log
            
            for entry in negative_entries[:limit]:
                log = log_map.get(entry["log_id"])
                if log:
                    results.append({
                        "question": log["question"],
                        "sql": log["generated_sql"],
                        "follow_up": entry.get("follow_up"),
                        "log_id": entry["log_id"],
                    })
        
        return results
    
    def get_stats(self) -> dict:
        """피드백 통계"""
        total = 0
        positive = 0
        negative = 0
        
        if self._feedback_file.exists():
            with open(self._feedback_file, "r", encoding="utf-8") as f:
                for line in f:
                    entry = json.loads(line)
                    total += 1
                    if entry.get("feedback") == FeedbackType.POSITIVE.value:
                        positive += 1
                    elif entry.get("feedback") == FeedbackType.NEGATIVE.value:
                        negative += 1
        
        return {
            "total_feedback": total,
            "positive": positive,
            "negative": negative,
            "accuracy": round(positive / total, 2) if total > 0 else 0,
        }
