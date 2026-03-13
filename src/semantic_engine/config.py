"""
환경 설정 관리 모듈
Pydantic Settings를 사용하여 환경변수 또는 .env 파일에서 설정을 로드합니다.

탐색 순서:
  1. 환경변수 (uvx / MCP env 블록)
  2. ~/.bridgesql/.env
  3. ./.env (로컬 개발용)
"""

import warnings
from functools import lru_cache
from pathlib import Path
from typing import Literal
from urllib.parse import quote_plus

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# 사용자 데이터 디렉토리
BRIDGESQL_DIR = Path.home() / ".bridgesql"

def _env_files() -> list[Path]:
    """탐색할 .env 파일 목록 (우선순위 낮은 것부터)"""
    candidates = [
        Path(".env"),                                           # 현재 디렉토리
        Path(__file__).parent.parent.parent / ".env",          # 로컬 개발 (프로젝트 루트)
        BRIDGESQL_DIR / ".env",                                 # 설치 후 기본 경로
    ]
    return [p for p in candidates if p.exists()]

# google.generativeai 경고 및 기타 FutureWarning 억제
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", message=".*google.generativeai.*")


class Settings(BaseSettings):
    """애플리케이션 전체 설정"""

    model_config = SettingsConfigDict(
        env_file=_env_files(),
        env_file_encoding="utf-8",
        extra="ignore",
    )
    
    # ===========================================
    # LLM Configuration (Gemini 2.0 Flash)
    # ===========================================
    gemini_api_key: SecretStr = Field(default=SecretStr(""), description="Google Gemini API Key")
    gemini_model: str = Field(default="gemini-2.0-flash", description="Gemini 모델명")
    
    # ===========================================
    # Database Connection
    # ===========================================
    database_url: str | None = Field(
        default=None, 
        description="SQLAlchemy 연결 문자열 (예: mysql+pymysql://user:pass@host/db)"
    )
    
    # 개별 DB 설정 (database_url이 없을 때 사용)
    db_host: str = Field(default="localhost", description="DB 호스트")
    db_port: int = Field(default=3306, description="DB 포트")
    db_user: str = Field(default="root", description="DB 사용자")
    db_password: SecretStr = Field(default=SecretStr(""), description="DB 비밀번호")
    db_name: str = Field(default="", description="DB 이름")
    
    # ===========================================
    # Vector Store (ChromaDB)
    # ===========================================
    chroma_persist_dir: Path = Field(
        default=BRIDGESQL_DIR / "chroma",
        description="ChromaDB 저장 경로"
    )
    chroma_collection_name: str = Field(
        default="semantic_catalog",
        description="ChromaDB 컬렉션 이름"
    )
    
    # ===========================================
    # Application Settings
    # ===========================================
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO",
        description="로그 레벨"
    )
    sample_data_limit: int = Field(
        default=10,
        description="컬럼별 샘플 데이터 수집 개수"
    )
    max_sql_retry: int = Field(
        default=3,
        description="SQL 생성 재시도 최대 횟수"
    )

    @model_validator(mode="after")
    def _resolve_paths(self) -> "Settings":
        """상대경로를 홈 디렉토리 기준 절대경로로 변환"""
        if not self.chroma_persist_dir.is_absolute():
            object.__setattr__(self, "chroma_persist_dir", (BRIDGESQL_DIR / self.chroma_persist_dir).resolve())
        return self

    def get_database_url(self) -> str:
        """데이터베이스 연결 URL 반환"""
        if self.database_url:
            return self.database_url
        
        user = self.db_user
        password = quote_plus(self.db_password.get_secret_value())
        host = self.db_host
        port = self.db_port
        name = self.db_name
        
        return f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}"


@lru_cache
def get_settings() -> Settings:
    """싱글톤 설정 인스턴스 반환"""
    return Settings()
