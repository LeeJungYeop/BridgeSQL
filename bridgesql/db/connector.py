"""
데이터베이스 연결 관리자
다중 DBMS 지원 (MySQL, PostgreSQL, SQLite, DuckDB)
"""

from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from bridgesql.config import get_settings


class DatabaseConnector:
    """데이터베이스 연결 및 세션 관리"""
    
    def __init__(self, database_url: str | None = None):
        """
        Args:
            database_url: SQLAlchemy 연결 문자열. None이면 설정에서 로드
        """
        settings = get_settings()
        self._url = database_url or settings.get_database_url()
        self._engine: Engine | None = None
        self._session_factory: sessionmaker[Session] | None = None
    
    @property
    def engine(self) -> Engine:
        """SQLAlchemy 엔진 (lazy initialization)"""
        if self._engine is None:
            self._engine = create_engine(
                self._url,
                pool_pre_ping=True,  # 연결 유효성 검사
                pool_recycle=3600,   # 1시간마다 연결 재생성
                echo=False,
            )
        return self._engine
    
    @property
    def session_factory(self) -> sessionmaker[Session]:
        """세션 팩토리"""
        if self._session_factory is None:
            self._session_factory = sessionmaker(
                bind=self.engine,
                autocommit=False,
                autoflush=False,
            )
        return self._session_factory
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """세션 컨텍스트 매니저"""
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def test_connection(self) -> bool:
        """연결 테스트 - 실패 시 예외 발생"""
        with self.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    
    def get_database_name(self) -> str:
        """현재 연결된 데이터베이스 이름 반환"""
        with self.engine.connect() as conn:
            if "mysql" in self._url:
                result = conn.execute(text("SELECT DATABASE()"))
            elif "postgresql" in self._url:
                result = conn.execute(text("SELECT current_database()"))
            elif "sqlite" in self._url:
                return self._url.split("/")[-1].replace(".db", "")
            else:
                return "unknown"
            
            row = result.fetchone()
            return row[0] if row else "unknown"
    
    def close(self) -> None:
        """연결 종료"""
        if self._engine:
            self._engine.dispose()
            self._engine = None
