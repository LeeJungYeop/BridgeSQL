"""Shared lazy state — connector, retriever, schema."""
import threading

DatabaseConnector = None
SemanticRetriever = None
SemanticCatalog = None

_connector = None
_retriever = None
_catalog = None
_schema = None
_retriever_lock = threading.Lock()


def _ensure_imports() -> None:
    global DatabaseConnector, SemanticRetriever, SemanticCatalog
    if DatabaseConnector is None:
        from semantic_engine.db.connector import DatabaseConnector as _dc
        from semantic_engine.rag.retriever import SemanticRetriever as _sr
        from semantic_engine.semantic.catalog import SemanticCatalog as _sc
        DatabaseConnector = _dc
        SemanticRetriever = _sr
        SemanticCatalog = _sc


def get_connector():
    global _connector
    _ensure_imports()
    if _connector is None:
        _connector = DatabaseConnector()
    return _connector


def get_retriever():
    global _retriever
    _ensure_imports()
    if _retriever is None:
        with _retriever_lock:
            if _retriever is None:
                _retriever = SemanticRetriever()
    return _retriever


def get_schema():
    global _schema, _catalog
    _ensure_imports()
    if _schema is None:
        if _catalog is None:
            _catalog = SemanticCatalog()
        connector = get_connector()
        database_name = connector.get_database_name()
        _schema = _catalog.load(database_name)
    return _schema


def invalidate_schema() -> None:
    """카탈로그 수정 후 캐시 무효화."""
    global _schema
    _schema = None
