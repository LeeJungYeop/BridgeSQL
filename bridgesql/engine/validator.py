"""
SQL 검증기
문법 오류, SQL Injection, 위험한 명령어를 탐지합니다.
"""

import re
from dataclasses import dataclass
from enum import Enum

import sqlparse
from sqlparse.sql import Statement
from sqlparse.tokens import Keyword, DML

from sqlalchemy import text
from sqlalchemy.engine import Engine


class ValidationErrorType(str, Enum):
    """검증 오류 유형"""
    SYNTAX_ERROR = "syntax_error"
    INJECTION_DETECTED = "injection_detected"
    DANGEROUS_OPERATION = "dangerous_operation"
    EXECUTION_ERROR = "execution_error"


@dataclass
class ValidationResult:
    """검증 결과"""
    is_valid: bool
    error_type: ValidationErrorType | None = None
    error_message: str | None = None
    sanitized_sql: str | None = None


class SQLValidator:
    """SQL 보안 및 문법 검증"""
    
    # 위험한 SQL 패턴
    DANGEROUS_PATTERNS = [
        r"\bDROP\s+(TABLE|DATABASE|INDEX)\b",
        r"\bTRUNCATE\s+TABLE\b",
        r"\bALTER\s+TABLE\b",
        r"\bCREATE\s+(TABLE|DATABASE|INDEX)\b",
        r"\bGRANT\b",
        r"\bREVOKE\b",
    ]
    
    # SQL Injection 패턴
    INJECTION_PATTERNS = [
        r";\s*(DROP|DELETE|UPDATE|INSERT)",  # 다중 쿼리
        r"--\s*$",                             # 주석 공격
        r"/\*.*\*/",                           # 블록 주석
        r"'\s*OR\s+'1'\s*=\s*'1",              # Classic OR injection
        r"UNION\s+SELECT\s+NULL",              # UNION injection
        r"WAITFOR\s+DELAY",                    # Time-based injection
        r"BENCHMARK\s*\(",                     # MySQL benchmark
    ]
    
    # 허용되는 DML (기본값: SELECT만)
    ALLOWED_DML = {"SELECT"}
    
    def __init__(self, allow_write: bool = False):
        """
        Args:
            allow_write: True면 INSERT, UPDATE, DELETE도 허용
        """
        if allow_write:
            self.allowed_dml = {"SELECT", "INSERT", "UPDATE", "DELETE"}
        else:
            self.allowed_dml = {"SELECT"}
    
    def validate(self, sql: str) -> ValidationResult:
        """SQL 검증"""
        
        # 1. 빈 쿼리 체크
        sql = sql.strip()
        if not sql:
            return ValidationResult(
                is_valid=False,
                error_type=ValidationErrorType.SYNTAX_ERROR,
                error_message="빈 SQL 쿼리입니다.",
            )
        
        # 2. Injection 패턴 체크
        injection_check = self._check_injection(sql)
        if not injection_check.is_valid:
            return injection_check
        
        # 3. 위험한 명령어 체크
        dangerous_check = self._check_dangerous(sql)
        if not dangerous_check.is_valid:
            return dangerous_check
        
        # 4. DML 유형 체크
        dml_check = self._check_dml_type(sql)
        if not dml_check.is_valid:
            return dml_check
        
        # 5. 문법 검증 (sqlparse)
        syntax_check = self._check_syntax(sql)
        if not syntax_check.is_valid:
            return syntax_check
        
        # 정리된 SQL 반환
        formatted_sql = sqlparse.format(
            sql, 
            reindent=True, 
            keyword_case="upper"
        )
        
        return ValidationResult(
            is_valid=True,
            sanitized_sql=formatted_sql,
        )
    
    def dry_run(self, sql: str, engine: Engine) -> ValidationResult:
        """실제 실행 없이 쿼리 검증 (EXPLAIN 사용)"""
        
        # 먼저 기본 검증
        basic_check = self.validate(sql)
        if not basic_check.is_valid:
            return basic_check
        
        try:
            with engine.connect() as conn:
                # MySQL: EXPLAIN으로 실행 계획 확인
                explain_sql = f"EXPLAIN {sql}"
                conn.execute(text(explain_sql))
            
            return ValidationResult(
                is_valid=True,
                sanitized_sql=basic_check.sanitized_sql,
            )
            
        except Exception as e:
            return ValidationResult(
                is_valid=False,
                error_type=ValidationErrorType.EXECUTION_ERROR,
                error_message=str(e),
            )
    
    def _check_injection(self, sql: str) -> ValidationResult:
        """SQL Injection 패턴 탐지"""
        sql_upper = sql.upper()
        
        for pattern in self.INJECTION_PATTERNS:
            if re.search(pattern, sql_upper, re.IGNORECASE):
                return ValidationResult(
                    is_valid=False,
                    error_type=ValidationErrorType.INJECTION_DETECTED,
                    error_message=f"SQL Injection 패턴이 탐지되었습니다: {pattern}",
                )
        
        return ValidationResult(is_valid=True)
    
    def _check_dangerous(self, sql: str) -> ValidationResult:
        """위험한 명령어 탐지"""
        sql_upper = sql.upper()
        
        for pattern in self.DANGEROUS_PATTERNS:
            if re.search(pattern, sql_upper, re.IGNORECASE):
                return ValidationResult(
                    is_valid=False,
                    error_type=ValidationErrorType.DANGEROUS_OPERATION,
                    error_message=f"위험한 SQL 명령어가 포함되어 있습니다: {pattern}",
                )
        
        return ValidationResult(is_valid=True)
    
    def _check_dml_type(self, sql: str) -> ValidationResult:
        """DML 유형 확인"""
        parsed = sqlparse.parse(sql)
        
        if not parsed:
            return ValidationResult(
                is_valid=False,
                error_type=ValidationErrorType.SYNTAX_ERROR,
                error_message="SQL 파싱 실패",
            )
        
        for statement in parsed:
            stmt_type = statement.get_type()
            if stmt_type and stmt_type.upper() not in self.allowed_dml:
                return ValidationResult(
                    is_valid=False,
                    error_type=ValidationErrorType.DANGEROUS_OPERATION,
                    error_message=f"허용되지 않은 SQL 유형: {stmt_type}. 허용: {self.allowed_dml}",
                )
        
        return ValidationResult(is_valid=True)
    
    def _check_syntax(self, sql: str) -> ValidationResult:
        """기본 문법 검증"""
        try:
            parsed = sqlparse.parse(sql)
            
            if not parsed or all(not str(s).strip() for s in parsed):
                return ValidationResult(
                    is_valid=False,
                    error_type=ValidationErrorType.SYNTAX_ERROR,
                    error_message="유효한 SQL 문이 아닙니다.",
                )
            
            return ValidationResult(is_valid=True)
            
        except Exception as e:
            return ValidationResult(
                is_valid=False,
                error_type=ValidationErrorType.SYNTAX_ERROR,
                error_message=f"SQL 문법 오류: {e}",
            )
