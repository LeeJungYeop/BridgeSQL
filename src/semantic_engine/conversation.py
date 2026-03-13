"""
대화 컨텍스트 관리
세션 내 이전 질문, 역질의 선택지 등을 기억합니다.
"""

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class ConversationTurn:
    """대화 한 턴"""
    timestamp: str
    user_input: str
    system_response: str | None = None
    clarification_options: list[str] | None = None  # 제시한 선택지
    selected_option: int | None = None  # 사용자가 선택한 번호


@dataclass  
class ConversationContext:
    """대화 컨텍스트"""
    session_id: str
    turns: list[ConversationTurn] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        """직렬화"""
        return {
            "session_id": self.session_id,
            "turns": [asdict(t) for t in self.turns]
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'ConversationContext':
        """역직렬화"""
        ctx = cls(session_id=data["session_id"])
        ctx.turns = [ConversationTurn(**t) for t in data.get("turns", [])]
        return ctx

    def add_turn(
        self,
        user_input: str,
        system_response: str | None = None,
        clarification_options: list[str] | None = None,
    ) -> None:
        """새 대화 턴 추가"""
        self.turns.append(ConversationTurn(
            timestamp=datetime.now().isoformat(),
            user_input=user_input,
            system_response=system_response,
            clarification_options=clarification_options,
        ))
    
    def get_last_turn(self) -> ConversationTurn | None:
        """마지막 대화 턴"""
        return self.turns[-1] if self.turns else None
    
    def get_last_options(self) -> list[str] | None:
        """마지막으로 제시한 선택지"""
        if not self.turns:
            return None
        for turn in reversed(self.turns):
            if turn.clarification_options:
                return turn.clarification_options
        return None
    
    def resolve_option_reference(self, user_input: str) -> str | None:
        """
        '1번', '2번' 같은 참조를 실제 선택지로 해석
        
        Returns:
            해석된 선택지 또는 None (참조가 아닌 경우)
        """
        user_input = user_input.strip()
        
        # 숫자만 또는 "N번" 패턴 감지
        option_num = None
        
        if user_input.isdigit():
            option_num = int(user_input)
        elif user_input.endswith("번"):
            num_part = user_input[:-1].strip()
            if num_part.isdigit():
                option_num = int(num_part)
        
        if option_num is None:
            return None
        
        # 마지막 선택지 확인
        options = self.get_last_options()
        if options and 1 <= option_num <= len(options):
            return options[option_num - 1]
        
        return None
    
    def get_context_summary(self, max_turns: int = 3) -> str:
        """최근 대화 요약 (프롬프트용)"""
        if not self.turns:
            return ""
        
        recent = self.turns[-max_turns:]
        lines = ["## 이전 대화:"]
        
        for turn in recent:
            lines.append(f"사용자: {turn.user_input}")
            if turn.clarification_options:
                lines.append("시스템 선택지:")
                for i, opt in enumerate(turn.clarification_options, 1):
                    lines.append(f"  {i}. {opt}")
            if turn.system_response:
                # 긴 응답은 요약
                resp = turn.system_response
                if len(resp) > 200:
                    resp = resp[:200] + "..."
                lines.append(f"시스템: {resp}")
        
        return "\n".join(lines)
    
    def clear(self) -> None:
        """대화 초기화"""
        self.turns.clear()


class ConversationManager:
    """세션별 대화 컨텍스트 관리 (디스크 영속성 포함)"""
    
    def __init__(self, storage_dir: str = "./data/conversation"):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self._contexts: dict[str, ConversationContext] = {}
    
    def get_context(self, session_id: str = "default") -> ConversationContext:
        """세션 컨텍스트 반환 (없으면 생성)"""
        if session_id not in self._contexts:
            # 먼저 로드 시도
            loaded = self.load_context(session_id)
            if loaded:
                self._contexts[session_id] = loaded
            else:
                self._contexts[session_id] = ConversationContext(session_id=session_id)
        return self._contexts[session_id]
    
    def save_context(self, session_id: str = "default") -> bool:
        """컨텍스트를 디스크에 저장"""
        if session_id not in self._contexts:
            return False
            
        file_path = self.storage_dir / f"{session_id}.json"
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(self._contexts[session_id].to_dict(), f, ensure_ascii=False, indent=2)
            return True
        except Exception:
            return False
            
    def load_context(self, session_id: str = "default") -> ConversationContext | None:
        """디스크에서 컨텍스트 로드"""
        file_path = self.storage_dir / f"{session_id}.json"
        if not file_path.exists():
            return None
            
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                return ConversationContext.from_dict(data)
        except Exception:
            return None

    def clear_session(self, session_id: str = "default") -> None:
        """세션 초기화 및 파일 삭제"""
        if session_id in self._contexts:
            self._contexts[session_id].clear()
        
        file_path = self.storage_dir / f"{session_id}.json"
        if file_path.exists():
            file_path.unlink()


# 전역 매니저 인스턴스
_manager: ConversationManager | None = None


def get_conversation_manager() -> ConversationManager:
    """전역 대화 관리자 반환"""
    global _manager
    if _manager is None:
        _manager = ConversationManager()
    return _manager
