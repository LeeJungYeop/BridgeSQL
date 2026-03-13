"""
BridgeSQL 대화형 TUI (Terminal User Interface)
OpenCode / Claude Code 스타일의 채팅 인터페이스
"""

import asyncio
import time
import json
import warnings
import logging
import os
from pathlib import Path
import pyfiglet

# HuggingFace / SentenceTransformer / Google Generative AI 경고 억제
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", message=".*UNEXPECTED.*")
warnings.filterwarnings("ignore", message=".*google.generativeai.*")
os.environ["TOKENIZERS_PARALLELISM"] = "false"
os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
os.environ["TQDM_DISABLE"] = "1"
logging.getLogger("sentence_transformers").setLevel(logging.ERROR)
logging.getLogger("huggingface_hub").setLevel(logging.ERROR)
logging.getLogger("model2vec").setLevel(logging.ERROR)
logging.getLogger("google.generativeai").setLevel(logging.ERROR)

from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich.syntax import Syntax
from rich.text import Text
from rich.columns import Columns
from rich.markdown import Markdown
from rich.rule import Rule
from rich.align import Align
from rich.padding import Padding
from rich import box

from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.formatted_text import HTML

from sqlalchemy import text

from semantic_engine.config import get_settings
from semantic_engine.profiler.connector import DatabaseConnector
from semantic_engine.profiler.schema_extractor import SchemaExtractor, SchemaInfo
from semantic_engine.profiler.sampler import DataSampler
from semantic_engine.semantic.catalog import SemanticCatalog
from semantic_engine.rag.retriever import SemanticRetriever
from semantic_engine.engine.planner import QueryPlanner
from semantic_engine.engine.sql_generator import SQLGenerator, format_schema_for_prompt
from semantic_engine.engine.validator import SQLValidator
from semantic_engine.conversation import ConversationManager, get_conversation_manager
from semantic_engine.feedback import FeedbackCollector, FeedbackType


console = Console()

# ─── 상수 ───────────────────────────────────────────────────────────────────
APP_NAME = "BridgeSQL"
VERSION = "0.1.0"
HISTORY_FILE = Path("./data/.chat_history")

# ─── 컬러 팔레트 ────────────────────────────────────────────────────────────
# ─── 컬러 팔레트 (Claude / Modern CLI Style) ──────────────────────────────
C_PRIMARY = "#a78bfa"        # Soft Purple (Claude Accent)
C_SECONDARY = "#818cf8"      # Indigo
C_USER = "#ffffff"           # White
C_AI = "#d1d5db"             # Light Gray (Text)
C_SQL = "#fde047"            # Yellow (Code)
C_RESULT = "#2dd4bf"         # Teal (Results)
C_SUCCESS = "#4ade80"        # Green
C_ERR = "#f87171"            # Red
C_DIM = "#6b7280"            # Gray
C_BG_DIM = "on #111827"      # Dark Background


# ─── 헤더/배너 ─────────────────────────────────────────────────────────────
def show_banner(db_name: str | None = None, model_name: str | None = None):
    """슬림하고 현대적인 상단 배너 표시"""
    console.print()
    
    # 상단 상태 칩들
    status_line = Text()
    status_line.append(" ʙʀɪᴅɢᴇSQL ", style=f"bold white on {C_PRIMARY}")
    status_line.append(f" v{VERSION} ", style=f"bold white on {C_SECONDARY}")
    status_line.append("  ")
    
    if db_name:
        status_line.append(f" 󰆼 {db_name} ", style="bold #064e3b on #059669")
    else:
        status_line.append(" 󰆼 Disconnected ", style="bold #450a0a on #dc2626")
        
    status_line.append("  ")
    if model_name:
        status_line.append(f" 󰧑 {model_name} ", style=f"bold #1e1b4b on {C_PRIMARY}")
        
    console.print(status_line)
    
    # 웰컴 메시지
    welcome = Text()
    welcome.append("\n  준비되었습니다. 자연어로 데이터베이스에 질문하세요.", style=C_DIM)
    welcome.append("\n  도움말이 필요하면 ", style=C_DIM)
    welcome.append("/help", style=f"bold {C_PRIMARY}")
    welcome.append("를 입력하세요.\n", style=C_DIM)
    console.print(welcome)


def show_help():
    """도움말 표시"""
    help_table = Table(
        show_header=False,
        box=None,
        padding=(0, 3),
        expand=True,
    )
    help_table.add_column("명령어", style=f"bold {C_PRIMARY}", min_width=18, no_wrap=True)
    help_table.add_column("설명", style="white")
    
    help_table.add_row("  /help", "이 도움말을 표시합니다")
    help_table.add_row("  /models", "AI 모델을 변경합니다")
    help_table.add_row("  /profile", "DB 스키마를 재분석합니다")
    help_table.add_row("  /status", "현재 연결 상태를 확인합니다")
    help_table.add_row("  /clear", "대화 내역을 초기화합니다")
    help_table.add_row("  /exit", "프로그램을 종료합니다")
    help_table.add_row()
    help_table.add_row("  [dim]그 외 입력[/dim]", "[dim]자연어 질문으로 처리됩니다[/dim]")
    
    console.print(Panel(
        help_table,
        title=f"[bold {C_PRIMARY}] 💡 명령어 [/]",
        title_align="left",
        border_style=C_PRIMARY,
        box=box.ROUNDED,
        padding=(1, 0),
    ))


# ─── 메시지 렌더링 ──────────────────────────────────────────────────────────
def render_user_message(message: str):
    """사용자 질문 렌더링"""
    console.print()
    prefix = Text(" 👤  You ", style=f"bold white on {C_SECONDARY}")
    console.print(prefix, f" {message}")
    console.print()


def render_ai_thinking(status_text: str = "생각 중"):
    """AI 처리 상태 (Thinking)"""
    return console.status(
        f"  [bold {C_PRIMARY}]󰧑[/] [italic {C_DIM}]{status_text}...[/]",
        spinner="dots12",
        spinner_style=C_PRIMARY,
    )


def render_candidates(plan):
    """관련 컬럼 후보 표시 (Claude 스타일)"""
    if plan.is_cached:
        console.print(f"  [bold {C_SUCCESS}]󰁔[/] [bold]이전 성공 사례를 사용하여 즉시 실행합니다.[/]")
        console.print(f"    [{C_DIM}]→ {', '.join(plan.recommended_columns)}[/]\n")
        return
    
    console.print(f"  [bold {C_PRIMARY}]󰧑[/] {plan.intent}\n")
    
    relevant = [c for c in plan.candidates if c.confidence >= 0.3] or plan.candidates[:1]
    
    for i, c in enumerate(relevant, 1):
        # 정교한 컨피던스 바
        filled = int(c.confidence * 8)
        bar = "▰" * filled + "▱" * (8 - filled)
        pct = f"{int(c.confidence * 100)}%"
        
        is_rec = f"{c.table}.{c.column}" in plan.recommended_columns
        
        item = Text()
        item.append(f"    {i}. ", style=f"bold {C_PRIMARY}")
        item.append(f"{c.business_name} ", style="bold white")
        
        if is_rec:
            item.append("󰄬 Recommended", style=f"italic {C_SUCCESS}")
            
        console.print(item)
        console.print(f"       {bar} {pct}  [{C_DIM}]{c.table}.{c.column}[/]")
        console.print(f"       [{C_DIM}]{c.reason}[/]\n")


def render_sql(sql: str, explanation: str, title: str = "생성된 SQL"):
    """SQL 렌더링 (코드 블록 스타일)"""
    syntax = Syntax(
        sql, "sql", 
        theme="monokai", 
        line_numbers=False,
        padding=(1, 2),
        background_color="default"
    )
    
    console.print(f"  [bold {C_SQL}]󰠵 {title}[/]")
    console.print(Padding(syntax, (0, 4)))
    if explanation:
        console.print(f"    [italic {C_DIM}]󰛨 {explanation}[/]\n")


def render_result_table(rows, columns):
    """쿼리 결과 테이블 (Zebra 스타일)"""
    table = Table(
        show_header=True,
        header_style=f"bold {C_RESULT}",
        border_style=C_DIM,
        box=box.SIMPLE,
        padding=(0, 2),
        expand=False,
    )
    for col in columns:
        table.add_column(str(col))
        
    for i, row in enumerate(rows[:20]):
        style = "on #1e293b" if i % 2 == 0 else ""
        table.add_row(*[str(v) for v in row], style=style)
    
    console.print(f"  [bold {C_RESULT}]󰆼 Result[/]")
    console.print(Padding(table, (0, 4)))
    
    if len(rows) > 20:
        console.print(f"    [dim]... 외 {len(rows) - 20}개 행이 더 있습니다.[/]")
    console.print()


def render_error(message: str):
    """에러 메시지"""
    console.print(f"  [bold {C_ERR}]✖ Error:[/] {message}")


def render_success(message: str):
    """성공 메시지"""
    console.print(f"  [bold {C_SUCCESS}]✔[/] {message}")


def render_info(message: str):
    """정보 메시지"""
    console.print(f"  [bold {C_DIM}]ℹ[/] {message}")


def render_separator():
    """간결한 구분선"""
    # Claude 스타일은 구분선 대신 여백을 선호하지만, 필요한 경우 얇은 점선 사용
    console.print(f"[{C_DIM}]" + "─" * (console.width - 4) + "[/]")


def render_schema_answer(plan, schema: SchemaInfo):
    """스키마 탐색 결과 (상세 목록)"""
    console.print(f"  [bold {C_PRIMARY}]󰧑[/] {plan.intent}\n")
    
    for c in plan.candidates:
        col_info = next((col for t in schema.tables if t.name == c.table 
                        for col in t.columns if col.name == c.column), None)
        
        item = Text()
        item.append(f"    • {c.table}", style=C_DIM)
        item.append(".", style=C_DIM)
        item.append(c.column, style=f"bold {C_PRIMARY}")
        
        if col_info:
            item.append(f" ({col_info.data_type})", style=C_DIM)
            if col_info.business_name:
                item.append(f" — {col_info.business_name}", style="white")
        console.print(item)
        
        if col_info and col_info.sample_values:
            samples = ", ".join(map(str, list(set(col_info.sample_values))[:5]))
            console.print(f"      [{C_DIM}]Samples:[/] [{C_SUCCESS}]{samples}[/]")
            
        console.print(f"      [{C_DIM}]{c.reason}[/]\n")


# ─── 핵심 쿼리 처리 ──────────────────────────────────────────────────────────
async def handle_query(
    question: str,
    connector: DatabaseConnector,
    schema: SchemaInfo,
    retriever: SemanticRetriever,
    collector: FeedbackCollector,
    conv_manager: ConversationManager,
    conv_context,
    session: PromptSession,
):
    """자연어 질문 → SQL 생성 → 실행 전체 파이프라인"""
    
    # 0. 대화 참조 해석 ("1번", "2번")
    resolved = conv_context.resolve_option_reference(question)
    if resolved:
        render_info(f"'{question}' = '{resolved}'")
        last_turn = conv_context.get_last_turn()
        if last_turn:
            question = f"{last_turn.user_input} - 선택: {resolved}"
    
    # 1. 컨텍스트 준비
    context = retriever.get_context_for_query(question, top_k=15)
    global_summary = schema.get_schema_summary()
    
    conv_history = conv_context.get_context_summary()
    if conv_history:
        context = f"{conv_history}\n\n{context}"
    
    positive_examples = collector.get_positive_examples(
        current_question=question, limit=5
    )
    
    # 2. 컬럼 후보 식별
    with render_ai_thinking("관련 데이터 분석 중"):
        planner = QueryPlanner()
        plan = await planner.plan(
            question, context,
            global_schema_summary=global_summary,
            history=positive_examples,
        )
    
    # 2.5. 의도 분류: 스키마 탐색이면 SQL 없이 즉시 응답
    if plan.intent_type == "schema_explore":
        render_schema_answer(plan, schema)
        conv_context.add_turn(question, system_response=plan.intent)
        conv_manager.save_context()
        return
    
    # 3. 후보 표시 + 선택
    render_candidates(plan)
    
    if plan.is_cached:
        selected_columns = plan.recommended_columns
    elif plan.needs_confirmation and len(plan.candidates) > 1:
        selection = await session.prompt_async(
            HTML(f"<style fg='{C_PRIMARY}'><b>  ? </b></style><b>선택</b> <style fg='{C_DIM}'><i>(번호 또는 Enter=추천)</i></style><b>: </b>")
        )
        selection = selection.strip() or "추천"
        selected_columns = planner.get_selected_columns(plan, selection)
    else:
        selected_columns = plan.recommended_columns
        render_info("추천 옵션 자동 선택")
    
    if not selected_columns:
        render_error("선택된 컬럼이 없습니다.")
        return
    
    console.print()
    
    # 4. SQL 생성
    column_hint = f"\n\n## 사용할 컬럼 (사용자 확인됨)\n{chr(10).join(f'- {c}' for c in selected_columns)}"
    enhanced_context = context + column_hint
    
    with render_ai_thinking("SQL 생성 중"):
        generator = SQLGenerator()
        schema_str = format_schema_for_prompt(schema)
        result = await generator.generate(
            question, schema_str, enhanced_context,
            few_shots=positive_examples,
        )
    
    # 5. SQL 검증
    validator = SQLValidator()
    validation = validator.validate(result.sql)
    
    if not validation.is_valid:
        render_error(f"SQL 검증 실패: {validation.error_message}")
        return
    
    render_sql(validation.sanitized_sql, result.explanation)
    
    # 6. 실행 루프 (수정 요청 포함)
    current_sql = validation.sanitized_sql
    user_feedbacks = []
    auto_correction_count = 0

    while True:
        run_choice = await session.prompt_async(
            HTML(f"<style fg='{C_PRIMARY}'><b>  ? </b></style><b>실행하시겠습니까?</b> <style fg='{C_DIM}'><i>(y/n/수정 피드백)</i></style><b>: </b>")
        )
        run_choice = run_choice.strip()
        
        if run_choice.lower() == "n" or not run_choice:
            render_info("취소됨")
            conv_manager.save_context()
            return
        
        if run_choice.lower() != "y":
            # 수정 요청
            user_feedbacks.append(run_choice)
            console.print(f"\n  [{C_SQL}]🔄 피드백 반영 중...[/]\n")
            
            feedback_hint = "\n\n## ⚠️ 사용자 피드백 (반드시 반영):\n"
            for i, fb in enumerate(user_feedbacks, 1):
                feedback_hint += f"{i}. {fb}\n"
            feedback_hint += f"\n이전 SQL:\n```sql\n{current_sql}\n```"
            
            refined_context = enhanced_context + feedback_hint
            
            with render_ai_thinking("SQL 재생성 중"):
                result = await generator.generate(
                    question, schema_str, refined_context,
                    few_shots=positive_examples,
                )
            
            validation = validator.validate(result.sql)
            if not validation.is_valid:
                render_error(f"SQL 검증 실패: {validation.error_message}")
                break
            
            current_sql = validation.sanitized_sql
            render_sql(current_sql, result.explanation, title="수정된 SQL")
            continue
        
        # y → 실행
        try:
            with connector.engine.connect() as conn:
                start_time = time.time()
                rs = conn.execute(text(current_sql))
                rows = rs.fetchall()
                columns = rs.keys()
                duration = int((time.time() - start_time) * 1000)
            
            console.print()
            if rows:
                render_result_table(rows, columns)
                render_info(f"{len(rows)}행 반환  •  {duration}ms")
                console.print()
            else:
                console.print(f"  [{C_SQL}]결과가 없습니다.[/]\n")
            
            # 피드백 요청
            fb = await session.prompt_async(
                HTML(f"<style fg='{C_PRIMARY}'><b>  ? </b></style><b>결과가 정확한가요?</b> <style fg='{C_DIM}'><i>(y/n/피드백/Enter=스킵)</i></style><b>: </b>")
            )
            fb = fb.strip()
            
            conv_context.add_turn(question, system_response=current_sql)
            
            POSITIVE_KEYWORDS = [
                "👍", "y", "Y", "yes", "ok", "okay",
                "맞아", "맞아요", "맞습니다", "네", "ㅇㅇ", "ㅇㅋ",
                "굿", "좋아", "좋아요", "괜찮", "비슷",
            ]
            
            fb_norm = fb.replace(" ", "")
            is_positive = not fb or any(kw in fb_norm or kw in fb for kw in POSITIVE_KEYWORDS)
            
            if is_positive:
                log_id = collector.log_query(
                    question=question,
                    generated_sql=current_sql,
                    executed=True,
                    row_count=len(rows) if rows else 0,
                    execution_time_ms=duration,
                    selected_columns=selected_columns,
                )
                collector.add_feedback(log_id, FeedbackType.POSITIVE)
                if fb:
                    render_success("감사합니다! 비슷한 질문에 활용하겠습니다.")
                break
            else:
                # 수정 요청 → 재생성
                user_feedbacks.append(fb)
                console.print(f"\n  [{C_SQL}]🔄 피드백 반영 중...[/]\n")
                
                feedback_hint = "\n\n## ⚠️ 사용자 피드백 (반드시 반영):\n"
                for i, ufb in enumerate(user_feedbacks, 1):
                    feedback_hint += f"{i}. {ufb}\n"
                feedback_hint += f"\n이전 SQL (오류):\n```sql\n{current_sql}\n```"
                
                refined_context = enhanced_context + feedback_hint
                
                with render_ai_thinking("SQL 재생성 중"):
                    result = await generator.generate(
                        question, schema_str, refined_context,
                        few_shots=positive_examples,
                    )
                
                validation = validator.validate(result.sql)
                if not validation.is_valid:
                    render_error(f"SQL 검증 실패: {validation.error_message}")
                    break
                
                current_sql = validation.sanitized_sql
                render_sql(current_sql, result.explanation, title="수정된 SQL")
                
        except Exception as e:
            error_str = str(e)
            render_error(f"실행 오류: {error_str}")

            if auto_correction_count < generator.max_retries:
                console.print(f"\n  [{C_SQL}]🔧 SQL 자동 수정 시도 ({auto_correction_count + 1}/{generator.max_retries})...[/]\n")
                try:
                    with render_ai_thinking(f"SQL 자동 수정 중"):
                        corrected = await generator.correct(
                            current_sql, error_str, schema_str,
                            retry_count=auto_correction_count,
                        )
                except ValueError:
                    render_error("자동 수정 실패: 최대 재시도 횟수 초과")
                    collector.log_query(
                        question=question,
                        generated_sql=current_sql,
                        executed=False,
                        error=error_str,
                        selected_columns=selected_columns,
                    )
                    break

                corr_validation = validator.validate(corrected.sql)
                if not corr_validation.is_valid:
                    render_error(f"수정된 SQL 검증 실패: {corr_validation.error_message}")
                    collector.log_query(
                        question=question,
                        generated_sql=current_sql,
                        executed=False,
                        error=error_str,
                        selected_columns=selected_columns,
                    )
                    break

                current_sql = corr_validation.sanitized_sql
                auto_correction_count += 1
                render_sql(current_sql, corrected.explanation,
                           title=f"자동 수정된 SQL (시도 {auto_correction_count})")
                # while True → 다시 "실행하시겠습니까?" 프롬프트
            else:
                collector.log_query(
                    question=question,
                    generated_sql=current_sql,
                    executed=False,
                    error=error_str,
                    selected_columns=selected_columns,
                )
                break
    
    conv_manager.save_context()


# ─── 슬래시 명령어 핸들러 ────────────────────────────────────────────────────
async def handle_profile(connector: DatabaseConnector):
    """DB 프로파일링 실행 (세련된 진행 상태)"""
    
    db_name = connector.get_database_name()
    console.print(f"\n  [bold {C_PRIMARY}]󰆼 {db_name} 프로파일링 시작[/]")
    
    with render_ai_thinking("스키마 추출 중"):
        extractor = SchemaExtractor(connector.engine)
        schema = extractor.extract_full_schema()
    render_success(f"{len(schema.tables)}개 테이블 발견")
    
    with render_ai_thinking("샘플 데이터 수집 중"):
        sampler = DataSampler(connector.engine)
        schema = sampler.enrich_schema_with_samples(schema)
    render_success("샘플 데이터 수집 완료")
    
    with render_ai_thinking("세만틱 설명 생성 중 (LLM)"):
        from semantic_engine.semantic.generator import SemanticGenerator
        generator = SemanticGenerator()
        schema = await generator.enrich_schema(schema)
    render_success("세만틱 설명 생성 완료")
    
    with render_ai_thinking("카탈로그 저장 중"):
        catalog = SemanticCatalog()
        filepath = catalog.save(schema)
    render_success(f"카탈로그 저장: {filepath}")
    
    with render_ai_thinking("벡터 인덱싱 중"):
        retriever = SemanticRetriever()
        count = retriever.index_schema(schema)
    render_success(f"{count}개 항목 인덱싱 완료")
    console.print()
    
    return schema


def handle_status(connector: DatabaseConnector | None, schema: SchemaInfo | None):
    """상태 표시 (미니멀 스타일)"""
    console.print(f"\n  [bold {C_PRIMARY}]󰠵 System Status[/]")
    
    # DB
    if connector:
        try:
            db_name = connector.get_database_name()
            console.print(f"    • [bold]Database   [/] [bold {C_SUCCESS}]Connected[/] — {db_name}")
        except Exception:
            console.print(f"    • [bold]Database   [/] [bold {C_ERR}]Error[/]")
    else:
        console.print(f"    • [bold]Database   [/] [bold {C_ERR}]Disconnected[/]")
    
    # 카탈로그
    if schema:
        total_cols = sum(len(t.columns) for t in schema.tables)
        console.print(f"    • [bold]Catalog    [/] [bold {C_SUCCESS}]Loaded[/] — {len(schema.tables)} tables, {total_cols} columns")
    else:
        console.print(f"    • [bold]Catalog    [/] [bold #facc15]Empty[/] — Run /profile to analyze")
    
    # 모델
    try:
        settings = get_settings()
        console.print(f"    • [bold]AI Model   [/] [bold {C_PRIMARY}]{settings.gemini_model}[/]")
    except Exception:
        console.print(f"    • [bold]AI Model   [/] [bold {C_ERR}]Not Configured[/]")
    
    console.print()


async def handle_models(session: PromptSession) -> str | None:
    """사용 가능한 모델 목록 표시 및 변경"""
    import google.generativeai as genai
    
    try:
        settings = get_settings()
        genai.configure(api_key=settings.gemini_api_key.get_secret_value())
        current_model = settings.gemini_model
    except Exception:
        render_error("GEMINI_API_KEY가 설정되지 않았습니다.")
        return None
    
    with render_ai_thinking("모델 목록 조회 중"):
        try:
            models = []
            for m in genai.list_models():
                if "generateContent" in m.supported_generation_methods:
                    name = m.name.replace("models/", "")
                    if "gemini" in name:
                        models.append(name)
        except Exception as e:
            render_error(f"모델 목록 조회 실패: {e}")
            return None
    
    # 모델 목록 표시
    console.print(f"\n  [bold {C_PRIMARY}]󰧑 Available AI Models[/]")
    
    for i, name in enumerate(models, 1):
        is_current = "  [bold white on #4338ca] ACTIVE [/]" if name == current_model else ""
        style = f"bold white" if name == current_model else f"{C_DIM}"
        console.print(f"    {i}. [{style}]{name}[/]{is_current}")
    
    console.print()
    
    choice = await session.prompt_async(
        HTML("<b>  모델 번호</b> <i>(Enter=취소)</i><b>: </b>")
    )
    choice = choice.strip()
    
    if not choice:
        return None
    
    try:
        idx = int(choice) - 1
        if 0 <= idx < len(models):
            new_model = models[idx]
            
            # .env 파일 업데이트
            env_path = Path(".env")
            if env_path.exists():
                lines = env_path.read_text(encoding="utf-8").splitlines()
                updated = False
                for j, line in enumerate(lines):
                    if line.startswith("GEMINI_MODEL="):
                        lines[j] = f"GEMINI_MODEL={new_model}"
                        updated = True
                        break
                if not updated:
                    lines.append(f"GEMINI_MODEL={new_model}")
                env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            
            # 런타임 설정 갱신
            import os
            os.environ["GEMINI_MODEL"] = new_model
            get_settings.cache_clear()
            
            render_success(f"모델 변경: {new_model}")
            return new_model
        else:
            render_error("유효하지 않은 번호입니다.")
    except ValueError:
        render_error("숫자를 입력해 주세요.")
    
    return None


# ─── 메인 TUI 루프 ──────────────────────────────────────────────────────────
async def run_tui():
    """TUI 메인 루프"""
    
    # 히스토리 디렉토리 생성
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    # prompt_toolkit 세션
    session = PromptSession(
        history=FileHistory(str(HISTORY_FILE)),
        auto_suggest=AutoSuggestFromHistory(),
    )
    
    # ─── 초기화 ───
    connector = None
    schema = None
    retriever = None
    collector = None
    conv_manager = None
    conv_context = None
    db_name = None
    model_name = None
    
    console.clear()
    
    # DB 연결
    try:
        settings = get_settings()
        model_name = settings.gemini_model
    except Exception:
        pass
    
    with render_ai_thinking("DB 연결 중"):
        try:
            connector = DatabaseConnector()
            connector.test_connection()
            db_name = connector.get_database_name()
        except Exception as e:
            error_msg = str(e)
            connector = None
    
    show_banner(db_name, model_name)
    
    if not connector:
        render_error(f"DB 연결 실패: {error_msg}")
        render_info(".env 파일의 DATABASE_URL을 확인하거나, 특수문자가 포함된 경우 개별 설정(DB_PASSWORD 등)을 사용하세요.")
        render_info("TIP: DATABASE_URL을 주석처리(#)하면 개별 설정값이 적용됩니다.")
        console.print()
    else:
        render_success(f"DB 연결 성공: {db_name}")
        
        # 카탈로그 로드
        catalog = SemanticCatalog()
        schema = catalog.load(db_name)
        
        if schema:
            total_cols = sum(len(t.columns) for t in schema.tables)
            render_success(f"카탈로그 로드: {len(schema.tables)}개 테이블, {total_cols}개 컬럼")
        else:
            console.print(f"  [{C_SQL}]⚠ 카탈로그가 없습니다. 자동으로 프로파일링을 시작합니다...[/]")
            console.print()
            schema = await handle_profile(connector)
        
        # RAG + 피드백 초기화
        retriever = SemanticRetriever()
        collector = FeedbackCollector(retriever=retriever)
        conv_manager = get_conversation_manager()
        conv_context = conv_manager.get_context()
        
        console.print()
    
    render_separator()
    console.print()
    
    # ─── REPL 루프 ───
    while True:
        try:
            user_input = await session.prompt_async(
                HTML("<style fg='#a78bfa'><b>❯ </b></style>"),
            )
            user_input = user_input.strip()
            
            if not user_input:
                continue
            
            # 슬래시 명령어
            cmd = user_input.lower()
            
            if cmd in ("/exit", "/quit", "/q"):
                console.print()
                render_info("BridgeSQL을 종료합니다. 다음에 또 만나요!")
                console.print()
                break
            
            if cmd == "/help":
                show_help()
                continue
            
            if cmd == "/clear":
                if conv_context:
                    conv_context.clear()
                    conv_manager.save_context()
                console.clear()
                show_banner(db_name, model_name)
                render_separator()
                render_success("대화 내역이 초기화되었습니다.")
                console.print()
                continue
            
            if cmd == "/status":
                handle_status(connector, schema)
                continue
            
            if cmd == "/models":
                new_model = await handle_models(session)
                if new_model:
                    model_name = new_model
                continue
            
            if cmd == "/profile":
                if not connector:
                    render_error("DB 연결이 필요합니다.")
                    continue
                schema = await handle_profile(connector)
                retriever = SemanticRetriever()
                collector = FeedbackCollector(retriever=retriever)
                continue
            
            # 자연어 질문 처리
            if not connector or not schema:
                render_error("DB가 연결되지 않았습니다. .env 설정 후 재시작하세요.")
                continue
            
            render_user_message(user_input)
            
            await handle_query(
                question=user_input,
                connector=connector,
                schema=schema,
                retriever=retriever,
                collector=collector,
                conv_manager=conv_manager,
                conv_context=conv_context,
                session=session,
            )
            
            render_separator()
            console.print()
            
        except KeyboardInterrupt:
            console.print()
            render_info("BridgeSQL을 종료합니다.")
            break
        except EOFError:
            break
        except Exception as e:
            render_error(f"예기치 못한 오류: {e}")
    
    # 정리
    if connector:
        connector.close()
    if conv_manager:
        conv_manager.save_context()


def main():
    """TUI 엔트리포인트"""
    asyncio.run(run_tui())


if __name__ == "__main__":
    main()
