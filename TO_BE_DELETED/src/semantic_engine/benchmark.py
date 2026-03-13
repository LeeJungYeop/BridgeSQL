"""
BridgeSQL 벤치마크 엔진
Q&A 테스트셋으로 SQL 생성 정확도, 실행 성공률, 자동수정 효과를 측정합니다.
Ablation Study: Baseline / +Semantic / +Semantic+RAG / +Semantic+RAG+SC 4가지 모드 비교
"""

import asyncio
import json
import time
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.rule import Rule
from rich import box

# benchmark.py → src/semantic_engine/ → src/ → 프로젝트 루트
PROJECT_ROOT = Path(__file__).parent.parent.parent

from semantic_engine.profiler.connector import DatabaseConnector
from semantic_engine.profiler.schema_extractor import SchemaInfo
from semantic_engine.semantic.catalog import SemanticCatalog
from semantic_engine.rag.retriever import SemanticRetriever
from semantic_engine.engine.sql_generator import SQLGenerator, format_schema_for_prompt, format_schema_raw, format_schema_tables_only
from semantic_engine.engine.validator import SQLValidator
from sqlalchemy import text

console = Console()

MODES = [
    {"name": "Baseline",                   "use_semantic": False, "use_rag": False, "use_sc": False, "use_fs": False, "use_rag_only": False, "style": "red"},
    {"name": "+ RAG-only",                 "use_semantic": False, "use_rag": True,  "use_sc": False, "use_fs": False, "use_rag_only": True,  "style": "cyan"},
    {"name": "+ Semantic",                 "use_semantic": True,  "use_rag": False, "use_sc": False, "use_fs": False, "use_rag_only": False, "style": "blue"},
    {"name": "+ Semantic + RAG",           "use_semantic": True,  "use_rag": True,  "use_sc": False, "use_fs": False, "use_rag_only": False, "style": "yellow"},
    {"name": "+ Semantic + RAG + FS",      "use_semantic": True,  "use_rag": True,  "use_sc": False, "use_fs": True,  "use_rag_only": False, "style": "magenta"},
    {"name": "+ Semantic + RAG + FS + SC", "use_semantic": True,  "use_rag": True,  "use_sc": True,  "use_fs": True,  "use_rag_only": False, "style": "green"},
]

# 테스트셋과 겹치지 않는 도메인 코드 패턴 시연용 시드 예제
SEED_EXAMPLES = [
    {
        "question": "MT 방법으로 검사한 건수는?",
        "sql": "SELECT COUNT(*) FROM `ndt_info` WHERE `방법` = 'MT'",
    },
    {
        "question": "비파괴 합격 판정 제품 수는?",
        "sql": "SELECT COUNT(*) FROM `product_info` WHERE `비파괴검사_판정` = '합격'",
    },
    {
        "question": "포로시티 결함 이미지 몇 장이야?",
        "sql": "SELECT COUNT(*) FROM `image_metadata` WHERE `결함_유형` = 'PO'",
    },
    {
        "question": "파이프 몸통 부분 사진은?",
        "sql": "SELECT COUNT(*) FROM `image_metadata` WHERE `강관_위치` = 'BODY'",
    },
    {
        "question": "비정상 판정 사진 수?",
        "sql": "SELECT COUNT(*) FROM `image_metadata` WHERE `정상여부` = 1",
    },
]


# ── 내부 유틸 ──────────────────────────────────────────────────────────────────

async def _api_call_with_retry(coro_fn, *args, **kwargs):
    """429 rate limit 대비 재시도 래퍼"""
    for attempt in range(3):
        try:
            return await coro_fn(*args, **kwargs)
        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "quota" in err_str.lower() or "Resource" in err_str:
                wait_sec = 30 * (attempt + 1)
                console.print(f"  [yellow]⚠ 429 rate limit — {wait_sec}초 대기[/yellow]")
                await asyncio.sleep(wait_sec)
            else:
                raise
    raise RuntimeError("429 재시도 3회 초과")


async def _try_correct(
    generator: SQLGenerator,
    validator: SQLValidator,
    original_sql: str,
    error_str: str,
    schema_str: str,
    max_attempts: int = 2,
) -> tuple[str | None, bool]:
    """자동수정 시도. (수정된 SQL, 성공여부) 반환"""
    for attempt in range(max_attempts):
        try:
            corrected = await _api_call_with_retry(
                generator.correct, original_sql, error_str, schema_str, retry_count=attempt
            )
        except (ValueError, RuntimeError):
            break
        validation = validator.validate(corrected.sql)
        if validation.is_valid:
            return validation.sanitized_sql, True
    return None, False


def _summarize(results: list[dict]) -> dict:
    """결과 리스트 → 요약 dict"""
    total = len(results)
    valid = sum(1 for r in results if r["is_valid"])
    exec_ok = sum(1 for r in results if r["exec_ok"])
    ans_ok = sum(1 for r in results if r.get("answer_correct") is True)
    ans_measurable = sum(1 for r in results if r.get("answer_correct") is not None)
    corr_tried = sum(1 for r in results if r["correction_attempted"])
    corr_ok = sum(1 for r in results if r["correction_ok"])
    avg_time = int(sum(r["response_time_ms"] for r in results) / total) if total else 0

    by_diff: dict[str, dict] = {}
    for r in results:
        d = r["difficulty"]
        if d not in by_diff:
            by_diff[d] = {"total": 0, "exec_ok": 0, "ans_ok": 0}
        by_diff[d]["total"] += 1
        if r["exec_ok"]:
            by_diff[d]["exec_ok"] += 1
        if r.get("answer_correct") is True:
            by_diff[d]["ans_ok"] += 1

    return {
        "total": total,
        "valid": valid,
        "exec_ok": exec_ok,
        "answer_ok": ans_ok,
        "answer_measurable": ans_measurable,
        "correction_attempted": corr_tried,
        "correction_ok": corr_ok,
        "validity_rate": round(valid / total, 4) if total else 0,
        "exec_success_rate": round(exec_ok / total, 4) if total else 0,
        "answer_accuracy": round(ans_ok / ans_measurable, 4) if ans_measurable else 0,
        "self_correction_rate": round(corr_ok / corr_tried, 4) if corr_tried else 0,
        "avg_response_time_ms": avg_time,
        "by_difficulty": by_diff,
    }


# ── 핵심 실행 함수 ─────────────────────────────────────────────────────────────

async def run_benchmark(
    qa_pairs: list[dict],
    schema: SchemaInfo,
    connector: DatabaseConnector,
    retriever: SemanticRetriever,
    generator: SQLGenerator,
    validator: SQLValidator,
    use_semantic: bool = True,
    use_rag: bool = True,
    use_self_correction: bool = True,
    use_few_shot: bool = False,
    use_rag_only: bool = False,
) -> list[dict]:
    """
    벤치마크 실행. 결과 레코드 리스트 반환.
    use_semantic=False  → 컬럼명+타입만 전달 (Baseline)
    use_rag=False       → RAG 컨텍스트 없음
    use_rag_only=True   → 테이블명만 전달, RAG context가 컬럼 정보 대체 (top_k=15)
    use_few_shot=False  → query_history few-shot 없음
    use_self_correction=False → 자동수정 건너뜀
    """
    if use_rag_only:
        schema_str = format_schema_tables_only(schema)
    elif use_semantic:
        schema_str = format_schema_for_prompt(schema)
    else:
        schema_str = format_schema_raw(schema)
    results = []

    for i, qa in enumerate(qa_pairs, 1):
        question = qa["question"]
        difficulty = qa.get("difficulty", "unknown")

        sem_tag = "[dim]SEM[/dim]" if use_semantic else "[dim]---[/dim]"
        rag_tag = "[dim]RAG[/dim]" if use_rag       else "[dim]---[/dim]"
        fs_tag  = "[dim]FS[/dim]"  if use_few_shot  else "[dim]--[/dim]"
        sc_tag  = "[dim]SC[/dim]"  if use_self_correction else "[dim]--[/dim]"
        console.print(f"  [{i:2d}/{len(qa_pairs)}] {sem_tag} {rag_tag} {fs_tag} {sc_tag}  {question}")

        expected_value = qa.get("expected_value")  # None이면 정확도 측정 제외

        record: dict = {
            "id": qa["id"],
            "question": question,
            "difficulty": difficulty,
            "tags": qa.get("tags", []),
            "generated_sql": "",
            "is_valid": False,
            "exec_ok": False,
            "row_count": None,
            "answer_correct": None,   # None=측정불가, True=정답, False=오답
            "actual_value": None,     # 실제 반환 값 (COUNT 결과 등)
            "expected_value": expected_value,
            "correction_attempted": False,
            "correction_ok": False,
            "corrected_sql": None,
            "error": None,
            "response_time_ms": 0,
        }

        # 1. 컨텍스트 결정
        context = ""
        if use_rag:
            try:
                rag_top_k = 15 if use_rag_only else 10
                context = retriever.get_context_for_query(question, top_k=rag_top_k)
            except Exception:
                context = ""

        # 2. Few-shot 검색 (use_few_shot=True 일 때만)
        few_shots = None
        if use_few_shot:
            try:
                examples = retriever.search_few_shot_examples(question, top_k=3)
                if examples:
                    few_shots = [{"question": e["question"], "sql": e["sql"]} for e in examples]
            except Exception:
                few_shots = None

        # 3. SQL 생성
        t0 = time.time()
        try:
            result = await _api_call_with_retry(
                generator.generate, question, schema_str, context, few_shots
            )
            record["response_time_ms"] = int((time.time() - t0) * 1000)
            record["generated_sql"] = result.sql
        except Exception as e:
            record["response_time_ms"] = int((time.time() - t0) * 1000)
            record["error"] = f"생성 실패: {e}"
            results.append(record)
            continue

        # 질문 사이 기본 딜레이 (분당 15req 제한 방지)
        await asyncio.sleep(4)

        # 4. 정적 검증
        validation = validator.validate(result.sql)
        record["is_valid"] = validation.is_valid
        if not validation.is_valid:
            record["error"] = validation.error_message
            results.append(record)
            continue

        current_sql = validation.sanitized_sql

        # 5. DB 실행
        try:
            with connector.engine.connect() as conn:
                rs = conn.execute(text(current_sql))
                rows = rs.fetchall()
            record["exec_ok"] = True
            record["row_count"] = len(rows)

            # 4-1. 정답 정확도 측정
            if expected_value is not None:
                if len(rows) == 1 and len(rows[0]) == 1:
                    # COUNT 등 단일 값 쿼리: 실제 값과 비교
                    actual = rows[0][0]
                    record["actual_value"] = actual
                    record["answer_correct"] = (int(actual) == int(expected_value))
                else:
                    # 다중 행 쿼리: row 수로 비교
                    record["actual_value"] = len(rows)
                    record["answer_correct"] = (len(rows) == int(expected_value))
        except Exception as exec_err:
            error_str = str(exec_err)
            record["error"] = error_str

            # 6. 자동수정 (use_self_correction=True 일 때만)
            if use_self_correction:
                record["correction_attempted"] = True
                corrected_sql, ok = await _try_correct(
                    generator, validator, current_sql, error_str, schema_str
                )
                record["correction_ok"] = ok
                record["corrected_sql"] = corrected_sql

                if ok and corrected_sql:
                    try:
                        with connector.engine.connect() as conn:
                            rs = conn.execute(text(corrected_sql))
                            rows = rs.fetchall()
                        record["exec_ok"] = True
                        record["row_count"] = len(rows)
                        record["error"] = None
                    except Exception as retry_err:
                        record["error"] = str(retry_err)

        results.append(record)

    return results


# ── 출력 함수 ──────────────────────────────────────────────────────────────────

def print_comparison(summaries: list[dict], db_name: str, total_q: int) -> None:
    """N개 모드 비교 테이블 출력 (MODES 길이에 맞게 동적 생성)"""

    def pct(n, d):
        return f"{n / d * 100:.1f}%" if d else "N/A"

    def delta(a: float, b: float) -> str:
        """b - a 차이를 +X.Xpp 형식으로"""
        diff = (b - a) * 100
        if diff > 0:
            return f"[green]+{diff:.1f}pp[/green]"
        elif diff < 0:
            return f"[red]{diff:.1f}pp[/red]"
        return "[dim]±0[/dim]"

    console.print()
    console.print(Rule(f"[bold cyan]📊 Ablation Study — {db_name} ({total_q}Q)[/bold cyan]"))
    console.print()

    mode_names = [m["name"] for m in MODES]
    n = len(summaries)

    # ── 메인 비교 테이블 ──
    t = Table(box=box.ROUNDED, header_style="bold cyan", show_lines=True)
    t.add_column("지표", style="bold white", min_width=18)
    for name in mode_names:
        t.add_column(name, justify="center", min_width=20)

    def make_row_cells(key_n, key_d, key_rate):
        cells = [pct(summaries[0][key_n], summaries[0][key_d])]
        for i in range(1, n):
            prev = summaries[i - 1]
            cur  = summaries[i]
            cells.append(f"{pct(cur[key_n], cur[key_d])}  {delta(prev[key_rate], cur[key_rate])}")
        return cells

    # SQL 유효율
    t.add_row("SQL 유효율",     *make_row_cells("valid",   "total", "validity_rate"))
    # 실행 성공률
    t.add_row("실행 성공률",    *make_row_cells("exec_ok", "total", "exec_success_rate"))

    # ★ 정답 정확도
    acc_cells = [f"[bold]{pct(summaries[0]['answer_ok'], summaries[0]['answer_measurable'])}[/bold]"]
    for i in range(1, n):
        prev_acc = summaries[i - 1]["answer_accuracy"]
        cur_acc  = summaries[i]["answer_accuracy"]
        cur = summaries[i]
        acc_cells.append(
            f"[bold]{pct(cur['answer_ok'], cur['answer_measurable'])}[/bold]  {delta(prev_acc, cur_acc)}"
        )
    t.add_row("[bold yellow]★ 정답 정확도[/bold yellow]", *acc_cells)

    # 자동수정 성공률 — SC 플래그가 있는 모드에만 표시
    sc_cells = []
    for i, mode in enumerate(MODES):
        if mode["use_sc"]:
            s = summaries[i]
            sc_cells.append(f"{pct(s['correction_ok'], s['correction_attempted'])}  ({s['correction_ok']}/{s['correction_attempted']}건)")
        else:
            sc_cells.append("[dim]N/A[/dim]")
    t.add_row("자동수정 성공률", *sc_cells)

    # 응답시간
    t.add_row("평균 응답시간", *[f"{s['avg_response_time_ms']:,}ms" for s in summaries])

    console.print(t)

    # ── 난이도별 상세 ──
    console.print()
    d_table = Table(title="난이도별 정답 정확도", box=box.SIMPLE, header_style="bold magenta")
    d_table.add_column("난이도", style="bold")
    d_table.add_column("문항수", justify="right")
    for name in mode_names:
        d_table.add_column(name, justify="center")

    for diff_name in ["easy", "medium", "hard"]:
        base_diff = summaries[0]["by_difficulty"].get(diff_name, {})
        total_in_diff = base_diff.get("total", 0)
        row_parts = [diff_name, str(total_in_diff)]
        for s in summaries:
            dd = s["by_difficulty"].get(diff_name, {"total": 0, "ans_ok": 0})
            row_parts.append(pct(dd["ans_ok"], dd["total"]))
        d_table.add_row(*row_parts)

    console.print(d_table)

    # ── 컴포넌트별 기여도 요약 ──
    # Baseline(0) / RAG-only(1) / +Semantic(2) / +Sem+RAG(3) / +Sem+RAG+FS(4) / +FS+SC(5)
    console.print()
    # (label, from_idx, to_idx, 비교 기준 설명)
    contributions = [
        ("RAG-only (vs Baseline)",         0, 1),
        ("Semantic 카탈로그 (vs Baseline)", 0, 2),
        ("RAG 보완 (+Semantic→+Sem+RAG)",   2, 3),
        ("Few-shot 학습",                   3, 4),
        ("Self-Correction",                 4, 5),
    ]
    for label, from_i, to_i in contributions:
        if to_i < n:
            gain_exec = (summaries[to_i]["exec_success_rate"] - summaries[from_i]["exec_success_rate"]) * 100
            gain_ans  = (summaries[to_i]["answer_accuracy"]   - summaries[from_i]["answer_accuracy"])   * 100
            exec_lbl = f"[green]+{gain_exec:.1f}pp[/green]" if gain_exec >= 0 else f"[red]{gain_exec:.1f}pp[/red]"
            ans_lbl  = f"[green]+{gain_ans:.1f}pp[/green]"  if gain_ans  >= 0 else f"[red]{gain_ans:.1f}pp[/red]"
            console.print(f"  [bold]{label}:[/bold]  실행 성공률 {exec_lbl} │ 정답 정확도 {ans_lbl}")
    console.print()


def save_ablation(all_results: list[dict], summaries: list[dict], db_name: str) -> Path:
    """Ablation 전체 결과 저장"""
    output_dir = PROJECT_ROOT / "data" / "benchmark"
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"ablation_{timestamp}.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "timestamp": datetime.now().isoformat(),
                "database": db_name,
                "modes": [m["name"] for m in MODES],
                "summaries": summaries,
                "details": all_results,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )
    return output_path


# ── CLI 진입점 ─────────────────────────────────────────────────────────────────

async def cmd_benchmark():
    """벤치마크 Ablation Study 실행 (CLI 진입점)"""

    console.print(Panel(
        "[bold cyan]BridgeSQL Ablation Study[/bold cyan]\n"
        "[dim]Baseline / +Semantic / +Semantic+RAG / +Semantic+RAG+SC 네 가지 모드를 순차 실행하여\n"
        "Semantic 카탈로그, RAG, Self-Correction 각 컴포넌트의 기여도를 측정합니다.[/dim]",
        border_style="cyan",
    ))

    # 1. Q&A 로드
    qa_path = PROJECT_ROOT / "data" / "benchmark_qa.json"
    if not qa_path.exists():
        console.print("[red]❌ data/benchmark_qa.json 파일이 없습니다.[/red]")
        return

    with open(qa_path, encoding="utf-8") as f:
        benchmark_data = json.load(f)
    qa_pairs = benchmark_data["qa_pairs"]
    console.print(f"[green]✅ Q&A {len(qa_pairs)}개 로드[/green]")

    # 2. DB 연결
    with console.status("[bold green]DB 연결 중..."):
        connector = DatabaseConnector()
        if not connector.test_connection():
            console.print("[red]❌ DB 연결 실패[/red]")
            return
        db_name = connector.get_database_name()
    console.print(f"[green]✅ DB: {db_name}[/green]")

    # 3. 카탈로그
    catalog = SemanticCatalog()
    schema = catalog.load(db_name)
    if not schema:
        console.print("[red]❌ 카탈로그가 없습니다. 'sqe profile' 먼저 실행하세요.[/red]")
        connector.close()
        return
    console.print(f"[green]✅ 카탈로그: {len(schema.tables)}개 테이블[/green]")

    # 4. 컴포넌트 초기화
    with console.status("[bold green]RAG 초기화 중..."):
        retriever = SemanticRetriever()
    generator = SQLGenerator()
    validator = SQLValidator()
    console.print(f"[green]✅ 준비 완료[/green]\n")

    # 5. N가지 모드 순차 실행
    all_results = []
    summaries = []

    for mode in MODES:
        # FS 모드 진입 전: 시드 예제 주입
        if mode["use_fs"]:
            with console.status("[bold green]Few-shot 시드 주입 중..."):
                for idx, ex in enumerate(SEED_EXAMPLES):
                    retriever.index_few_shot_example(
                        log_id=f"seed_{idx}",
                        question=ex["question"],
                        sql=ex["sql"],
                    )
            console.print(f"[green]✅ Few-shot 시드 {len(SEED_EXAMPLES)}개 주입 완료[/green]")

        console.print(Rule(f"[bold {mode['style']}]▶ {mode['name']}[/bold {mode['style']}]"))
        results = await run_benchmark(
            qa_pairs, schema, connector, retriever, generator, validator,
            use_semantic=mode["use_semantic"],
            use_rag=mode["use_rag"],
            use_self_correction=mode["use_sc"],
            use_few_shot=mode["use_fs"],
            use_rag_only=mode["use_rag_only"],
        )

        # FS 모드 종료 후: 시드 제거 (다음 모드 오염 방지)
        if mode["use_fs"]:
            try:
                coll = retriever._get_history_collection()
                coll.delete(ids=[f"seed_{i}" for i in range(len(SEED_EXAMPLES))])
            except Exception:
                pass
        summary = _summarize(results)
        summaries.append(summary)

        # 모드별 레코드에 모드명 태깅
        for r in results:
            r["mode"] = mode["name"]
        all_results.extend(results)

        exec_rate = f"{summary['exec_ok']}/{summary['total']} ({summary['exec_success_rate']*100:.1f}%)"
        console.print(f"  → 실행 성공률: [bold]{exec_rate}[/bold]\n")

    # 6. 비교 테이블 출력
    print_comparison(summaries, db_name, len(qa_pairs))

    # 7. 저장
    output_path = save_ablation(all_results, summaries, db_name)
    console.print(f"[dim]결과 저장: {output_path}[/dim]\n")

    connector.close()
