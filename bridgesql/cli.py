"""
BridgeSQL CLI
"""

import asyncio
import json
import sys
from pathlib import Path

from rich import box as rich_box
from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt, Confirm, IntPrompt

console = Console(highlight=False)

_OK  = "[green]done[/green]"
_FAIL = "[red]failed[/red]"


def main():
    if len(sys.argv) < 2:
        _help()
        return

    command = sys.argv[1]
    args = sys.argv[2:]

    commands = {
        "init":     lambda: asyncio.run(cmd_init()),
        "config":   lambda: cmd_config(),
        "profile":  lambda: asyncio.run(cmd_profile()),
        "status":   lambda: asyncio.run(cmd_status()),
        "catalog":  lambda: asyncio.run(cmd_catalog(args[0] if args else "show", args[1:])),
        "examples": lambda: asyncio.run(cmd_examples(args[0] if args else "list", args[1:])),
    }

    if command in commands:
        commands[command]()
    else:
        _help()


def _help():
    console.print()
    console.print("  [bold]BridgeSQL[/bold]  natural language interface for your database")
    console.print()
    console.print("  [dim]Usage[/dim]")
    console.print("    sqe init                   first-time setup (API key, database URL)")
    console.print("    sqe config                 show current configuration")
    console.print("    sqe profile                analyze schema and build semantic catalog")
    console.print("    sqe status                 check current state")
    console.print()
    console.print("    sqe catalog show [table]   inspect catalog")
    console.print("    sqe catalog edit <table>   edit table description")
    console.print("    sqe catalog clear          reset catalog")
    console.print()
    console.print("    sqe examples list          list saved examples")
    console.print("    sqe examples add           add an example")
    console.print("    sqe examples delete <id>   delete an example")
    console.print("    sqe examples clear         delete all examples")
    console.print()
    console.print("  Run [bold]sqe init[/bold] to get started.")
    console.print()


def _step(label: str, width: int = 38) -> None:
    console.print(f"  {label:<{width}}", end="")


def _done() -> None:
    console.print(_OK)


def _fail(msg: str = "") -> None:
    console.print(_FAIL + (f"  {msg}" if msg else ""))


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------

async def cmd_init():
    env_path = Path.home() / ".bridgesql" / ".env"
    env_path.parent.mkdir(parents=True, exist_ok=True)
    existing: dict[str, str] = {}
    if env_path.exists():
        with open(env_path, encoding="utf-8") as f:
            for line in f:
                if "=" in line and not line.startswith("#"):
                    k, v = line.strip().split("=", 1)
                    existing[k] = v

    console.print()
    console.print("  [bold]sqe init[/bold]")
    console.print()

    try:
        # Database
        console.print("  [dim]Database[/dim]")
        db_url = Prompt.ask(
            "  DATABASE_URL",
            default=existing.get("DATABASE_URL", "mysql+pymysql://root:password@localhost:3306/mydb"),
        )
        console.print()

        # Gemini
        console.print("  [dim]Gemini[/dim]")
        api_key = Prompt.ask("  GEMINI_API_KEY", default=existing.get("GEMINI_API_KEY", ""), password=True)

        selected = existing.get("GEMINI_MODEL", "gemini-2.0-flash")
        models: list[str] = []

        _step("  Fetching available models...")
        try:
            import google.generativeai as genai
            genai.configure(api_key=api_key)
            models = [
                m.name.replace("models/", "")
                for m in genai.list_models()
                if "generateContent" in m.supported_generation_methods
            ]
            _done()
        except Exception:
            _fail("using default")

        if models:
            console.print()
            for i, m in enumerate(models, 1):
                marker = "[green]>[/green]" if m == selected else " "
                console.print(f"    {marker} {i:2}.  {m}")
            console.print()

            current_idx = models.index(selected) + 1 if selected in models else 1
            choice = IntPrompt.ask("  Model", default=current_idx, show_choices=False)
            selected = models[choice - 1]

        console.print()

        # Save
        existing.update({
            "DATABASE_URL": db_url,
            "GEMINI_API_KEY": api_key,
            "GEMINI_MODEL": selected,
        })
        existing.setdefault("CHROMA_PERSIST_DIR", "./data/chroma")
        existing.setdefault("CHROMA_COLLECTION_NAME", "semantic_catalog")
        existing.setdefault("SAMPLE_DATA_LIMIT", "10")
        existing.setdefault("MAX_SQL_RETRY", "3")

        lines = ["# BridgeSQL\n"] + [f"{k}={v}\n" for k, v in existing.items()]
        with open(env_path, "w", encoding="utf-8") as f:
            f.writelines(lines)

        console.print(f"  Saved to [dim]{env_path}[/dim]")
        console.print()
        console.print("  Next: run [bold]sqe profile[/bold] to build the semantic catalog.")
        console.print()

    except (EOFError, KeyboardInterrupt):
        console.print("\n  Cancelled.")
        console.print()


# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------

def cmd_config():
    env_path = Path.home() / ".bridgesql" / ".env"
    env_path.parent.mkdir(parents=True, exist_ok=True)
    if not env_path.exists():
        console.print()
        console.print("  No configuration found. Run [bold]sqe init[/bold] first.")
        console.print()
        return

    console.print()
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            if any(s in k for s in ("KEY", "PASSWORD", "SECRET")):
                v = v[:4] + "****" if len(v) > 4 else "****"
            console.print(f"  [dim]{k:<30}[/dim] {v}")
    console.print()


# ---------------------------------------------------------------------------
# profile
# ---------------------------------------------------------------------------

async def cmd_profile():
    from bridgesql.db.connector import DatabaseConnector
    from bridgesql.db.schema_extractor import SchemaExtractor
    from bridgesql.db.sampler import DataSampler
    from bridgesql.semantic.generator import SemanticGenerator
    from bridgesql.semantic.catalog import SemanticCatalog
    from bridgesql.rag.retriever import SemanticRetriever

    console.print()
    console.print("  [bold]sqe profile[/bold]")
    console.print()

    _step("Connecting to database...")
    try:
        connector = DatabaseConnector()
        if not connector.test_connection():
            _fail("check DATABASE_URL in sqe config")
            console.print()
            return
        db_name = connector.get_database_name()
        _done()
    except Exception as e:
        _fail(str(e))
        console.print()
        return

    _step("Extracting schema...")
    schema = SchemaExtractor(connector.engine).extract_full_schema()
    console.print(f"[green]done[/green]  [dim]{len(schema.tables)} tables[/dim]")

    _step("Collecting sample data...")
    schema = DataSampler(connector.engine).enrich_schema_with_samples(schema)
    _done()

    _step("Generating semantic descriptions...")
    schema = await SemanticGenerator().enrich_schema(schema)
    _done()

    _step("Saving catalog...")
    SemanticCatalog().save(schema)
    _done()

    _step("Building vector index...")
    count = SemanticRetriever().index_schema(schema)
    console.print(f"[green]done[/green]  [dim]{count} items[/dim]")

    connector.close()
    console.print()
    console.print(f"  Profile complete. Start the MCP server with [bold]sqe-mcp[/bold].")
    console.print()


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

async def cmd_status():
    from bridgesql.db.connector import DatabaseConnector
    from bridgesql.semantic.catalog import SemanticCatalog
    from bridgesql.rag.retriever import SemanticRetriever
    from datetime import datetime

    console.print()
    console.print("  [bold]sqe status[/bold]")
    console.print()

    # Database
    try:
        connector = DatabaseConnector()
        ok = connector.test_connection()
        db = connector.get_database_name() if ok else None
        connector.close()
        status = f"[green]{db}[/green]  connected" if ok else "[red]connection failed[/red]"
    except Exception as e:
        status = f"[red]{e}[/red]"
    console.print(f"  [dim]{'database':<14}[/dim] {status}")

    # Catalog
    try:
        catalog = SemanticCatalog()
        dbs = catalog.list_catalogs()
        if dbs:
            schema = catalog.load(dbs[0])
            n = len(schema.tables) if schema else "?"
            # 파일 수정 시간
            path = catalog.storage_dir / f"{dbs[0]}_catalog.json"
            mtime = datetime.fromtimestamp(path.stat().st_mtime)
            age = (datetime.now() - mtime)
            age_str = f"{age.days}d ago" if age.days else "today"
            status = f"[green]{n} tables[/green]  last profiled {age_str}"
        else:
            status = "[yellow]not found[/yellow]  run sqe profile"
    except Exception as e:
        status = f"[red]{e}[/red]"
    console.print(f"  [dim]{'catalog':<14}[/dim] {status}")

    # Examples
    try:
        retriever = SemanticRetriever()
        count = len(retriever._get_history_collection().get()["ids"])
        status = f"[green]{count} saved[/green]" if count else "[dim]none[/dim]"
    except Exception as e:
        status = f"[red]{e}[/red]"
    console.print(f"  [dim]{'examples':<14}[/dim] {status}")

    console.print()


# ---------------------------------------------------------------------------
# catalog
# ---------------------------------------------------------------------------

async def cmd_catalog(subcommand: str, args: list[str]):
    from bridgesql.semantic.catalog import SemanticCatalog
    from bridgesql.rag.retriever import SemanticRetriever
    from bridgesql.db.connector import DatabaseConnector

    catalog = SemanticCatalog()

    def _get_db_name() -> str | None:
        dbs = catalog.list_catalogs()
        if not dbs:
            return None
        try:
            c = DatabaseConnector()
            name = c.get_database_name()
            c.close()
            return name
        except Exception:
            return dbs[0]

    if subcommand == "show":
        db = _get_db_name()
        if not db:
            console.print()
            console.print("  No catalog found. Run [bold]sqe profile[/bold] first.")
            console.print()
            return

        schema = catalog.load(db)
        if not schema:
            return

        table_name = args[0] if args else None
        console.print()

        if table_name:
            tbl = schema.get_table(table_name)
            if not tbl:
                console.print(f"  Table '{table_name}' not found.")
                console.print()
                return
            console.print(f"  [bold]{tbl.name}[/bold]  [dim]{tbl.business_name or ''}[/dim]")
            if tbl.description:
                console.print(f"  {tbl.description}")
            console.print()
            t = Table(show_header=True, header_style="dim", box=rich_box.SIMPLE_HEAD, show_lines=True, padding=(0, 2))
            t.add_column("column")
            t.add_column("type", style="dim")
            t.add_column("business name")
            t.add_column("description", max_width=50, style="dim")
            for col in tbl.columns:
                flags = []
                if col.is_primary_key: flags.append("PK")
                if col.is_foreign_key: flags.append("FK")
                name = col.name + (f" [{','.join(flags)}]" if flags else "")
                t.add_row(name, col.data_type, col.business_name or "", col.description or "")
            console.print(t)
        else:
            t = Table(show_header=True, header_style="dim", box=rich_box.SIMPLE_HEAD, show_lines=True, padding=(0, 2))
            t.add_column("table")
            t.add_column("business name")
            t.add_column("columns", justify="right", style="dim")
            t.add_column("rows", justify="right", style="dim")
            for tbl in schema.tables:
                t.add_row(
                    tbl.name,
                    tbl.business_name or "",
                    str(len(tbl.columns)),
                    f"{tbl.row_count:,}" if tbl.row_count else "?",
                )
            console.print(t)

        console.print()

    elif subcommand == "edit":
        if not args:
            console.print()
            console.print("  Usage: sqe catalog edit <table>")
            console.print()
            return

        db = _get_db_name()
        if not db:
            console.print()
            console.print("  No catalog found.")
            console.print()
            return

        schema = catalog.load(db)
        tbl = schema.get_table(args[0]) if schema else None
        if not tbl:
            console.print()
            console.print(f"  Table '{args[0]}' not found.")
            console.print()
            return

        console.print()
        console.print(f"  [bold]{tbl.name}[/bold]  (press enter to keep current value)")
        console.print()

        new_business = Prompt.ask("  business name", default=tbl.business_name or "")
        new_desc = Prompt.ask("  description", default=tbl.description or "")

        tbl.business_name = new_business
        tbl.description = new_desc

        path = catalog.storage_dir / f"{db}_catalog.json"
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        for t in data["schema"]["tables"]:
            if t["name"] == args[0]:
                t["business_name"] = new_business
                t["description"] = new_desc
                break
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        _step("  Updating vector index...")
        try:
            SemanticRetriever().index_schema(schema)
            _done()
        except Exception:
            _fail()

        console.print()

    elif subcommand == "clear":
        console.print()
        if not Confirm.ask("  Reset catalog? (requires sqe profile to rebuild)"):
            console.print()
            return
        for db in catalog.list_catalogs():
            (catalog.storage_dir / f"{db}_catalog.json").unlink(missing_ok=True)
        try:
            SemanticRetriever().clear()
        except Exception:
            pass
        console.print("  Catalog cleared.")
        console.print()

    else:
        console.print()
        console.print("  Usage: sqe catalog [show|edit|clear]")
        console.print()


# ---------------------------------------------------------------------------
# examples
# ---------------------------------------------------------------------------

async def cmd_examples(subcommand: str, args: list[str]):
    from bridgesql.rag.retriever import SemanticRetriever
    retriever = SemanticRetriever()

    if subcommand == "list":
        data = retriever._get_history_collection().get(include=["metadatas"])
        console.print()
        if not data["ids"]:
            console.print("  No examples saved.")
            console.print()
            return
        t = Table(show_header=True, header_style="dim", box=rich_box.SIMPLE_HEAD, show_lines=True, padding=(0, 2))
        t.add_column("id", style="dim", max_width=10)
        t.add_column("question", max_width=40)
        t.add_column("sql", style="dim", max_width=60)
        for doc_id, meta in zip(data["ids"], data["metadatas"]):
            t.add_row(doc_id[:8], meta.get("question", ""), meta.get("sql", ""))
        console.print(t)
        console.print()

    elif subcommand == "add":
        import uuid
        console.print()
        question = Prompt.ask("  question")
        if not question.strip():
            console.print()
            return
        console.print("  sql  (empty line to finish)")
        sql_lines = []
        while True:
            line = input("  ")
            if not line:
                break
            sql_lines.append(line)
        sql = "\n".join(sql_lines).strip()
        if not sql:
            console.print()
            return
        console.print()
        console.print(f"  [dim]question[/dim]  {question}")
        console.print(f"  [dim]sql[/dim]       {sql[:80]}{'...' if len(sql) > 80 else ''}")
        console.print()
        if Confirm.ask("  Save?"):
            retriever.index_few_shot_example(log_id=str(uuid.uuid4()), question=question, sql=sql)
            console.print("  Saved.")
        console.print()

    elif subcommand == "delete":
        if not args:
            console.print()
            console.print("  Usage: sqe examples delete <id>")
            console.print()
            return
        data = retriever._get_history_collection().get(include=["metadatas"])
        matched = [i for i in data["ids"] if i.startswith(args[0])]
        console.print()
        if not matched:
            console.print(f"  No example matching '{args[0]}'.")
            console.print()
            return
        if len(matched) > 1:
            console.print("  Multiple matches — provide more characters:")
            for m in matched:
                console.print(f"    {m}")
            console.print()
            return
        full_id = matched[0]
        meta = data["metadatas"][data["ids"].index(full_id)]
        console.print(f"  [dim]question[/dim]  {meta.get('question', '')}")
        console.print(f"  [dim]sql[/dim]       {meta.get('sql', '')[:80]}")
        console.print()
        if Confirm.ask("  Delete?"):
            retriever._get_history_collection().delete(ids=[full_id])
            console.print("  Deleted.")
        console.print()

    elif subcommand == "clear":
        console.print()
        if Confirm.ask("  Delete all examples?"):
            col = retriever._get_history_collection()
            ids = col.get()["ids"]
            if ids:
                col.delete(ids=ids)
            console.print("  Cleared.")
        console.print()

    else:
        console.print()
        console.print("  Usage: sqe examples [list|add|delete <id>|clear]")
        console.print()


if __name__ == "__main__":
    main()
