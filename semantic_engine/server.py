"""BridgeSQL MCP Server entry point."""
import asyncio
import threading

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent

from semantic_engine import prompts, resources, tools
from semantic_engine.state import _ensure_imports, get_retriever, get_schema

app = Server('bridgesql')


@app.list_resources()
async def list_resources():
    return resources.RESOURCES


@app.list_resource_templates()
async def list_resource_templates():
    return resources.RESOURCE_TEMPLATES


@app.read_resource()
async def read_resource(uri) -> str:
    return await resources.read(str(uri))


@app.list_prompts()
async def list_prompts():
    return prompts.PROMPTS


@app.get_prompt()
async def get_prompt(name: str, arguments: dict | None = None):
    return await prompts.get(name, arguments)


@app.list_tools()
async def list_tools():
    return tools.ALL_TOOLS


@app.call_tool()
async def call_tool(name: str, arguments: dict):
    try:
        result = await tools.dispatch(name, arguments)
        if result is not None:
            return result
        return [TextContent(type='text', text=f'Unknown tool: {name}')]
    except Exception as error:
        import sys
        import traceback
        traceback.print_exc(file=sys.stderr)
        return [TextContent(type='text', text=f'오류 발생: {type(error).__name__}: {error}')]


def _preload_in_background() -> None:
    def _load():
        try:
            _ensure_imports()
            get_retriever()
            get_schema()
        except Exception:
            pass

    threading.Thread(target=_load, daemon=True).start()


async def main() -> None:
    import sys
    print('[BridgeSQL] MCP server starting (preloading in background)', file=sys.stderr)
    _preload_in_background()
    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())


if __name__ == '__main__':
    asyncio.run(main())
