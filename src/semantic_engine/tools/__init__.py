"""MCP Tools — exports ALL_TOOLS and dispatch()."""
from typing import Any

from mcp.types import TextContent, Tool

from semantic_engine.tools import catalog, context, examples, sql

ALL_TOOLS: list[Tool] = context.TOOLS + sql.TOOLS + examples.TOOLS + catalog.TOOLS

_MODULES = [context, sql, examples, catalog]


async def dispatch(name: str, arguments: dict[str, Any]) -> list[TextContent] | None:
    for module in _MODULES:
        result = await module.handle(name, arguments)
        if result is not None:
            return result
    return None
