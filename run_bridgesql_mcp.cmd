@echo off
setlocal
cd /d "%~dp0"
set PYTHONPATH=%~dp0src
"%~dp0Scripts\python.exe" -m semantic_engine.mcp.server
