if not exist "steam_client" mkdir steam_client
if not exist "sql" mkdir sql
uvicorn main:app --host 127.0.0.1 --port 8000