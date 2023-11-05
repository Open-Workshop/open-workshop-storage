if not exist "steam_client" mkdir steam_client
if not exist "mods" mkdir mods
if not exist "sql" mkdir sql
if not exist "users_files_processing" mkdir users_files_processing
uvicorn main:app --host 127.0.0.1 --port 8000