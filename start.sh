mkdir -p steam_client
mkdir -p mods
mkdir -p sql
while true; do
    screen -S open-workshop-backend-executor gunicorn main:app -b 0.0.0.0:8000 --access-logfile access.log --error-logfile error.log -c gunicorn_config.py --worker-class uvicorn.workers.UvicornWorker
    sleep 30
done