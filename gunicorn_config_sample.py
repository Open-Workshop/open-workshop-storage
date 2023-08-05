bind = "0.0.0.0:443"  # Указывает Gunicorn слушать все IP-адреса на порту 443
certfile = "/etc/letsencrypt/live/YOU_DOMEN.com/fullchain.pem"  # Путь к вашему SSL сертификату
keyfile = "/etc/letsencrypt/live/YOU_DOMEN.com/privkey.pem"  # Путь к вашему приватному ключу
workers = 1  # Количество рабочих процессов Gunicorn