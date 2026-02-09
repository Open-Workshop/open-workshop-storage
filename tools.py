import os
from typing import Optional
import ow_config as config
import bcrypt

ALLOWED_FILENAME_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")


def build_download_filename(requested_name: Optional[str], real_path: str) -> Optional[str]:
    if not requested_name:
        return None
    # Разрешены только латиница/цифры/_/-, без точек и пробелов
    for ch in requested_name:
        if ch not in ALLOWED_FILENAME_CHARS:
            return None
    ext = os.path.splitext(real_path)[1]
    return requested_name + ext


async def check_token(token_name: str, token: str) -> bool:
    # Получаем значение хеша токена из config по имени token_name
    stored_token_hash = getattr(config, token_name, None)
    
    if stored_token_hash is None:
        print(f"Токен `{token_name}` не найден в config!")
        return False
    
    # Хеш из config должен быть строкой, конвертируем в байты
    stored_token_hash = stored_token_hash.encode()
    
    # Хешируем переданный токен с использованием bcrypt и проверяем соответствие
    return bcrypt.checkpw(token.encode(), stored_token_hash)
