import bcrypt
import secrets
import string

def generate_token(length=32):
    """Генерация случайного токена"""
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))

def hash_token(token):
    """Хеширование токена с помощью bcrypt"""
    # Генерируем соль и хешируем токен
    salt = bcrypt.gensalt()
    hashed_token = bcrypt.hashpw(token.encode(), salt)
    return hashed_token.decode()

def generate_token_pairs():
    """Генерация всех необходимых пар токенов"""
    tokens = {}
    
    # Отправляемые токены
    check_access_token = generate_token()
    tokens['check_access'] = {
        'plain': check_access_token,
        'hashed': hash_token(check_access_token)
    }
    
    # Принимаемые токены
    delete_file_token = generate_token()
    tokens['delete_file'] = {
        'plain': delete_file_token,
        'hashed': hash_token(delete_file_token)
    }
    
    upload_file_token = generate_token()
    tokens['upload_file'] = {
        'plain': upload_file_token,
        'hashed': hash_token(upload_file_token)
    }
    
    return tokens

def main():
    """Основная функция"""
    print("Генерация токенов...")
    print("-" * 50)
    
    # Генерируем токены
    tokens = generate_token_pairs()
    
    # Выводим результат
    print("Сгенерированные токены:")
    for token_name, token_data in tokens.items():
        print(f"\n{token_name}:")
        print(f"  Отправляемый: {token_data['plain']}")
        print(f"  Принимаемый: {token_data['hashed']}")
    
    print("\n" + "=" * 50)
    print("ВАЖНО!")
    print("1. Сохраните исходные токены в надежном месте")
    print("2. Скопируйте хешированные токены в ваш ow_config.py")
    print("3. Никогда не коммитьте ow_config.py в git!")

if __name__ == "__main__":
    main()