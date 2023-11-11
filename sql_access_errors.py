from sqlalchemy import create_engine, Column, Integer, String, DateTime, insert
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from fastapi import Request
import bcrypt


engine = create_engine('sqlite:///sql/access.db')
base = declarative_base()

class AccessError(base): # Таблица "игры"
    __tablename__ = 'access_errors'
    id = Column(Integer, primary_key=True)

    func_name = Column(String)
    type = Column(String)
    when_error = Column(DateTime)

    user_ip = Column(String)
    user_port = Column(Integer)
    target_url = Column(String)
    user_agent = Column(String)

    token = Column(String)

class AccessOK(base): # Таблица "игры"
    __tablename__ = 'access_ok'
    id = Column(Integer, primary_key=True)

    func_name = Column(String)
    when_access = Column(DateTime)

    user_ip = Column(String)
    user_port = Column(Integer)

async def access(request: Request, user_token: str, real_token: str, func_name: str = "unknown"):
    try:
        access_result = await check_access(request=request, user_token=user_token, real_token=real_token)

        # Создание сессии
        Session = sessionmaker(bind=engine)
        session = Session()

        # Получаем текущее время
        current_date = datetime.now()

        if access_result == True:
            ## Все ок, доступ разрешен

            # Сохраняем в базу
            session.execute(insert(AccessOK).values(
                func_name=func_name,
                when_access=current_date,
                user_ip=request.client.host,
                user_port=request.client.port
            ))
            session.commit()
            session.close()

            return True
        else:
            ## Сейвим ошибку редактирования базы

            # Сохраняем юзер-агент
            user_agent_date = str(request.headers.__dict__["_list"]).replace("'", "`").replace("\"", "`")

            # Сохраняем в базу
            session.execute(insert(AccessError).values(
                func_name=func_name,
                type=access_result,
                when_error=current_date,
                user_ip=request.client.host,
                user_port=request.client.port,
                target_url=request.url._url,
                user_agent=user_agent_date,
                token=user_token
            ))
            session.commit()
            session.close()

            return False
    except:
        return False

async def check_access(request: Request, user_token: str, real_token: str):
    # Проверяем источник запроса (особо не полагаемся, но пусть будет)
    try:
        if request.client.host != "127.0.0.1" or not request.url._url.startswith("http://127.0.0.1:8000/account/"):
            return "user is incorrect"
        elif request.headers:
            return "user substitution attempt"
    except:
        return "user error"

    # Сравниваем хеши
    try:
        # Получаем хеш переданного пароля
        hash_user_token = bcrypt.hashpw(user_token)

        # Сравниваем хеши
        if not bcrypt.checkpw(real_token, hash_user_token):
            return "hash is incorrect"
    except:
        return "hash error"

    # Все ок, доступ разрешен
    return True

base.metadata.create_all(engine)