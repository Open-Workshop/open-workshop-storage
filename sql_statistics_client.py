from datetime import datetime, date
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Date, insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


allow_language_type_map = {"ru": "Нет перевода 0_о"}

# Таблица с типами
type_map = {
    "start": {"ru": "Запусков сервера"},
    "/statistics/info/type_map/": {"ru": "Запросов к карте переводов"},
    "/statistics/info/all/": {"ru": "Запросов к общей статистической сводке"},
    "/statistics/day/": {"ru": "Запросов к ежедневной статистике"},
    "/statistics/hour/": {"ru": "Запросов к ежечасовой статистике"},
    "/statistics/delay/": {"ru": "Запросов к информации о задержке"},
    "/condition/mod/": {"ru": "Запросов к состоянию нескольких модов"},
    "/info/mod/": {"ru": "Запросов к информации о модах"},
    "/info/game/": {"ru": "Запросов к информации об играх"},
    "/list/resources_mods/": {"ru": "Запросов к ресурсам модов"},
    "/list/genres/": {"ru": "Запросов к списку жанров"},
    "/list/tags/": {"ru": "Запросов к списку тегов для модов"},
    "/list/games/": {"ru": "Запросов к списку игр"},
    "/list/mods/": {"ru": "Запросов к списку модов"},
    "/download/": {"ru": "Запросов к локальной загрузке"},
    "mod_not_found_local": {"ru": "При запросе к локальному моду, его не было найдено"},
    "files_sent": {"ru": "Файлов отправлено с сервера"},
    "damaged_mod": {"ru": "Обнаружено поврежденных записей"},
    "/download/steam/": {"ru": "Запросов к загрузке со Steam"},
    "download_from_steam_error": {"ru": "Загрузок со Steam окончено провалом"},
    "download_from_steam_ok": {"ru": "Загрузок со Steam прошло успешно"},
    "updating_mod": {"ru": "Модов поставлено на обновление"},
    "/": {"ru": "Перенаправлений на документацию"}
}
cache = {}

def cache_types_data(code: str):
    if cache.get(code):
        return cache.get(code)
    else:
        output = {}
        for key in type_map.keys():
            output[key] = type_map[key].get(code, allow_language_type_map[code])
        cache[code] = output
        return output


engine = create_engine('sqlite:///sql/statistics.db')
base = declarative_base()


# Определяем модель
class StatisticsHour(base):
    __tablename__ = 'statistics_hour'
    id = Column(Integer, primary_key=True)
    date_time = Column(DateTime)

    # download_steam, download, info_mod, etc
    type = Column(String)
    count = Column(Integer)


class StatisticsDay(base):
    __tablename__ = 'statistics_day'
    id = Column(Integer, primary_key=True)
    date = Column(Date)

    #download_steam, download, info_mod, etc
    type = Column(String)
    count = Column(Integer)


class ProcessingTime(base):
    __tablename__ = 'processing_time'
    time = Column(DateTime, primary_key=True)
    type = Column(String)
    delay = Column(Integer)

def create_processing(type, time_start):
    milliseconds = int((datetime.now()-time_start).total_seconds() * 1000)

    req = insert(ProcessingTime).values(
        time=time_start,
        type=type,
        delay=milliseconds
    )

    Session = sessionmaker(bind=engine)
    session = Session()
    session.execute(req)
    session.commit()
    session.close()


def update(type:str):
    Session = sessionmaker(bind=engine)
    session = Session()

    update_hour(session=session, type=type)
    update_day(session=session, type=type)

    session.commit()
    session.close()

def update_hour(session, type:str):
    # Получение текущего часа
    current_hour = datetime.now().replace(minute=0, second=0, microsecond=0)

    # Запрос к базе данных для получения колонки
    query = session.query(StatisticsHour).filter_by(date_time=current_hour, type=str(type))
    column = query.first()

    if column:
        query.update({'count': column.count+1})
    else:
        session.execute(insert(StatisticsHour).values(
            date_time=current_hour,
            type=type,
            count=1
        ))
def update_day(session, type:str):
    # Получение текущего часа
    current_day = date.today()

    # Запрос к базе данных для получения колонки
    query = session.query(StatisticsDay).filter_by(date=current_day, type=str(type))
    column = query.first()

    if column:
        query.update({'count': column.count+1})
    else:
        session.execute(insert(StatisticsDay).values(
            date=current_day,
            type=type,
            count=1
        ))


# Создаем таблицу в базе данных
base.metadata.create_all(engine)