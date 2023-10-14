from sqlalchemy import create_engine, Column, Integer, String, DateTime, Table, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


engine = create_engine('sqlite:///sql/database.db')
base = declarative_base()


# Связывающие БД
game_genres = Table('unity_games_genres', base.metadata, # Теги для игр
    Column('game_id', Integer, ForeignKey('games.id')),
    Column('genre_id', Integer, ForeignKey('genres.id'))
)

allowed_mods_tags = Table('unity_allowed_mods_tags', base.metadata, # Разрешенные игрой теги для модов
    Column('game_id', Integer, ForeignKey('games.id')),
    Column('tag_id', Integer, ForeignKey('tags.id'))
)

mods_tags = Table('unity_mods_tags', base.metadata, # Теги для игр
    Column('mod_id', Integer, ForeignKey('mods.id')),
    Column('tag_id', Integer, ForeignKey('tags.id'))
)

mods_dependencies = Table('unity_mods_dependencies', base.metadata, # Зависимости мода
    Column('mod_id', Integer, ForeignKey('mods.id')),
    Column('dependence', Integer, ForeignKey('mods.id')),
    extend_existing=True
)


# Основные БД
class Game(base): # Таблица "игры"
    __tablename__ = 'games'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    type = Column(String)
    logo = Column(String)

    short_description = Column(String)
    description = Column(String)

    mods_downloads = Column(Integer)
    mods_count = Column(Integer)

    creation_date = Column(DateTime)

    source = Column(String)

    genres = relationship('Genres', secondary=game_genres, backref='games')
    allowed_tags_for_mods = relationship('ModTag', secondary=allowed_mods_tags, backref='games', viewonly=True)

class Mod(base): # Таблица "моды"
    __tablename__ = 'mods'
    id = Column(Integer, primary_key=True)
    name = Column(String)

    short_description = Column(String)
    description = Column(String)

    size = Column(Integer)

    condition = Column(Integer) #0 - загружен, 1 - загружается

    date_creation = Column(DateTime)
    date_update = Column(DateTime)
    date_request = Column(DateTime)

    source = Column(String)
    downloads = Column(Integer)

    tags = relationship('ModTag', secondary=mods_tags, backref='mods')
    dependencies = relationship('Mod', secondary=mods_dependencies, primaryjoin=(mods_dependencies.c.mod_id == id),
        secondaryjoin=(mods_dependencies.c.dependence == id), backref='mods',
        foreign_keys=[mods_dependencies.c.mod_id, mods_dependencies.c.dependence]
    )
    game = Column(Integer)

class ResourceMod(base): # Ресурсы (скриншоты и лого)
    __tablename__ = 'resources_mods'
    id = Column(Integer, primary_key=True)
    type = Column(String)
    url = Column(String)
    date_event = Column(DateTime)

    owner_id = Column(Integer, ForeignKey('mods.id'))


# Теги
class Genres(base): # Жанры для игр
    __tablename__ = 'genres'
    id = Column(Integer, primary_key=True)
    name = Column(String)

class ModTag(base): # Теги для модов
    __tablename__ = 'tags'
    id = Column(Integer, primary_key=True)
    name = Column(String)

    associated_games = relationship('Game', secondary=allowed_mods_tags, backref='tags', viewonly=True)


base.metadata.create_all(engine)