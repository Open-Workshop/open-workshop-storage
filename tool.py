import os
import json
import shutil
import zipfile
import sql_data_client as sdc
from pathlib import Path
from datetime import datetime
from sqlalchemy.sql.expression import desc


def zipping(game_id: int, mod_id: int, target_size: int) -> bool: \
        # Запаковываем сохраненный мод в архив (для экономии места и трафика)
    directory_path = f"steamapps/workshop/content/{game_id}/{mod_id}"  # Укажите путь к вашей папке
    zip_path = f"steamapps/workshop/content/{game_id}/{mod_id}.zip"  # Укажите путь к ZIP-архиву, который вы хотите создать

    if not os.path.isdir(directory_path) or not any(Path(directory_path).iterdir()):
        if os.path.isdir(directory_path):
            shutil.rmtree(directory_path)
        return False

    total_size = 0
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_BZIP2) as zipf:
        for root, dirs, files in os.walk(directory_path):
            for file in files:
                file_path = os.path.join(root, file)
                zipf.write(file_path, os.path.relpath(file_path, directory_path))
                # пропускаем символические ссылки
                if not os.path.islink(file_path):
                    total_size += os.path.getsize(file_path)
            for dir in dirs:
                dir_path = os.path.join(root, dir)
                zipf.write(dir_path, os.path.relpath(dir_path, directory_path))

    # Удаление исходной папки и её содержимого
    shutil.rmtree(directory_path)

    print(f"mod size target {target_size}")
    print(f"mod size total  {total_size}")

    # Проверяем полностью ли установился мод
    if total_size != target_size or target_size <= 0:
        os.remove(zip_path)
        return False
    else:
        Path(f"mods/{game_id}").mkdir(parents=True, exist_ok=True)
        os.replace(src=zip_path, dst=f"mods/{game_id}/{mod_id}.zip")

        return True


def sort_mods(sort_by: str):
    match sort_by:
        case 'NAME':
            return sdc.Mod.name
        case 'iNAME':
            return desc(sdc.Mod.name)
        case 'SIZE':
            return sdc.Mod.size
        case 'iSIZE':
            return desc(sdc.Mod.size)
        case 'DATE_CREATION':
            return sdc.Mod.date_creation
        case 'iDATE_CREATION':
            return desc(sdc.Mod.date_creation)
        case 'DATE_UPDATE':
            return sdc.Mod.date_update
        case 'iDATE_UPDATE':
            return desc(sdc.Mod.date_update)
        case 'DATE_REQUEST':
            return sdc.Mod.date_request
        case 'iDATE_REQUEST':
            return desc(sdc.Mod.date_request)
        case 'SOURCE':
            return sdc.Mod.source
        case 'iSOURCE':
            return desc(sdc.Mod.source)
        case 'iDOWNLOADS':
            return desc(sdc.Mod.downloads)
        case _:
            return sdc.Mod.downloads  # По умолчанию сортируем по загрузкам


def sort_games(sort_by: str):
    match sort_by:
        case 'NAME':
            return sdc.Game.name
        case 'iNAME':
            return desc(sdc.Game.name)
        case 'TYPE':
            return sdc.Game.type
        case 'iTYPE':
            return desc(sdc.Game.type)
        case 'CREATION_DATE':
            return sdc.Game.creation_date
        case 'iCREATION_DATE':
            return desc(sdc.Game.creation_date)
        case 'SOURCE':
            return sdc.Game.source
        case 'iSOURCE':
            return desc(sdc.Game.source)
        case 'MODS_COUNT':
            return sdc.Game.mods_count
        case 'iMODS_COUNT':
            return desc(sdc.Game.mods_count)
        case 'MODS_DOWNLOADS':
            return sdc.Game.mods_downloads
        case _:
            return desc(sdc.Game.mods_downloads)


def downloads_count_update(session, mod):
    # Может работать не сильно наглядно из-за кеширования браузера.
    # Т.е. несколько запросов подряд не увеличит кол-во загрузок!
    query = session.query(sdc.Mod).filter_by(id=int(mod.id))
    query.update({'downloads': mod.downloads + 1, 'date_request': datetime.now()})

    # Проходит по всем связанным играм и устанавливает количество скачиваний +1
    game = session.query(sdc.Game).filter_by(id=query.first().game)
    game.update({"mods_downloads": game.first().mods_downloads+1})

    session.commit()


def get_mods_count(session, game_id: int):
    query = session.query(sdc.Mod)
    query = query.filter(sdc.Mod.game == int(game_id))
    return query.count()


def str_to_list(string: str):
    try:
        string = json.loads(string)
        if type(string) is not list:
            string = []
    except:
        string = []
    return string


# Ограничивает текст до указанного количества символов
def truncate_text(text: str, length: int = 256) -> str:
    if len(text) > length:
        text = text[:length-3] + "..."
    return text

