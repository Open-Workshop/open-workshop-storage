import os
import json
import shutil
import zipfile
import sql_data_client as sdc
from pathlib import Path
from datetime import datetime
from sqlalchemy.sql.expression import desc


# Запаковываем сохраненный мод в архив (для экономии места и трафика)
def zipping(game_id: int, mod_id: int, target_size: int) -> bool:
    game_id = int(game_id)
    mod_id = int(mod_id)
    target_size = int(target_size)

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

    print(f"mod size target {target_size} {type(target_size)}")
    print(f"mod size total  {total_size} {type(total_size)}")

    # Проверяем полностью ли установился мод
    if total_size != target_size or target_size <= 0:
        os.remove(zip_path)
        return False
    else:
        Path(f"mods/{game_id}").mkdir(parents=True, exist_ok=True)
        os.replace(src=zip_path, dst=f"mods/{game_id}/{mod_id}.zip")

        return True

async def calculate_uncompressed_size(file_path):
    try:
        with zipfile.ZipFile(file_path) as zip_file:
            total_size = sum(file.file_size for file in zip_file.infolist())
            return total_size
    except Exception as e:
        return str(e)


async def zip_standart(archive_path: str):
    try:
        # Проверяем, является ли архив архивом ZIP_BZIP2
        with zipfile.ZipFile(archive_path, "r") as archive:
            compression_type = archive.compression
            if compression_type == zipfile.ZIP_BZIP2:
                print("Архив уже заархивирован по стандарту ZIP_BZIP2")
                return archive_path
            else:
                print("Архив не заархивирован по стандарту ZIP_BZIP2")

                # Приводим архив к стандарту ZIP_BZIP2
                new_archive_path = archive_path.replace(".zip", "_new.zip")
                with zipfile.ZipFile(new_archive_path, "w", compression=zipfile.ZIP_BZIP2) as new_archive:
                    for file_name in archive.namelist():
                        with archive.open(file_name) as file:
                            new_archive.writestr(file_name, file.read())

        os.remove(archive_path)

        print("Архив успешно приведен к стандарту ZIP_BZIP2")
        return new_archive_path
    except:
        os.remove(archive_path)
        print("При проверке стандарта архива произошла непредвиденная ошибка!")
        return ""



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
        case 'CREATION_DATE':
            return sdc.Mod.date_creation
        case 'iCREATION_DATE':
            return desc(sdc.Mod.date_creation)
        case 'UPDATE_DATE':
            return sdc.Mod.date_update
        case 'iUPDATE_DATE':
            return desc(sdc.Mod.date_update)
        case 'REQUEST_DATE':
            return sdc.Mod.date_request
        case 'iREQUEST_DATE':
            return desc(sdc.Mod.date_request)
        case 'SOURCE':
            return sdc.Mod.source
        case 'iSOURCE':
            return desc(sdc.Mod.source)
        case 'iMOD_DOWNLOADS':
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
        case 'iMOD_COUNT':
            return desc(sdc.Game.mods_count)
        case 'MOD_DOWNLOADS':
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

