import os
import tool
import json
import requests
import sql_data_client as sdc
from bs4 import BeautifulSoup
from datetime import datetime
from sqlalchemy import delete, insert
from fastapi.responses import FileResponse
from user_agent import generate_user_agent

headers = {
    "Content-type": "application/x-www-form-urlencoded",
    "Accept": "text/plain"
}

def get_mod(id:int):
    JSON = None
    try:
        response = requests.post(
            "https://api.steampowered.com/ISteamRemoteStorage/GetPublishedFileDetails/v0001/",
            "itemcount=1&publishedfileids[0]="+str(id),
            headers=headers, timeout=5)
        JSON = json.loads(response.text)["response"]["publishedfiledetails"][0]
        if JSON.get("result", None) != 1:
            JSON = None
    except:
        print("Ошибка! Не удалось получить информацию о моде с серверов Valve :(")

    return JSON

def get_html_data(id:int):
    result = {"dependencies": [], "screenshots": []}

    try:
        d = 'https://steamcommunity.com/sharedfiles/filedetails/?id='+str(id)
        agent = generate_user_agent(device_type="desktop")
        print(agent)
        response = requests.get(url=d, timeout=10, headers={"User-Agent": agent})

        try:
            soup = BeautifulSoup(response.content, "html.parser")

            # Используйте метод `find_all` для поиска всех элементов с классом "requiredItemsContainer" и id "RequiredItems"
            containers = soup.find_all("div", class_="requiredItemsContainer", id="RequiredItems")

            # Для каждого контейнера найдите все ссылки внутри него
            for container in containers:
                links = container.find_all("a")
                for link in links:
                    out = link.get("href").removeprefix("https://steamcommunity.com/workshop/filedetails/?id=")
                    if out.isdigit():
                        result["dependencies"].append(int(out))
        except:
            print(f"Ошибка! Не удалось получить информацию о зависимостях мода! ({id})")

        try:
            # Создаем объект Beautiful Soup для парсинга HTML-кода страницы
            soup = BeautifulSoup(response.content, 'html.parser')

            # Находим все элементы <a> с классом 'highlight_screenshot_link'
            screenshot_links = soup.find_all('div', class_="screenshot_holder")#class_='highlight_player_item highlight_screenshot')

            #Извлекаем ссылки на скриншоты
            for div in screenshot_links:
                lin = div.find('a')
                print(lin)

                # Извлекаем значение атрибута onclick
                onclick_value = lin.get('onclick')

                # Ищем начальную и конечную позиции ссылки внутри значения атрибута onclick
                start_index = onclick_value.find("'") + 1
                end_index = onclick_value.rfind("'")

                # Извлекаем ссылку
                image_url = onclick_value[start_index:end_index]

                #lin = lin['href']
                #start_index = lin.find("'")+1
                #lin = str(lin[start_index:lin.find("'", start_index)])
                if image_url.startswith("https://"):
                    result["screenshots"].append(image_url)
        except:
            print(f"Ошибка! Не удалось получить скриншоты мода! ({id})")
    except:
        print(f"Ошибка! Глобальная ошибка при получении информации о моде! ({id})")

    return result


def get_app(id:int):
    JSON = None
    try:
        response = requests.get(f"https://store.steampowered.com/api/appdetails?appids={str(id)}&cc=tw",
            headers=headers, timeout=5)
        JSON = json.loads(response.text)[str(id)]
        if not JSON["success"]:
            JSON = None
        else:
            JSON = JSON["data"]
    except:
        print("Ошибка! Не удалось получить информацию о приложении с серверов Valve :(")

    return JSON


def checker(rows, path, mod_id, session):
    if rows is not None and len(rows) > 0:  # Если в БД уже есть запись об этом моде
        bind = session.query(sdc.games_mods).filter_by(mod_id=int(mod_id)).all()
        if bind != None and len(bind) > 0:
            bind = bind[0].game_id
        else:
            bind = "null"

        print(bind)
        print(mod_id)
        path_real = path + f'{bind}/{mod_id}'  # Получаем реальный путь до файла
        if os.path.isfile(path_real + '.zip'):  # Если это ZIP архив - отправляем
            return FileResponse(path=path_real+'.zip', filename=f"{rows[0].name}.zip")
        elif os.path.isdir(path):  # Если это по какой-то причине - папка
            # Пытаемся фиксануть проблему
            tool.zipping(game_id=bind, mod_id=mod_id)

            # Шлем пользователю
            return FileResponse(path=path_real+'.zip', filename=f"{rows[0].name}.zip")
        else:  # Удаляем запись в БД как не действительную
            delete_bind = delete(sdc.games_mods).where(sdc.games_mods.mod_id == int(mod_id))
            delete_statement = delete(sdc.Mod).where(sdc.Mod.id == int(mod_id))
            # Выполнение операции DELETE
            session.execute(delete_statement)
            session.execute(delete_bind)
            session.commit()
    return None


def setters(session, mod_data):
    # Обновляем информацию о тегах (при этом удалив записи со старым вариантом связи их с конкретным модом)
    delete_tags = sdc.mods_tags.delete().where(sdc.mods_tags.c.mod_id == int(mod_data['publishedfileid']))
    session.execute(delete_tags)
    session.commit()
    set_tags(session=session, mod_data=mod_data)

    delete_gen = sdc.game_genres.delete().where(sdc.game_genres.c.game_id == int(mod_data['consumer_app_id']))
    session.execute(delete_gen)
    session.commit()
    set_game(session=session, mod_data=mod_data)

    delete_dep = sdc.mods_dependencies.delete().where(sdc.mods_dependencies.c.mod_id == int(mod_data['publishedfileid']))
    session.execute(delete_dep)
    delete_screens = delete(sdc.ResourceMod).where(sdc.ResourceMod.owner_id == int(mod_data['publishedfileid']))
    session.execute(delete_screens)
    session.commit()
    set_dependence_and_screenshots(session=session, mod_data=mod_data)

    session.query(sdc.Mod).filter_by(id=int(mod_data['publishedfileid'])).update({'condition': 0})
    session.commit()
    session.close()

def set_dependence_and_screenshots(session, mod_data):
    dep = get_html_data(id=int(mod_data['publishedfileid']))

    for d in dep["dependencies"]:
        # Регистрируем зависимость
        result = session.query(sdc.mods_dependencies).filter_by(dependence=d,
                                                                mod_id=int(mod_data['publishedfileid'])).first()
        if result is None:
            ex = insert(sdc.mods_dependencies).values(mod_id=int(mod_data['publishedfileid']), dependence=int(d))
            session.execute(ex)

    for screenshot in dep["screenshots"]:
        # Регистрируем скриншоты
        result = session.query(sdc.ResourceMod).filter_by(url=screenshot).first()
        if result is None:
            ex = insert(sdc.ResourceMod).values(type="screenshot", url=screenshot, date_event=datetime.now(), owner_id=int(mod_data['publishedfileid']))
            session.execute(ex)

    # Регистрируем лого
    result = session.query(sdc.ResourceMod).filter_by(url=mod_data['preview_url']).first()
    if result is None:
        ex = insert(sdc.ResourceMod).values(type="logo", url=mod_data['preview_url'], date_event=datetime.now(), owner_id=int(mod_data['publishedfileid']))
        session.execute(ex)

    session.commit()
def set_tags(session, mod_data):
    orig_tags = []
    for t in mod_data['tags']:
        orig_tags.append(t['tag'])
    if len(orig_tags) > 0:
        for tag in orig_tags:
            # Создаем тег
            result = session.query(sdc.ModTag).filter_by(name=tag).first()
            if result is None:
                dat = sdc.ModTag(name=tag)
                session.add(dat)
                session.commit()
                session.refresh(dat)
                result = dat

            # Проверяем зарегистрирован ли в разрешенных модах
            output = session.query(sdc.allowed_mods_tags).filter_by(tag_id=result.id,
                                                                    game_id=mod_data["consumer_app_id"]).first()
            if output is None:
                insert_statement = insert(sdc.allowed_mods_tags).values(tag_id=result.id,
                                                                        game_id=mod_data["consumer_app_id"])
                session.execute(insert_statement)

            # Проверяем зарегистрирован ли в тегах мода
            output = session.query(sdc.mods_tags).filter_by(tag_id=result.id,
                                                            mod_id=mod_data['publishedfileid']).first()
            if output is None:
                insert_statement = insert(sdc.mods_tags).values(tag_id=result.id,
                                                                mod_id=mod_data['publishedfileid'])
                session.execute(insert_statement)
        session.commit()
def set_game(session, mod_data):
    rows = session.query(sdc.Game).filter(sdc.Game.id == int(mod_data['consumer_app_id'])).all()

    if rows is None or len(rows) <= 0:
        # Отправка запроса на сервер
        dat = get_app(mod_data["consumer_app_id"])
        if dat != None:
            insert_statement = insert(sdc.Game).values(
                id=mod_data["consumer_app_id"],
                name=dat['name'],
                type=dat['type'],
                logo=dat['header_image'],
                short_description=dat['short_description'],
                description=dat['detailed_description'],
                mods_downloads=0,
                mods_count=tool.get_mods_count(session=session, game_id=mod_data["consumer_app_id"]),
                creation_date=datetime.now(),
                source='steam',
            )

            session.execute(insert_statement)
            session.commit()

            for genre in dat['genres']:
                # Создаем жанр
                result = session.query(sdc.Genres).filter_by(id=genre['id']).first()
                if result is None:
                    insert_genre = insert(sdc.Genres).values(id=genre['id'], name=genre.get('description', 'No data'))
                    session.execute(insert_genre)
                    session.commit()

                # Проверяем зарегистрирован ли в разрешенных модах
                output = session.query(sdc.game_genres).filter_by(game_id=mod_data["consumer_app_id"],
                                                                  genre_id=genre['id']).first()
                if output is None:
                    insert_statement = insert(sdc.game_genres).values(game_id=mod_data["consumer_app_id"],
                                                                      genre_id=genre['id'])
                    session.execute(insert_statement)
                    session.commit()
    else:
        session.query(sdc.Game).filter_by(id=int(mod_data['consumer_app_id'])).update({'mods_count': tool.get_mods_count(session=session, game_id=mod_data["consumer_app_id"])})
    session.commit()
