import os
from typing import Optional
import anyio
import aiohttp
import tools
import ow_config as config
from fastapi import FastAPI, Request, UploadFile, Form
from fastapi.responses import FileResponse, PlainTextResponse


MAIN_DIR = config.MAIN_DIR
MANAGER_URL = config.MANAGER_URL


# Создание приложения
app = FastAPI(
    title="Open Workshop",
    contact={
        "name": "GitHub",
        "url": "https://github.com/Open-Workshop"
    },
    license_info={
        "name": "MPL-2.0 license",
        "identifier": "MPL-2.0"
    },
    docs_url="/"
)

@app.middleware("http")
async def modify_header(request: Request, call_next):
    response = await call_next(request)
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Expose-Headers"] = "Content-Type,Content-Disposition"
    return response


@app.get(
    "/download/{type}/{path:path}",
    status_code=200,
    response_class=FileResponse,
    responses={
        200: {
            "description": "File send successfully",
            "content": {"application/octet-stream": {}},
        },
        400: {
            "description": "Invalid type",
            "content": {"text/plain": {'example': 'Invalid type'}}
        },
        403: {
            "description": "Access denied",
            "content": {"text/plain": {'example': 'Access denied'}}
        },
        404: {
            "description": "File not found on server",
            "content": {"text/plain": {'example': 'File not found'}}
        },
        503: {
            "description": "Manager unavailable",
            "content": {"text/plain": {'example': 'Manager unavailable'}}
        }
    },
)
async def download(request: Request, type: str, path: str, filename: Optional[str] = None):
    """
    Возвращает запрашиваемый файл, если он существует.
    """
    if not tools.is_allowed_type(type):
        return PlainTextResponse(status_code=400, content="Invalid type")
    base_dir = os.path.join(MAIN_DIR, type)
    try:
        real_path = tools.safe_path(base_dir, path)
    except ValueError:
        return PlainTextResponse(status_code=403, content="Access denied")
    
    if os.path.isfile(real_path):
        download_name = tools.build_download_filename(filename, real_path)
        if type == 'archive' and path.startswith('mod/'):
            parts = path.split('/', 2)
            if len(parts) < 2:
                return PlainTextResponse(status_code=404, content="File not found")
            try:
                mod_id = int(parts[1])
            except ValueError:
                return PlainTextResponse(status_code=404, content="File not found")
            # Асинхронно спрашиваем у Manager правомерность доступа к файлу
            async with aiohttp.ClientSession() as session:
                user = request.cookies.get('userID', 0)
                headers = {
                    "x-token": f"{config.check_access}",
                }
                async with session.get(f"{MANAGER_URL}/list/mods/access/[{mod_id}]?user={user}", headers=headers) as resp:
                    if resp.status == 200:
                        # Возвращает такой же список, проверяем, есть ли в нем интересующий нас ID
                        data = await resp.json()
                        if mod_id in data:
                            # Если есть, то возвращаем сам файл
                            return FileResponse(real_path, filename=download_name)
                        else:
                            return PlainTextResponse(status_code=403, content="Access denied")
                    else:
                        return PlainTextResponse(status_code=503, content="Manager unavailable")
        else:
            return FileResponse(real_path, filename=download_name)
    else:
        return PlainTextResponse(status_code=404, content="File not found")

@app.post(
    "/upload", 
    status_code=201, 
    response_class=PlainTextResponse, 
    response_model=str, 
    responses={
        201: {
            "description": "File uploaded successfully", 
            "content": {"text/plain": {'example': 'file/is/saved/as.tmp'}}, 
            "model": str
        },
        400: {
            "description": "Invalid type",
            "content": {"text/plain": {'example': 'Invalid type'}},
            "model": str
        },
    }
)
async def upload(request: Request, file: UploadFile, type: str = Form(), path: str = Form(), token: str = Form()):
    """
    Загружает файл в Storage (микросервис хранения, функция управляется другим микросервисов).

    type: Тип файла. Поддерживает следующие значения: img, archive. От этого зависит ключевая директория и доп. действия предпринимаемые сервером.

    path: Путь и имя файла. В формате "директории/поддиректории/имя.файла". Если под папок нет существует, то они создаются.
    """
    if not tools.check_token('upload_file', token):
        return PlainTextResponse(status_code=403, content="Access denied")
    if not tools.is_allowed_type(type):
        return PlainTextResponse(status_code=400, content="Invalid type")

    base_dir = os.path.join(MAIN_DIR, type)
    try:
        real_path = tools.safe_path(base_dir, path)
    except ValueError:
        return PlainTextResponse(status_code=403, content="Access denied")
    # Проверяем существует ли директория, если нет, то создаем
    if not os.path.exists(os.path.dirname(real_path)):
        os.makedirs(os.path.dirname(real_path))

    match type:
        case "archive":
            # Если передан просто файл, то конвертируем его в архив
            if not path.endswith(".zip"):
                # Создаем временный файл для архива
                tmp_path = tools.safe_path(base_dir, f"{path}.tmp")
                # Сохраняем файл в временный файл
                await anyio.to_thread.run_sync(tools.copy_fileobj_to_path, file.file, tmp_path)

                # Валидируем путь (расширение в конце заменяем)
                real_root, _ = os.path.splitext(real_path)
                real_path = real_root + '.zip'

                if '.' not in path and file.filename and '.' in file.filename:
                    path += '.' + file.filename.split('.')[-1]

                # Создаем архив
                await anyio.to_thread.run_sync(
                    tools.zip_single_file,
                    tmp_path,
                    real_path,
                    os.path.basename(path),
                )
                # Удаляем временный файл
                await anyio.to_thread.run_sync(os.remove, tmp_path)

                # Удаляем из начала "{MAIN_DIR}/{type}/"
                return os.path.relpath(real_path, base_dir)
            # Если передан архив, то просто сохраняем
            else:
                # Сохраняем архив
                await anyio.to_thread.run_sync(tools.copy_fileobj_to_path, file.file, real_path)
                return path
        case _:
            # Сохраняем файл
            await anyio.to_thread.run_sync(tools.copy_fileobj_to_path, file.file, real_path)
            return path

@app.delete(
    "/delete",
    status_code=200,
    response_class=PlainTextResponse,
    response_model=str,
    responses={
        200: {
            "description": "File deleted successfully", 
            "content": {"text/plain": {'example': 'File deleted'}}, 
            "model": str
        },
        400: {
            "description": "Invalid type",
            "content": {"text/plain": {'example': 'Invalid type'}},
            "model": str
        },
        404: {
            "description": "File not found on server",
            "content": {"text/plain": {'example': 'File not found'}},
            "model": str
        }
    },
)
async def delete(request: Request, type: str = Form(), path: str = Form(), token: str = Form()):
    """
    Удаляет файл из Steam Workshop (микросервис хранения, функция управляется другим микросервисов).

    К удалению разрешены только файлы.

    Если после удаления файла, папка пуста, то она тоже удаляется (так же происходит со всеми родительскими папками)
    """
    if not tools.check_token('delete_file', token):
        return PlainTextResponse(status_code=403, content="Access denied")
    if not tools.is_allowed_type(type):
        return PlainTextResponse(status_code=400, content="Invalid type")


    base_dir = os.path.join(MAIN_DIR, type)
    try:
        real_path = tools.safe_path(base_dir, path)
    except ValueError:
        return PlainTextResponse(status_code=403, content="Access denied")


    # Функция которая удаляет файл и после этого рекурсивно удаляет все родительские папки, если они пусты
    def delete_file_and_parent_folders(file_path: str, root_dir: str):
        """
        Удаляет файл и после этого рекурсивно удаляет все родительские папки, если они пусты. Рекурсия прерывается при доходе до неприкосновенной части (no_delete).

        Parameters:
            file_path (str): The path of the file to be deleted.

        Returns:
            PlainTextResponse
        """
        # Если файл не существует, то ничего не делаем
        if not os.path.isfile(file_path):
            return PlainTextResponse(status_code=404, content="File not found")
        # Удаляем файл
        os.remove(file_path)
        # Получаем путь к родительской папке файла
        folder_path = os.path.dirname(file_path)
        root_dir = os.path.abspath(root_dir)
        # Удаляем папку, если она пуста
        while (
            folder_path
            and os.path.commonpath([folder_path, root_dir]) == root_dir
            and folder_path != root_dir
        ):
            if not os.listdir(folder_path):
                # удаляем ее
                os.rmdir(folder_path)
                # получаем путь к родительской папке
                folder_path = os.path.dirname(folder_path)
            # Если в папке есть файлы,
            else:
                # то ничего не делаем
                break
        
        return PlainTextResponse(status_code=200, content="File deleted")

    return await anyio.to_thread.run_sync(delete_file_and_parent_folders, real_path, base_dir)
