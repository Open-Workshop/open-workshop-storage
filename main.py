import os
import shutil
from zipfile import ZipFile, ZIP_LZMA
from fastapi import FastAPI, Request, UploadFile, Form
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse


MAIN_DIR = 'storage'


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
    "/download/{type}/{filename:path}",
    status_code=200,
    response_class=FileResponse,
    responses={
        404: {
            "description": "File not found on server",
            "content": {"text/plain": {'example': 'File not found'}}
        },
        200: {
            "description": "File send successfully",
            "content": {"application/octet-stream": {}},
        }
    },
)
async def download(request: Request, type: str, filename: str):
    """
    Возвращает запрашиваемый файл, если он существует.
    """
    path = f"{MAIN_DIR}/{type}/{filename.replace('%2', '/')}"
    
    if os.path.exists(path):
        return FileResponse(path)
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
    }
)
async def upload(request: Request, token: str, file: UploadFile, type: str = Form(), filename: str = Form()):
    """
    Загружает файл в Steam Workshop (микросервис хранения, функция управляется другим микросервисов).

    type: Тип файла. Поддерживает следующие значения: img, archive. От этого зависит ключевая директория и доп. действия предпринимаемые сервером.

    filename: Имя файла. В формате "директории/поддиректории/имя.файла". Если под папок нет существует, то они создаются.
    """
    path = f"{MAIN_DIR}/{type}/{filename}"
    # Удаляем из пути файл
    filename = filename.split('/')[-1]
    # Проверяем существует ли директория, если нет, то создаем
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))

    if type == "archive":
        # Если передан просто файл, то конвертируем его в архив
        if not filename.endswith(".zip"):
            # Создаем временный файл для архива
            tmp_path = f"{MAIN_DIR}/{type}/{filename}.tmp"
            # Сохраняем файл в временный файл
            with open(tmp_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
            
            # Валидируем путь (расширение в конце заменяем)
            if '.' in path:
                # Удалем все что после точки
                path = path[:path.rindex('.')]
            path += '.zip'

            if '.' not in filename:
                filename += '.'+file.filename.split('.')[-1]

            # Создаем архив
            with ZipFile(path, "w", compression=ZIP_LZMA, compresslevel=9) as zipped:
                zipped.write(tmp_path, filename.split('/')[-1])
            # Удаляем временный файл
            os.remove(tmp_path)

            # Удаляем из начала "{MAIN_DIR}/{type}/"
            path = path.replace(f"{MAIN_DIR}/{type}/", "")
            return path
        # Если передан архив, то просто сохраняем
        else:
            # Сохраняем архив
            with open(path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
            return filename
    else:
        # Сохраняем файл
        with open(path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        return filename

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
        404: {
            "description": "File not found on server",
            "content": {"text/plain": {'example': 'File not found'}},
            "model": str
        }
    },
)
async def delete(request: Request, token: str, type: str = Form(), filename: str = Form()):
    """
    Удаляет файл из Steam Workshop (микросервис хранения, функция управляется другим микросервисов).

    К удалению разрешены только файлы.

    Если после удаления файла, папка пуста, то она тоже удаляется (так же происходит со всеми родительскими папками)
    """
    # Функция которая удаляет файл и после этого рекурсивно удаляет все родительские папки, если они пусты
    def delete_file_and_parent_folders(file_path: str):
        """
        Удаляет файл и после этого рекурсивно удаляет все родительские папки, если они пусты. Рекурсия прерывается при доходе до неприкосновенной части (no_delete).

        Parameters:
            file_path (str): The path of the file to be deleted.

        Returns:
            None
        """
        # Если файл не существует, то ничего не делаем
        if not os.path.isfile(file_path):
            return JSONResponse(status_code=404, content="File not found")
        # Удаляем файл
        os.remove(file_path)
        # Получаем путь к родительской папке файла
        folder_path = os.path.dirname(file_path)
        # Удаляем папку, если она пуста
        while folder_path != "":
            if not os.listdir(folder_path):
                # удаляем ее
                os.rmdir(folder_path)
                # получаем путь к родительской папке
                folder_path = os.path.dirname(folder_path)
            # Если в папке есть файлы,
            else:
                # то ничего не делаем
                break
        
        return JSONResponse(status_code=200, content="File deleted")

    return delete_file_and_parent_folders(f"{MAIN_DIR}/{type}/{filename}")
