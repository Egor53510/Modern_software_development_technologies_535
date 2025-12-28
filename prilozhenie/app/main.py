import os
from fastapi import FastAPI, Request, Form, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from datetime import datetime
import json
import logging
from contextlib import asynccontextmanager

from .database import Database
from .crud import CRUD
from .models import SQLQuery, ExportFormat, BackupRequest, ArchiveRequest

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Инициализация при запуске
    logger.info("Инициализация приложения...")
    # Убираем вызов Database.initialize(), так как его больше нет
    logger.info("Приложение успешно запущено!")
    yield
    # Очистка при завершении
    logger.info("Завершение работы приложения...")
    logger.info("Все подключения закрыты")

# Инициализация FastAPI приложения
app = FastAPI(lifespan=lifespan, title="Система управления библиотекой")

# Настройка шаблонов и статических файлов
templates = Jinja2Templates(directory="app/templates")
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Главная страница - панель управления
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Главная страница - панель управления"""
    try:
        tables = CRUD.get_tables()
        table_data = []
        for table in tables:
            result = CRUD.get_table_data(table, page=1, page_size=1)
            if result and "total_count" in result:
                columns = [col["name"] for col in CRUD.get_table_columns(table)]
                table_data.append({
                    "name": table,
                    "count": result["total_count"],
                    "columns": columns
                })
        
        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "tables": table_data
        })
    except Exception as e:
        logger.error(f"Ошибка загрузки главной страницы: {e}")
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": str(e)
        })

# CRUD операции - просмотр таблицы
@app.get("/table/{table_name}", response_class=HTMLResponse)
async def view_table(request: Request, table_name: str, page: int = 1):
    """Просмотр данных таблицы"""
    try:
        # Проверяем существование таблицы
        tables = CRUD.get_tables()
        if table_name not in tables:
            raise HTTPException(status_code=404, detail="Таблица не найдена")
        
        # Получаем данные таблицы
        result = CRUD.get_table_data(table_name, page=page, page_size=200)
        columns = CRUD.get_table_columns(table_name)
        
        return templates.TemplateResponse("table_view.html", {
            "request": request,
            "table_name": table_name,
            "columns": columns,
            "data": result["data"],
            "total_count": result["total_count"],
            "current_page": page,
            "total_pages": result["total_pages"],
            "page_size": result["page_size"],
            "all_tables": tables
        })
    except Exception as e:
        logger.error(f"Ошибка просмотра таблицы {table_name}: {e}")
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": str(e)
        })

# Форма добавления записи
@app.get("/table/{table_name}/add", response_class=HTMLResponse)
async def add_record_form(request: Request, table_name: str):
    """Форма добавления новой записи"""
    try:
        tables = CRUD.get_tables()
        if table_name not in tables:
            raise HTTPException(status_code=404, detail="Таблица не найдена")
        
        columns = CRUD.get_table_columns(table_name)
        
        return templates.TemplateResponse("add_record.html", {
            "request": request,
            "table_name": table_name,
            "columns": columns,
            "all_tables": tables
        })
    except Exception as e:
        logger.error(f"Ошибка загрузки формы добавления для таблицы {table_name}: {e}")
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": str(e)
        })

# Добавление записи
@app.post("/table/{table_name}/add", response_class=HTMLResponse)
async def add_record(request: Request, table_name: str):
    """Добавление новой записи в таблицу"""
    try:
        form_data = await request.form()
        
        # Преобразуем данные формы в словарь
        data = {}
        for key, value in form_data.items():
            if value and value.strip() != "":
                # Пытаемся преобразовать в нужный тип
                if "_id" in key or key in ["publication_year", "page_count"]:
                    try:
                        data[key] = int(value)
                    except:
                        data[key] = value
                elif key in ["price", "fine_amount"]:
                    try:
                        data[key] = float(value)
                    except:
                        data[key] = value
                elif key in ["birth_date", "due_date", "return_date", "paid_date"]:
                    if value:
                        data[key] = value
                elif key in ["is_active", "is_returned", "is_paid"]:
                    data[key] = value == "on"
                else:
                    data[key] = value
        
        # Добавляем запись
        result = CRUD.insert_data(table_name, data)
        
        if result["success"]:
            return RedirectResponse(
                url=f"/table/{table_name}?success=Запись успешно добавлена",
                status_code=status.HTTP_303_SEE_OTHER
            )
        else:
            raise HTTPException(status_code=400, detail=result["error"])
    
    except Exception as e:
        logger.error(f"Ошибка добавления записи в таблицу {table_name}: {e}")
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": str(e)
        })

# Форма обновления записи
@app.get("/table/{table_name}/update", response_class=HTMLResponse)
async def update_record_form(request: Request, table_name: str):
    """Форма обновления записей"""
    try:
        tables = CRUD.get_tables()
        if table_name not in tables:
            raise HTTPException(status_code=404, detail="Таблица не найдена")
        
        columns = CRUD.get_table_columns(table_name)
        
        return templates.TemplateResponse("update_record.html", {
            "request": request,
            "table_name": table_name,
            "columns": columns,
            "all_tables": tables
        })
    except Exception as e:
        logger.error(f"Ошибка загрузки формы обновления для таблицы {table_name}: {e}")
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": str(e)
        })

# Обновление записей
@app.post("/table/{table_name}/update", response_class=HTMLResponse)
async def update_record(request: Request, table_name: str):
    """Обновление записей в таблице"""
    try:
        form_data = await request.form()
        
        # Разделяем данные на условия WHERE и поля для обновления
        update_data = {}
        where_condition = {}
        
        for key, value in form_data.items():
            if key.startswith("where_") and value and value.strip() != "":
                field_name = key[6:]  # Убираем префикс "where_"
                where_condition[field_name] = value
            elif value and value.strip() != "":
                # Пытаемся преобразовать в нужный тип
                if "_id" in key or key in ["publication_year", "page_count"]:
                    try:
                        update_data[key] = int(value)
                    except:
                        update_data[key] = value
                elif key in ["price", "fine_amount"]:
                    try:
                        update_data[key] = float(value)
                    except:
                        update_data[key] = value
                elif key in ["birth_date", "due_date", "return_date", "paid_date"]:
                    if value:
                        update_data[key] = value
                elif key in ["is_active", "is_returned", "is_paid"]:
                    update_data[key] = value == "on"
                else:
                    update_data[key] = value
        
        if not where_condition:
            raise HTTPException(status_code=400, detail="Не указаны условия для обновления")
        
        # Обновляем записи
        result = CRUD.update_data(table_name, update_data, where_condition)
        
        if result["success"]:
            message = f"Успешно обновлено записей: {result['updated_count']}"
            return RedirectResponse(
                url=f"/table/{table_name}?success={message}",
                status_code=status.HTTP_303_SEE_OTHER
            )
        else:
            raise HTTPException(status_code=400, detail=result["error"])
    
    except Exception as e:
        logger.error(f"Ошибка обновления записей в таблице {table_name}: {e}")
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": str(e)
        })

# Форма удаления записей
@app.get("/table/{table_name}/delete", response_class=HTMLResponse)
async def delete_record_form(request: Request, table_name: str):
    """Форма удаления записей"""
    try:
        tables = CRUD.get_tables()
        if table_name not in tables:
            raise HTTPException(status_code=404, detail="Таблица не найдена")
        
        columns = CRUD.get_table_columns(table_name)
        
        return templates.TemplateResponse("delete_record.html", {
            "request": request,
            "table_name": table_name,
            "columns": columns,
            "all_tables": tables
        })
    except Exception as e:
        logger.error(f"Ошибка загрузки формы удаления для таблицы {table_name}: {e}")
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": str(e)
        })

# Удаление записей
@app.post("/table/{table_name}/delete", response_class=HTMLResponse)
async def delete_record(request: Request, table_name: str):
    """Удаление записей из таблицы"""
    try:
        form_data = await request.form()
        
        # Формируем условие WHERE
        where_condition = {}
        for key, value in form_data.items():
            if value and value.strip() != "":
                # Пытаемся преобразовать в нужный тип
                if "_id" in key or key in ["publication_year", "page_count"]:
                    try:
                        where_condition[key] = int(value)
                    except:
                        where_condition[key] = value
                else:
                    where_condition[key] = value
        
        if not where_condition:
            raise HTTPException(status_code=400, detail="Не указаны условия для удаления")
        
        # Удаляем записи
        result = CRUD.delete_data(table_name, where_condition)
        
        if result["success"]:
            message = f"Успешно удалено записей: {result['deleted_count']}"
            return RedirectResponse(
                url=f"/table/{table_name}?success={message}",
                status_code=status.HTTP_303_SEE_OTHER
            )
        else:
            error_msg = result["error"]
            # Если есть информация о зависимостях, добавляем её в сообщение
            if "dependencies" in result:
                deps_info = ", ".join([f"{k}: {v}" for k, v in result["dependencies"].items() if v > 0])
                error_msg += f". Зависимые данные: {deps_info}"
            
            return templates.TemplateResponse("delete_record.html", {
                "request": request,
                "table_name": table_name,
                "columns": CRUD.get_table_columns(table_name),
                "all_tables": CRUD.get_tables(),
                "error": error_msg,
                "where_condition": where_condition
            })
    
    except Exception as e:
        logger.error(f"Ошибка удаления записей из таблицы {table_name}: {e}")
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": str(e)
        })

# API эндпоинт для получения таблиц
@app.get("/api/tables", response_class=JSONResponse)
async def get_tables_api():
    """API для получения списка таблиц"""
    try:
        tables = CRUD.get_tables()
        return {"tables": tables}
    except Exception as e:
        logger.error(f"Ошибка получения таблиц: {e}")
        return {"error": str(e)}

# API эндпоинт для получения бэкапов
@app.get("/api/backups", response_class=JSONResponse)
async def get_backups_api():
    """API для получения списка бэкапов"""
    try:
        backup_dir = os.getenv("BACKUP_DIR", "backups")
        backup_files = []
        if os.path.exists(backup_dir):
            for file in os.listdir(backup_dir):
                if file.endswith(".backup"):
                    file_path = os.path.join(backup_dir, file)
                    backup_files.append({
                        "name": file,
                        "size": os.path.getsize(file_path),
                        "date": datetime.fromtimestamp(os.path.getmtime(file_path)).strftime("%Y-%m-%d %H:%M:%S")
                    })
        return sorted(backup_files, key=lambda x: x["date"], reverse=True)[:10]
    except Exception as e:
        logger.error(f"Ошибка получения бэкапов: {e}")
        return {"error": str(e)}

# API эндпоинт для получения архивов
@app.get("/api/archives", response_class=JSONResponse)
async def get_archives_api():
    """API для получения списка архивов"""
    try:
        archive_dir = os.getenv("ARCHIVE_DIR", "archives")
        archive_folders = []
        if os.path.exists(archive_dir):
            for folder in sorted(os.listdir(archive_dir), reverse=True):
                folder_path = os.path.join(archive_dir, folder)
                if os.path.isdir(folder_path):
                    archive_folders.append({
                        "name": folder,
                        "date": datetime.strptime(folder, "%Y%m%d_%H%M%S").strftime("%Y-%m-%d %H:%M:%S") if len(folder) == 15 else folder,
                        "path": folder_path
                    })
        return archive_folders[:10]
    except Exception as e:
        logger.error(f"Ошибка получения архивов: {e}")
        return {"error": str(e)}

# Удаление бэкапа
@app.post("/delete-backup", response_class=JSONResponse)
async def delete_backup(request: dict):
    """Удаление бэкапа"""
    try:
        backup_name = request.get("backup_name")
        if not backup_name:
            return {"success": False, "error": "Имя бэкапа не указано"}
        
        backup_dir = os.getenv("BACKUP_DIR", "backups")
        backup_path = os.path.join(backup_dir, backup_name)
        
        if not os.path.exists(backup_path):
            return {"success": False, "error": "Файл бэкапа не найден"}
        
        os.remove(backup_path)
        return {"success": True, "message": "Бэкап успешно удален"}
    except Exception as e:
        logger.error(f"Ошибка удаления бэкапа: {e}")
        return {"success": False, "error": str(e)}

# Удаление архива
@app.post("/delete-archive", response_class=JSONResponse)
async def delete_archive(request: dict):
    """Удаление архива"""
    try:
        archive_name = request.get("archive_name")
        if not archive_name:
            return {"success": False, "error": "Имя архива не указано"}
        
        archive_dir = os.getenv("ARCHIVE_DIR", "archives")
        archive_path = os.path.join(archive_dir, archive_name)
        
        if not os.path.exists(archive_path):
            return {"success": False, "error": "Архив не найден"}
        
        import shutil
        shutil.rmtree(archive_path)
        return {"success": True, "message": "Архив успешно удален"}
    except Exception as e:
        logger.error(f"Ошибка удаления архива: {e}")
        return {"success": False, "error": str(e)}

# API эндпоинт для получения колонок таблицы
@app.get("/table/{table_name}/columns", response_class=JSONResponse)
async def get_table_columns_api(table_name: str):
    """API для получения колонок таблицы"""
    try:
        columns = CRUD.get_table_columns(table_name)
        return columns
    except Exception as e:
        logger.error(f"Ошибка получения колонок таблицы {table_name}: {e}")
        return {"error": str(e)}

# Конструктор SQL запросов
@app.get("/sql-builder", response_class=HTMLResponse)
async def sql_builder(request: Request):
    """Страница конструктора SQL запросов"""
    try:
        tables = CRUD.get_tables()
        return templates.TemplateResponse("sql_builder.html", {
            "request": request,
            "tables": tables
        })
    except Exception as e:
        logger.error(f"Ошибка загрузки конструктора SQL: {e}")
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": str(e)
        })

# Выполнение SQL запроса
@app.post("/execute-sql", response_class=JSONResponse)
async def execute_sql(query: SQLQuery):
    """Выполнение SQL запроса"""
    try:
        result = CRUD.execute_sql(query.query, query.params)
        return result
    except Exception as e:
        logger.error(f"Ошибка выполнения SQL запроса: {e}")
        return {
            "success": False,
            "error": str(e)
        }

# Экспорт результатов SQL запроса

@app.post("/export-sql-results", response_class=FileResponse)
async def export_sql_results(
    request: Request,
    format: str = Form(...),
    query: str = Form(...)
):
    """Экспорт результатов SQL запроса в файл"""
    # Валидация формата
    if format not in ["excel", "json", "csv"]:
        raise HTTPException(status_code=400, detail="Недопустимый формат экспорта")
    
    try:
        import tempfile
        import os
        from datetime import datetime
        
        # Выполняем запрос
        result = CRUD.execute_sql(query)
        
        if not result["success"] or not result.get("data"):
            raise HTTPException(status_code=400, detail="Нет данных для экспорта")
        
        # Создаем временный файл
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"sql_export_{timestamp}"
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{format}") as temp_file:
            if format == "excel":
                # Экспорт в Excel
                import pandas as pd
                df = pd.DataFrame(result["data"])
                df.to_excel(temp_file.name, index=False)
                media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                filename += ".xlsx"
            elif format == "json":
                # Экспорт в JSON
                with open(temp_file.name, "w", encoding="utf-8") as f:
                    json.dump(result["data"], f, indent=2, ensure_ascii=False)
                media_type = "application/json"
                filename += ".json"
            elif format == "csv":
                # Экспорт в CSV
                import pandas as pd
                df = pd.DataFrame(result["data"])
                df.to_csv(temp_file.name, index=False)
                media_type = "text/csv"
                filename += ".csv"
        
        # Возвращаем файл
        return FileResponse(
            temp_file.name,
            media_type=media_type,
            filename=filename,
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    
    except Exception as e:
        logger.error(f"Ошибка экспорта результатов SQL: {e}")
        if 'temp_file' in locals() and os.path.exists(temp_file.name):
            os.unlink(temp_file.name)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # Удаляем временный файл после отправки
        if 'temp_file' in locals() and os.path.exists(temp_file.name):
            os.unlink(temp_file.name)

# Сервисные функции
@app.get("/admin", response_class=HTMLResponse)
async def admin_panel(request: Request):
    """Административная панель"""
    try:
        # Получаем информацию о бэкапах
        backup_dir = os.getenv("BACKUP_DIR", "backups")
        backup_files = []
        if os.path.exists(backup_dir):
            for file in os.listdir(backup_dir):
                if file.endswith(".backup"):
                    file_path = os.path.join(backup_dir, file)
                    backup_files.append({
                        "name": file,
                        "size": os.path.getsize(file_path),
                        "date": datetime.fromtimestamp(os.path.getmtime(file_path)).strftime("%Y-%m-%d %H:%M:%S")
                    })
        
        # Получаем информацию об архивах
        archive_dir = os.getenv("ARCHIVE_DIR", "archives")
        archive_folders = []
        if os.path.exists(archive_dir):
            for folder in sorted(os.listdir(archive_dir), reverse=True):
                folder_path = os.path.join(archive_dir, folder)
                if os.path.isdir(folder_path):
                    archive_folders.append({
                        "name": folder,
                        "date": datetime.strptime(folder, "%Y%m%d_%H%M%S").strftime("%Y-%m-%d %H:%M:%S") if len(folder) == 15 else folder,
                        "path": folder_path
                    })
        
        return templates.TemplateResponse("admin_panel.html", {
            "request": request,
            "backup_files": sorted(backup_files, key=lambda x: x["date"], reverse=True)[:10],
            "archive_folders": archive_folders[:10],
            "db_name": os.getenv("DB_NAME", "library_management")
        })
    except Exception as e:
        logger.error(f"Ошибка загрузки административной панели: {e}")
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": str(e)
        })

# Создание бэкапа
@app.post("/create-backup", response_class=JSONResponse)
async def create_backup(request: BackupRequest):
    """Создание резервной копии базы данных"""
    try:
        result = CRUD.create_backup(
            backup_name=request.backup_name,
            tables=request.tables
        )
        return result
    except Exception as e:
        logger.error(f"Ошибка создания бэкапа: {e}")
        return {
            "success": False,
            "error": str(e)
        }

# Восстановление из бэкапа
@app.post("/restore-backup", response_class=JSONResponse)
async def restore_backup(backup_path: str):
    """Восстановление базы данных из резервной копии"""
    try:
        # Проверяем, что путь находится внутри директории бэкапов
        backup_dir = os.getenv("BACKUP_DIR", "backups")
        full_path = os.path.join(backup_dir, backup_path)
        
        if not os.path.exists(full_path):
            return {
                "success": False,
                "error": f"Файл бэкапа не найден: {backup_path}"
            }
        
        result = CRUD.restore_backup(full_path)
        return result
    except Exception as e:
        logger.error(f"Ошибка восстановления бэкапа: {e}")
        return {
            "success": False,
            "error": str(e)
        }

# Архивация таблиц
@app.post("/archive-tables", response_class=JSONResponse)
async def archive_tables(request: ArchiveRequest):
    """Архивация таблиц"""
    try:
        result = CRUD.archive_tables(request.tables, request.reason)
        return result
    except Exception as e:
        logger.error(f"Ошибка архивации таблиц: {e}")
        return {
            "success": False,
            "error": str(e)
        }

# Экспорт таблиц в файл
@app.post("/export-tables", response_class=FileResponse)
async def export_tables(
    request: Request,
    format: str = Form(...),
    tables: list = Form(...)
):
    """Экспорт таблиц в файл"""
    # Валидация формата
    if format not in ["excel", "json"]:
        raise HTTPException(status_code=400, detail="Недопустимый формат экспорта")
    
    try:
        import tempfile
        import os
        from datetime import datetime
        
        # Создаем временный файл
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"tables_export_{timestamp}"
        
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            if format == "excel":
                # Экспорт в Excel с несколькими листами
                import pandas as pd
                from openpyxl import Workbook
                
                excel_path = temp_file.name + ".xlsx"
                with pd.ExcelWriter(excel_path) as writer:
                    for table in tables:
                        try:
                            with Database.get_connection() as conn:
                                df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
                                # Ограничиваем длину имени листа до 31 символа
                                sheet_name = table[:31]
                                df.to_excel(writer, sheet_name=sheet_name, index=False)
                        except Exception as e:
                            logger.error(f"Ошибка экспорта таблицы {table}: {e}")
                            continue
                
                media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                filename += ".xlsx"
                temp_file_path = excel_path
            elif format == "json":
                # Экспорт в JSON
                export_data = {}
                for table in tables:
                    try:
                        result = CRUD.get_table_data(table, page=1, page_size=10000)
                        export_data[table] = result["data"]
                    except Exception as e:
                        logger.error(f"Ошибка экспорта таблицы {table}: {e}")
                        continue
                
                json_path = temp_file.name + ".json"
                with open(json_path, "w", encoding="utf-8") as f:
                    json.dump(export_data, f, indent=2, ensure_ascii=False, default=str)
                
                media_type = "application/json"
                filename += ".json"
                temp_file_path = json_path
        
        # Возвращаем файл
        response = FileResponse(
            temp_file_path,
            media_type=media_type,
            filename=filename,
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
        # Планируем удаление файла после отправки
        def cleanup():
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
        
        # В FastAPI нет встроенного способа выполнить код после отправки ответа,
        # поэтому файл будет удален при следующей очистке или перезапуске
        return response
    
    except Exception as e:
        logger.error(f"Ошибка экспорта таблиц: {e}")
        # Удаляем временный файл в случае ошибки
        if 'temp_file_path' in locals() and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
        raise HTTPException(status_code=500, detail=str(e))

# Обработчик ошибок
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return templates.TemplateResponse("error.html", {
        "request": request,
        "error": str(exc.detail),
        "status_code": exc.status_code
    })

@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    logger.error(f"Необработанное исключение: {exc}")
    return templates.TemplateResponse("error.html", {
        "request": request,
        "error": "Произошла внутренняя ошибка сервера",
        "status_code": 500
    })