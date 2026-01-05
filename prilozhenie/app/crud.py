import psycopg2
import json
import os
from datetime import datetime, date
from .database import Database
from typing import List, Dict, Any, Optional

class CRUD:
    @staticmethod
    def get_tables():
        """Получение списка всех таблиц в базе данных"""
        with Database.get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE'
                    AND table_name NOT LIKE 'pg_%'
                    AND table_name NOT LIKE 'sql_%'
                    ORDER BY table_name;
                """)
                tables = [row[0] for row in cur.fetchall()]
                return tables
    
    @staticmethod
    def get_table_columns(table_name):
        """Получение списка столбцов и их типов для указанной таблицы"""
        with Database.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                    AND table_name = %s
                    ORDER BY ordinal_position;
                """, (table_name,))
                columns = cur.fetchall()
                return [
                    {
                        "name": col[0],
                        "type": col[1],
                        "nullable": col[2] == "YES",
                        "default": col[3]
                    }
                    for col in columns
                ]
    
    @staticmethod
    def get_primary_key(table_name):
        """Получение имени первичного ключа таблицы"""
        try:
            with Database.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT column_name 
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu 
                        ON tc.constraint_name = kcu.constraint_name
                        WHERE tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY'
                    """, (table_name,))
                    
                    result = cur.fetchone()
                    return result[0] if result else 'id'
        except Exception:
            return 'id'

    @staticmethod
    def get_record_by_id(table_name, record_id):
        """Получение записи по ID"""
        try:
            with Database.get_connection() as conn:
                with conn.cursor() as cur:
                    # Получаем первичный ключ таблицы
                    cur.execute("""
                        SELECT column_name 
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu 
                        ON tc.constraint_name = kcu.constraint_name
                        WHERE tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY'
                    """, (table_name,))
                    
                    pk_result = cur.fetchone()
                    if pk_result:
                        pk_column = pk_result[0]
                    else:
                        pk_column = 'id'  # По умолчанию
                    
                    cur.execute(f"SELECT * FROM {table_name} WHERE {pk_column} = %s", (record_id,))
                    record = cur.fetchone()
                    
                    if record:
                        columns = CRUD.get_table_columns(table_name)
                        return dict(zip([col['name'] for col in columns], record))
                    else:
                        return None
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    @staticmethod
    def update_record(table_name, record_id, data):
        """Обновление записи"""
        try:
            with Database.get_connection_context() as conn:
                with conn.cursor() as cur:
                    # Получаем первичный ключ таблицы
                    cur.execute("""
                        SELECT column_name 
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu 
                        ON tc.constraint_name = kcu.constraint_name
                        WHERE tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY'
                    """, (table_name,))
                    
                    pk_result = cur.fetchone()
                    if pk_result:
                        pk_column = pk_result[0]
                    else:
                        pk_column = 'id'  # По умолчанию
                    
                    # Формируем SET часть запроса
                    set_parts = []
                    values = []
                    
                    for key, value in data.items():
                        if key != pk_column and value != "" and value is not None:  # Не обновляем первичный ключ и пустые значения
                            set_parts.append(f"{key} = %s")
                            values.append(value)
                    
                    if not set_parts:
                        return {"success": False, "error": "Нет данных для обновления"}
                    
                    values.append(record_id)
                    
                    query = f"UPDATE {table_name} SET {', '.join(set_parts)} WHERE {pk_column} = %s"
                    cur.execute(query, values)
                    conn.commit()
                    
                    return {"success": True, "message": "Запись успешно обновлена"}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    @staticmethod
    def delete_record(table_name, record_id):
        """Удаление записи"""
        try:
            with Database.get_connection_context() as conn:
                with conn.cursor() as cur:
                    # Получаем первичный ключ таблицы
                    cur.execute("""
                        SELECT column_name 
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu 
                        ON tc.constraint_name = kcu.constraint_name
                        WHERE tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY'
                    """, (table_name,))
                    
                    pk_result = cur.fetchone()
                    if pk_result:
                        pk_column = pk_result[0]
                    else:
                        pk_column = 'id'  # По умолчанию
                    
                    query = f"DELETE FROM {table_name} WHERE {pk_column} = %s"
                    cur.execute(query, (record_id,))
                    conn.commit()
                    
                    return {"success": True, "message": "Запись успешно удалена"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    @staticmethod
    def get_table_data(table_name, page=1, page_size=200):
        """Получение данных из таблицы с пагинацией"""
        offset = (page - 1) * page_size
        with Database.get_connection_context() as conn:
            with conn.cursor() as cur:
                # Получаем общее количество записей
                cur.execute(f"SELECT COUNT(*) FROM {table_name};")
                total_count = cur.fetchone()[0]
                
                # Получаем названия столбцов
                cur.execute("""
                    SELECT column_name 
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                    AND table_name = %s
                    ORDER BY ordinal_position;
                """, (table_name,))
                columns = [row[0] for row in cur.fetchall()]
                
                # Определяем столбец для сортировки
                # Ищем первичный ключ или столбец с суффиксом _id
                sort_column = "ctid"  # fallback к системному ctid
                
                # Проверяем, есть ли столбец с суффиксом _id (например, author_id, book_id)
                for col in columns:
                    if col.endswith('_id'):
                        sort_column = col
                        break
                
                # Если не нашли _id столбец, используем первый столбец
                if sort_column == "ctid" and columns:
                    sort_column = columns[0]
                
                # Получаем данные с пагинацией
                cur.execute(f"""
                    SELECT * FROM {table_name}
                    ORDER BY {sort_column}
                    LIMIT %s OFFSET %s;
                """, (page_size, offset))
                rows = cur.fetchall()
                
                # Преобразуем данные в список словарей
                data = []
                for row in rows:
                    row_dict = {}
                    for i, value in enumerate(row):
                        # Специальная обработка для datetime объектов
                        if isinstance(value, datetime):
                            row_dict[columns[i]] = value.isoformat()
                        # Специальная обработка для date объектов
                        elif isinstance(value, date):
                            row_dict[columns[i]] = value.isoformat()
                        else:
                            row_dict[columns[i]] = value
                    data.append(row_dict)
                
                return {
                    "total_count": total_count,
                    "page": page,
                    "page_size": page_size,
                    "total_pages": (total_count + page_size - 1) // page_size,
                    "data": data,
                    "columns": columns
                }
    
    @staticmethod
    def execute_sql(query, params=None):
        """Выполнение произвольного SQL-запроса"""
        with Database.get_connection() as conn:
            with conn.cursor() as cur:
                start_time = datetime.now()
                try:
                    if params:
                        cur.execute(query, params)
                    else:
                        cur.execute(query)
                    
                    # Если запрос возвращает данные
                    if cur.description:
                        rows = cur.fetchall()
                        column_names = [desc[0] for desc in cur.description]
                        data = []
                        for row in rows:
                            row_dict = {}
                            for i, value in enumerate(row):
                                if isinstance(value, datetime):
                                    row_dict[column_names[i]] = value.isoformat()
                                elif isinstance(value, date):
                                    row_dict[column_names[i]] = value.isoformat()
                                else:
                                    row_dict[column_names[i]] = value
                            data.append(row_dict)
                        
                        execution_time = (datetime.now() - start_time).total_seconds()
                        return {
                            "success": True,
                            "data": data,
                            "columns": column_names,
                            "rowcount": cur.rowcount,
                            "execution_time": execution_time
                        }
                    else:
                        # Для запросов, которые не возвращают данные (INSERT, UPDATE, DELETE)
                        conn.commit()
                        execution_time = (datetime.now() - start_time).total_seconds()
                        return {
                            "success": True,
                            "message": f"Запрос успешно выполнен. Затронуто строк: {cur.rowcount}",
                            "rowcount": cur.rowcount,
                            "execution_time": execution_time
                        }
                except Exception as e:
                    conn.rollback()
                    print(f"Ошибка выполнения SQL: {e}")
                    return {
                        "success": False,
                        "error": str(e),
                        "execution_time": (datetime.now() - start_time).total_seconds()
                    }
    
    @staticmethod
    def insert_data(table_name, data):
        """Вставка данных в таблицу"""
        with Database.get_connection() as conn:
            with conn.cursor() as cur:
                try:
                    # Фильтруем поля, которые могут быть NULL или имеют значения по умолчанию
                    columns = []
                    values = []
                    
                    for key, value in data.items():
                        # Пропускаем поля с пустыми значениями, если они могут быть NULL
                        if value == "" or value is None:
                            continue
                        columns.append(key)
                        values.append(value)
                    
                    if not columns:
                        return {"success": False, "error": "Нет данных для вставки"}
                    
                    # Используем %s для psycopg2
                    placeholders = ["%s"] * len(columns)
                    
                    query = f"""
                        INSERT INTO {table_name} ({", ".join(columns)})
                        VALUES ({", ".join(placeholders)})
                        RETURNING *;
                    """
                    
                    cur.execute(query, values)
                    result = cur.fetchone()
                    conn.commit()
                    
                    # Получаем названия столбцов
                    column_names = [desc[0] for desc in cur.description]
                    
                    # Преобразуем результат в словарь
                    return {
                        "success": True,
                        "data": dict(zip(column_names, result))
                    }
                except Exception as e:
                    conn.rollback()
                    print(f"Ошибка вставки данных: {e}")
                    return {"success": False, "error": str(e)}
    
    @staticmethod
    def update_data(table_name, data, condition):
        """Обновление данных в таблице"""
        with Database.get_connection() as conn:
            with conn.cursor() as cur:
                try:
                    # Подготавливаем SET часть запроса
                    set_parts = []
                    values = []
                    
                    for key, value in data.items():
                        if value == "" or value is None:
                            continue
                        set_parts.append(f"{key} = %s")
                        values.append(value)
                    
                    if not set_parts:
                        return {"success": False, "error": "Нет данных для обновления"}
                    
                    # Подготавливаем WHERE часть запроса
                    where_clause = []
                    for key, value in condition.items():
                        where_clause.append(f"{key} = %s")
                        values.append(value)
                    
                    if not where_clause:
                        return {"success": False, "error": "Не указаны условия для обновления"}
                    
                    # Формируем и исполняем запрос
                    query = f"""
                        UPDATE {table_name}
                        SET {", ".join(set_parts)}
                        WHERE {" AND ".join(where_clause)}
                        RETURNING *;
                    """
                    
                    cur.execute(query, values)
                    result = cur.fetchall()
                    conn.commit()
                    
                    # Получаем названия столбцов
                    column_names = [desc[0] for desc in cur.description]
                    
                    # Преобразуем результат в список словарей
                    updated_data = []
                    for row in result:
                        updated_data.append(dict(zip(column_names, row)))
                    
                    return {
                        "success": True,
                        "updated_count": cur.rowcount,
                        "data": updated_data
                    }
                except Exception as e:
                    conn.rollback()
                    print(f"Ошибка обновления данных: {e}")
                    return {"success": False, "error": str(e)}
    
    @staticmethod
    def delete_data(table_name, condition):
        """Удаление данных из таблицы"""
        with Database.get_connection() as conn:
            with conn.cursor() as cur:
                try:
                    # Проверяем наличие зависимостей, если это системная таблица
                    if table_name in ['authors', 'genres', 'publishers', 'readers']:
                        # Проверяем зависимые записи
                        dependencies = {}
                        
                        if table_name == 'authors':
                            cur.execute("""
                                SELECT COUNT(*) FROM book_authors ba
                                JOIN books b ON ba.book_id = b.book_id
                                WHERE ba.author_id = %s;
                            """, (condition.get('author_id'),))
                            dependencies['books'] = cur.fetchone()[0]
                        
                        elif table_name == 'genres':
                            cur.execute("""
                                SELECT COUNT(*) FROM book_genres bg
                                JOIN books b ON bg.book_id = b.book_id
                                WHERE bg.genre_id = %s;
                            """, (condition.get('genre_id'),))
                            dependencies['books'] = cur.fetchone()[0]
                        
                        elif table_name == 'publishers':
                            cur.execute("""
                                SELECT COUNT(*) FROM books
                                WHERE publisher_id = %s;
                            """, (condition.get('publisher_id'),))
                            dependencies['books'] = cur.fetchone()[0]
                        
                        elif table_name == 'readers':
                            cur.execute("""
                                SELECT COUNT(*) FROM book_loans
                                WHERE reader_id = %s AND is_returned = false;
                            """, (condition.get('reader_id'),))
                            dependencies['active_loans'] = cur.fetchone()[0]
                        
                        # Если есть зависимости, возвращаем ошибку
                        has_dependencies = any(count > 0 for count in dependencies.values())
                        if has_dependencies:
                            return {
                                "success": False, 
                                "error": "Невозможно удалить запись из-за наличия зависимых данных",
                                "dependencies": dependencies
                            }
                    
                    # Формируем WHERE часть запроса
                    where_clause = []
                    values = []
                    
                    for key, value in condition.items():
                        where_clause.append(f"{key} = %s")
                        values.append(value)
                    
                    if not where_clause:
                        return {"success": False, "error": "Не указаны условия для удаления"}
                    
                    # Формируем и исполняем запрос
                    query = f"""
                        DELETE FROM {table_name}
                        WHERE {" AND ".join(where_clause)}
                        RETURNING *;
                    """
                    
                    cur.execute(query, values)
                    result = cur.fetchall()
                    conn.commit()
                    
                    # Получаем названия столбцов
                    column_names = [desc[0] for desc in cur.description]
                    
                    # Преобразуем результат в список словарей
                    deleted_data = []
                    for row in result:
                        deleted_data.append(dict(zip(column_names, row)))
                    
                    return {
                        "success": True,
                        "deleted_count": cur.rowcount,
                        "data": deleted_data
                    }
                except Exception as e:
                    conn.rollback()
                    print(f"Ошибка удаления данных: {e}")
                    return {"success": False, "error": str(e)}
    
    @staticmethod
    def create_backup(backup_name=None, tables=None):
        """Создание резервной копии базы данных"""
        import subprocess
        import os
        from datetime import datetime
        
        # Создаем директорию для бэкапов, если её нет
        backup_dir = os.getenv("BACKUP_DIR", "backups")
        os.makedirs(backup_dir, exist_ok=True)
        
        # Формируем имя файла
        if not backup_name:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"library_backup_{timestamp}.backup"
        else:
            # Добавляем расширение .backup если его нет
            if not backup_name.endswith(".backup"):
                backup_name += ".backup"
        
        backup_path = os.path.join(backup_dir, backup_name)
        
        # Формируем команду pg_dump
        pg_dump_cmd = [
            "pg_dump",
            "-h", os.getenv("DB_HOST", "localhost"),
            "-p", os.getenv("DB_PORT", "5432"),
            "-U", os.getenv("DB_USER", "admin"),
            "-d", os.getenv("DB_NAME", "library_management"),
            "-F", "c",  # custom format
            "-f", backup_path
        ]
        
        # Добавляем таблицы, если указаны
        if tables:
            for table in tables:
                pg_dump_cmd.extend(["-t", table])
        
        # Устанавливаем переменную окружения для пароля
        env = os.environ.copy()
        env["PGPASSWORD"] = os.getenv("DB_PASSWORD", "123")
        
        try:
            # Выполняем команду
            result = subprocess.run(
                pg_dump_cmd,
                env=env,
                capture_output=True,
                text=True,
                check=True
            )
            
            # Проверяем размер файла
            file_size = os.path.getsize(backup_path)
            
            return {
                "success": True,
                "backup_path": backup_path,
                "file_size": file_size,
                "tables": tables if tables else "all",
                "timestamp": datetime.now().isoformat()
            }
        except subprocess.CalledProcessError as e:
            print(f"Ошибка создания бэкапа: {e.stderr}")
            return {
                "success": False,
                "error": e.stderr,
                "command": " ".join(pg_dump_cmd)
            }
        except Exception as e:
            print(f"Ошибка создания бэкапа: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    @staticmethod
    def restore_backup(backup_path):
        """Восстановление базы данных из резервной копии (безопасный режим)"""
        import subprocess
        import os
        
        # Проверяем существование файла
        if not os.path.exists(backup_path):
            return {
                "success": False,
                "error": f"Файл бэкапа не существует: {backup_path}"
            }
        
        # Устанавливаем переменную окружения для пароля
        env = os.environ.copy()
        env["PGPASSWORD"] = os.getenv("DB_PASSWORD", "123")
        
        try:
            # Очищаем схему public, чтобы избежать ошибок зависимостей при --clean
            db_host = os.getenv("DB_HOST", "localhost")
            db_port = os.getenv("DB_PORT", "5432")
            db_user = os.getenv("DB_USER", "admin")
            db_name = os.getenv("DB_NAME", "library_management")

            psql_cmd = [
                "psql",
                "-h", db_host,
                "-p", db_port,
                "-U", db_user,
                "-d", db_name,
                "-v", "ON_ERROR_STOP=1",
                "-c",
                "DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public; GRANT ALL ON SCHEMA public TO \"%s\"; GRANT ALL ON SCHEMA public TO public;" % db_user,
            ]

            psql_result = subprocess.run(
                psql_cmd,
                env=env,
                capture_output=True,
                text=True
            )

            if psql_result.returncode != 0:
                error_text = psql_result.stderr.strip() or psql_result.stdout.strip() or "Неизвестная ошибка очистки схемы"
                return {
                    "success": False,
                    "error": f"Ошибка подготовки БД (очистка схемы public): {error_text}",
                    "command": " ".join(psql_cmd)
                }

            restore_cmd = [
                "pg_restore",
                "-h", db_host,
                "-p", db_port,
                "-U", db_user,
                "-d", db_name,
                "-v",
                "--no-owner",
                "--no-privileges",
                backup_path
            ]

            result = subprocess.run(
                restore_cmd,
                env=env,
                capture_output=True,
                text=True
            )

            stderr_lines = [line.strip() for line in (result.stderr or "").split("\n") if line.strip()]
            warnings = []
            errors = []

            for line in stderr_lines:
                lower = line.lower()

                # pg_restore может ругаться на параметры, которых нет в PostgreSQL 14
                if "transaction_timeout" in lower and "unrecognized configuration parameter" in lower:
                    warnings.append(line)
                    continue
                if lower.startswith("command was:") and "transaction_timeout" in lower:
                    warnings.append(line)
                    continue

                if "warning:" in lower:
                    warnings.append(line)
                    continue

                if "error:" in lower:
                    errors.append(line)
                    continue

            if result.returncode != 0 and errors:
                error_text = "\n".join(errors)
                return {
                    "success": False,
                    "error": error_text,
                    "command": " ".join(restore_cmd),
                    "warnings": warnings
                }

            return {
                "success": True,
                "message": "База данных успешно восстановлена",
                "backup_path": backup_path,
                "timestamp": datetime.now().isoformat(),
                "warnings": warnings
            }
            
        except subprocess.CalledProcessError as e:
            print(f"Ошибка восстановления бэкапа: {e.stderr}")
            return {
                "success": False,
                "error": e.stderr,
                "command": " ".join(restore_cmd) if 'restore_cmd' in locals() else "Unknown"
            }
        except FileNotFoundError as e:
            return {
                "success": False,
                "error": f"Не найдена утилита для восстановления: {e}. Убедитесь, что установлен postgresql-client (psql/pg_restore)"
            }
        except Exception as e:
            print(f"Ошибка восстановления бэкапа: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    @staticmethod
    def archive_tables(tables, reason):
        """Архивация таблиц с сохранением данных в разных форматах"""
        import pandas as pd
        import json
        import os
        from datetime import datetime
        
        # Создаем директорию для архивов, если её нет
        archive_dir = os.getenv("ARCHIVE_DIR", "archives")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_subdir = os.path.join(archive_dir, timestamp)
        os.makedirs(archive_subdir, exist_ok=True)
        
        archive_report = {
            "timestamp": timestamp,
            "tables": [],
            "reason": reason,
            "total_rows_archived": 0
        }
        
        for table in tables:
            try:
                # Получаем данные из таблицы
                with Database.get_connection() as conn:
                    data = pd.read_sql_query(f"SELECT * FROM {table}", conn)
                
                # Сохраняем в Excel
                excel_path = os.path.join(archive_subdir, f"{table}.xlsx")
                data.to_excel(excel_path, index=False)
                
                # Сохраняем в JSON
                json_path = os.path.join(archive_subdir, f"{table}.json")
                data.to_json(json_path, orient="records", indent=2, date_format="iso")
                
                # Создаем бэкап таблицы через pg_dump
                backup_path = os.path.join(archive_subdir, f"{table}.backup")
                pg_dump_cmd = [
                    "pg_dump",
                    "-h", os.getenv("DB_HOST", "localhost"),
                    "-p", os.getenv("DB_PORT", "5432"),
                    "-U", os.getenv("DB_USER", "lib_admin"),
                    "-d", os.getenv("DB_NAME", "library_management"),
                    "-t", table,
                    "-F", "c",
                    "-f", backup_path
                ]
                
                env = os.environ.copy()
                env["PGPASSWORD"] = os.getenv("DB_PASSWORD", "securepass123")
                
                import subprocess
                subprocess.run(pg_dump_cmd, env=env, check=True)
                
                # Удаляем данные из таблицы
                with Database.get_connection() as conn:
                    with conn.cursor() as cur:
                        # Очищаем таблицу в зависимости от её типа
                        if table in ['books', 'readers', 'authors', 'genres', 'publishers']:
                            # Для основных таблиц сначала удаляем зависимые данные
                            if table == 'books':
                                cur.execute("DELETE FROM book_loans WHERE book_id IN (SELECT book_id FROM books)")
                                cur.execute("DELETE FROM book_authors")
                                cur.execute("DELETE FROM book_genres")
                            elif table == 'readers':
                                cur.execute("DELETE FROM book_loans WHERE reader_id IN (SELECT reader_id FROM readers)")
                            
                            cur.execute(f"DELETE FROM {table}")
                            rowcount = cur.rowcount
                        else:
                            cur.execute(f"DELETE FROM {table}")
                            rowcount = cur.rowcount
                        
                        conn.commit()
                
                # Добавляем информацию в отчет
                archive_report["tables"].append({
                    "name": table,
                    "rows_archived": len(data),
                    "excel_path": excel_path,
                    "json_path": json_path,
                    "backup_path": backup_path
                })
                archive_report["total_rows_archived"] += len(data)
                
            except Exception as e:
                print(f"Ошибка архивации таблицы {table}: {e}")
                archive_report["tables"].append({
                    "name": table,
                    "error": str(e)
                })
        
        # Сохраняем отчет об архивации
        report_path = os.path.join(archive_subdir, "archive_report.json")
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(archive_report, f, indent=2, ensure_ascii=False)
        
        archive_report["report_path"] = report_path
        return archive_report