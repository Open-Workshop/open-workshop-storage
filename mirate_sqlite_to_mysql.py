import sqlite3
import mysql.connector


# Подключение к SQLite базе данных
sqlite_conn = sqlite3.connect('sql/database.db')
sqlite_cursor = sqlite_conn.cursor()

# Подключение к MySQL базе данных
mysql_conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="123456"
)
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute("DROP DATABASE IF EXISTS catalog")
mysql_cursor.execute("CREATE DATABASE catalog")
mysql_conn.database = "catalog"

# Получаем список таблиц в SQLite базе данных
sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = sqlite_cursor.fetchall()

# Перебираем таблицы и переносим данные
for table in tables:
    table_name = table[0]

    print(f"Перенос данных таблицы {table_name} из SQLite в MySQL...")
    # Получаем структуру таблицы из SQLite
    sqlite_cursor.execute(f"PRAGMA table_info({table_name});")
    columns = sqlite_cursor.fetchall()

    # Получаем данные из SQLite таблицы
    sqlite_cursor.execute(f"SELECT * FROM {table_name};")
    data = sqlite_cursor.fetchall()

    # Формируем запрос для создания таблицы в MySQL
    replace_types = {
        'id': 'BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY',
        'size': 'BIGINT UNSIGNED',
        'mod_id': 'BIGINT UNSIGNED',
        'dependence': 'BIGINT UNSIGNED',
        'owner_id': 'BIGINT UNSIGNED',
        'short_description': 'TEXT',
        'description': 'TEXT'
    }
    list_params = [
        f'`{col[1]}` {replace_types.get(col[1], col[2])}'
        for col in columns
    ]
    list_params = [f'{param}({1000})' if param.endswith('VARCHAR') else param for param in list_params]
    list_columns = [f'`{col[1]}`' for col in columns]

    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(list_params)});"
    print(create_table_query)
    mysql_cursor.execute(create_table_query)

    # Вставляем данные в MySQL таблицу
    for row in data:
        placeholders = ', '.join(['%s'] * len(row))
        insert_query = f"INSERT INTO {table_name} ({', '.join(list_columns)}) VALUES ({placeholders})"
        mysql_cursor.execute(insert_query, row)

    mysql_conn.commit()

# Закрываем соединения
sqlite_conn.close()
mysql_conn.close()

print("Данные успешно перенесены из SQLite в MySQL.")
