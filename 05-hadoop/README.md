# Лабораторная работа 5-1. Развертывание и настройка кластера Hadoop

## Вариант 5.

## Цель работы:
получить практические навыки развертывания одноузлового
кластера Hadoop, освоить базовые операции с распределенной файловой системой
HDFS, выполнить загрузку и простейшую обработку данных, а также научиться
выгружать результаты для последующего анализа и визуализации во внешней
среде (Jupyter Notebook / Google Colab).

## Задание:

Рынок недвижимости - Средняя цена и подсчет объектов по району (HiveQL)
ссылка на датасет: https://www.kaggle.com/datasets/dansbecker/melbourne-housing-snapshot

## Ход работы



```python
# create_table_correct.py
import psycopg2
import pandas as pd
import os
 
# Настройки подключения
DB_NAME = "postgres"
DB_USER = "postgres" 
DB_PASS = "123"
DB_HOST = "localhost"
DB_PORT = "5432"
 
def safe_int(value, default=None):
    """Безопасное преобразование в int"""
    if pd.isna(value) or value == '' or value is None:
        return default
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default
 
def safe_float(value, default=None):
    """Безопасное преобразование в float"""
    if pd.isna(value) or value == '' or value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default
 
def safe_str(value, default=None):
    """Безопасное преобразование в строку"""
    if pd.isna(value) or value == '' or value is None:
        return default
    try:
        return str(value).strip()
    except:
        return default
 
try:
    # Читаем CSV
    print("📊 Чтение CSV файла...")
    df = pd.read_csv('melb_data.csv')
    print(f"Размер данных: {df.shape}")
 
    # Подключаемся к PostgreSQL
    print("🔌 Подключение к PostgreSQL...")
    conn = psycopg2.connect(
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )
    conn.autocommit = True
    cursor = conn.cursor()
    print("✅ Подключение успешно!")
 
    # Создаем таблицу
    print("🗃️ Создание таблицы...")
    cursor.execute("DROP TABLE IF EXISTS melbourne_housing")
 
    create_table_query = """
    CREATE TABLE melbourne_housing (
        id SERIAL PRIMARY KEY,
        suburb VARCHAR(100),
        address TEXT,
        rooms INTEGER,
        type VARCHAR(50),
        price DECIMAL(15,2),
        method VARCHAR(50),
        seller_g VARCHAR(100),
        date TEXT,
        distance DECIMAL(8,2),
        postcode VARCHAR(10),
        bedroom INTEGER,
        bathroom INTEGER,
        car INTEGER,
        landsize INTEGER,
        building_area DECIMAL(10,2),
        year_built INTEGER,
        council_area VARCHAR(100),
        regionname VARCHAR(100),
        property_count INTEGER
    )
    """
    cursor.execute(create_table_query)
    print("✅ Таблица создана успешно!")
 
    # Загружаем данные с МИНИМАЛЬНЫМИ проверками
    print("⬆️ Загрузка данных...")
    total_rows = len(df)
    success_count = 0
    error_count = 0
 
    for index, row in df.iterrows():
        if index % 2000 == 0:
            print(f"Обработано {index} строк...")
 
        try:
            # ОСНОВНЫЕ ДАННЫЕ - должны быть обязательно для анализа
            suburb = safe_str(row.get('Suburb'))
            price = safe_float(row.get('Price'))
 
            # Если нет suburb или price - пропускаем (это ключевые поля)
            if not suburb or price is None:
                error_count += 1
                continue
 
            # Все остальные данные могут быть NULL - это нормально!
            cursor.execute("""
                INSERT INTO melbourne_housing 
                (suburb, address, rooms, type, price, method, seller_g, date, 
                 distance, postcode, bedroom, bathroom, car, landsize, 
                 building_area, year_built, council_area, regionname, property_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                suburb,                                                   # suburb
                safe_str(row.get('Address')),                            # address
                safe_int(row.get('Rooms')),                              # rooms
                safe_str(row.get('Type')),                               # type
                price,                                                   # price
                safe_str(row.get('Method')),                             # method
                safe_str(row.get('SellerG')),                            # seller_g
                safe_str(row.get('Date')),                               # date
                safe_float(row.get('Distance')),                         # distance
                safe_str(row.get('Postcode')),                           # postcode
                safe_int(row.get('Bedroom2')),                           # bedroom
                safe_int(row.get('Bathroom')),                           # bathroom
                safe_int(row.get('Car')),                                # car
                safe_int(row.get('Landsize')),                           # landsize
                safe_float(row.get('BuildingArea')),                     # building_area
                safe_int(row.get('YearBuilt')),                          # year_built
                safe_str(row.get('CouncilArea')),                        # council_area
                safe_str(row.get('Regionname')),                         # regionname
                safe_int(row.get('Propertycount'))                       # property_count
            ))
 
            success_count += 1
 
        except Exception as e:
            error_count += 1
            if error_count <= 3:  # Покажем только первые 3 ошибки
                print(f"   Ошибка в строке {index}: {e}")
            continue
 
    print(f"\n🎉 ЗАГРУЗКА ЗАВЕРШЕНА!")
    print(f"✅ Успешно загружено: {success_count}/{total_rows} записей ({success_count/total_rows*100:.1f}%)")
    print(f"❌ Пропущено: {error_count} записей")
 
    # Проверим основные данные для задания
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(DISTINCT suburb) as unique_suburbs,
            AVG(price) as avg_price,
            MIN(price) as min_price,
            MAX(price) as max_price
        FROM melbourne_housing
    """)
    stats = cursor.fetchone()
    print(f"\n📊 Статистика для задания:")
    print(f"   Всего объектов: {stats[0]}")
    print(f"   Уникальных районов: {stats[1]}")
    print(f"   Средняя цена: ${stats[2]:,.2f}")
    print(f"   Минимальная цена: ${stats[3]:,.2f}")
    print(f"   Максимальная цена: ${stats[4]:,.2f}")
 
    conn.close()
 
except Exception as e:
    print(f"❌ Критическая ошибка: {e}")
    import traceback
    traceback.print_exc()
```

```python
# postgres_to_hdfs_correct.py
from pyspark.sql import SparkSession
import subprocess
 
# Создаем Spark-сессию
spark = SparkSession.builder \
    .appName("PostgreSQL to HDFS") \
    .config("spark.jars", "/home/hadoop/housing_project/postgresql-42.6.0.jar") \
    .getOrCreate()
 
# Параметры подключения к PostgreSQL
jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
connection_properties = {
    "user": "postgres",
    "password": "123", 
    "driver": "org.postgresql.Driver"
}
 
print("📊 Чтение данных из PostgreSQL...")
 
try:
    # Читаем данные из таблицы
    df = spark.read \
        .jdbc(url=jdbc_url, table="melbourne_housing", properties=connection_properties)
 
    print(f"✅ Прочитано {df.count()} записей из PostgreSQL")
 
    # Покажем схему данных
    print("📋 Схема данных:")
    df.printSchema()
 
    # Покажем пример данных
    print("🔍 Пример данных:")
    df.select("suburb", "price", "rooms").show(10)
 
    # Используем правильный HDFS URI
    hdfs_uri = "hdfs://localhost:9000"
    hdfs_path = f"{hdfs_uri}/user/hadoop/melbourne_housing/data.parquet"
 
    print(f"📍 HDFS путь: {hdfs_path}")
 
    # Создаем директорию в HDFS
    print("📁 Создаем директорию в HDFS...")
    subprocess.run(['hdfs', 'dfs', '-mkdir', '-p', '/user/hadoop/melbourne_housing'])
 
    # Сохраняем данные в HDFS
    print("💾 Сохраняем в HDFS...")
    df.write \
        .mode("overwrite") \
        .parquet(hdfs_path)
 
    print("✅ Данные успешно сохранены в HDFS!")
 
    # Проверим через HDFS команды
    print("🔍 Проверка через HDFS...")
    result = subprocess.run(['hdfs', 'dfs', '-ls', '/user/hadoop/melbourne_housing/data.parquet'], 
                          capture_output=True, text=True)
    if result.returncode == 0:
        print("✅ Файлы в HDFS:")
        print(result.stdout)
    else:
        print("❌ Не удалось прочитать HDFS")
 
    # Проверим через Spark
    saved_df = spark.read.parquet(hdfs_path)
    print(f"🔍 Проверка через Spark: {saved_df.count()} записей")
 
    # Посмотрим на сохраненные данные
    print("🔍 Пример сохраненных данных из HDFS:")
    saved_df.select("suburb", "price", "rooms").show(10)
 
except Exception as e:
    print(f"❌ Ошибка: {e}")
    import traceback
    traceback.print_exc()
 
finally:
    spark.stop()
    print("🎉 Готово!")
```


