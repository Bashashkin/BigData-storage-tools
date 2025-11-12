# Лабораторная работа 4-1. Сравнение подходов хранения больших данных

## Вариант 5.

## Цель работы: 
Сравнить производительность и эффективность различных
подходов к хранению и обработке больших данных на примере реляционной базы
данных PostgreSQL и документо-ориентированной базы данных MongoDB.

## Задание
### PostgreSQL
CRM-система. Создать таблицу customers с полем status. Выполнить операцию UPDATE, изменяющую статус для 10% случайно выбранных клиентов. Измерить время.

### MongoDB
CRM-система. Создать коллекцию customers с полем status. Выполнить операцию update_many, изменяющую статус для 10% клиентов. Измерить время.\

### Анализ в Jupyter Notebook
Сравнить производительность массовых обновлений данных. Проанализировать, как механизм хранения данных влияет на скорость операций UPDATE.

## Решение
1. Импорт библиотек
```python
import psycopg2
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import time
import random
from datetime import datetime, timedelta
```
2. Генерация данных
```python
# Генерация тестовых данных
def generate_customers(num_records=10000):
    customers = []
    statuses = ['active', 'inactive', 'pending', 'blocked']
    
    for i in range(1, num_records + 1):
        customer = {
            'customer_id': i,
            'name': f'Customer_{i}',
            'email': f'customer{i}@example.com',
            'status': random.choice(statuses),
            'registration_date': (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d'),
            'total_orders': random.randint(1, 100),
            'total_spent': round(random.uniform(100, 10000), 2)
        }
        customers.append(customer)
    
    return customers

# Генерируем 10000 записей
customers_data = generate_customers(10000)
print(f"Сгенерировано {len(customers_data)} записей клиентов")
```
3. Подключение к PostgreSQL
```python
# PostgreSQL операции
class PostgresCRUD:
    def __init__(self):
        try:
            self.connection = psycopg2.connect(
                host="postgresql",
                port="5432",
                database="studpg",
                user="student",
                password="Stud2024!!!"
            )
            self.cursor = self.connection.cursor()
            print("✅ Успешное подключение к PostgreSQL")
        except Exception as e:
            print(f"❌ Ошибка подключения к PostgreSQL: {e}")
```
4. Работа в PostgreSQL 
```python
# PostgreSQL операции с JSON
class PostgresCRUD:
    def __init__(self):
        try:
            self.connection = psycopg2.connect(
                host="postgresql",
                port="5432",
                database="studpg",
                user="pguser",
                password="pgpass"
            )
            self.cursor = self.connection.cursor()
            print("✅ Успешное подключение к PostgreSQL")
        except Exception as e:
            print(f"❌ Ошибка подключения к PostgreSQL: {e}")
 
    def create_table(self):
        create_table_query = """
        DROP TABLE IF EXISTS customers;
        CREATE TABLE customers (
            customer_id SERIAL PRIMARY KEY,
            customer_data JSONB NOT NULL,
            status VARCHAR(20) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
 
        -- Создаем индекс для JSON поля для ускорения поиска
        CREATE INDEX idx_customer_data ON customers USING GIN (customer_data);
        CREATE INDEX idx_status ON customers (status);
        """
        self.cursor.execute(create_table_query)
        self.connection.commit()
        print("✅ Таблица 'customers' с JSON полем создана в PostgreSQL")
 
    def insert_data_json(self, customers):
        insert_query = """
        INSERT INTO customers (customer_data, status)
        VALUES (%s::jsonb, %s)
        """
 
        success_count = 0
        for customer in customers:
            try:
                # Создаем JSON объект из данных клиента
                customer_json = {
                    "name": customer['name'],
                    "email": customer['email'],
                    "registration_date": customer['registration_date'],
                    "total_orders": customer['total_orders'],
                    "total_spent": customer['total_spent'],
                    "original_id": customer['customer_id']
                }
 
                self.cursor.execute(insert_query, (
                    json.dumps(customer_json),  # Преобразуем в JSON строку
                    customer['status']
                ))
                success_count += 1
            except Exception as e:
                print(f"Ошибка вставки клиента {customer['customer_id']}: {e}")
                continue
 
        self.connection.commit()
        print(f"✅ Вставлено {success_count} JSON записей в PostgreSQL")
 
    def update_random_10_percent_json(self):
        # Получаем общее количество записей
        self.cursor.execute("SELECT COUNT(*) FROM customers")
        total_count = self.cursor.fetchone()[0]
        update_count = int(total_count * 0.1)
 
        print(f"📊 Обновление {update_count} случайных записей из {total_count}")
 
        # Выбираем случайные 10% ID через JSON данные
        self.cursor.execute(f"""
        SELECT customer_id FROM customers 
        ORDER BY RANDOM() 
        LIMIT {update_count}
        """)
        random_ids = [row[0] for row in self.cursor.fetchall()]
 
        # Измеряем время выполнения UPDATE
        start_time = time.time()
 
        # Обновляем статус в основном поле и внутри JSON
        id_placeholders = ','.join(['%s'] * len(random_ids))
        update_query = f"""
        UPDATE customers 
        SET status = 'updated_status',
            customer_data = jsonb_set(
                customer_data, 
                '{{status}}', 
                '"updated_status"'
            )
        WHERE customer_id IN ({id_placeholders})
        """
 
        self.cursor.execute(update_query, random_ids)
        self.connection.commit()
 
        end_time = time.time()
        execution_time = end_time - start_time
 
        print(f"⏱️ PostgreSQL (JSON): Обновлено {len(random_ids)} записей за {execution_time:.4f} секунд")
        return execution_time
 
    def demonstrate_json_queries(self):
        """Демонстрация работы с JSON данными"""
        print("\n🔍 ДЕМОНСТРАЦИЯ JSON ЗАПРОСОВ:")
 
        # 1. Поиск по полю в JSON
        self.cursor.execute("""
        SELECT COUNT(*) FROM customers 
        WHERE customer_data->>'email' LIKE '%@example.com%'
        """)
        email_count = self.cursor.fetchone()[0]
        print(f"• Клиентов с email @example.com: {email_count}")
 
        # 2. Агрегация по числовому полю в JSON
        self.cursor.execute("""
        SELECT AVG((customer_data->>'total_orders')::numeric) 
        FROM customers
        """)
        avg_orders = self.cursor.fetchone()[0]
        print(f"• Среднее количество заказов: {float(avg_orders):.1f}")
 
        # 3. Поиск по диапазону в JSON
        self.cursor.execute("""
        SELECT COUNT(*) FROM customers 
        WHERE (customer_data->>'total_spent')::numeric > 5000
        """)
        big_spenders = self.cursor.fetchone()[0]
        print(f"• Клиентов с тратами > 5000: {big_spenders}")
 
    def close(self):
        self.cursor.close()
        self.connection.close()
 
# Обновляем функцию генерации данных для JSON
def generate_customers_json(num_records=10000):
    customers = []
    statuses = ['active', 'inactive', 'pending', 'blocked']
 
    for i in range(1, num_records + 1):
        customer = {
            'customer_id': i,
            'name': f'Customer_{i}',
            'email': f'customer{i}@example.com',
            'status': random.choice(statuses),
            'registration_date': (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d'),
            'total_orders': random.randint(1, 100),
            'total_spent': round(random.uniform(100, 10000), 2)
        }
        customers.append(customer)
 
    return customers
```
```python
# Тестируем PostgreSQL с JSON
print("=== ТЕСТИРОВАНИЕ POSTGRESQL С JSON ===")
customers_data = generate_customers_json(10000)
 
pg_db = PostgresCRUD()
pg_db.create_table()
pg_db.insert_data_json(customers_data)
pg_time = pg_db.update_random_10_percent_json()
pg_db.demonstrate_json_queries()  # Покажем возможности JSON запросов
pg_db.close()
```
5. Подключение к MongoDB
```python
# MongoDB операции
class MongoCRUD:
    def __init__(self):
        try:
            # Используем имя контейнера вместо localhost
            self.client = MongoClient('mongodb://mongouser:mongopass@mongodb:27017/')
            self.db = self.client['studmongo']
            self.collection = self.db['customers']
            print("✅ Успешное подключение к MongoDB")
        except Exception as e:
            print(f"❌ Ошибка подключения к MongoDB: {e}")
            # Пробуем альтернативные варианты
            try:
                self.client = MongoClient('mongodb://mongouser:mongopass@localhost:27017/',
                                         serverSelectionTimeoutMS=5000)
                self.db = self.client['studmongo']
                self.collection = self.db['customers']
                print("✅ Успешное подключение к MongoDB через localhost")
            except Exception as e2:
                print(f"❌ Ошибка подключения с localhost: {e2}")
```
6. Работа в MongoDB
```python
    def insert_data(self, customers):
        try:
            # Очищаем коллекцию перед вставкой новых данных
            self.collection.delete_many({})
            
            # Вставляем данные
            result = self.collection.insert_many(customers)
            print(f"✅ Вставлено {len(result.inserted_ids)} записей в MongoDB")
        except Exception as e:
            print(f"❌ Ошибка при вставке данных: {e}")
    
    def update_random_10_percent(self):
        try:
            # Получаем общее количество документов
            total_count = self.collection.count_documents({})
            update_count = int(total_count * 0.1)
            
            print(f"📊 Обновление {update_count} случайных документов из {total_count}")
            
            # Выбираем случайные 10% документов
            pipeline = [
                {'$sample': {'size': update_count}},
                {'$project': {'_id': 1}}
            ]
            
            random_docs = list(self.collection.aggregate(pipeline))
            random_ids = [doc['_id'] for doc in random_docs]
            
            # Измеряем время выполнения update_many
            start_time = time.time()
            
            result = self.collection.update_many(
                {'_id': {'$in': random_ids}},
                {'$set': {'status': 'updated_status'}}
            )
            
            end_time = time.time()
            execution_time = end_time - start_time
            
            print(f"⏱️ MongoDB: Обновлено {result.modified_count} записей за {execution_time:.4f} секунд")
            return execution_time
        except Exception as e:
            print(f"❌ Ошибка при обновлении данных: {e}")
            return 0
    
    def close(self):
        self.client.close()
```
```python
# Тестируем MongoDB
print("\n=== ТЕСТИРОВАНИЕ MONGODB ===")
mongo_db = MongoCRUD()
mongo_db.insert_data(customers_data)
mongo_time = mongo_db.update_random_10_percent()
mongo_db.close()
```
7. Анализ
```python
# Тестирование на разных объемах данных
def test_different_volumes():
    volumes = [1000, 5000, 10000, 50000]  # Разные объемы данных
    results = []
 
    for volume in volumes:
        print(f"\n🔬 ТЕСТИРОВАНИЕ НА {volume:,} ЗАПИСЯХ")
        print("=" * 50)
 
        # Генерируем данные
        test_data = generate_customers_json(volume)
 
        # PostgreSQL тест
        pg_db = PostgresCRUD()
        pg_db.create_table()
        pg_db.insert_data_json(test_data)
        pg_time = pg_db.update_random_10_percent_json()
        pg_db.close()
 
        # MongoDB тест  
        mongo_db = MongoCRUD()
        mongo_db.insert_data(test_data)
        mongo_time = mongo_db.update_random_10_percent()
        mongo_db.close()
 
        results.append({
            'Volume': volume,
            'PostgreSQL_Time': pg_time,
            'MongoDB_Time': mongo_time,
            'PostgreSQL_Wins': pg_time < mongo_time
        })
 
        print(f"📊 РЕЗУЛЬТАТ: PostgreSQL {'быстрее' if pg_time < mongo_time else 'медленнее'} "
              f"({pg_time:.4f}s vs {mongo_time:.4f}s)")
 
    return pd.DataFrame(results)
 
# Запускаем тестирование на разных объемах
print("🎯 ТЕСТИРОВАНИЕ ВЛИЯНИЯ ОБЪЕМА ДАННЫХ НА ПРОИЗВОДИТЕЛЬНОСТЬ")
volume_results = test_different_volumes()
 
# Визуализация результатов
plt.figure(figsize=(12, 6))
 
plt.subplot(1, 2, 1)
plt.plot(volume_results['Volume'], volume_results['PostgreSQL_Time'], 
         marker='o', linewidth=2, label='PostgreSQL', color='blue')
plt.plot(volume_results['Volume'], volume_results['MongoDB_Time'], 
         marker='s', linewidth=2, label='MongoDB', color='green')
plt.xlabel('Количество записей')
plt.ylabel('Время обновления (секунды)')
plt.title('Влияние объема данных на производительность')
plt.legend()
plt.grid(True, alpha=0.3)
 
plt.subplot(1, 2, 2)
# Относительная производительность
relative_speed = volume_results['PostgreSQL_Time'] / volume_results['MongoDB_Time']
plt.plot(volume_results['Volume'], relative_speed, 
         marker='o', linewidth=2, color='red')
plt.axhline(y=1, color='gray', linestyle='--', alpha=0.5)
plt.xlabel('Количество записей')
plt.ylabel('PostgreSQL время / MongoDB время')
plt.title('Относительная производительность\n(<1 - PostgreSQL быстрее)')
plt.grid(True, alpha=0.3)
 
plt.tight_layout()
plt.show()
 
print("\n📈 АНАЛИЗ РЕЗУЛЬТАТОВ:")
for _, row in volume_results.iterrows():
    faster = "PostgreSQL" if row['PostgreSQL_Wins'] else "MongoDB"
    ratio = row['PostgreSQL_Time'] / row['MongoDB_Time']
    print(f"• {row['Volume']:6,} записей: {faster} быстрее в {ratio:.2f} раз")
```

## Выводы
<img width="907" height="433" alt="image" src="https://github.com/user-attachments/assets/36b44ba6-e636-48bf-9d4c-6b1521bc7a9c" />

*  5,000 записей: PostgreSQL быстрее в 0.88 раз
* 10,000 записей: MongoDB быстрее в 1.14 раз
* 50,000 записей: MongoDB быстрее в 5.65 раз
* 100,000 записей: MongoDB быстрее в 3.66 раз
