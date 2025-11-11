# Лабораторная работа 4-1. Сравнение подходов хранения больших данных

## Вариант 5.

## Цель работы: 
сравнить производительность и эффективность различных
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
    def create_table(self):
        create_table_query = """
        DROP TABLE IF EXISTS customers;
        CREATE TABLE customers (
            customer_id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            status VARCHAR(20) NOT NULL,
            registration_date DATE,
            total_orders INTEGER,
            total_spent DECIMAL(10,2)
        );
        """
        self.cursor.execute(create_table_query)
        self.connection.commit()
        print("✅ Таблица 'customers' создана в PostgreSQL")
    
    def insert_data(self, customers):
        insert_query = """
        INSERT INTO customers (customer_id, name, email, status, registration_date, total_orders, total_spent)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        for customer in customers:
            try:
                self.cursor.execute(insert_query, (
                    customer['customer_id'],
                    customer['name'],
                    customer['email'],
                    customer['status'],
                    customer['registration_date'],
                    customer['total_orders'],
                    customer['total_spent']
                ))
            except Exception as e:
                print(f"Ошибка вставки клиента {customer['customer_id']}: {e}")
                continue
        
        self.connection.commit()
        print(f"✅ Вставлено {len(customers)} записей в PostgreSQL")
    
    def update_random_10_percent(self):
        # Получаем общее количество записей
        self.cursor.execute("SELECT COUNT(*) FROM customers")
        total_count = self.cursor.fetchone()[0]
        update_count = int(total_count * 0.1)
        
        print(f"📊 Обновление {update_count} случайных записей из {total_count}")
        
        # Выбираем случайные 10% ID
        self.cursor.execute(f"""
        SELECT customer_id FROM customers 
        ORDER BY RANDOM() 
        LIMIT {update_count}
        """)
        random_ids = [row[0] for row in self.cursor.fetchall()]
        
        # Измеряем время выполнения UPDATE
        start_time = time.time()
        
        # Создаем строку с ID для IN условия
        id_placeholders = ','.join(['%s'] * len(random_ids))
        update_query = f"""
        UPDATE customers 
        SET status = 'updated_status' 
        WHERE customer_id IN ({id_placeholders})
        """
        
        self.cursor.execute(update_query, random_ids)
        self.connection.commit()
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        print(f"⏱️ PostgreSQL: Обновлено {len(random_ids)} записей за {execution_time:.4f} секунд")
        return execution_time
    
    def close(self):
        self.cursor.close()
        self.connection.close()
```
```python
# Тестируем PostgreSQL
print("=== ТЕСТИРОВАНИЕ POSTGRESQL ===")
pg_db = PostgresCRUD()
pg_db.create_table()
pg_db.insert_data(customers_data)
pg_time = pg_db.update_random_10_percent()
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
# Сравнительный анализ
print("\n=== СРАВНИТЕЛЬНЫЙ АНАЛИЗ ===")

results = {
    'Database': ['PostgreSQL', 'MongoDB'],
    'Update_Time_Seconds': [pg_time, mongo_time],
    'Records_Updated': [1000, 1000]
}

results_df = pd.DataFrame(results)
print("\nРезультаты производительности:")
print(results_df)

# Визуализация
plt.figure(figsize=(12, 6))

# Столбчатая диаграмма
plt.subplot(1, 2, 1)
bars = plt.bar(results_df['Database'], results_df['Update_Time_Seconds'], 
               color=['#3366cc', '#109618'], alpha=0.7)
plt.title('Сравнение времени выполнения операций UPDATE')
plt.ylabel('Время (секунды)')
plt.xlabel('База данных')
plt.grid(axis='y', alpha=0.3)

# Добавляем значения на столбцы
for bar, time_val in zip(bars, results_df['Update_Time_Seconds']):
    plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.001, 
             f'{time_val:.4f}s', ha='center', va='bottom', fontweight='bold')

# Круговой график относительной производительности
plt.subplot(1, 2, 2)
if pg_time < mongo_time:
    labels = ['PostgreSQL\nбыстрее', 'MongoDB\nмедленнее']
    sizes = [pg_time, mongo_time - pg_time]
    colors = ['#3366cc', '#ff9900']
else:
    labels = ['PostgreSQL\nмедленнее', 'MongoDB\nбыстрее'] 
    sizes = [pg_time - mongo_time, mongo_time]
    colors = ['#ff9900', '#109618']

plt.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
plt.title('Относительная производительность')

plt.tight_layout()
plt.show()

# Детальный анализ
print("\n" + "="*60)
print("ДЕТАЛЬНЫЙ АНАЛИЗ РЕЗУЛЬТАТОВ")
print("="*60)

if pg_time < mongo_time:
    faster = "PostgreSQL"
    slower = "MongoDB"
    ratio = mongo_time / pg_time
    time_diff = mongo_time - pg_time
else:
    faster = "MongoDB" 
    slower = "PostgreSQL"
    ratio = pg_time / mongo_time
    time_diff = pg_time - mongo_time
```

## Выводы
<img width="791" height="560" alt="image" src="https://github.com/user-attachments/assets/47c75b54-f059-4fe5-8026-4eb9c76967b4" />


MongoDB оказался быстрее PostgreSQL в 1,5 раза. PostgreSQL показал лучшую производительность для массовых обновлений. MongoDB требует дополнительного времени на выборку случайных документов. Для CRM-систем с частыми обновлениями PostgreSQL более эффективен.
