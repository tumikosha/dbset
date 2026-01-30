# DBSet - Async Database Library Documentation

## Обзор

**DBSet** — это Python-библиотека для упрощенной работы с базами данных, построенная на основе SQLAlchemy 2.x с поддержкой асинхронных операций. Библиотека вдохновлена оригинальной библиотекой `dataset`, но предоставляет нативную поддержку async/await и дуальный синхронный/асинхронный API.

### Ключевые возможности

- **SQLAlchemy 2.x** - тонкая обертка над SQLAlchemy с Pythonic API
- **Дуальный API** - синхронный и асинхронный интерфейсы с идентичным API
- **Автоматическое управление схемой** - авто-создание таблиц и колонок при вставке
- **Режим только для чтения** - встроенная безопасность для маркетинговых запросов
- **Connection Pooling** - эффективное переиспользование соединений через SQLAlchemy
- **Dict-based фильтрация** - Pythonic query API с продвинутыми фильтрами
- **Автоматический вывод типов** - автоматическое сопоставление Python → SQLAlchemy типов
- **Поддержка JSON/JSONB** - нативная работа с вложенными dict и list (JSONB для PostgreSQL)
- **Поддержка UUID** - первичные ключи на основе UUID
- **Управление индексами** - автоматическое и ручное создание индексов
- **Транзакции** - поддержка транзакций через контекстные менеджеры

### Поддерживаемые базы данных

- PostgreSQL (asyncpg, psycopg2)
- SQLite (aiosqlite, встроенный драйвер)
- MySQL (планируется)
- MongoDB (планируется)

---

## Установка


```toml
dependencies = [
    "sqlalchemy[asyncio]>=2.0.25",
    "asyncpg>=0.29.0",           # Async PostgreSQL driver
    "psycopg2-binary>=2.9.9",    # Sync PostgreSQL driver
    "aiosqlite>=0.19.0",         # Async SQLite driver
]
```

Для установки зависимостей:

```bash
cd agent
uv sync
```

---

## Быстрый старт

### Асинхронный API (рекомендуется)

```python
from dbset import async_connect

async def main():
    # Подключение к базе данных
    db = await async_connect('postgresql+asyncpg://localhost/mydb')

    # Получение таблицы (авто-создается если не существует)
    users = db['users']

    # Вставка данных
    pk = await users.insert({'name': 'John', 'age': 30})
    print(f"Inserted user with ID: {pk}")

    # Поиск с фильтрами
    async for user in users.find(age={'>=': 18}):
        print(f"{user['name']}: {user['age']} years old")

    # Обновление
    await users.update({'age': 31}, name='John')

    # Удаление
    await users.delete(name='John')

    # Закрытие соединения
    await db.close()
```

### Синхронный API (для простых скриптов)

```python
from dbset import connect

# Подключение к базе данных
db = connect('postgresql://localhost/mydb')

# Получение таблицы
users = db['users']

# Вставка данных
pk = users.insert({'name': 'John', 'age': 30})

# Поиск с фильтрами
for user in users.find(age={'>=': 18}):
    print(f"{user['name']}: {user['age']} years old")

# Закрытие соединения
db.close()
```

### Режим только для чтения

```python
# Маркетинговые запросы с защитой от записи
db = await async_connect(
    'postgresql+asyncpg://localhost/clinic',
    read_only=True  # Разрешены только SELECT запросы
)

patients = db['patients']

# Это работает - SELECT запрос
async for patient in patients.find(last_visit={'<': '2024-01-01'}):
    print(patient)

# Это вызовет ReadOnlyError
await patients.insert({'name': 'Hacker'})  # ❌ Заблокировано!
```

---

## API Reference

### Подключение к базе данных

#### `async_connect(url, **kwargs)`

Создает асинхронное подключение к базе данных.

**Параметры:**
- `url` (str): URL базы данных с async драйвером
  - PostgreSQL: `postgresql+asyncpg://user:pass@host/db`
  - SQLite: `sqlite+aiosqlite:///path/to/db.sqlite`
- `read_only` (bool): Если True, разрешены только SELECT запросы (default: False)
- `ensure_schema` (bool): Если True, авто-создавать таблицы/колонки (default: True)
- `primary_key_type` (str | PrimaryKeyType): Тип первичного ключа ('integer', 'uuid')
- `primary_key_column` (str): Имя колонки первичного ключа (default: 'id')
- `pk_config` (PrimaryKeyConfig): Расширенная конфигурация первичного ключа
- `text_index_prefix` (int): Длина префикса для индексов TEXT колонок в MySQL/MariaDB (default: 255)

**Возвращает:** `AsyncDatabase`

**Пример:**
```python
# PostgreSQL с Integer PK
db = await async_connect('postgresql+asyncpg://localhost/mydb')

# UUID первичные ключи
db = await async_connect(
    'postgresql+asyncpg://localhost/mydb',
    primary_key_type='uuid'
)

# Кастомное имя PK колонки
db = await async_connect(
    'postgresql+asyncpg://localhost/mydb',
    primary_key_type='uuid',
    primary_key_column='user_id'
)

# SQLite для тестирования
db = await async_connect('sqlite+aiosqlite:///:memory:')
```

#### `connect(url, **kwargs)`

Создает синхронное подключение к базе данных. Принимает те же параметры, что и `async_connect()`.

**Пример:**
```python
db = connect('postgresql://localhost/mydb')
db = connect('sqlite:///:memory:')
```

---

### Класс AsyncDatabase

#### Методы

##### `db[table_name]` - Получение таблицы

Возвращает объект `AsyncTable` для указанной таблицы. Таблица создается автоматически при первой вставке.

```python
users = db['users']
orders = db['orders']
```

##### `await db.close()` - Закрытие соединения

Закрывает соединение с базой данных и освобождает ресурсы.

```python
await db.close()
```

##### `async with db.transaction()` - Транзакция

Контекстный менеджер для выполнения операций в транзакции.

```python
async with db.transaction():
    await users.insert({'name': 'Alice'})
    await orders.insert({'user_id': 1, 'total': 100})
    # Обе операции будут закоммичены вместе
```

##### `async for row in db.query(stmt)` - Выполнение SQLAlchemy запроса

Выполняет SQLAlchemy statement напрямую.

```python
from sqlalchemy import select, func

users_table = await users.table
stmt = select(func.count()).select_from(users_table)
async for row in db.query(stmt):
    print(row)
```

---

### Класс AsyncTable

#### Вставка данных

##### `await table.insert(row, ensure=True)`

Вставляет одну строку в таблицу.

**Параметры:**
- `row` (dict): Словарь с данными
- `ensure` (bool): Авто-создать таблицу/колонки если не существуют

**Возвращает:** Primary key вставленной строки

**Пример:**
```python
pk = await users.insert({
    'name': 'John',
    'age': 30,
    'email': 'john@example.com'
})
print(f"Inserted with ID: {pk}")
```

##### `await table.insert_many(rows, chunk_size=1000, ensure=True)`

Вставляет несколько строк за один раз.

**Параметры:**
- `rows` (list[dict]): Список словарей с данными
- `chunk_size` (int): Размер батча для вставки (default: 1000)
- `ensure` (bool): Авто-создать таблицу/колонки

**Возвращает:** Количество вставленных строк

**Пример:**
```python
rows = [
    {'name': 'John', 'age': 30},
    {'name': 'Jane', 'age': 25},
    {'name': 'Bob', 'age': 35},
]
count = await users.insert_many(rows)
print(f"Inserted {count} rows")
```

#### Поиск данных

##### `async for row in table.find(**filters)`

Ищет строки с заданными фильтрами.

**Параметры:**
- `**filters`: Фильтры для поиска (см. раздел "Фильтры")
- `_order_by` (str | list[str]): Сортировка (например, 'age', '-age', ['name', '-age'])
- `_limit` (int): Максимальное количество строк
- `_offset` (int): Смещение для пагинации

**Возвращает:** AsyncIterator[dict]

**Примеры:**
```python
# Простой фильтр
async for user in users.find(age=30):
    print(user)

# Фильтры с операторами
async for user in users.find(age={'>=': 18}):
    print(user)

# Множественные фильтры (AND)
async for user in users.find(age={'>=': 18}, status='active'):
    print(user)

# С сортировкой
async for user in users.find(_order_by='-age', _limit=10):
    print(user)

# Пагинация
async for user in users.find(_limit=20, _offset=40):
    print(user)
```

##### `await table.find_one(**filters)`

Находит первую строку, соответствующую фильтрам.

**Возвращает:** dict | None

**Пример:**
```python
user = await users.find_one(email='john@example.com')
if user:
    print(f"Found: {user['name']}")
```

##### `await table.all()`

Возвращает все строки таблицы.

**Возвращает:** list[dict]

**Пример:**
```python
all_users = await users.all()
print(f"Total users: {len(all_users)}")
```

#### Обновление данных

##### `await table.update(data, **filters)`

Обновляет строки, соответствующие фильтрам.

**Параметры:**
- `data` (dict): Новые значения для обновления
- `**filters`: Фильтры для выбора строк

**Возвращает:** Количество обновленных строк

**Пример:**
```python
# Обновить возраст для John
updated = await users.update({'age': 31}, name='John')
print(f"Updated {updated} rows")

# Обновить всех пользователей старше 30
updated = await users.update({'status': 'senior'}, age={'>': 30})
```

##### `await table.upsert(row, keys, ensure=True)`

Вставляет строку или обновляет, если существует.

**Параметры:**
- `row` (dict): Данные для вставки/обновления
- `keys` (list[str]): Колонки для проверки существования
- `ensure` (bool): Авто-создать таблицу/колонки и индексы

**Возвращает:** Primary key строки

**Пример:**
```python
# Вставить или обновить по email
pk = await users.upsert(
    {'email': 'john@example.com', 'name': 'John', 'age': 31},
    keys=['email']
)

# Вставить или обновить по составному ключу
pk = await users.upsert(
    {'email': 'bob@example.com', 'country': 'US', 'age': 25},
    keys=['email', 'country']
)
```

**Примечание:** При `ensure=True` автоматически создается индекс на колонках `keys` для оптимальной производительности.

**Обработка несуществующих ключей:** Если параметр `keys` содержит колонки, которые не существуют в таблице, upsert вставит новую строку вместо обновления. Это соответствует поведению библиотеки `dataset` и позволяет корректно обрабатывать несоответствия схемы:

```python
# Если колонка 'nonexistent' не существует в таблице,
# будет выполнена INSERT вместо UPDATE
pk = await users.upsert(
    {'email': 'john@example.com', 'name': 'John'},
    keys=['email', 'nonexistent']  # 'nonexistent' нет в таблице → INSERT
)
```

См. раздел [Обработка несуществующих ключей](#обработка-несуществующих-ключей) для подробного описания поведения `update()`, `upsert()` и `upsert_many()`

##### `await table.upsert_many(rows, keys, chunk_size=1000, ensure=True)`

Массовая операция upsert.

**Пример:**
```python
rows = [
    {'email': 'alice@example.com', 'name': 'Alice', 'age': 30},
    {'email': 'bob@example.com', 'name': 'Bob', 'age': 25},
]
count = await users.upsert_many(rows, keys=['email'], ensure=True)
```

#### Удаление данных

##### `await table.delete(**filters)`

Удаляет строки, соответствующие фильтрам.

**Возвращает:** Количество удаленных строк

**Пример:**
```python
# Удалить конкретного пользователя
deleted = await users.delete(name='John')

# Удалить всех неактивных пользователей
deleted = await users.delete(status='inactive')

# Удалить ВСЕ строки (осторожно!)
deleted = await users.delete()
```

#### Агрегация

##### `await table.count(**filters)`

Подсчитывает строки с фильтрами.

**Пример:**
```python
# Всего пользователей
total = await users.count()

# Взрослых пользователей
adults = await users.count(age={'>=': 18})
```

##### `async for row in table.distinct(column, **filters)`

Возвращает уникальные значения колонки.

**Пример:**
```python
# Уникальные возрасты
async for row in users.distinct('age'):
    print(f"Age: {row['age']}")

# Уникальные страны активных пользователей
async for row in users.distinct('country', status='active'):
    print(f"Country: {row['country']}")
```

#### Управление индексами

##### `await table.create_index(columns, name=None, unique=False, text_index_prefix=None, **kwargs)`

Создает индекс на указанных колонках.

**Параметры:**
- `columns` (str | list[str]): Колонка или список колонок
- `name` (str): Имя индекса (опционально, генерируется автоматически)
- `unique` (bool): Создать уникальный индекс
- `text_index_prefix` (int): Длина префикса для TEXT колонок в MySQL/MariaDB (использует значение по умолчанию базы данных, если не указано)
- `**kwargs`: Дополнительные параметры (например, `postgresql_where`)

**Возвращает:** Имя созданного индекса

**Примеры:**
```python
# Индекс на одной колонке
idx_name = await users.create_index('email')
# Возвращает: 'idx_users_email'

# Составной индекс
idx_name = await users.create_index(['country', 'city'])
# Возвращает: 'idx_users_country_city'

# Уникальный индекс с кастомным именем
idx_name = await users.create_index(
    'username',
    name='unique_username',
    unique=True
)

# Частичный индекс (PostgreSQL)
from sqlalchemy import text
idx_name = await users.create_index(
    'email',
    postgresql_where=text("status = 'active'")
)
```

##### `await table.has_index(columns)`

Проверяет существование индекса на колонках.

**Возвращает:** bool

**Пример:**
```python
if not await users.has_index('email'):
    await users.create_index('email')

# Проверка составного индекса
has_compound = await users.has_index(['country', 'city'])
```

#### Доступ к SQLAlchemy

##### `await table.table` - Получение SQLAlchemy Table

Возвращает объект `sqlalchemy.Table` для прямого использования SQLAlchemy API.

**Пример:**
```python
from sqlalchemy import select, func

users_table = await users.table

# Сложный запрос с SQLAlchemy
stmt = (
    select(users_table.c.name, func.count().label('count'))
    .where(users_table.c.age > 18)
    .group_by(users_table.c.name)
    .order_by(func.count().desc())
)

async for row in db.query(stmt):
    print(f"{row['name']}: {row['count']}")
```

---

## Фильтры запросов

DBSet поддерживает мощную систему dict-based фильтров для запросов.

### Простые фильтры

```python
# Точное совпадение
users.find(status='active')
users.find(age=30)

# Множественные условия (AND)
users.find(status='active', age=30)
```

### Операторы сравнения

```python
# Больше/меньше
users.find(age={'>': 18})
users.find(age={'>=': 18})
users.find(age={'<': 65})
users.find(age={'<=': 65})

# Не равно
users.find(status={'!=': 'deleted'})
```

### IN запросы

```python
# IN список значений
users.find(status={'in': ['active', 'pending', 'approved']})
users.find(age={'in': [25, 30, 35]})
```

### LIKE паттерны

```python
# LIKE с подстановочными символами
users.find(email={'like': '%@gmail.com'})

# Специальные операторы
users.find(name={'startswith': 'John'})  # name LIKE 'John%'
users.find(name={'endswith': 'son'})      # name LIKE '%son'
users.find(name={'contains': 'doe'})      # name LIKE '%doe%'
```

### BETWEEN

```python
# BETWEEN (включительно)
users.find(age={'between': [18, 65]})
users.find(created_at={'between': ['2024-01-01', '2024-12-31']})
```

### NULL проверки

```python
# IS NULL
users.find(deleted_at={'is': None})

# IS NOT NULL
users.find(deleted_at={'is_not': None})
```

### Комбинированные фильтры

```python
# Все условия объединяются через AND
async for user in users.find(
    age={'>=': 18, '<': 65},
    status='active',
    country={'in': ['US', 'UK', 'CA']},
    email={'like': '%@gmail.com'}
):
    print(user)
```

---

## Примеры использования

### Пример 1: Маркетинговый запрос - Поиск оттока клиентов

```python
from dbset import async_connect
from datetime import datetime, timedelta

async def find_churn_customers(db_url: str):
    """Найти пациентов, не посещавших клинику 6+ месяцев."""
    db = await async_connect(db_url, read_only=True)

    six_months_ago = datetime.now() - timedelta(days=180)
    patients = db['patients']

    churn_list = []
    async for patient in patients.find(
        last_visit={'<': six_months_ago},
        status='active',
        _limit=100,
        _order_by='-last_visit'
    ):
        churn_list.append({
            'name': patient['name'],
            'email': patient['email'],
            'last_visit': patient['last_visit']
        })

    await db.close()
    return churn_list
```

### Пример 2: Импорт CSV с авто-созданием схемы

```python
from dbset import connect
import csv

def import_customers(csv_path: str):
    """Импортировать CSV с автоматическим созданием таблицы."""
    db = connect('postgresql://localhost/clinic')
    customers = db['customers']

    with open(csv_path) as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Авто-создание таблицы с колонками из CSV заголовков
    count = customers.insert_many(rows, ensure=True)

    print(f"Imported {count} rows")
    db.close()
```

### Пример 3: Upsert с UUID первичными ключами

```python
from dbset import async_connect

async def sync_users_with_uuid():
    """Синхронизировать пользователей с UUID первичными ключами."""
    db = await async_connect(
        'postgresql+asyncpg://localhost/mydb',
        primary_key_type='uuid'
    )

    users = db['users']

    # Upsert автоматически создаст индекс на email
    await users.upsert(
        {
            'email': 'alice@example.com',
            'name': 'Alice',
            'age': 30
        },
        keys=['email'],
        ensure=True
    )

    # Массовая синхронизация
    new_users = [
        {'email': 'bob@example.com', 'name': 'Bob', 'age': 25},
        {'email': 'charlie@example.com', 'name': 'Charlie', 'age': 35},
    ]
    await users.upsert_many(new_users, keys=['email'])

    await db.close()
```

### Пример 4: Транзакции

```python
async def transfer_money(from_user_id: int, to_user_id: int, amount: float):
    """Перевод денег между пользователями с транзакцией."""
    db = await async_connect('postgresql+asyncpg://localhost/bank')
    accounts = db['accounts']

    async with db.transaction():
        # Снять у отправителя
        await accounts.update(
            {'balance': {'decrement': amount}},
            user_id=from_user_id
        )

        # Добавить получателю
        await accounts.update(
            {'balance': {'increment': amount}},
            user_id=to_user_id
        )

        # Если ошибка - автоматический rollback

    await db.close()
```

### Пример 5: Прямой SQLAlchemy запрос

```python
from dbset import async_connect
from sqlalchemy import select, func, and_

async def advanced_analytics():
    """Сложная аналитика с SQLAlchemy."""
    db = await async_connect('postgresql+asyncpg://localhost/clinic')

    patients = db['patients']
    appointments = db['appointments']

    patients_table = await patients.table
    appointments_table = await appointments.table

    # Сложный запрос с JOIN и агрегацией
    stmt = (
        select(
            patients_table.c.name,
            func.count(appointments_table.c.id).label('visit_count'),
            func.max(appointments_table.c.date).label('last_visit')
        )
        .select_from(
            patients_table.join(
                appointments_table,
                patients_table.c.id == appointments_table.c.patient_id
            )
        )
        .where(appointments_table.c.status == 'completed')
        .group_by(patients_table.c.id, patients_table.c.name)
        .having(func.count(appointments_table.c.id) > 5)
        .order_by(func.count(appointments_table.c.id).desc())
    )

    async for row in db.query(stmt):
        print(f"{row['name']}: {row['visit_count']} visits, last: {row['last_visit']}")

    await db.close()
```

---

## Архитектура

### Структура модуля

```
dbset/
├── __init__.py           # Публичный API (connect, async_connect)
├── async_core.py         # AsyncDatabase, AsyncTable (async API)
├── sync_core.py          # Database, Table (sync API)
├── schema.py             # Управление схемой (DDL операции)
├── query.py              # FilterBuilder (dict → SQLAlchemy WHERE)
├── types.py              # TypeInference (Python → SQLAlchemy типы)
├── validators.py         # ReadOnlyValidator (SQL безопасность)
├── connection.py         # Connection pooling
└── exceptions.py         # Иерархия исключений
```

### Как это работает

1. **Schema Discovery**: Отражает схему БД используя SQLAlchemy MetaData
2. **Auto-Create**: Автоматически создает таблицы/колонки при вставке
3. **Type Inference**: Выводит типы SQLAlchemy из значений Python
4. **Query Building**: Транслирует dict фильтры в SQLAlchemy WHERE условия
5. **Validation**: Проверяет безопасность SQL в read-only режиме
6. **Execution**: Выполняет через SQLAlchemy async/sync engines

### Интеграция с SQLAlchemy

DBSet является **тонкой оберткой** над SQLAlchemy:

```python
# Упрощенный API DBSet
await table.insert({'name': 'John', 'age': 30})

# Транслируется в SQLAlchemy под капотом
from sqlalchemy import insert
stmt = insert(table._table).values(name='John', age=30)
await conn.execute(stmt)
```

**У вас всегда есть прямой доступ к SQLAlchemy:**
- `table.table` → SQLAlchemy Table object
- `db.query(sqlalchemy_statement)` → Выполнение SQLAlchemy statements
- `db.engine` → SQLAlchemy Engine
- `db.metadata` → SQLAlchemy MetaData

---

## Конфигурация первичных ключей

### PrimaryKeyType

Enum с поддерживаемыми типами первичных ключей:

- `PrimaryKeyType.INTEGER` - Авто-инкремент integer (по умолчанию)
- `PrimaryKeyType.UUID` - UUID строки (String(36))
- `PrimaryKeyType.CUSTOM` - Кастомный тип с пользовательским генератором

### PrimaryKeyConfig

Класс для расширенной конфигурации первичных ключей.

**Параметры:**
- `pk_type` (PrimaryKeyType | str): Тип первичного ключа
- `column_name` (str): Имя колонки PK (default: 'id')
- `generator` (Callable): Функция генерации значений (для UUID/CUSTOM)
- `sqlalchemy_type` (TypeEngine): SQLAlchemy тип (для CUSTOM)

**Примеры:**

```python
from dbset import async_connect, PrimaryKeyConfig, PrimaryKeyType
from uuid import uuid4
from sqlalchemy import String

# Integer авто-инкремент (по умолчанию)
db = await async_connect('postgresql+asyncpg://localhost/mydb')

# UUID первичные ключи
db = await async_connect(
    'postgresql+asyncpg://localhost/mydb',
    primary_key_type='uuid'
)

# UUID с кастомным именем колонки
db = await async_connect(
    'postgresql+asyncpg://localhost/mydb',
    primary_key_type='uuid',
    primary_key_column='user_id'
)

# UUID в верхнем регистре через PrimaryKeyConfig
pk_config = PrimaryKeyConfig(
    pk_type='uuid',
    generator=lambda: str(uuid4()).upper()
)
db = await async_connect(
    'postgresql+asyncpg://localhost/mydb',
    pk_config=pk_config
)

# Полностью кастомный первичный ключ
pk_config = PrimaryKeyConfig(
    pk_type='custom',
    column_name='custom_id',
    generator=lambda: f"USER_{uuid4()}",
    sqlalchemy_type=String(50)
)
db = await async_connect(
    'postgresql+asyncpg://localhost/mydb',
    pk_config=pk_config
)
```

---

## Исключения

### Иерархия исключений

```
DatasetError (базовый класс)
├── ConnectionError         - Ошибка подключения к БД
├── TableNotFoundError      - Таблица не существует
├── ColumnNotFoundError     - Колонка не существует
├── ReadOnlyError          - Попытка записи в read-only режиме
├── TransactionError       - Ошибка транзакции
├── ValidationError        - Ошибка валидации данных
├── SchemaError           - Ошибка DDL операции
├── QueryError            - Ошибка выполнения запроса
└── TypeInferenceError    - Ошибка вывода типа
```

### Примеры обработки

```python
from dbset import async_connect
from dbset import (
    ReadOnlyError,
    TableNotFoundError,
    ValidationError
)

async def safe_operation():
    db = await async_connect(
        'postgresql+asyncpg://localhost/mydb',
        read_only=True
    )

    try:
        users = db['users']
        await users.insert({'name': 'John'})
    except ReadOnlyError as e:
        print(f"Write operation blocked: {e}")
    except TableNotFoundError as e:
        print(f"Table not found: {e.table_name}")
    except ValidationError as e:
        print(f"Validation failed: {e}")
    finally:
        await db.close()
```

---

## Обработка несуществующих ключей

Параметр `keys` в функциях `update()`, `upsert()` и `upsert_many()` поддерживает корректную обработку несуществующих колонок. Это поведение соответствует библиотеке `dataset` для совместимости и позволяет безопасно работать с динамическими схемами.

### Зачем это нужно

При работе с:
- **Динамическими схемами** - колонки могут добавляться или удаляться со временем
- **Внешними источниками данных** - входящие данные могут ссылаться на несуществующие колонки
- **Развивающимися кодовыми базами** - имена ключевых колонок могут меняться между версиями
- **Мультитенантными системами** - разные тенанты могут иметь разные схемы

DBSet обрабатывает эти сценарии корректно, не вызывая ошибок.

### Поведение по функциям

| Функция | Часть ключей существует | Все ключи несуществующие |
|---------|------------------------|--------------------------|
| `upsert()` | INSERT новой строки (совпадение не найдено) | INSERT новой строки |
| `upsert_many()` | INSERT новых строк | INSERT новых строк |
| `update()` | Обновление с валидными ключами | Вызывает `QueryError` |

### Поведение upsert()

Когда `keys` содержит колонки, которые не существуют в таблице, поисковый запрос не находит совпадение, что приводит к INSERT вместо UPDATE:

```python
# Начальные данные
await users.insert({'name': 'John', 'age': 30})

# Upsert с несуществующим ключом 'tenant_id'
# Запрос WHERE name='John' AND tenant_id=NULL не находит совпадений
await users.upsert(
    {'name': 'John', 'age': 31, 'tenant_id': 'abc'},
    keys=['name', 'tenant_id']  # 'tenant_id' не существует → INSERT
)

# Результат: 2 строки (оригинальная + новая), а не 1 обновленная
count = await users.count()  # Возвращает 2
```

**Обоснование:** Несуществующий ключ приводит к включению `NULL` для этой колонки в поиске, что никогда не совпадает с существующими строками. Это безопасное поведение по умолчанию - оно предотвращает случайные перезаписи при несовпадении схем.

### Поведение upsert_many()

Аналогично `upsert()` - несуществующие ключи приводят к INSERT для всех строк:

```python
rows = [
    {'email': 'a@test.com', 'name': 'A'},
    {'email': 'b@test.com', 'name': 'B'},
]

# С несуществующим ключом 'region' все строки вставляются
await users.upsert_many(rows, keys=['email', 'region'])
```

### Поведение update()

Функция `update()` отфильтровывает несуществующие ключи и работает с валидными:

```python
# Подготовка
await users.insert({'name': 'John', 'age': 30})

# Обновление со смешанными валидными/невалидными ключами
# Только 'name' используется для WHERE
count = await users.update(
    {'name': 'John', 'age': 99, 'nonexistent': 'val'},
    keys=['name', 'nonexistent']  # 'nonexistent' отфильтрован
)
# Результат: 1 строка обновлена где name='John'
```

**Когда ВСЕ ключи несуществующие:**

```python
from dbset import QueryError

# Это вызывает QueryError - невозможно построить WHERE
try:
    await users.update(
        {'name': 'John', 'age': 99},
        keys=['foo', 'bar']  # Ни один не существует
    )
except QueryError as e:
    print("Невозможно обновить: нет валидных ключей для WHERE")
```

**Почему отличается от upsert?**
- `upsert()` имеет безопасный fallback (INSERT), поэтому может продолжить
- `update()` без WHERE обновил бы ВСЕ строки - это опасно и должно быть явным

### Лучшие практики

1. **Валидируйте ключи перед массовыми операциями**, если нужно строгое поведение:
   ```python
   table_cols = {col.name for col in (await users.table).columns}
   valid_keys = [k for k in keys if k in table_cols]
   if not valid_keys:
       raise ValueError("Валидные ключевые колонки не найдены")
   ```

2. **Используйте явные проверки колонок**, когда несоответствие схемы должно быть ошибкой:
   ```python
   if not await users.has_column('tenant_id'):
       raise SchemaError("Обязательная колонка 'tenant_id' отсутствует")
   ```

3. **Логируйте отфильтрованные ключи** для отладки в продакшене:
   ```python
   import logging
   # DBSet логирует отфильтрованные ключи на уровне DEBUG
   logging.getLogger('dbset').setLevel(logging.DEBUG)
   ```

---

## Управление индексами

### Автоматическое создание индексов

При использовании `upsert()` или `upsert_many()` с `ensure=True`, индексы **автоматически создаются** на ключевых колонках:

```python
# Автоматическое создание индекса при upsert
await table.upsert(
    {'email': 'alice@example.com', 'name': 'Alice', 'age': 30},
    keys=['email'],
    ensure=True  # Авто-создает таблицу, колонки И индекс на 'email'
)

# Проверка создания индекса
assert await table.has_index(['email']) is True

# Составные ключи создают составные индексы
await table.upsert(
    {'email': 'bob@example.com', 'country': 'US', 'age': 25},
    keys=['email', 'country'],
    ensure=True  # Авто-создает индекс на ['email', 'country']
)
```

### Зачем автоматические индексы при upsert?

- Upsert выполняет lookup (`find_one`) на каждом вызове используя параметр `keys`
- Без индекса это полное сканирование таблицы - O(n) сложность
- С индексом lookup выполняется за O(log n) - драматически быстрее для больших таблиц
- `ensure=True` означает "настроить все необходимое для оптимальной работы"

### Когда индексы НЕ создаются автоматически:

- `insert()` / `insert_many()` - lookup не требуется
- `upsert()` с `ensure=False` - явный контроль пользователя
- `update()` методы - используют существующие ключи

### Ручное создание индексов

```python
# Индекс на одной колонке
idx_name = await table.create_index('email')
# Возвращает: 'idx_users_email'

# Составной индекс
idx_name = await table.create_index(['country', 'city'])
# Возвращает: 'idx_users_country_city'

# Уникальный индекс с кастомным именем
idx_name = await table.create_index(
    'username',
    name='unique_username',
    unique=True
)

# Идемпотентность - повторное создание не вызывает ошибку
idx_name = await table.create_index('email')  # Первый раз
idx_name = await table.create_index('email')  # Второй раз - нет ошибки

# Проверка существования
if not await table.has_index('email'):
    await table.create_index('email')

# Частичный индекс (PostgreSQL)
from sqlalchemy import text
idx_name = await table.create_index(
    'email',
    postgresql_where=text("status = 'active'")
)
```

### Именование индексов

- Автоматически генерируемые имена: `idx_{table}_{col1}_{col2}`
- Длинные имена обрезаются до 63 символов (лимит PostgreSQL) с hash суффиксом
- Кастомные имена можно указать через параметр `name`

### Индексирование TEXT колонок в MySQL/MariaDB

MySQL/MariaDB требует указания длины префикса при создании индексов на TEXT колонках. DBSet автоматически обрабатывает это с настраиваемой длиной префикса.

**Конфигурация:**

```python
# По умолчанию: 255 символов префикса для TEXT колонок
db = await async_connect('mysql+aiomysql://localhost/mydb')

# Кастомная длина префикса на уровне базы данных
db = await async_connect(
    'mysql+aiomysql://localhost/mydb',
    text_index_prefix=191  # Для utf8mb4 с лимитом ключа 767 байт
)

# Кастомная длина префикса на уровне таблицы
users = db['users']
await users.create_index('description', text_index_prefix=100)
```

**Примечание:** Эта настройка влияет только на MySQL/MariaDB. PostgreSQL и SQLite работают с TEXT индексами без указания длины префикса.

### Когда использовать индексы

- Колонки, часто используемые в WHERE условиях
- Колонки для JOIN операций
- Колонки для сортировки (ORDER BY)
- Foreign key колонки
- Email/username поля для аутентификации

### Лучшие практики

- Создавайте индексы после массового импорта данных для лучшей производительности
- Используйте составные индексы для запросов с фильтрацией по нескольким колонкам
- Используйте уникальные индексы для обеспечения целостности данных
- Мониторьте использование индексов - неиспользуемые индексы замедляют запись

---

## Вывод типов

### TypeInference

Класс для автоматического вывода типов SQLAlchemy из значений Python.

**Поддерживаемые типы:**

| Python тип | SQLAlchemy тип | Примечания |
|-----------|----------------|-----------|
| `int` | `Integer()` | |
| `float` | `Float()` | |
| `Decimal` | `Numeric(p, s)` | Автоматический расчет precision/scale |
| `bool` | `Boolean()` | Проверяется до int (bool - подкласс int) |
| `str` | `Text()` | Всегда TEXT для максимальной гибкости |
| `bytes` | `Text()` | Может быть улучшено для binary типов |
| `datetime` | `DateTime()` | |
| `date` | `Date()` | |
| `dict` | `JSON()` или `JSONB()` | JSONB для PostgreSQL, JSON для остальных |
| `list` | `JSON()` или `JSONB()` | JSONB для PostgreSQL, JSON для остальных |
| `None` | `Text()` | Nullable по умолчанию |

**Примеры:**

```python
from dbset.types import TypeInference
from decimal import Decimal

# Вывод типов из значений
TypeInference.infer_type(42)                    # Integer()
TypeInference.infer_type(3.14)                  # Float()
TypeInference.infer_type(Decimal('123.45'))     # Numeric(5, 2)
TypeInference.infer_type(True)                  # Boolean()
TypeInference.infer_type('hello')               # Text()
TypeInference.infer_type('x' * 300)             # Text()
TypeInference.infer_type(datetime.now())        # DateTime()

# Вывод типов из строки
row = {'name': 'John', 'age': 30, 'active': True}
types = TypeInference.infer_types_from_row(row)
# {'name': Text(), 'age': Integer(), 'active': Boolean()}

# Слияние типов (для множественных строк)
TypeInference.merge_types(Integer(), Float())   # Float()
TypeInference.merge_types(String(50), String(100))  # String(100)
TypeInference.merge_types(Date(), DateTime())   # DateTime()

# JSON типы (авто-определение диалекта)
TypeInference.infer_type({'key': 'value'})                    # JSON()
TypeInference.infer_type({'key': 'value'}, dialect='postgresql')  # JSONB()
TypeInference.infer_type([1, 2, 3], dialect='postgresql')     # JSONB()
```

---

## Поддержка JSON/JSONB

DBSet автоматически обрабатывает вложенные Python dict и list, сохраняя их как JSON колонки. Для PostgreSQL автоматически используется оптимизированный тип **JSONB**.

### Вставка JSON данных

```python
# Вставка данных с вложенными структурами - ручная сериализация не нужна!
await users.insert({
    'name': 'John',
    'metadata': {
        'role': 'admin',
        'permissions': ['read', 'write', 'delete']
    },
    'tags': ['python', 'sql', 'async'],
    'orders': [
        {'product': 'Book', 'qty': 2, 'price': 29.99},
        {'product': 'Pen', 'qty': 5, 'price': 4.99}
    ]
})

# Данные сохраняются как:
# - PostgreSQL: JSONB колонки (быстрые запросы, индексируемые)
# - SQLite/другие: JSON колонки
```

### Запросы JSON данных

```python
# Данные возвращаются как Python dict/list
user = await users.find_one(name='John')
print(user['metadata']['role'])       # 'admin'
print(user['orders'][0]['product'])   # 'Book'
print(user['tags'])                   # ['python', 'sql', 'async']
```

### Маппинг типов по базам данных

| Python тип | PostgreSQL | SQLite | Другие |
|------------|------------|--------|--------|
| `dict` | JSONB | JSON | JSON |
| `list` | JSONB | JSON | JSON |

### Почему JSONB для PostgreSQL?

- **Бинарный формат хранения** - быстрее чтение и запросы
- **Поддержка GIN индексов** - быстрые запросы по содержимому JSON
- **Нативные операторы** - `->`, `->>`, `@>`, `?` для запросов внутри JSON
- **Без дублирующих ключей** - автоматическая дедупликация
- **Без сохранения пробелов** - более компактное хранение

### Продвинутое: SQLAlchemy JSON запросы

Для сложных JSON запросов используйте SQLAlchemy напрямую:

```python
from sqlalchemy import select

users_table = await users.table

# PostgreSQL JSONB операторы через SQLAlchemy
stmt = select(users_table).where(
    users_table.c.metadata['role'].astext == 'admin'
)

async for row in db.query(stmt):
    print(row)
```

---

## Тестирование

### Запуск тестов

```bash
# Все тесты
uv run pytest tests/unit/dbset/ -v

# Конкретный файл
uv run pytest tests/unit/dbset/test_async_core.py -v

# Конкретный тест
uv run pytest tests/unit/dbset/test_async_core.py::test_insert -v

# С покрытием кода
uv run pytest tests/unit/dbset/ --cov=src/dbset --cov-report=html
```

### Примеры тестов

```python
import pytest
from dbset import async_connect

@pytest.mark.asyncio
async def test_insert_and_find():
    """Тест вставки и поиска."""
    db = await async_connect('sqlite+aiosqlite:///:memory:')
    users = db['users']

    # Вставка
    pk = await users.insert({'name': 'John', 'age': 30})
    assert pk is not None

    # Поиск
    user = await users.find_one(name='John')
    assert user['name'] == 'John'
    assert user['age'] == 30

    await db.close()

@pytest.mark.asyncio
async def test_read_only_mode():
    """Тест read-only режима."""
    db = await async_connect(
        'sqlite+aiosqlite:///:memory:',
        read_only=True
    )
    users = db['users']

    # Попытка вставки должна вызвать ReadOnlyError
    with pytest.raises(ReadOnlyError):
        await users.insert({'name': 'Hacker'})

    await db.close()
```

---

## Лучшие практики

### 1. Используйте async API для современных приложений

```python
# ✅ Рекомендуется
db = await async_connect('postgresql+asyncpg://localhost/mydb')

# ⚠️ Только для простых скриптов
db = connect('postgresql://localhost/mydb')
```

### 2. Всегда закрывайте соединения

```python
# ✅ С контекстным менеджером (будущая версия)
async with await async_connect(url) as db:
    users = db['users']
    await users.insert({'name': 'John'})

# ✅ Ручное закрытие
db = await async_connect(url)
try:
    users = db['users']
    await users.insert({'name': 'John'})
finally:
    await db.close()
```

### 3. Используйте read-only режим для безопасности

```python
# Для маркетинговых запросов и аналитики
db = await async_connect(db_url, read_only=True)
```

### 4. Создавайте индексы для часто используемых колонок

```python
# При upsert с ensure=True индексы создаются автоматически
await users.upsert(
    {'email': 'alice@example.com', 'name': 'Alice'},
    keys=['email'],
    ensure=True
)

# Или создайте вручную для сложных случаев
await users.create_index(['country', 'city'])
```

### 5. Используйте транзакции для связанных операций

```python
async with db.transaction():
    await users.insert({'name': 'Alice'})
    await orders.insert({'user_id': 1, 'total': 100})
    # Обе операции закоммитятся вместе
```

### 6. Используйте batch операции для больших объемов

```python
# ✅ Эффективно
rows = [{'name': f'User{i}', 'age': i} for i in range(1000)]
await users.insert_many(rows, chunk_size=500)

# ❌ Неэффективно
for i in range(1000):
    await users.insert({'name': f'User{i}', 'age': i})
```

### 7. Для сложных запросов используйте SQLAlchemy напрямую

```python
from sqlalchemy import select, func

users_table = await users.table
stmt = (
    select(users_table.c.country, func.count().label('count'))
    .group_by(users_table.c.country)
    .order_by(func.count().desc())
)

async for row in db.query(stmt):
    print(f"{row['country']}: {row['count']}")
```

### 8. Используйте UUID для распределенных систем

```python
# UUID лучше для распределенных систем, где нет центрального генератора ID
db = await async_connect(
    'postgresql+asyncpg://localhost/mydb',
    primary_key_type='uuid'
)
```

---

## Производительность

### Connection Pooling

DBSet использует connection pooling SQLAlchemy для эффективного переиспользования соединений:

```python
db = await async_connect(
    'postgresql+asyncpg://localhost/mydb',
    pool_size=10,        # Размер пула (default: 5)
    max_overflow=20,     # Дополнительные соединения (default: 10)
)
```

### Batch операции

Используйте `insert_many()` и `upsert_many()` для вставки больших объемов данных:

```python
# Вставка 10,000 строк пачками по 1000
rows = [{'name': f'User{i}', 'age': i % 100} for i in range(10000)]
await users.insert_many(rows, chunk_size=1000)
```

### Индексы

Создавайте индексы для колонок, используемых в WHERE, JOIN, ORDER BY:

```python
# Индекс автоматически создается при upsert с ensure=True
await users.upsert(data, keys=['email'], ensure=True)

# Или создайте вручную
await users.create_index('email')
await users.create_index(['country', 'city'])
```

### Limit и Offset

Используйте пагинацию для больших результатов:

```python
# Первая страница (0-20)
async for user in users.find(_limit=20, _offset=0):
    print(user)

# Вторая страница (20-40)
async for user in users.find(_limit=20, _offset=20):
    print(user)
```

---

## FAQ

### Q: Чем DBSet отличается от SQLAlchemy ORM?

**A:** DBSet - это тонкая обертка над SQLAlchemy Core (не ORM), предоставляющая упрощенный dict-based API. В отличие от ORM, нет необходимости определять модели классов - таблицы создаются автоматически из данных.

### Q: Можно ли использовать DBSet с существующими базами данных?

**A:** Да! DBSet отражает существующую схему и работает с ней. Авто-создание срабатывает только для несуществующих таблиц/колонок.

### Q: Как обрабатывать миграции схемы?

**A:** DBSet не предназначен для сложных миграций. Для production используйте Alembic или другие инструменты миграций. DBSet автоматически добавляет новые колонки при вставке.

### Q: Поддерживаются ли JOIN запросы?

**A:** Для JOIN используйте прямой SQLAlchemy API через `db.query()` с SQLAlchemy statements.

### Q: Можно ли использовать DBSet в продакшене?

**A:** Да, DBSet построен на SQLAlchemy 2.x и использует его connection pooling и безопасность. Для критичных систем рекомендуется тщательное тестирование.

### Q: Как обрабатывать ошибки соединения?

**A:** Используйте try/except для перехвата `ConnectionError`:

```python
from dbset import ConnectionError

try:
    db = await async_connect('postgresql+asyncpg://bad-url')
except ConnectionError as e:
    print(f"Connection failed: {e}")
```

---

## Статус разработки

**Фаза 1-3 Завершена:**
- ✅ Инфраструктура (exceptions, types, validators, connection, query)
- ✅ Управление схемой (DDL операции)
- ✅ Async API (AsyncDatabase, AsyncTable)
- ✅ Sync API (Database, Table)
- ✅ Поддержка JSON/JSONB (авто-определение по диалекту)
- ✅ Unit тесты (170+ тестов)

**Оставшиеся фазы:**
- [ ] Интеграционные тесты с PostgreSQL
- [ ] Benchmarks производительности
- [ ] Расширенная документация и примеры

---

## Философия дизайна

**DBSet = Упрощенный API + Мощь SQLAlchemy**

- Используйте простой API DBSet для обычных операций (80% случаев)
- Используйте SQLAlchemy напрямую для сложных запросов (20% случаев)
- Никакой магии - все транслируется в стандартный SQLAlchemy код
- Всегда возможно перейти на SQLAlchemy при необходимости

---


