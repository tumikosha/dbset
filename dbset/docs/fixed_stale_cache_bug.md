# Баг: Stale Cache в create_index после ensure_columns

## Описание проблемы

При вызове `upsert_many` (или `upsert`) с `ensure=True`, метод `create_index` может использовать устаревший кэш таблицы (`self._table`), который не содержит колонки, добавленные `ensure_columns`.

### Сценарий воспроизведения

1. Таблица `tg_posts` уже существует в БД с колонками: `id`, `text`, `image`
2. Вызывается `upsert_many` с новыми колонками: `channel`, `post_id`, `views`
3. `ensure_columns` добавляет новые колонки в БД
4. `create_index(keys=['channel', 'post_id'])` пытается создать индекс
5. **Ошибка**: `Column 'channel' does not exist in table 'tg_posts'`

### Причина

```python
# async_core.py, метод upsert (строки ~760-783)

if ensure:
    # Получаем таблицу (локальная переменная, НЕ self._table)
    table = await self._schema.get_table(...)

    # Добавляем колонки в БД
    await self._schema.ensure_columns(table, inferred_types)

    # Фильтруем ключи (использует локальную table + row.keys())
    table_cols = {col.name for col in table.columns} | set(row.keys())
    index_keys = [k for k in keys if k in table_cols]

    # ПРОБЛЕМА: create_index вызывает _get_table() внутри
    # Если self._table закэширован от ПРЕДЫДУЩЕГО вызова,
    # он вернёт устаревшую схему без новых колонок!
    if index_keys:
        await self.create_index(index_keys)

    # Кэш очищается ПОСЛЕ create_index (слишком поздно!)
    self._table = None
```

### Путь выполнения create_index

```python
# async_core.py, метод create_index (строки ~953-1015)

async def create_index(self, columns, ...):
    # Вызывает _get_table() который возвращает self._table если не None
    table = await self._get_table()

    # schema.create_index проверяет колонки в table.columns
    return await self._schema.create_index(table, columns, ...)

# schema.py, метод create_index (строки ~371-374)
table_cols = {col.name for col in table.columns}
for col_name in columns:
    if col_name not in table_cols:
        raise ColumnNotFoundError(col_name, table.name)  # <-- ОШИБКА!
```

## Как воспроизвести

### Минимальный тест

```python
import asyncio
from dbset import async_connect

async def test_stale_cache_bug():
    """
    Тест на баг с устаревшим кэшем.

    Для воспроизведения нужно:
    1. Создать таблицу с одними колонками
    2. Закэшировать self._table через любую операцию
    3. Вызвать upsert_many с НОВЫМИ колонками и keys
    """
    db = await async_connect('postgresql+asyncpg://user:pass@host/db')

    # Шаг 1: Создаём таблицу с базовыми колонками
    table = db['test_table']
    await table.insert({'name': 'test'}, ensure=True)

    # Шаг 2: Кэш self._table теперь содержит схему с колонкой 'name'
    # (insert вызывает _get_table() который кэширует)

    # Шаг 3: Вызываем upsert_many с НОВОЙ колонкой 'email' как ключом
    # ensure_columns добавит 'email' в БД, но self._table всё ещё старый
    try:
        await table.upsert_many([
            {'name': 'test', 'email': 'test@example.com'}
        ], keys=['email'], ensure=True)
        print("OK - баг не воспроизвёлся")
    except Exception as e:
        print(f"BUG: {e}")

    await db.close()

asyncio.run(test_stale_cache_bug())
```

### Реальный сценарий (instagram проект)

```bash
# moldova.sh
tele posts trendu -n 20 \
    --db-url postgresql+asyncpg://postgres:20_Meet_24#@home.dirs.info:5432/media \
    --table tg_posts \
    --tag MD \
    --delay 1
```

Ошибки:
```
Error saving posts for trendu: Column 'channel' does not exist in table 'tg_posts'
Error saving channel trendu: Column 'channel' does not exist in table 'tg_channels'
```

## Временное решение (workaround)

В коде приложения перед вызовом `upsert_many` очистить кэш вручную:

```python
# src/tele/core/tele_cli.py, функция save_to_db

if keys:
    # Workaround для dbset cache issue: очистить кэш таблицы перед upsert
    tbl._table = None
    count = await tbl.upsert_many(normalized, keys=keys, ensure=True)
```

## Попытка исправления и почему не сработала

### Попытка 1: Очистить кэш ДО create_index

```python
# Добавил в async_core.py:
self._table = None  # <-- добавлено
if index_keys:
    await self.create_index(index_keys)
self._table = None
```

**Результат**: Вызвало **race condition** в тестах!

```
FAILED dbset/tests/test_async_core.py::test_upsert - assert 30 == 31
# Тест flaky - иногда проходит, иногда падает
```

### Причина race condition

1. `self._table = None` вызывает `reflect()` при следующем `_get_table()`
2. `reflect()` делает `self._metadata.clear()` и затем рефлектирует из БД
3. Между `clear()` и `reflect()` есть окно где метаданные пустые
4. Параллельные async операции могут увидеть пустые метаданные

## Правильное решение (TODO)

### Вариант A: Передавать table в create_index

```python
# Изменить сигнатуру create_index
async def create_index(self, columns, table=None, ...):
    if table is None:
        table = await self._get_table()
    return await self._schema.create_index(table, columns, ...)

# В upsert/upsert_many после ensure_columns:
# Получить свежую таблицу и передать напрямую
fresh_table = await self._schema.get_table(self._name, ensure_exists=False)
if index_keys:
    await self.create_index(index_keys, table=fresh_table)
```

### Вариант B: Инвалидация кэша в ensure_columns

```python
async def ensure_columns(self, table, types):
    # ... добавляем колонки ...

    # Инвалидировать кэш таблицы после изменения схемы
    # Нужно как-то сообщить AsyncTable что кэш устарел
```

### Вариант C: Ленивая перезагрузка схемы

```python
# Добавить флаг "schema_dirty"
async def ensure_columns(self, table, types):
    # ... добавляем колонки ...
    self._schema_version += 1  # или подобный механизм

async def _get_table(self):
    if self._table is None or self._cached_schema_version != self._schema_version:
        self._table = await self._schema.get_table(...)
        self._cached_schema_version = self._schema_version
    return self._table
```

## Тестирование исправления

После реализации фикса, запустить:

```bash
cd /media/tumi/nvme2/prj/prj_upcode/dbset

# Все тесты должны пройти
pytest dbset/tests/ -v

# Особенно этот тест (он был flaky при неправильном фиксе)
for i in {1..10}; do pytest dbset/tests/test_async_core.py::test_upsert -q; done

# Тест на PostgreSQL (реальный сценарий)
pytest dbset/tests/test_async_core.py -v --db-url "postgresql+asyncpg://..."
```

## Связанные файлы

- `/media/tumi/nvme2/prj/prj_upcode/dbset/dbset/src/dbset/async_core.py` - async версия (строки 760-783, 953-1015)
- `/media/tumi/nvme2/prj/prj_upcode/dbset/dbset/src/dbset/sync_core.py` - sync версия (аналогичные места)
- `/media/tumi/nvme2/prj/prj_upcode/dbset/dbset/src/dbset/schema.py` - `reflect()` и `create_index` на уровне схемы
- `/media/tumi/nvme2/prj/prj_python/instagram/src/tele/core/tele_cli.py` - workaround в функции `save_to_db`

## Дата обнаружения

2026-02-06
