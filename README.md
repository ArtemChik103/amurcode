# Простая аналитика расходов

Локальный MVP для кейса БФТ и Минфина Амурской области. Приложение запускается одной командой, читает CSV из `case/`, держит данные в памяти и отдаёт Vue 3 UI без Vite, npm, PostgreSQL и отдельного ETL-сервиса.

## Запуск

```powershell
python app.py 8000
```

Открыть в браузере:

```text
http://127.0.0.1:8000
```

## Простой режим

Первый экран построен вокруг задач, а не фильтров. Пользователь может нажать быстрый сценарий, ввести код или название в единую строку, получить короткий вывод, посмотреть понятную таблицу со статусами и скачать таблицу.

## Быстрый старт

Доступны готовые сценарии:

- Показать СКК.
- Показать КИК.
- Показать 2/3.
- Показать ОКВ.
- Сравнить СКК.
- Найти проблемы с исполнением.

Все сценарии используют существующие `/api/query` и `/api/compare`.

## Assistant

Endpoint `POST /api/assistant` принимает обычный текст и возвращает intent, объяснение и действие для UI. Без `GROQ_API_KEY` assistant работает по правилам. Если ключ задан, он может использовать Groq как enhancer, но суммы всё равно считает только backend.

Опциональные переменные окружения:

```env
GROQ_API_KEY=
GROQ_MODEL=openai/gpt-oss-120b
ASSISTANT_ENABLED=auto
```

В Groq не отправляются raw records. Используются только запрос пользователя, список шаблонов, список метрик, доступные даты и короткий RAG-контекст из `docs/rag`.

## API

- `GET /api/meta`
- `GET /api/query?q=&code=&budget=&source=&start=&end=&template=&metrics=`
- `GET /api/compare?base=&target=&q=&code=&budget=&source=&template=&metrics=`
- `GET /api/quality`
- `GET /api/trace?id=`
- `GET /api/catalog/dates`
- `GET /api/catalog/sources`
- `GET /api/catalog/budgets`
- `GET /api/catalog/templates`
- `GET /api/catalog/metrics`
- `GET /api/catalog/objects?q=&template=`
- `GET /api/catalog/quick-actions`
- `POST /api/assistant`

## Тесты

```powershell
python -m unittest discover -s tests -v
```

## Ограничения

- Данные хранятся in-memory.
- Суммы хранятся как `float`, не `Decimal`.
- Trace показывает источник, файл и порядковую строку записи внутри источника; это не всегда физическая строка CSV.
- Векторная база не используется. Мини-RAG реализован чтением markdown из `docs/rag`.
