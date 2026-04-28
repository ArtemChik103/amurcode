# Конструктор аналитических выборок

Рабочий MVP для кейса БФТ и Минфина Амурской области. Приложение запускается одной командой, читает CSV из папки `case`, загружает данные в память и отдаёт локальный UI без PostgreSQL, FastAPI, Django и отдельного ETL-сервиса.

## Запуск

```powershell
python app.py 8000
```

Открыть в браузере:

```text
http://127.0.0.1:8000
```

## Frontend

Frontend сделан на Vue 3 через CDN, без Vite, npm, Vue Router, Pinia и сборки `.vue` компонентов. Backend по-прежнему напрямую раздаёт файлы из `static`, поэтому запуск остаётся `python app.py 8000`.

- `static/index.html` - декларативный Vue-шаблон с `#app`.
- `static/app.js` - Vue Options API приложение через `Vue.createApp(...)`.
- `static/styles.css` - стили интерфейса, loading/error/empty состояния и оформление compare delta.

## Тесты

```powershell
python -m unittest discover -s tests -v
```

Тесты написаны на `unittest` и также могут запускаться через `pytest`, если он установлен:

```powershell
python -m pytest
```

## Архитектура

- `app.py` - локальный HTTP-сервер на `ThreadingHTTPServer`, загрузка CSV, нормализация, фильтрация, агрегация и JSON API.
- `static/index.html` - рабочий экран конструктора.
- `static/app.js` - фильтры, шаблоны, метрики, сравнение, trace-модалка, canvas-график и CSV-экспорт.
- `static/styles.css` - стили интерфейса.
- `case/` - исходные данные кейса.
- `docs/` - текущее состояние MVP и предметные правила.

Данные загружаются в глобальный `STORE` при старте процесса. Базы данных, миграций, фоновых импортов и постоянного кеша результатов нет.

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

## Ограничения

- суммы хранятся как `float`, не `Decimal`;
- модель данных in-memory, trace указывает источник и порядковый номер записи внутри источника, но не всегда физическую строку конкретного CSV;
- ГЗ использует дату контракта/платежа как snapshot;
- БУ/АУ snapshot определяется первым числом месяца из имени файла;
- quality endpoint уже есть, но текущие загрузчики в основном сохраняют нулевую сводку, потому что историческая загрузка не прерывалась на ошибках парсинга.
