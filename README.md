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

Первый экран построен вокруг задач, а не фильтров. Пользователь может нажать быстрый сценарий, ввести код или название в единую строку, получить короткий вывод на отчетную дату, посмотреть таблицу объектов со статусами и скачать таблицу.

## Быстрый старт

Доступны готовые сценарии:

- Собрать отчет СКК.
- Собрать отчет КИК.
- Собрать отчет 2/3.
- Собрать отчет ОКВ.
- Найти проблемные объекты.
- Сравнить две даты.
- Найти объект.

UI использует `/api/query?view=as_of&date=...` и `/api/compare`. Старый периодический режим `/api/query` без `view` сохранен для совместимости.

## Семантика дат

Основной режим MVP - состояние на дату. РЧБ и соглашения считаются месячными срезами с семантикой `balance_as_of`: выбирается последний срез не позже выбранной даты, а не сумма срезов за период. Контракты, платежи и БУАУ считаются событиями с накоплением до выбранной даты.

Отчетные даты берутся только из РЧБ и соглашений через `GET /api/catalog/reporting-dates`, чтобы пользователь не выбирал одиночную дату платежа как дату месячного среза.

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
- `GET /api/query?view=as_of&date=&q=&code=&budget=&source=&template=&metrics=&post_filter=`
- `GET /api/query?q=&code=&budget=&source=&start=&end=&template=&metrics=` legacy period mode
- `GET /api/compare?base=&target=&q=&code=&budget=&source=&template=&metrics=`
- `GET /api/quality`
- `GET /api/trace?id=`
- `GET /api/catalog/dates`
- `GET /api/catalog/reporting-dates`
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

В набор входят backend-тесты, безбраузерные проверки Vue-логики через Node VM и Playwright-тесты реальных кликов в Chromium. Для новой машины:

```powershell
python -m pip install playwright
python -m playwright install chromium
```

## Ограничения

- Данные хранятся in-memory.
- Суммы хранятся как `float`, не `Decimal`.
- Trace показывает источник, файл и строку исходного CSV там, где она доступна.
- Векторная база не используется. Мини-RAG реализован чтением markdown из `docs/rag`.
