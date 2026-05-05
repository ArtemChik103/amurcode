# Деплой на Render

Проект подготовлен для Render Web Service.

## Что уже настроено

- `app.py` читает порт из переменной окружения `PORT`.
- `python app.py` запускает FastAPI через uvicorn и слушает `0.0.0.0`, чтобы Render мог открыть приложение наружу.
- ASGI app доступен как `analytics.api:app`; legacy `Handler` сохранен для совместимости тестов.
- `requirements.txt` содержит зависимости для Excel/PDF-экспорта и FastAPI runtime.
- `render.yaml` описывает бесплатный web service.

## Деплой через GitHub

1. Залей репозиторий в GitHub. Если данные из `case/` чувствительные, используй приватный репозиторий.
2. Открой Render и выбери `New` -> `Blueprint`.
3. Подключи репозиторий.
4. Render прочитает `render.yaml` и создаст сервис.
5. Если нужен Groq assistant, добавь в Environment переменную `GROQ_API_KEY`.

## Ручные настройки, если не использовать Blueprint

- Type: `Web Service`
- Runtime: `Python`
- Build Command:

```bash
pip install -r requirements.txt
```

- Start Command:

```bash
python app.py
```

Прямой ASGI-запуск для локальной проверки:

```bash
uvicorn analytics.api:app --host 127.0.0.1 --port 8000
```

## Переменные окружения

Обязательных переменных нет.

Опционально:

```env
GROQ_API_KEY=
GROQ_MODEL=openai/gpt-oss-120b
ASSISTANT_ENABLED=auto
```

`GROQ_API_KEY` не нужно коммитить в репозиторий. Добавляй его только в настройках Render.

## Локальные runtime-файлы

`data/reviews.json` и `data/uploads/` не коммитятся. На Render free filesystem они не являются долговременным хранилищем между пересозданиями сервиса; для демо и локальной проверки это ожидаемое поведение.

## Проверка перед деплоем

```bash
python -m unittest discover -s tests -v
python app.py 8000
```

После запуска проверь `/`, `/api/meta`, `/api/query?view=as_of&date=2026-04-01&template=skk`, `/api/export.xlsx?date=2026-04-01&template=skk`, `/api/export.pdf?date=2026-04-01&template=skk`.
