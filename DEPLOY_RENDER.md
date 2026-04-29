# Деплой на Render

Проект подготовлен для Render Web Service.

## Что уже настроено

- `app.py` читает порт из переменной окружения `PORT`.
- Сервер слушает `0.0.0.0`, чтобы Render мог открыть приложение наружу.
- `requirements.txt` содержит зависимости для Excel/PDF-экспорта.
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

## Переменные окружения

Обязательных переменных нет.

Опционально:

```env
GROQ_API_KEY=
GROQ_MODEL=openai/gpt-oss-120b
ASSISTANT_ENABLED=auto
```

`GROQ_API_KEY` не нужно коммитить в репозиторий. Добавляй его только в настройках Render.
