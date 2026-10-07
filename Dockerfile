FROM python:3.14-slim AS deps

WORKDIR /build

# Копируем только файл зависимостей для использования кэша Docker
COPY requirements.txt .

# Устанавливаем зависимости в изолированный префикс.
# --no-cache-dir уменьшает размер слоя.
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# Финальная стадия (Runtime)
FROM python:3.14-slim

WORKDIR /app

# Копируем установленные зависимости из стадии deps.
# Префикс /install корректно ложится в /usr/local, 
# который уже находится в PATH и PYTHONPATH базового образа.
COPY --from=deps /install /usr/local
ENV PYTHONUNBUFFERED=1
# Копируем исходный код проекта
COPY . .

# Создаем непривилегированного пользователя для безопасности
RUN useradd -m -r appuser && chown -R appuser:appuser /app
USER appuser

# Точка входа
ENTRYPOINT ["python", "frigate_s3_archiver.py"]