# Используем официальный образ Python
FROM python:slim

# Устанавливаем зависимости для PostgreSQL
RUN apt update && apt upgrade -y && \
    apt install --no-install-recommends -y \
    clang libjpeg-dev \
    libjpeg62-turbo \
    libjpeg62-turbo-dev \
    libwebp-dev \
    zlib1g zlib1g-dev \
    gcc libpq-dev postgresql-client

# Создаём рабочую директорию
WORKDIR /app

# Копируем requirements
COPY requirements.txt .

# Копируем скрипт и папку data
COPY . /app

RUN apt-get update && apt-get install -y

# Устанавливаем Python зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Запуск скрипта
CMD ["python", "src/main.py"]
