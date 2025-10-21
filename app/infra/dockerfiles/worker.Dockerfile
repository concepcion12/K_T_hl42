FROM python:3.11-slim

WORKDIR /app

RUN pip install poetry==1.8.3

COPY pyproject.toml poetry.lock* ./

RUN poetry config virtualenvs.create false && poetry install --no-interaction --no-ansi

COPY . .

CMD ["rq", "worker", "--url", "redis://redis:6379/0", "connectors"]

