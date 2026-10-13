FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir .

# Mount a volume here so the database survives between container runs
ENV STOCKS_DB_PATH=/data/stocks.duckdb
VOLUME ["/data"]

ENTRYPOINT ["stock-pipeline"]
