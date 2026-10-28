FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir .

# Mount a volume here so the warehouse files survive between container runs
ENV STOCKS_WAREHOUSE_DIR=/data/warehouse
VOLUME ["/data"]

ENTRYPOINT ["stock-pipeline"]
