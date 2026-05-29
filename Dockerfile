FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
COPY sql ./sql
RUN pip install --no-cache-dir .
ENTRYPOINT ["python", "-m", "alation_rdf_sync"]
