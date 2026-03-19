# ---- Build Stage ----
FROM python:3.11-slim AS build

WORKDIR /build

COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install --no-cache-dir --prefix=/install -r requirements.txt

# ---- Runtime Stage ----
# Azure Functions host + Python 3.11 worker
FROM mcr.microsoft.com/azure-functions/python:4-python3.11 AS runtime

WORKDIR /home/site/wwwroot

# Non-root user (Azure Functions host runs as root by default in ACA;
# create the user but the host process needs root — use a dedicated app user
# for the file ownership only)
RUN groupadd --gid 1001 appgroup \
    && useradd --uid 1001 --gid appgroup --shell /bin/bash --no-create-home appuser

# Copy installed Python packages from build stage
COPY --from=build /install /usr/local

# Copy application code
COPY --chown=appuser:appgroup function_app.py host.json requirements.txt ./
COPY --chown=appuser:appgroup src/ ./src/
COPY --chown=appuser:appgroup schemas/ ./schemas/

# Azure Functions host port
EXPOSE 7071

# Managed identity — no hardcoded credentials.
# In ACA production: set AZURE_CLIENT_ID to the user-assigned managed identity client ID.
# DefaultAzureCredential will pick it up automatically.
# In local dev: AzureWebJobsStorage points to Azurite (set via docker-compose env).
ENV FUNCTIONS_WORKER_RUNTIME=python \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true

# /admin/host/status returns {"state":"Running",...} when the host is healthy
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:7071/admin/host/status || exit 1
