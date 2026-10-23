---
name: dockerfile
description: >
  Generate production-ready Dockerfiles and docker-compose files for Python
  Azure Functions, .NET BFF APIs, and supporting services. Enforces
  multi-stage builds, non-root users, managed identity compatibility,
  minimal image size, and consistent local dev parity with Azure Container Apps.
  Use when containerizing any service in this project or adding a new service
  to docker-compose.
---

# Dockerfile Skill — Project Standards

## Stack Coverage

| Service Type          | Base Image                          | Target Runtime         |
|-----------------------|-------------------------------------|------------------------|
| Python pipeline/funcs | `python:3.11-slim`                  | Azure Container Apps   |
| .NET BFF API          | `mcr.microsoft.com/dotnet/aspnet:8` | Azure Container Apps   |
| Local dev deps        | `mcr.microsoft.com/azure-storage/azurite` | docker-compose only |

---

## Non-Negotiable Standards (Apply to Every Dockerfile)

1. **Multi-stage build** — separate `build` and `runtime` stages. Never ship build tools to production.
2. **Non-root user** — always create and switch to a dedicated user in the final stage.
3. **Pinned base image tags** — never use `latest`. Use specific version tags.
4. **`.dockerignore` required** — always generate alongside the Dockerfile.
5. **Health check** — every service must declare a `HEALTHCHECK`.
6. **Managed identity compatible** — no secrets in image. Credentials via environment variables or Azure SDK DefaultAzureCredential.
7. **Deterministic dependency install** — use lock files (`requirements.txt` pinned, `packages.lock.json` for .NET).

---

## Pattern 1: Python Azure Functions / Data Pipeline

```dockerfile
# ---- Build Stage ----
FROM python:3.11-slim AS build

WORKDIR /build

# Install build deps separately for layer caching
COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install --no-cache-dir --prefix=/install -r requirements.txt

# ---- Runtime Stage ----
FROM python:3.11-slim AS runtime

WORKDIR /app

# Non-root user
RUN groupadd --gid 1001 appgroup \
    && useradd --uid 1001 --gid appgroup --shell /bin/bash --create-home appuser

# Copy installed packages from build stage
COPY --from=build /install /usr/local

# Copy application code
COPY --chown=appuser:appgroup . .

# Azure Functions host port
EXPOSE 7071

# Managed identity — no hardcoded credentials
# DefaultAzureCredential will use:
# - AZURE_CLIENT_ID (user-assigned managed identity in ACA)
# - Workload identity in AKS
# - Local: az login / environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    FUNCTIONS_WORKER_RUNTIME=python

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:7071/api/health || exit 1

USER appuser

CMD ["func", "start", "--python"]
```

**.dockerignore for Python:**
```
__pycache__/
*.pyc
*.pyo
*.pyd
.Python
.env
.env.*
!.env.example
.venv/
venv/
.pytest_cache/
.coverage
cov_annotate/
*.egg-info/
dist/
.git/
.gitignore
local.settings.json
```

---

## Pattern 2: .NET 8 BFF API (OpenIddict)

```dockerfile
# ---- Build Stage ----
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build

WORKDIR /src

# Restore dependencies first (layer cache optimization)
COPY ["src/YourApi/YourApi.csproj", "src/YourApi/"]
RUN dotnet restore "src/YourApi/YourApi.csproj" --locked-mode

# Copy source and build
COPY src/ src/
WORKDIR /src/src/YourApi
RUN dotnet publish "YourApi.csproj" \
    -c Release \
    -o /app/publish \
    --no-restore \
    /p:UseAppHost=false

# ---- Runtime Stage ----
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime

WORKDIR /app

# Non-root user
RUN groupadd --gid 1001 appgroup \
    && useradd --uid 1001 --gid appgroup --shell /bin/bash --create-home appuser

COPY --from=build --chown=appuser:appgroup /app/publish .

# ASP.NET Core
EXPOSE 8080
ENV ASPNETCORE_URLS=http://+:8080 \
    ASPNETCORE_ENVIRONMENT=Production \
    DOTNET_RUNNING_IN_CONTAINER=true

# OpenIddict / Azure — no secrets in image
# Inject via ACA secrets or Key Vault references:
# AZURE_CLIENT_ID, ConnectionStrings__SecurityDb, OpenIddict__*

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

USER appuser

ENTRYPOINT ["dotnet", "YourApi.dll"]
```

**.dockerignore for .NET:**
```
**/.git/
**/.gitignore
**/.vs/
**/.vscode/
**/bin/
**/obj/
**/*.user
**/appsettings.Development.json
**/secrets.json
.env
.env.*
!.env.example
**/TestResults/
```

---

## Pattern 3: docker-compose for Local Development

Full local stack with Azurite, SQL Server, and both services:

```yaml
# docker-compose.yml
services:

  # ---- Azure Storage Emulator ----
  azurite:
    image: mcr.microsoft.com/azure-storage/azurite:3.29.0
    container_name: azurite
    command: azurite --blobHost 0.0.0.0 --queueHost 0.0.0.0 --tableHost 0.0.0.0
    ports:
      - "10000:10000"   # Blob
      - "10001:10001"   # Queue
      - "10002:10002"   # Table
    volumes:
      - azurite_data:/data
    healthcheck:
      test: ["CMD", "nc", "-z", "localhost", "10000"]
      interval: 10s
      timeout: 5s
      retries: 5

  # ---- SQL Server (SecurityDB / PlatformDB) ----
  sqlserver:
    image: mcr.microsoft.com/mssql/server:2022-latest
    container_name: sqlserver
    environment:
      SA_PASSWORD: "${SQL_SA_PASSWORD}"
      ACCEPT_EULA: "Y"
      MSSQL_PID: Developer
    ports:
      - "1433:1433"
    volumes:
      - sqlserver_data:/var/opt/mssql
    healthcheck:
      test: ["CMD", "/opt/mssql-tools/bin/sqlcmd", "-S", "localhost",
             "-U", "sa", "-P", "${SQL_SA_PASSWORD}", "-Q", "SELECT 1"]
      interval: 15s
      timeout: 10s
      retries: 10
      start_period: 30s

  # ---- .NET BFF API ----
  bff-api:
    build:
      context: .
      dockerfile: src/YourApi/Dockerfile
      target: runtime
    container_name: bff-api
    depends_on:
      sqlserver:
        condition: service_healthy
    environment:
      ASPNETCORE_ENVIRONMENT: Development
      ASPNETCORE_URLS: http://+:8080
      ConnectionStrings__SecurityDb: "Server=sqlserver;Database=SecurityDB;User Id=sa;Password=${SQL_SA_PASSWORD};TrustServerCertificate=true"
      # Managed identity replaced by local env vars in dev:
      AZURE_CLIENT_ID: "${AZURE_CLIENT_ID}"
      AZURE_TENANT_ID: "${AZURE_TENANT_ID}"
      AZURE_CLIENT_SECRET: "${AZURE_CLIENT_SECRET}"
    ports:
      - "8080:8080"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 15s
      timeout: 5s
      retries: 5

  # ---- Python Pipeline ----
  pipeline:
    build:
      context: ./pipeline
      dockerfile: Dockerfile
      target: runtime
    container_name: pipeline
    depends_on:
      azurite:
        condition: service_healthy
    environment:
      PYTHONUNBUFFERED: "1"
      AZURE_STORAGE_CONNECTION_STRING: "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IcFD/PDSzB3aHe0nBF3nKA==;BlobEndpoint=http://azurite:10000/devstoreaccount1;QueueEndpoint=http://azurite:10001/devstoreaccount1;TableEndpoint=http://azurite:10002/devstoreaccount1"
      # In ACA production, this is replaced by managed identity (AZURE_CLIENT_ID only)
      FUNCTIONS_WORKER_RUNTIME: python
    ports:
      - "7071:7071"

volumes:
  azurite_data:
  sqlserver_data:
```

**`.env.example`** (commit this, never `.env` itself):
```env
SQL_SA_PASSWORD=YourStrong@Password123
AZURE_CLIENT_ID=
AZURE_TENANT_ID=
AZURE_CLIENT_SECRET=
```

---

## Azure Container Apps Deployment Notes

When deploying to ACA, replace local credential env vars with:

```yaml
# ACA managed identity — no secrets needed in app config
identity:
  type: UserAssigned
  userAssignedIdentities:
    /subscriptions/.../managedIdentities/your-identity: {}

env:
  - name: AZURE_CLIENT_ID
    value: "<managed-identity-client-id>"
  # All other Azure SDK calls use DefaultAzureCredential automatically
```

Never set `AZURE_CLIENT_SECRET` in ACA — managed identity replaces it.

---

## Checklist Before Committing a Dockerfile

- [ ] Multi-stage build — build tools not in final image
- [ ] Non-root user created and switched to
- [ ] Base image tag is pinned (not `latest`)
- [ ] `.dockerignore` exists and excludes `*.env`, `local.settings.json`, `obj/`, `bin/`
- [ ] `HEALTHCHECK` defined
- [ ] No secrets, passwords, or connection strings hardcoded
- [ ] `docker build` completes without warnings
- [ ] `docker run` starts and health check passes
- [ ] Service connects to Azurite (not real Azure) in local dev
- [ ] `docker-compose up` brings full stack up cleanly from scratch
