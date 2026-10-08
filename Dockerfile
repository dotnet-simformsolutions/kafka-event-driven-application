# Single Dockerfile for both OrderFlow services, selected by build target.
#
#   docker build --target producer -t orderflow-producer .
#   docker build --target consumer -t orderflow-consumer .
#
# docker-compose.yml picks the target per service, so `docker compose build` produces
# both images from this one file. Build context is the repository root, because each
# service needs the sibling Contracts and Shared projects.
#
# Note: with two final stages, a bare `docker build .` (no --target) builds the LAST
# stage only, which is the consumer. Always pass --target when building by hand.

# ─── Build: restore and publish both services in one pass ────────────────────
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /source

# Copy only the project files first so the restore layer stays cached until a
# dependency actually changes, not on every source edit.
COPY src/OrderFlow.Contracts/OrderFlow.Contracts.csproj  src/OrderFlow.Contracts/
COPY src/OrderFlow.Shared/OrderFlow.Shared.csproj        src/OrderFlow.Shared/
COPY src/OrderFlow.Producer/OrderFlow.Producer.csproj    src/OrderFlow.Producer/
COPY src/OrderFlow.Consumer/OrderFlow.Consumer.csproj    src/OrderFlow.Consumer/

RUN dotnet restore src/OrderFlow.Producer/OrderFlow.Producer.csproj \
 && dotnet restore src/OrderFlow.Consumer/OrderFlow.Consumer.csproj

COPY src/ src/

# Publish to separate directories so each runtime stage copies only its own output.
RUN dotnet publish src/OrderFlow.Producer/OrderFlow.Producer.csproj \
      -c Release -o /app/producer --no-restore \
 && dotnet publish src/OrderFlow.Consumer/OrderFlow.Consumer.csproj \
      -c Release -o /app/consumer --no-restore

# ─── Runtime: producer (serves HTTP, so needs the ASP.NET stack) ─────────────
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS producer
WORKDIR /app

# curl is needed by the compose healthcheck; the aspnet image does not ship it.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/producer .

# The aspnet image already sets HTTP_PORTS=8080; setting ASPNETCORE_URLS as well
# makes the host log an override warning at startup, so leave the default alone.
ENV DOTNET_EnableDiagnostics=0

EXPOSE 8080

# Run as the image's non-root user rather than root.
USER $APP_UID

ENTRYPOINT ["dotnet", "OrderFlow.Producer.dll"]

# ─── Runtime: consumer (no HTTP listener, so the smaller base image) ─────────
FROM mcr.microsoft.com/dotnet/runtime:10.0 AS consumer
WORKDIR /app

COPY --from=build /app/consumer .

ENV DOTNET_EnableDiagnostics=0

USER $APP_UID

ENTRYPOINT ["dotnet", "OrderFlow.Consumer.dll"]
