# Linux image: the engine plus the Python runtime its transforms run on.
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY nuget.config Hie.slnx ./
COPY src/Hie/Hie.csproj src/Hie/
RUN dotnet restore src/Hie/Hie.csproj
COPY src/Hie/ src/Hie/
RUN dotnet publish src/Hie/Hie.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /hie
COPY --from=build /app /app
COPY hie_runtime/ /hie/hie_runtime/
COPY transforms/ /hie/transforms/
COPY feeds/ /hie/feeds/
COPY hie.yaml /hie/hie.yaml
ENV HIE_CONFIG=/hie/hie.yaml \
    HIE_RUNTIME_DIR=/hie/hie_runtime \
    HIE_LOG=Information
EXPOSE 8090
ENTRYPOINT ["dotnet", "/app/hie.dll"]
