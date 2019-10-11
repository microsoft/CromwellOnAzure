# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

FROM mcr.microsoft.com/dotnet/core/sdk:3.0 AS build-env
WORKDIR /app
COPY ./TriggerService/*.* ./TriggerService/
COPY ./CromwellApiClient/*.* ./CromwellApiClient/
COPY ./TriggerService.Core/*.* ./TriggerService.Core/
WORKDIR /app/TriggerService
RUN dotnet publish -c Release -o out
FROM mcr.microsoft.com/dotnet/core/runtime:3.0
WORKDIR /app
COPY --from=build-env /app/TriggerService/out .
ENTRYPOINT ["dotnet", "TriggerService.dll"]
