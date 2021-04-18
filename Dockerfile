# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

FROM mcr.microsoft.com/dotnet/sdk:3.1 AS build-env
WORKDIR /app
COPY ./src/TriggerService/*.* ./TriggerService/
COPY ./src/CromwellApiClient/*.* ./CromwellApiClient/
COPY ./src/Common/*.* ./Common/
WORKDIR /app/TriggerService
RUN dotnet publish -c Release -o out
FROM mcr.microsoft.com/dotnet/runtime:3.1
WORKDIR /app
COPY --from=build-env /app/TriggerService/out .
ENTRYPOINT ["dotnet", "TriggerService.dll"]
