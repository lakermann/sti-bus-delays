# STI Bus Delays

## Actual data downloader

Event JSON to download the dataset from a custom dataset URL:

```json
{
  "dataset-url": "https://opentransportdata.swiss/dataset/0edc74a3-ad4d-486e-8657-f8f3b34a0979/resource/19095461-4ded-4678-9c9b-442ae3a834d3/download/2024-08-18_istdaten.csv"
}
```

## Deployment

Prerequisites:
- AWS CLI, Typescript & AWS CDK CLI
  - <https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html>
- Docker

Deploy stack to your default AWS account/region:

```shell
cd infrastructure
cdk deploy
```

### Infrastructure Development

Install dependencies:

```shell
cd infrastructure
npm install
```

Run tests:

```shell
cd infrastructure
npm run build
npm run test
```

## Application Development

Prerequisites:
- Homebrew (package manager for macOS)
  - <https://brew.sh/>

Install pipx:

```shell
brew install pipx
pipx ensurepath
```

Install poetry:

```shell
pipx install poetry==1.8.3
```

Install dependencies (for each application):

```shell
cd application/actual-data-downloader
poetry install
```

### Run tests 

Run minio (local object storage):

```shell
docker compose up -d
```

Run all tests (for each application):

```shell
cd application/actual-data-downloader
poetry run pytest 
```

Run unit tests (for each application):

```shell
cd application/actual-data-downloader
poetry run pytest tests/unit
```

Run integration tests (for each application):

```shell
cd application/actual-data-downloader
poetry run pytest tests/integration
```

Access to minio:
- URL: <http://localhost:9001/>
- Username: minio
- Password: minio123
