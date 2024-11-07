# mizu-node

## Installation and usage


### Install

```shell
git clone <this repo> validator
cd validator
poetry install
poetry run pre-commit install
poetry self add poetry-plugin-dotenv
```

Connect to redis and run following command to enable keyspace notification:
```shell
config set notify-keyspace-events Ex
```

### Environment variables

Mandatory:
- `VERIFY_JOB_QUEUE`
- `VERIFY_JOB_CALLBACK_URL`

### Run

```shell
poetry run dev # for dev
poetry run start # for prod
```

### Run as a dependency for other services
```shell
docker-compose up --build # This will start all services required along with `poetry run start`
```

### Test

```shell
poetry run pytest
```
