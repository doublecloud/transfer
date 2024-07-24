# Healthcheck

Healthcheck application provide check URLs and store results into YDB.

## Usage

### Running as application

```bash
go get -u github.com/yandex-cloud/ydb-go-sdk/v2/example/healthcheck
YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=/path/to/sa/key/file \
healthcheck \
   -link=grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1g8skpblkos03malf3s/etn01f8gv9an9sedo9fu \
   -url=www.ya.ru
   -url=google.com
   -url=rampler.ru
```

### Running as serverless function
Yandex function needs a go module project. First you must create go.mod file.
```bash
go mod init example && go mod tidy
zip archive.zip service.go go.mod
yc sls fn version create \
   --service-account-id=aje46n285h0re8nmm5u6 \
   --runtime=golang116 \
   --entrypoint=main.Serverless \
   --memory=128m \
   --execution-timeout=1s \
   --environment YDB=grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1g8skpblkos03malf3s/etn01f8gv9an9sedo9fu \
   --environment URLS=ya.ru,google.com,rambler.ru \
   --source-path=./archive.zip \
   --function-id=d4empp866m0b4m2gspu9
```
