# Authenticate with environ

`environ` example provide code snippet for authenticate to YDB with environ variables

Authenticate produced with one of next environment variables:

* YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=<path/to/sa_key_file> — used service account key file by path
* YDB_ANONYMOUS_CREDENTIALS="1" — used for authenticate with anonymous access. Anonymous access needs for connect to testing YDB installation
* YDB_METADATA_CREDENTIALS="1" — used metadata service for authenticate to YDB from yandex cloud virtual machine or from yandex function
* YDB_ACCESS_TOKEN_CREDENTIALS=<access_token> — used for authenticate to YDB with short-life access token. For example, access token may be IAM token

## Runing code snippet
```bash
YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=/Users/user/.ydb/sa.jsoon environ -ydb="grpcs://endpoint/?database=database"
# or
YDB_ANONYMOUS_CREDENTIALS="1" environ -ydb="grpcs://endpoint/?database=database"
# or
YDB_METADATA_CREDENTIALS="1" environ -ydb="grpcs://endpoint/?database=database"
# or
YDB_ACCESS_TOKEN_CREDENTIALS="YDB_ACCESS_TOKEN" environ -ydb="grpcs://endpoint/?database=database"
```
