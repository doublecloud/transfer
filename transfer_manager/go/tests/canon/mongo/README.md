### local run

Start docker for server:
```shell
docker run -d --name some-mongo -p 27117:27017 \
	-e MONGO_INITDB_ROOT_USERNAME=user \
	-e MONGO_INITDB_ROOT_PASSWORD=password \
	-e MONGO_INITDB_DATABASE=canondb \
	mongo
```

end then:

```shell
CGO_ENABLED=0;
DB0_MONGO_LOCAL_PASSWORD=password;
DB0_MONGO_LOCAL_PORT=27117;
DB0_MONGO_LOCAL_USER=user;
```
