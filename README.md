# pull_crypto_details

##This is for creating a docker mysql image and then ingesting crypto details

## Build Docker Image

* Start Docker Desktop

* Build Image in CLI

```shell
docker build -t local-mysql .
```

* Run Container

```shell
docker run -dp 3306:3306 local-mysql
```
