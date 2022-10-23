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
##Ensure you have python requirements
 
* Depending on what version of python you are using, you might need to use pip3


```shell
pip install -r requirements
```
