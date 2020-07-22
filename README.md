# Kafka Connect Apache Pinot

kafka-connect-apache-pinot is a [Kafka Connector](http://kafka.apache.org/documentation.html#connect)
for loading data to and from any [Apache Pinot](https://pinot.apache.org/).

# Running in development
To build a development version you'll need a recent version of Kafka as well as a set of upstream Confluent projects, 
which you'll have to build from their appropriate snapshot branch. See the [FAQ](https://github.com/mhomaid/kafka-connect-apache-pinot/wiki/FAQ)
for guidance on this process.

You can build kafka-connect-apache-pinot with Maven using the standard lifecycle phases.

The [docker-compose.yml](docker/kafka/docker-compose.yml) that is included in this repository is based on the Confluent Platform Docker
images. Take a look at the [quickstart](http://docs.confluent.io/current/cp-docker-images/docs/quickstart.html#getting-started-with-docker-client)
for the Docker images. 

Your development workstation needs to be able to resolve the hostnames that are listed in the `docker-compose.yml` 
file in the root of this repository. If you are using [Docker for Mac](https://docs.docker.com/v17.12/docker-for-mac/install/)
your containers will be available at the ip address `127.0.0.1`. If you are running docker-machine
you will need to determine the ip address of the virtual machine with `docker-machine ip confluent`
to determine the ip address.

```
127.0.0.1 zookeeper
127.0.0.1 kafka
127.0.0.1 schema-registry
```


```
docker-compose up -d
```

The debug script assumes that `connect-standalone` is in the path on your local workstation. Download 
the latest version of the [Kafka](https://www.confluent.io/download/) to get started.

# FAQ

Refer frequently asked questions on Kafka Connect Apache Pinot here -
https://github.com/mhomaid/kafka-connect-apache-pinot/wiki/FAQ

# Contribute

Contributions can only be accepted if they contain appropriate testing. For example, adding a new dialect of JDBC will require an integration test.

- Source Code: https://github.com/mhomaid/kafka-connect-apache-pinot
- Issue Tracker: https://github.com/mhomaid/kafka-connect-apache-pinot/issues

# License