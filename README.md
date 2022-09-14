# data-cleaning Project

## Preparations

### Prerequisites

- JDK 11
- Python 3.8+ (with pip)

### Running the model

Download the pretained model & and extract it

```shell script
cd model
wget https://drive.google.com/file/d/10ayQ4r8VE2EGJvFWEetVyjuuqSILLevj/view?usp=sharing
tar -xzvf ontology-matching-base-uncased.tar.gz
```

Install the requirements

```shell script
source venv/bin/activate
pip install -r requirements.txt
```

Run the model

```shell script
python serve.py
```

### Run the matcher

### Getting Google's Word2Vec model

```shell script
wget -P data https://dl4jdata.blob.core.windows.net/resources/wordvectors/GoogleNews-vectors-negative300.bin.gz
```

If you are not using Linux x86 you need to adapt the `cpu.arch` property in the [pom.xml](pom.xml) according to your
machine.

This project uses Quarkus, the Supersonic Subatomic Java Framework.

If you want to learn more about Quarkus, please visit its website: https://quarkus.io/ .

## Running the application in dev mode

You can run your application in dev mode that enables live coding using:

```shell script
./mvnw compile quarkus:dev
```

> **_NOTE:_** If using Word2Vec you need to increase the memory limit of the JVM. E.g. execute
> with `-Xms1024m -Xmx10g -XX:MaxPermSize=2g`
>

## Packaging and running the application

The application can be packaged using:

```shell script
./mvnw package
```

It produces the `quarkus-run.jar` file in the `target/quarkus-app/` directory.
Be aware that it’s not an _über-jar_ as the dependencies are copied into the `target/quarkus-app/lib/` directory.

The application is now runnable using `java -jar target/quarkus-app/quarkus-run.jar`.

If you want to build an _über-jar_, execute the following command:

```shell script
./mvnw package -Dquarkus.package.type=uber-jar
```

> **Note**: Creating an über-jar is time consuming

The application, packaged as an _über-jar_, is now runnable using `java -jar target/*-runner.jar`.

## Usage

```shell script
java -jar target/quarkus-app/quarkus-run.jar instance -kb data/dbpedia -wt data/webtables/ -t 0.5
```

See the provided help command for usage instructions.
