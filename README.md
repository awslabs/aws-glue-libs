# aws-glue-libs
This repository contains:
 * `awsglue` - the Python libary you can use to author [AWS Glue](https://aws.amazon.com/glue) ETL job. This library extends [Apache Spark](https://spark.apache.org/) with additional data types and operations for ETL workflows. It's an interface for Glue ETL library in Python.
 * `bin` - this directory hosts several executables that allow you to run the Python library locall or open up a PySpark shell to run Glue Spark code interactively.


## Setup guide

The `awsglue` library provides only the Python interface to the Glue Spark runtime, you need the Glue ETL jar to run it locally. The jar is now available via the maven build system in a s3 backed maven repository. Here are the steps to set up your dev environment locally.

1. install Apache Maven from the following location: https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-common/apache-maven-3.6.0-bin.tar.gz
1. use `copy-dependencies` target in Apache Maven to download the jar from S3 to your local dev environment.
1. download and extract the Apache Spark distribution based on the Glue version you're using:
   * Glue version 0.9: `https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-0.9/spark-2.2.1-bin-hadoop2.7.tgz`
   * Glue version 1.0: `https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-1.0/spark-2.4.3-bin-hadoop2.8.tgz1`
1. export the `SPARK_HOME` environmental variable to the extracted location of the above Spark distribution. For example:
    ```
    Glue version 0.9: export SPARK_HOME=/home/$USER/spark-2.2.1-bin-hadoop2.7
    Glue version 1.0: export SPARK_HOME=/home/$USER/spark-2.4.3-bin-spark-2.4.3-bin-hadoop2.8
    ```
1. now you can run the executables in the `bin` directory to start a Glue Shell or submit a Glue Spark application.
    ```
    Glue shell: ./bin/gluepyspark
    Glue submit: ./bin/gluesparksubmit
    pytest: ./bin/gluepytest
    ```
(The `gluepytest` script assumes that the pytest module is installed and available in the `PATH` env variable)

## Licensing

The libraries in this repository licensed under the [Amazon Software License](http://aws.amazon.com/asl/) (the "License"). They may not be used except in compliance with the License, a copy of which is included here in the LICENSE file.

