# aws-glue-libs

This repository supports python libraries for local development of glue pyspark batch jobs. Glue streaming is not supported with this library.

## Contents
This repository contains:
 * `awsglue` - the Python libary you can use to author [AWS Glue](https://aws.amazon.com/glue) ETL job. This library extends [Apache Spark](https://spark.apache.org/) with additional data types and operations for ETL workflows. It's an interface for Glue ETL library in Python.
 * `bin` - this directory hosts several executables that allow you to run the Python library locally or open up a PySpark shell to run Glue Spark code interactively.

## Python versions by Glue Version

Different Glue versions support different Python versions. The following table below is for your reference, which also includes the associated repository's branch for each glue version.

| Glue Version  | Python 2 Version  | Python 3 Version  | aws-glue-libs branch|
|---|---|---| --- |
| 0.9  | 2.7  | Not supported | glue-0.9 |
| 1.0  | 2.7  | 3.6  | glue-1.0 |
| 2.0  |  Not supported | 3.7  | glue-2.0 |
| 3.0  | Not supported  | 3.7  | master |

You may refer to AWS Glue's official [release notes](https://docs.aws.amazon.com/glue/latest/dg/release-notes.html) for more information

## Setup guide

If you haven't already, please refer to the [official AWS Glue Python local development documentation](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html#develop-local-python) for the official setup documentation. The following is a summary of the AWS documentation:

The `awsglue` library provides only the Python interface to the Glue Spark runtime, you need the Glue ETL jar to run it locally. The jar is now available via the maven build system in a s3 backed maven repository. Here are the steps to set up your dev environment locally.

1. install Apache Maven from the following location: https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-common/apache-maven-3.6.0-bin.tar.gz
1. use `copy-dependencies` target in Apache Maven to download the jar from S3 to your local dev environment.
1. download and extract the Apache Spark distribution based on the Glue version you're using:
   * Glue version 0.9: `https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-0.9/spark-2.2.1-bin-hadoop2.7.tgz`
   * Glue version 1.0: `https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-1.0/spark-2.4.3-bin-hadoop2.8.tgz1`
   * Glue version 2.0: `https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-2.0/spark-2.4.3-bin-hadoop2.8.tgz1`
   * Glue version 3.0: `https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-3.0/spark-3.1.1-amzn-0-bin-3.2.1-amzn-3.tgz`
1. export the `SPARK_HOME` environmental variable to the extracted location of the above Spark distribution. For example:
    ```
    Glue version 0.9: export SPARK_HOME=/home/$USER/spark-2.2.1-bin-hadoop2.7
    Glue version 1.0: export SPARK_HOME=/home/$USER/spark-2.4.3-bin-hadoop2.8
    Glue version 2.0: export SPARK_HOME=/home/$USER/spark-2.4.3-bin-hadoop2.8
    Glue version 3.0: export SPARK_HOME=/home/$USER/spark-3.1.1-amzn-0-bin-3.2.1-amzn-3
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

---

# Release Notes

## August 27 2021
* The master branch has been modified from representing Glue 0.9 to Glue 3.0, we have also created a glue-0.9 branch to reflect the former state of the master branch with Glue 0.9. To rename your local clone of the older master branch and point to the glue-0.9 branch, you may use the following commands:
```
git branch -m master glue-0.9
git fetch origin
git branch -u origin/glue-0.9 glue-0.9
git remote set-head origin -a
```

