# awsglue

The awsglue Python package contains the Python portion of the [AWS Glue](https://aws.amazon.com/glue) library. This library extends [PySpark](http://spark.apache.org/docs/2.1.0/api/python/pyspark.html) to support serverless ETL on AWS.

Note that this package must be used in conjunction with the AWS Glue service and is not executable independently. Many of the classes and methods use the Py4J library to interface with code that is available on the Glue platform. This repository can be used as a reference and aid for writing Glue scripts.

While scripts using this library can only be run on the AWS Glue service, it is possible to import this library locally. This may be helpful to provide auto-completion in an IDE, for instance. To import the library successfully you will need to install PySpark, which can be done using pip:

      pip install pyspark

## Content

This package contains Python interfaces to the key data structures and methods used in AWS Glue. The following are some important modules. More information can be found in the public documentation.


#### GlueContext
The file [context.py](context.py) contains the GlueContext class. GlueContext extends PySpark's [SQLContext](https://github.com/apache/spark/blob/master/python/pyspark/sql/context.py) class to provide Glue-specific operations. Most Glue programs will start by instantiating a GlueContext and using it to construct a DynamicFrame. 


#### DynamicFrame
The DynamicFrame, defined in [dynamicframe.py](dynamicframe.py), is the core data structure used in Glue scripts. DynamicFrames are similar to Spark SQL's [DataFrames](https://github.com/apache/spark/blob/master/python/pyspark/sql/dataframe.py) in that they represent distributed collections of data records, but DynamicFrames provide more flexible handling of data sets with inconsistent schemas. By representing records in a self-describing way, they can be used without specifying a schema up front or requiring a costly schema inference step. 

DynamicFrames support many operations, but it is also possible to convert them to DataFrames using the `toDF` method to make use of existing Spark SQL operations. 


#### Transforms

The [transforms](transforms/) directory contains a variety of operations that can be performed on DynamicFrames. These include simple operations, such as `DropFields`, as well as more complex transformations like `Relationalize`, which flattens a nested data set into a collection of tables that can be loaded into a Relational Database. Once imported, transforms can be invoked using the following syntax:

        TransformClass.apply(args...)

## Additional Resources 

- The [aws-glue-samples](https://github.com/awslabs/aws-glue-samples) repository contains sample scripts that make use of awsglue library and can be submitted directly to the AWS Glue service.

- The public [Glue Documentation](http://docs.aws.amazon.com/glue/latest/dg/index.html) contains information about the AWS Glue service as well as addditional information about the Python library.

