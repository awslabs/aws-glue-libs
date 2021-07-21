# Copyright 2016-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# Licensed under the Amazon Software License (the "License"). You may not use
# this file except in compliance with the License. A copy of the License is
# located at
#
#  http://aws.amazon.com/asl/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express
# or implied. See the License for the specific language governing
# permissions and limitations under the License.

from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from py4j.java_gateway import java_import
from pyspark.sql.utils import StreamingQueryException

from awsglue.data_source import DataSource
from awsglue.streaming_data_source import StreamingDataSource
from awsglue.data_sink import DataSink
from awsglue.dataframereader import DataFrameReader
from awsglue.dynamicframe import DynamicFrame, DynamicFrameReader, DynamicFrameWriter, DynamicFrameCollection
from awsglue.gluetypes import DataType
from awsglue.utils import makeOptions, callsite
from pyspark.sql.dataframe import DataFrame
import pyspark
import os
import re
import uuid
from py4j.java_gateway import JavaClass
import time
import logging

def register(sc):
    java_import(sc._jvm, "com.amazonaws.services.glue.*")
    java_import(sc._jvm, "com.amazonaws.services.glue.log.GlueLogger")
    java_import(sc._jvm, "com.amazonaws.services.glue.schema.*")
    java_import(sc._jvm, "com.amazonaws.services.glue.util.JsonOptions")
    java_import(sc._jvm, "org.apache.spark.sql.glue.util.SparkUtility")
    java_import(sc._jvm, "com.amazonaws.services.glue.util.Job")
    java_import(sc._jvm, "com.amazonaws.services.glue.util.AWSConnectionUtils")
    java_import(sc._jvm, "com.amazonaws.services.glue.util.GluePythonUtils")
    java_import(sc._jvm, "com.amazonaws.services.glue.errors.CallSite")
    # java_import(sc._jvm, "com.amazonaws.services.glue.ml.FindMatches")
    # java_import(sc._jvm, "com.amazonaws.services.glue.ml.FindIncrementalMatches")
    # java_import(sc._jvm, "com.amazonaws.services.glue.ml.FillMissingValues")


class GlueContext(SQLContext):
    Spark_SQL_Formats = {"parquet", "orc"}

    def __init__(self, sparkContext, **options):
        super(GlueContext, self).__init__(sparkContext)
        register(sparkContext)
        self._glue_scala_context = self._get_glue_scala_context(**options)
        self.create_dynamic_frame = DynamicFrameReader(self)
        self.create_data_frame = DataFrameReader(self)
        self.write_dynamic_frame = DynamicFrameWriter(self)
        self.spark_session = SparkSession(sparkContext, self._glue_scala_context.getSparkSession())
        self._glue_logger = sparkContext._jvm.GlueLogger()

    @property
    def _ssql_ctx(self):
        if not hasattr(self, '_glue_scala_context'):
            self._glue_scala_context = self._get_glue_scala_context()
        return self._glue_scala_context

    def _get_glue_scala_context(self, **options):
        min_partitions = target_partitions = None
        if 'minPartitions' in options:
            min_partitions = options['minPartitions']
            target_partitions = options.get('targetPartitions', min_partitions)
        elif 'targetPartitions' in options:
            min_partitions = target_partitions = options.get('targetPartitions')

        if min_partitions is None:
            return self._jvm.GlueContext(self._jsc.sc())
        else:
            return self._jvm.GlueContext(self._jsc.sc(), min_partitions, target_partitions)

    def getSource(self, connection_type, format = None, transformation_ctx = "", push_down_predicate= "", **options):
        """Creates a DataSource object.

        This can be used to read DynamicFrames from external sources.

        Example:
        >>> data_source = context.getSource("file", paths=["/in/path"])
        >>> data_source.setFormat("json")
        >>> myFrame = data_source.getFrame()
        """
        options["callSite"] = callsite()
        if(format and format.lower() in self.Spark_SQL_Formats):
            connection_type = format

        j_source = self._ssql_ctx.getSource(connection_type,
                                            makeOptions(self._sc, options), transformation_ctx, push_down_predicate)

        prefix = None
        if 'paths' in options and options['paths'] != None:
            paths = options['paths']
            prefix = os.path.commonprefix(paths)
            if prefix != None:
                prefix = prefix.split(':')[-1]
                prefix = re.sub('[:/.]', '', prefix)

        # in case paths is not in options or no common prefix
        if prefix == None:
            prefix = str(uuid.uuid1())
            prefix = re.sub('[-]', '_', prefix)

        return DataSource(j_source, self, prefix)

    def getStreamingSource(self, connection_type, format = None, transformation_ctx = "", push_down_predicate= "", **options):
        """Creates a Streaming Data Source object.

        This can be used to read Dataframes from external sources.
        """
        options["callSite"] = callsite()
        if(format and format.lower() in self.Spark_SQL_Formats):
            connection_type = format

        j_source = self._ssql_ctx.getSource(connection_type,
                                            makeOptions(self._sc, options), transformation_ctx, push_down_predicate)

        prefix = None
        if 'paths' in options and options['paths'] != None:
            paths = options['paths']
            prefix = os.path.commonprefix(paths)
            if prefix != None:
                prefix = prefix.split(':')[-1]
                prefix = re.sub('[:/.]', '', prefix)

        # in case paths is not in options or no common prefix
        if prefix == None:
            prefix = str(uuid.uuid1())
            prefix = re.sub('[-]', '_', prefix)

        return StreamingDataSource(j_source, self, prefix)

    def get_catalog_schema_as_spark_schema(self, database = None, table_name = None, catalog_id = None):
        return self._ssql_ctx.getCatalogSchemaAsSparkSchema(database, table_name, catalog_id)

    def create_dynamic_frame_from_rdd(self, data, name, schema=None, sample_ratio=None, transformation_ctx=""):
        """Creates a DynamicFrame from an RDD.
        """
        df = super(GlueContext, self).createDataFrame(data, schema, sample_ratio)
        return DynamicFrame.fromDF(df, self, name)

    def create_dynamic_frame_from_catalog(self, database = None, table_name = None, redshift_tmp_dir = "",
                                          transformation_ctx = "", push_down_predicate="", additional_options = {},
                                          catalog_id = None, **kwargs):
        """
        Creates a DynamicFrame with catalog database, table name and an optional catalog id
        :param database: database in catalog
        :param table_name: table name
        :param redshift_tmp_dir: tmp dir
        :param transformation_ctx: transformation context
        :param push_down_predicate
        :param additional_options
        :param catalog_id catalog id of the DataCatalog being accessed (account id of the data catalog).
                Set to None by default (None defaults to the catalog id of the calling account in the service)
        :return: dynamic frame with potential errors
        """
        if database is not None and "name_space" in kwargs:
            raise Exception("Parameter name_space and database are both specified, choose one.")
        elif database is None and "name_space" not in kwargs:
            raise Exception("Parameter name_space or database is missing.")
        elif "name_space" in kwargs:
            db = kwargs.pop("name_space")
        else:
            db = database

        if table_name is None:
            raise Exception("Parameter table_name is missing.")
        source = DataSource(self._ssql_ctx.getCatalogSource(db, table_name, redshift_tmp_dir, transformation_ctx,
                                                            push_down_predicate,
                                                            makeOptions(self._sc, additional_options), catalog_id),
                            self, table_name)
        return source.getFrame(**kwargs)

    def create_data_frame_from_catalog(self, database = None, table_name = None, redshift_tmp_dir = "",
                                          transformation_ctx = "", push_down_predicate="", additional_options = {},
                                          catalog_id = None, **kwargs):
        """
        Creates a DataFrame with catalog database, table name and an optional catalog id
        :param database: database in catalog
        :param table_name: table name
        :param redshift_tmp_dir: tmp dir
        :param transformation_ctx: transformation context
        :param push_down_predicate
        :param additional_options
        :param catalog_id catalog id of the DataCatalog being accessed (account id of the data catalog).
                Set to None by default (None defaults to the catalog id of the calling account in the service)
        :return: data frame with potential errors
        """
        if database is not None and "name_space" in kwargs:
            raise Exception("Parameter name_space and database are both specified, choose one.")
        elif database is None and "name_space" not in kwargs:
            raise Exception("Parameter name_space or database is missing.")
        elif "name_space" in kwargs:
            db = kwargs.pop("name_space")
        else:
            db = database

        if table_name is None:
            raise Exception("Parameter table_name is missing.")
        source = StreamingDataSource(self._ssql_ctx.getCatalogSource(db, table_name, redshift_tmp_dir, transformation_ctx,
                                                            push_down_predicate,
                                                            makeOptions(self._sc, additional_options), catalog_id),
                            self, table_name)
        return source.getFrame()

    def create_dynamic_frame_from_options(self, connection_type, connection_options={},
                                          format=None, format_options={}, transformation_ctx = "", push_down_predicate= "",  **kwargs):
        """Creates a DynamicFrame with the specified connection and format.

        Example:
        >>> myFrame = context.createDynamicFrame(connection_type="file",
        >>>                                      connection_options={"paths": ["/in/path"]},
        >>>                                      format="json")

        """
        source = self.getSource(connection_type, format, transformation_ctx, push_down_predicate, **connection_options)

        if (format and format not in self.Spark_SQL_Formats):
            source.setFormat(format, **format_options)

        return source.getFrame(**kwargs)

    def create_data_frame_from_options(self, connection_type, connection_options={},
                                       format=None, format_options={}, transformation_ctx = "", push_down_predicate= "",  **kwargs):
        """Creates a DataFrame with the specified connection and format. Used for streaming data sources
        """
        source = self.getStreamingSource(connection_type, format, transformation_ctx, push_down_predicate, **connection_options)

        if (format and format not in self.Spark_SQL_Formats):
            source.setFormat(format, **format_options)

        return source.getFrame()

    def getSink(self, connection_type, format = None, transformation_ctx = "", **options):
        """Gets a DataSink object.

        This can be used to write DynamicFrames to external targets.
        Check SparkSQL format first to make sure to return the expected sink

        Example:
        >>> data_sink = context.getSink("s3")
        >>> data_sink.setFormat("json"),
        >>> data_sink.writeFrame(myFrame)
        """

        if(format and format.lower() in self.Spark_SQL_Formats):
            connection_type = format
        j_sink = self._ssql_ctx.getSink(connection_type,
                                        makeOptions(self._sc, options), transformation_ctx)
        return DataSink(j_sink, self)

    def write_dynamic_frame_from_options(self, frame, connection_type, connection_options={},
                                         format=None, format_options={}, transformation_ctx = ""):
        """
        Writes a DynamicFrame using the specified connection and format
        :param frame:
        :param connection_type: s3, redshift, jdbc, dynamo and so on
        :param connection_options: like path, dbtable
        :param format: json, csv or other format, this is used for s3 or tape connection which supports multiple format
        :param format_options: delimiter and so on
        :return: dynamic_frame with potential errors

        >>> data_sink = context.write_dynamic_frame_by_options(frame,
        >>>                                                    connection_type="s3",
        >>>                                                    path="/out/path",
        >>>                                                    format="json")
        """
        return self.write_from_options(frame, connection_type,
                                       connection_options,
                                       format, format_options, transformation_ctx)

    def write_from_options(self, frame_or_dfc, connection_type,
                           connection_options={}, format={}, format_options={},
                           transformation_ctx = "", **kwargs):
        if isinstance(frame_or_dfc, DynamicFrameCollection):
            new_options = dict(list(connection_options.items())
                               + [("useFrameName", True)])
        elif isinstance(frame_or_dfc, DynamicFrame):
            new_options = connection_options
        else:
            raise TypeError("frame_or_dfc must be DynamicFrame or"
                            "DynamicFrameCollection. Got " +
                            str(type(frame_or_dfc)))

        # Handle parquet and ORC case, make sure to get the right SparkSQL sink
        sink = self.getSink(connection_type, format, transformation_ctx, **new_options)
        if (format and format not in self.Spark_SQL_Formats):
            sink.setFormat(format, **format_options)

        if 'accumulator_size' in kwargs and kwargs['accumulator_size'] > 0:
            sink.setAccumulableSize(kwargs['accumulator_size'])

        return sink.write(frame_or_dfc)

    # Note that since the table name is included in the catalog specification,
    # it doesn't make sense to include a version of this method for DFCs.
    def write_dynamic_frame_from_catalog(self, frame, database = None, table_name = None, redshift_tmp_dir = "",
                                         transformation_ctx = "", additional_options = {}, catalog_id = None, **kwargs):
        """
        Writes a DynamicFrame to a location defined in the catalog's database, table name and an optional catalog id
        :param frame: dynamic frame to be written
        :param database: database in catalog
        :param table_name: table name
        :param redshift_tmp_dir: tmp dir
        :param transformation_ctx: transformation context
        :param additional_options
        :param catalog_id catalog_id catalog id of the DataCatalog being accessed (account id of the data catalog).
                Set to None by default (None defaults to the catalog id of the calling account in the service)
        :return: dynamic frame with potential errors
        """

        if database is not None and "name_space" in kwargs:
            raise Exception("Parameter name_space and database are both specified, choose one.")
        elif database is None and "name_space" not in kwargs:
            raise Exception("Parameter name_space or database is missing.")
        elif "name_space" in kwargs:
            db = kwargs.pop("name_space")
        else:
            db = database

        if table_name is None:
            raise Exception("Parameter table_name is missing.")

        j_sink = self._ssql_ctx.getCatalogSink(db, table_name, redshift_tmp_dir, transformation_ctx,
                                               makeOptions(self._sc, additional_options), catalog_id)
        return DataSink(j_sink, self).write(frame)

    def write_dynamic_frame_from_jdbc_conf(self, frame, catalog_connection, connection_options={},
                                           redshift_tmp_dir = "", transformation_ctx = "", catalog_id = None):
        """
        :param frame: dynamic frame to be written
        :param catalog_connection: catalog connection name, used to access JDBC configuration
        :param connection_options: dbtable and so on
        :param redshift_tmp_dir: tmp dir
        :param transformation_ctx: transformation context
        :param catalog_id catalog id of the DataCatalog being accessed (account id of the data catalog).
                Set to None by default (None defaults to the catalog id of the calling account in the service)
        :return: dynamic frame with potential errors
        """
        self.write_from_jdbc_conf(frame, catalog_connection, connection_options, redshift_tmp_dir, transformation_ctx,
                                  catalog_id)

    def write_from_jdbc_conf(self, frame_or_dfc, catalog_connection, connection_options={},
                             redshift_tmp_dir = "", transformation_ctx = "", catalog_id = None):
        if isinstance(frame_or_dfc, DynamicFrameCollection):
            new_options = dict(list(connection_options.items())
                               + [("useFrameName", True)])
        elif isinstance(frame_or_dfc, DynamicFrame):
            new_options = connection_options
        else:
            raise TypeError("frame_or_dfc must be DynamicFrame or"
                            "DynamicFrameCollection. Got " +
                            str(type(frame_or_dfc)))

        j_sink = self._ssql_ctx.getJDBCSink(catalog_connection, makeOptions(self._sc, new_options), redshift_tmp_dir,
                                            transformation_ctx, catalog_id)
        return DataSink(j_sink, self).write(frame_or_dfc)

    def convert_resolve_option(self, path, action, target):

        if action.upper() == "KEEPASSTRUCT":
            return self._jvm.ResolveSpec.apply(path, "make_struct")
        elif action.upper() == "PROJECT":
            if target is None or not isinstance(target, DataType):
                raise ValueError("Target type must be specified with project action.")

            return self._jvm.ResolveSpec.apply(path, "project:{}".format(target.typeName()))
        else:
            raise ValueError("Invalid resolve action {}. ".format(action) +
                             "Action must be one of KeepAsStruct and Project.")

    def extract_jdbc_conf(self, connection_name, catalog_id=None):
        """
        Get the username, password, vendor and url from the connection object in the catalog
        :param connection_name: name of the connection in the catalog
        :param catalog_id: catalog id of the DataCatalog being accessed (account id of the data catalog).
                Set to None by default (None defaults to the catalog id of the calling account in the service)
        :return: dict with keys "user", "password", "vendor", "url"
        """
        return self._ssql_ctx.extractJDBCConf(connection_name, catalog_id)

    def purge_table(self, database, table_name, options={}, transformation_ctx="", catalog_id=None):
        """
        Delete files from s3 for the given catalog's database and table. If all files in a partition are deleted, that
        partition is deleted from the catalog too
        :param database: database name in catalog
        :param table_name: table name in catalog
        :param options: Options to filter files to be deleted and manifest file generation
            retentionPeriod: Number of hours. Files newer than the retention period will be retained.
                168 hours - (7 days) by default
            partitionPredicate: Partitions satisfying this predicate will be deleted.
                Files within the retention period in these partitions will not be deleted.
                "" - empty by default
            excludeStorageClasses: Files with storage class in the excludeStorageClasses set are not deleted.
                Set() - empty set by default
            manifestFilePath: optional path for manifest file generation. All files that were successfully purged
                or transitioned will be recorded in Success.csv and those that failed in Failed.csv
        :param transformation_ctx: transformation context (used in manifest file path)
        :param catalog_id: catalog id of the DataCatalog being accessed (account id of the data catalog).
                Set to None by default (None defaults to the catalog id of the calling account in the service)
        :return: void return type
        """
        self._ssql_ctx.purgeTable(database, table_name, makeOptions(self._sc, options), transformation_ctx, catalog_id)

    def purge_s3_path(self, s3_path, options={}, transformation_ctx=""):
        """
        Deletes files from a given s3 path recursively
        :param s3_path: s3 path of the files to be deleted in the format s3://<bucket>/<prefix>/
        :param options: Options to filter files to be deleted and manifest file generation
            retentionPeriod: Number of hours. Files newer than the retention period will be retained.
                168 hours - (7 days) by default
            excludeStorageClasses: Files with storage class in the excludeStorageClasses set are not deleted.
                Set() - empty set by default
            manifestFilePath: optional path for manifest file generation. All files that were successfully purged
                or transitioned will be recorded in Success.csv and those that failed in Failed.csv
        :param transformation_ctx: transformation context (used in manifest file path)
        :return: void return type
        """
        self._ssql_ctx.purgeS3Path(s3_path, makeOptions(self._sc, options), transformation_ctx)

    def transition_table(self, database, table_name, transition_to, options={}, transformation_ctx="", catalog_id=None):
        """
        Transitions the storage class of the files stored on s3 for the given catalog's database and table
        :param database: database name in catalog
        :param table_name: table name in catalog
        :param transition_to: S3 storage class to transition to
            https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/StorageClass.html
        :param options: Options to filter files to be transitioned and manifest file generation
            retentionPeriod: Number of hours. Files newer than the retention period will be retained.
                168 hours - (7 days) by default
            partitionPredicate: Partitions satisfying this predicate will be deleted.
                Files within the retention period in these partitions will not be deleted.
                "" - empty by default
            excludeStorageClasses: Files with storage class in the excludeStorageClasses set are not deleted.
                Set() - empty set by default
            manifestFilePath: optional path for manifest file generation. All files that were successfully purged
                or transitioned will be recorded in Success.csv and those that failed in Failed.csv
            accountId: AWS accountId to run the Transition batch job. Mandatory for Transition transform
            roleArn: AWS role to run the Transition batch job. Mandatory for Transition transform
        :param transformation_ctx: transformation context (used in manifest file path)
        :param catalog_id: catalog id of the DataCatalog being accessed (account id of the data catalog).
                Set to None by default (None defaults to the catalog id of the calling account in the service)
        :return: void return type
        """
        self._ssql_ctx.transitionTable(database, table_name, transition_to, makeOptions(self._sc, options),
                                       transformation_ctx, catalog_id)

    def transition_s3_path(self, s3_path, transition_to, options={}, transformation_ctx=""):
        """
        Transition files in a given s3 path recursively
        :param s3_path: s3 path of the files to be transitioned in the format s3://<bucket>/<prefix>/
        :param transition_to: S3 storage class to transition to
            https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/StorageClass.html
        :param options: Options to filter files to be deleted and manifest file generation
            retentionPeriod Number of hours. Files newer than the retention period will be retained.
                168 hours - (7 days) by default
            excludeStorageClasses Files with storage class in the excludeStorageClasses set are not deleted.
                Set() - empty set by default
            manifestFilePath optional path for manifest file generation. All files that were successfully purged
                or transitioned will be recorded in Success.csv and those that failed in Failed.csv
            accountId: AWS accountId to run the Transition batch job. Mandatory for Transition transform
            roleArn: AWS role to run the Transition batch job. Mandatory for Transition transform
        :param transformation_ctx: transformation context (used in manifest file path)
        :return: void return type
        """
        self._ssql_ctx.transitionS3Path(s3_path, transition_to, makeOptions(self._sc, options), transformation_ctx)

    def get_logger(self):
        return self._glue_logger

    def currentTimeMillis(self):
        return int(round(time.time() * 1000))

    def forEachBatch(self, frame, batch_function, options = {}):
        if "windowSize" not in options:
            raise Exception("Missing windowSize argument")
        if "checkpointLocation" not in options:
            raise Exception("Missing checkpointLocation argument")

        windowSize = options["windowSize"]
        checkpointLocation = options["checkpointLocation"]

        # Check the Glue version
        glue_ver = self.getConf('spark.glue.GLUE_VERSION', '')
        java_import(self._jvm, "org.apache.spark.metrics.source.StreamingSource")

        # Converting the S3 scheme to S3a for the Glue Streaming checkpoint location in connector jars.
        # S3 scheme on checkpointLocation currently doesn't work on Glue 2.0 (non-EMR).
        # Will remove this once the connector package is imported as brazil package.
        if (glue_ver == '2.0' or glue_ver == '2' or glue_ver == '3.0' or glue_ver == '3'):
            if (checkpointLocation.startswith( 's3://' )):
                java_import(self._jvm, "com.amazonaws.regions.RegionUtils")
                java_import(self._jvm, "com.amazonaws.services.s3.AmazonS3")
                self._jsc.hadoopConfiguration().set("fs.s3a.endpoint", self._jvm.RegionUtils.getRegion(
                    self._jvm.AWSConnectionUtils.getRegion()).getServiceEndpoint(self._jvm.AmazonS3.ENDPOINT_PREFIX))
                checkpointLocation = checkpointLocation.replace( 's3://', 's3a://', 1)

        run = {'value': 0}
        retry_attempt = {'value': 0}

        def batch_function_with_persist(data_frame, batchId):

            # This condition is true when the previous batch succeeded
            if run['value'] > retry_attempt['value']:
                run['value'] = 0
                if retry_attempt['value'] > 0:
                    retry_attempt['value'] = 0
                    logging.warning("The batch is now succeeded. Resetting retry attempt counter to zero.")
            run['value'] += 1

            # process the batch
            startTime = self.currentTimeMillis()
            if "persistDataFrame" in options and options["persistDataFrame"].lower() == "false":
                if len(data_frame.take(1)):
                    batch_function(data_frame, batchId)
            else:
                storage_level = options.get("storageLevel", "MEMORY_AND_DISK").upper()
                data_frame.persist(getattr(pyspark.StorageLevel, storage_level))
                num_records = data_frame.count()
                if num_records > 0:
                    batch_function(data_frame, batchId)
                data_frame.unpersist()
                self._jvm.StreamingSource.updateNumRecords(num_records)
            self._jvm.StreamingSource.updateBatchProcessingTimeInMs(self.currentTimeMillis() - startTime)

        query = frame.writeStream.foreachBatch(batch_function_with_persist).trigger(processingTime=windowSize).option("checkpointLocation", checkpointLocation)

        batch_max_retries = int(options.get('batchMaxRetries', 3))
        if batch_max_retries < 0 or batch_max_retries > 100:
            raise ValueError('Please specify the number of retries as an integer in the range of [0, 100].')

        while (True):
            try:
                query.start().awaitTermination()
            except Exception as e:
                retry_attempt['value'] += 1
                logging.warning("StreamingQueryException caught. Retry number " + str(retry_attempt['value']))

                if retry_attempt['value'] > batch_max_retries:
                    logging.error("Exceeded maximuim number of retries in streaming interval, exception thrown")
                    raise e
                # lastFailedAttempt = failedTime
                backOffTime = retry_attempt['value'] if (retry_attempt['value'] < 3) else 5
                time.sleep(backOffTime)

    """
        Appends ingestion time columns like ingest_year, ingest_month, ingest_day, ingest_hour, ingest_minute to the
        input DataFrame.
        :param df Input DataFrame in which to append the ingestion time columns.
        :param timeGranularity Time Granularity until which to add the time granularity columns.
        :return DataFrame after appending the time granularity columns.  
    """
    def add_ingestion_time_columns(self, frame, time_granularity):
        return DataFrame(self._ssql_ctx.addIngestionTimeColumns(frame._jdf, time_granularity), frame.sql_ctx)

    def begin_transaction(self, read_only):
        return self._ssql_ctx.beginTransaction(read_only)

    def commit_transaction(self, transaction_id):
        return self._ssql_ctx.commitTransaction(transaction_id)

    def abort_transaction(self, transaction_id):
        return self._ssql_ctx.abortTransaction(transaction_id)