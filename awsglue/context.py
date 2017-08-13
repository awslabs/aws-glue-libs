# Copyright 2016-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
from awsglue.data_source import DataSource
from awsglue.data_sink import DataSink
from awsglue.dynamicframe import DynamicFrame, DynamicFrameReader, DynamicFrameWriter, DynamicFrameCollection
from awsglue.utils import makeOptions, callsite
import pyspark
import os
import re
import uuid
from py4j.java_gateway import JavaClass

def register(sc):
    java_import(sc._jvm, "com.amazonaws.services.glue.*")
    java_import(sc._jvm, "com.amazonaws.services.glue.schema.*")
    java_import(sc._jvm, "org.apache.spark.sql.glue.GlueContext")
    java_import(sc._jvm, "com.amazonaws.services.glue.util.JsonOptions")
    java_import(sc._jvm, "org.apache.spark.sql.glue.util.SparkUtility")
    java_import(sc._jvm, 'org.apache.spark.sql.glue.gluefunctions')
    java_import(sc._jvm, "com.amazonaws.services.glue.util.Job")
    java_import(sc._jvm, "com.amazonaws.services.glue.util.AWSConnectionUtils")


class GlueContext(SQLContext):
    Spark_SQL_Formats = {"parquet", "orc"}

    def __init__(self, sparkContext, **options):
        super(GlueContext, self).__init__(sparkContext)
        register(sparkContext)
        self._glue_scala_context = self._get_glue_scala_context(**options)
        self.create_dynamic_frame = DynamicFrameReader(self)
        self.write_dynamic_frame = DynamicFrameWriter(self)
        self.spark_session = SparkSession(sparkContext, self._glue_scala_context.getSparkSession())

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

    def getSource(self, connection_type, format = None, transformation_ctx = "", **options):
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
                                            makeOptions(self._sc, options), transformation_ctx)

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

    def create_dynamic_frame_from_rdd(self, data, name, schema=None, sample_ratio=None, transformation_ctx=""):
        """Creates a DynamicFrame from an RDD.
        """
        df = super(GlueContext, self).createDataFrame(data, schema, sample_ratio)
        return DynamicFrame.fromDF(df, self, name)

    def create_dynamic_frame_from_catalog(self, database = None, table_name = None, redshift_tmp_dir = "", transformation_ctx = "", **kwargs):
        """Creates a DynamicFrame with catalog database and table name
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
        source = DataSource(self._ssql_ctx.getCatalogSource(db, table_name, redshift_tmp_dir, transformation_ctx), self, table_name)
        return source.getFrame(**kwargs)

    def create_dynamic_frame_from_options(self, connection_type, connection_options={},
                                          format=None, format_options={}, transformation_ctx = "", **kwargs):
        """Creates a DynamicFrame with the specified connection and format.

        Example:
        >>> myFrame = context.createDynamicFrame(connection_type="file",
        >>>                                      connection_options={"paths": ["/in/path"]},
        >>>                                      format="json")

        """
        source = self.getSource(connection_type, format, transformation_ctx, **connection_options)

        if (format and format not in self.Spark_SQL_Formats):
            source.setFormat(format, **format_options)

        return source.getFrame(**kwargs)

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
            new_options = dict(connection_options.items()
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
    def write_dynamic_frame_from_catalog(self, frame, database = None, table_name = None, redshift_tmp_dir = "", transformation_ctx = "", **kwargs):
        """
        Creates a DynamicFrame with catalog database and table name
        :param frame: dynamic frame to be written
        :param database: database in catalog
        :param table_name: table name
        :param redshift_tmp_dir: tmp dir
        :param transformation_ctx: transformation context
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

        j_sink = self._ssql_ctx.getCatalogSink(db, table_name, redshift_tmp_dir)
        return DataSink(j_sink, self).write(frame)

    def write_dynamic_frame_from_jdbc_conf(self, frame, catalog_connection, connection_options={}, redshift_tmp_dir = "", transformation_ctx = ""):
        """
        :param frame: dynamic frame to be written
        :param catalog_connection: catalog connection name, used to access JDBC configuration
        :param connection_type: redshift, mysql, postgres or aurora
        :param connection_options: dbtable and so on
        :return: dynamic frame with potential errors
        """
        self.write_from_jdbc_conf(frame, catalog_connection, connection_options, redshift_tmp_dir, transformation_ctx)

    def write_from_jdbc_conf(self, frame_or_dfc, catalog_connection, connection_options={}, redshift_tmp_dir = "", transformation_ctx = ""):
        if isinstance(frame_or_dfc, DynamicFrameCollection):
            new_options = dict(connection_options.items()
                               + [("useFrameName", True)])
        elif isinstance(frame_or_dfc, DynamicFrame):
            new_options = connection_options
        else:
            raise TypeError("frame_or_dfc must be DynamicFrame or"
                            "DynamicFrameCollection. Got " +
                            str(type(frame_or_dfc)))

        j_sink = self._ssql_ctx.getJDBCSink(catalog_connection, makeOptions(self._sc, new_options), redshift_tmp_dir)
        return DataSink(j_sink, self).write(frame_or_dfc)

    def convert_resolve_option(self, path, action, target):
        if target is None:
            return self._jvm.ResolveOption(path, action)
        else:
            return self._jvm.ResolveOption(path, action, self._ssql_ctx.parseDataType(target.json()))

    def extract_jdbc_conf(self, connection_name):
        return self._ssql_ctx.extractJDBCConf(connection_name)
