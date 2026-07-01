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
from py4j.java_gateway import java_import # type: ignore
class Job:
    @classmethod
    def continuation_options(cls):
        return [ '--continuation-option', 'continuation-enabled', 'continuation-readonly', 'continuation-ignore' ]

    @classmethod
    def job_bookmark_options(cls):
        return [ '--job-bookmark-option', 'job-bookmark-enable', 'job-bookmark-pause', 'job-bookmark-disable' ]
    @classmethod
    def job_bookmark_range_options(cls):
        return [ '--job-bookmark-from', '--job-bookmark-to' ]

    @classmethod
    def id_params(cls):
        return [ '--JOB_NAME', '--JOB_ID', '--JOB_RUN_ID', '--SECURITY_CONFIGURATION' ]

    @classmethod
    def encryption_type_options(cls):
        return [ '--encryption-type' , 'sse-s3' ]

    @classmethod
    def data_lineage_options(cls):
        return [ '--enable-data-lineage']
    def __init__(self, glue_context_or_spark_session):
        from pyspark.sql import SparkSession
        from awsglue.context import GlueContext
        if isinstance(glue_context_or_spark_session, GlueContext):
            self._job = glue_context_or_spark_session._jvm.Job
            self._glue_context = glue_context_or_spark_session
            self._spark_session = glue_context_or_spark_session.sparkSession
        elif isinstance(glue_context_or_spark_session, SparkSession):
            java_import(glue_context_or_spark_session._jvm, "com.amazonaws.services.glue.util.Job")
            self._job = glue_context_or_spark_session._jvm.Job
            self._glue_context = None
            self._spark_session = glue_context_or_spark_session
        else:
            raise Exception("cannot init Job instance given input parameter type: " + str(type(glue_context_or_spark_session)))

    def init(self, job_name, args = {}):
        self._job.init(job_name, self._spark_session._jsparkSession, args)

    def isInitialized(self):
        return self._job.isInitialized()

    def commit(self):
        self._job.commit()

