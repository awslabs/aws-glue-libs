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
    
    def __init__(self, glue_context):
        self._job = glue_context._jvm.Job
        self._glue_context = glue_context

    def init(self, job_name, args = {}):
        self._job.init(job_name, self._glue_context._glue_scala_context, args)

    def isInitialized(self):
        return self._job.isInitialized()

    def commit(self):
        self._job.commit()

