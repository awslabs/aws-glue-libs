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

from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import makeOptions, callsite

class DataSource(object):
    def __init__(self, j_source, sql_ctx, name):
        self._jsource = j_source
        self._sql_ctx = sql_ctx
        self.name = name

    def setFormat(self, format, **options):
        options["callSite"] = callsite()
        self._jsource.setFormat(format, makeOptions(self._sql_ctx._sc, options))

    def getFrame(self, **options):
        minPartitions = targetPartitions = None

        if 'minPartitions' in options:
            minPartitions = options['minPartitions']
            targetPartitions = options.get('targetPartitions', minPartitions)
        elif 'targetPartitions' in options:
            minPartitions = targetPartitions = options['targetPartitions']

        if minPartitions is None:
            jframe = self._jsource.getDynamicFrame()
        else:
            jframe = self._jsource.getDynamicFrame(minPartitions, targetPartitions)

        return DynamicFrame(jframe, self._sql_ctx, self.name)
