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

from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
from awsglue.utils import makeOptions, callsite

class DataSink(object):
    def __init__(self, j_sink, sql_ctx):
        self._jsink = j_sink
        self._sql_ctx = sql_ctx

    def setFormat(self, format, **options):
        self._jsink.setFormat(format, makeOptions(self._sql_ctx._sc, options))

    def setAccumulableSize(self, size):
        self._jsink.setAccumulableSize(size)

    def writeFrame(self, dynamic_frame, info = ""):
        return DynamicFrame(self._jsink.pyWriteDynamicFrame(dynamic_frame._jdf, callsite(), info), dynamic_frame.glue_ctx, dynamic_frame.name + "_errors")

    def write(self, dynamic_frame_or_dfc, info = ""):
        if isinstance(dynamic_frame_or_dfc, DynamicFrame):
            return self.writeFrame(dynamic_frame_or_dfc, info)

        elif isinstance(dynamic_frame_or_dfc, DynamicFrameCollection):
            res_frames = [self.writeFrame(frame)
                          for frame in dynamic_frame_or_dfc.values()]
            return DynamicFrameCollection(res_frames, self._sql_ctx)

        else:
            raise TypeError("dynamic_frame_or_dfc must be an instance of"
                            "DynamicFrame or DynamicFrameCollection. Got "
                            + str(type(dynamic_frame_or_dfc)))
