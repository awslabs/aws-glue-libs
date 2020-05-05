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

from transform import GlueTransform

class Coalesce(GlueTransform):
    def __call__(self, frame, num_partitions, shuffle = False, transformation_ctx = "", info = "",
                 stageThreshold = 0, totalThreshold = 0):
        return frame.coalesce(num_partitions, shuffle, transformation_ctx, info, stageThreshold, totalThreshold)

    @classmethod
    def describeArgs(cls):
        arg1 = {"name": "num_partitions",
                "type": "DynamicFrame",
                "description": "Number of partitions",
                "optional": False,
                "defaultValue": None}
        arg2 = {"name": "shuffle",
                "type": "Boolean",
                "description": "A boolean indicating whether shuffling enabled for the coalesce process",
                "optional": True,
                "defaultValue": False}
        arg3 = {"name": "transformation_ctx",
                "type": "String",
                "description": "A unique string that is used to identify stats / state information",
                "optional": True,
                "defaultValue": ""}
        arg4 = {"name": "info",
                "type": "String",
                "description": "Any string to be associated with errors in the transformation",
                "optional": True,
                "defaultValue": "\"\""}
        arg5 = {"name": "stageThreshold",
                "type": "Integer",
                "description": "Max number of errors in the transformation until processing will error out",
                "optional": True,
                "defaultValue": "0"}
        arg6 = {"name": "totalThreshold",
                "type": "Integer",
                "description": "Max number of errors total until processing will error out.",
                "optional": True,
                "defaultValue": "0"}

        return [arg1, arg2, arg3, arg4, arg5, arg6]

    @classmethod
    def describeTransform(cls):
        return "Coalesces a DynamicFrame."

    @classmethod
    def describeErrors(cls):
        return []

    @classmethod
    def describeReturn(cls):
        return {"type": "DynamicFrame",
                "description": "The coalesced DynamicFrame."}
