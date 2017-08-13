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

from awsglue.transforms import GlueTransform

class UnnestFrame(GlueTransform):
    """
    unnest a dynamic frame. i.e. flattens nested objects to top level elements.
    It also generates joinkeys for array objects
    """

    def __call__(self, frame, transformation_ctx = "", info="", stageThreshold=0, totalThreshold=0):
        """
        unnest a dynamic frame. i.e. flattens nested objects to top level elements.
        It also generates joinkeys for array objects
        :param frame: DynamicFrame, the dynamicframe to unnest
        :param info: String, any string to be associated with errors in this transformation.
        :param stageThreshold: Long, number of errors in the given transformation for which the processing needs to error out.
        :param totalThreshold: Long, total number of errors upto and including in this transformation
          for which the processing needs to error out.
        :return: a new unnested dynamic frame
        """
        return frame.unnest(transformation_ctx, info, stageThreshold, totalThreshold)

    @classmethod
    def describeArgs(cls):
        arg1 = {"name": "frame",
                "type": "DynamicFrame",
                "description": "The DynamicFrame to unnest",
                "optional": False,
                "defaultValue": None}
        arg2 = {"name": "transformation_ctx",
                "type": "String",
                "description": "A unique string that is used to identify stats / state information",
                "optional": True,
                "defaultValue": ""}
        arg3 = {"name": "info",
                "type": "String",
                "description": "Any string to be associated with errors in the transformation",
                "optional": True,
                "defaultValue": "\"\""}
        arg4 = {"name": "stageThreshold",
                "type": "Integer",
                "description": "Max number of errors in the transformation until processing will error out",
                "optional": True,
                "defaultValue": "0"}
        arg5 = {"name": "totalThreshold",
                "type": "Integer",
                "description": "Max number of errors total until processing will error out.",
                "optional": True,
                "defaultValue": "0"}

        return [arg1, arg2, arg3, arg4, arg5]

    @classmethod
    def describeTransform(cls):
        return "unnest a dynamic frame. i.e. flatten nested objects to top level elements."

    @classmethod
    def describeErrors(cls):
        return []

    @classmethod
    def describeReturn(cls):
        return {"type": "DynamicFrame",
                "description": "new unnested DynamicFrame"}
