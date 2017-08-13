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

from awsglue.transforms import DropFields, GlueTransform

class ApplyMapping(GlueTransform):
    def __call__(self, frame, mappings, case_sensitive = False,
                 transformation_ctx = "", info = "", stageThreshold = 0, totalThreshold = 0):
        return frame.apply_mapping(mappings, case_sensitive)

    @classmethod
    def describeArgs(cls):
        arg1 = {"name": "frame",
                "type": "DynamicFrame",
                "description": "DynamicFrame to transform",
                "optional": False,
                "defaultValue": None}
        arg2 = {"name": "mappings",
                "type": "DynamicFrame",
                "description": "List of mapping tuples (source col, source type, target col, target type)",
                "optional": False,
                "defaultValue": None}
        arg3 = {"name": "case_sensitive",
                "type": "Boolean",
                "description": "Whether ",
                "optional": True,
                "defaultValue": "False"}
        arg4 = {"name": "transformation_ctx",
                "type": "String",
                "description": "A unique string that is used to identify stats / state information",
                "optional": True,
                "defaultValue": ""}
        arg5 = {"name": "info",
                "type": "String",
                "description": "Any string to be associated with errors in the transformation",
                "optional": True,
                "defaultValue": "\"\""}
        arg6 = {"name": "stageThreshold",
                "type": "Integer",
                "description": "Max number of errors in the transformation until processing will error out",
                "optional": True,
                "defaultValue": "0"}
        arg7 = {"name": "totalThreshold",
                "type": "Integer",
                "description": "Max number of errors total until processing will error out.",
                "optional": True,
                "defaultValue": "0"}

        return [arg1, arg2, arg3, arg4, arg5, arg6, arg7]

    @classmethod
    def describeTransform(cls):
        return "Apply a declarative mapping to this DynamicFrame."

    @classmethod
    def describeErrors(cls):
        return []

    @classmethod
    def describeReturn(cls):
        return {"type": "DynamicFrame",
                "description": "DynamicFrame after applying mappings."}
