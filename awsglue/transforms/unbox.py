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

from transform import GlueTransform

class Unbox(GlueTransform):

    def __call__(self, frame, path, format, transformation_ctx = "", info="", stageThreshold=0, totalThreshold=0, **options):
        """
        unbox a string field

        :param frame: dynamicFrame on which to call unbox
        :param path: full path to the StringNode you want to unbox
        :param format: "avro" or "json"
        :param info: String, any string to be associated with errors in this transformation.
        :param stageThreshold: Long, number of errors in the given transformation for which the processing needs to error out.
        :param totalThreshold: Long, total number of errors upto and including in this transformation
          for which the processing needs to error out.
        :param options:
            separator: String,
            escaper: String,
            skipFirst: Boolean,
            withSchema: String, schema string should always be called by using StructType.json()
            withHeader: Boolean
        """
        return frame.unbox(path, format, transformation_ctx, info, stageThreshold, totalThreshold, **options)

    @classmethod
    def describeArgs(cls):
        arg1 = {"name": "frame",
                "type": "DynamicFrame",
                "description": "The DynamicFrame on which to call Unbox",
                "optional": False,
                "defaultValue": None}
        arg2 = {"name": "path",
                "type": "String",
                "description": "full path to the StringNode to unbox",
                "optional": False,
                "defaultValue": None}
        arg3 = {"name": "format",
                "type": "String",
                "description": "file format -- \"avro\" or \"json\" only",
                "optional": False,
                "defaultValue": None}
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
        arg8 = {"name": "separator",
                "type": "String",
                "description": "separator token",
                "optional": True,
                "defaultValue": "None, but individual readers may have their own defaults"}
        arg9 = {"name": "escaper",
                "type": "String",
                "description": "escape token",
                "optional": True,
                "defaultValue": "None, but individual readers may have their own defaults"}
        arg10 = {"name": "skipFirst",
                "type": "Boolean",
                "description": "whether to skip the first line of data",
                "optional": True,
                "defaultValue": "None, but individual readers may have their own defaults"}
        arg11 = {"name": "withSchema",
                "type": "String",
                "description":"schema for data to unbox, should always be created by using StructType.json()",
                "optional": True,
                "defaultValue": "None, but individual readers may have their own defaults"}
        arg12 = {"name": "withHeader",
                "type": "Boolean",
                "description": "whether data being unpacked includes a header",
                "optional": True,
                "defaultValue": "None, but individual readers may have their own defaults"}
        return [arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12]

    @classmethod
    def describeTransform(cls):
        return "unbox a string field"

    @classmethod
    def describeErrors(cls):
        return []

    @classmethod
    def describeReturn(cls):
        return {"type": "DynamicFrame",
                "description": "new DynamicFrame with unboxed DynamicRecords"}
