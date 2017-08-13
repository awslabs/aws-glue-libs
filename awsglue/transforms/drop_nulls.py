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
from awsglue.gluetypes import ArrayType, NullType, StructType

class DropNullFields(GlueTransform):
    def _find_null_fields(self, ctx, schema, path, output):
        if isinstance(schema, StructType):
            for field in schema:
                new_path = path + "." if path != "" else path
                self._find_null_fields(ctx, field.dataType, new_path + ctx._jvm.RecordUtils.quoteName(field.name), output)

        elif isinstance(schema, ArrayType):
            # For the moment we only remove null fields in nested array columns.
            # We don't change ArrayType(NullType).
            if isinstance(schema.elementType, StructType):
                self._find_null_fields(ctx, schema.elementType, path, output)

        elif isinstance(schema, NullType):
            output.append(path)

        # Note: dropFields currently does not work through maps,
        # so neither does DropNullFields

    def __call__(self, frame, transformation_ctx = "", info = "", stageThreshold = 0, totalThreshold = 0):
        null_fields = []
        self._find_null_fields(frame.glue_ctx, frame.schema(), "", null_fields)
        print "null_fields", null_fields

        return DropFields.apply(frame, null_fields, transformation_ctx,
                                info, stageThreshold, totalThreshold)

    @classmethod
    def describeArgs(cls):
        arg1 = {"name": "frame",
                "type": "DynamicFrame",
                "description": "Drop all null fields in this DynamicFrame",
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
        return "Drop all null fields in this DynamicFrame"

    @classmethod
    def describeErrors(cls):
        return []

    @classmethod
    def describeReturn(cls):
        return {"type": "DynamicFrame",
                "description": "DynamicFrame without null fields."}
