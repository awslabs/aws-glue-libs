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

class SelectFromCollection(GlueTransform):

    def __call__(self, dfc, key, transformation_ctx = ""):
        return dfc.select(key, transformation_ctx)

    @classmethod
    def describeArgs(cls):
        arg1 = {"name": "dfc",
                "type": "DynamicFrameCollection",
                "description": "select one DynamicFrame from this DynamicFrameCollection",
                "optional": False,
                "defaultValue": None}

        arg2 = {"name": "key",
                "type": "String",
                "description": "The key to select",
                "optional": False,
                "defaultValue": None}

        arg3 = {"name": "transformation_ctx",
                "type": "String",
                "description": "A unique string that is used to identify stats / state information",
                "optional": True,
                "defaultValue": ""}

        return [arg1, arg2, arg3]

    @classmethod
    def describeTransform(cls):
        return "Select one DynamicFrame out from the DynamicFrameCollection"

    @classmethod
    def describeErrors(cls):
        return []

    @classmethod
    def describeReturn(cls):
        return {"type": "DynamicFrame",
                "description": "Dynamic Frame corresponding to name"}

class MapToCollection(GlueTransform):

    def __call__(self, dfc, callable, transformation_ctx = ""):
        return dfc.map(callable, transformation_ctx)

    @classmethod
    def describeArgs(cls):
        arg1 = {"name": "dfc",
                "type": "CollectionDynamicFrame",
                "description": "apply function on this DynamicFrameCollection",
                "optional": False,
                "defaultValue": None}

        arg2 = {"name": "callable",
                "type": "Callable",
                "description": "apply this Callable on DynamicFrameCollection",
                "optional": False,
                "defaultValue": None}

        arg3 = {"name": "transformation_ctx",
                "type": "String",
                "description": "A unique string that is used to identify stats / state information",
                "optional": True,
                "defaultValue": ""}

        return [arg1, arg2, arg3]

    @classmethod
    def describeTransform(cls):
        return "Apply a transform on each DynamicFrame of this DynamicFrameCollection"

    @classmethod
    def describeErrors(cls):
        return []

    @classmethod
    def describeReturn(cls):
        return {"type": "DynamicFrameCollection",
                "description": "A new DynamicFrameCollection after apply transform on each element"}


class FlatMap(GlueTransform):

    def __call__(self, dfc, BaseTransform, frame_name, transformation_ctx = "", **base_kwargs):
        args = {}

        def apply_inner(frame, transformation_ctx):
            args.clear()
            args.update(base_kwargs)
            args[frame_name] = frame
            args["transformation_ctx"] = transformation_ctx
            return BaseTransform.apply(**args)

        return dfc.flatmap(apply_inner, transformation_ctx)

    @classmethod
    def describeArgs(cls):
        arg1 = {"name": "dfc",
                "type": "DynamicFrameCollection",
                "description": "The collection over which to flatmap.",
                "optional": False,
                "defaultValue": None}

        arg2 = {"name": "BaseTransform",
                "type": "GlueTransform",
                "description": "A GlueTransform to apply to each member of the collection.",
                "optional": False,
                "defaultValue": None}

        arg3 = {"name": "frame_name",
                "type": "String",
                "description": "The argument name to which to pass the elements of the collection.",
                "optional": False,
                "defaultValue": None}

        arg4 = {"name": "transformation_ctx",
                "type": "String",
                "description": "A unique string that is used to identify stats / state information",
                "optional": True,
                "defaultValue": ""}

        arg5 = {"name": "base_kwargs",
                "type": "dict",
                "description": "Arguments to pass to the base transform.",
                "optional": False,
                "defaultValue": None}

        return [arg1, arg2, arg3, arg4, arg5]

    @classmethod
    def describeTransform(cls):
        return "Applies a transform to each DynamicFrame in a collection and flattens the results."

    @classmethod
    def describeErrors(cls):
        return []

    @classmethod
    def describeReturn(cls):
        return {"type": "DynamicFrameCollection",
                "description": "A new DynamicFrameCollection after applying the transform on each element"}
