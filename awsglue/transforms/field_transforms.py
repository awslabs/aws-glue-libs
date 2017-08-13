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

class RenameField(GlueTransform):
    """
    Rename a node within a DynamicFrame

    :param frame: DynamicFrame
    :param oldName: String, full path to the node you want to rename
    :param newName: String, new name including full path
    :param info: String, any string to be associated with errors in this transformation.
    :param stageThreshold: Long, number of errors in the given transformation for which the processing needs to error out.
    :param totalThreshold: Long, total number of errors upto and including in this transformation
      for which the processing needs to error out.
    :return: DynamicFrame
    """

    def __call__(self, frame, old_name, new_name, transformation_ctx = "", info = "", stageThreshold = 0, totalThreshold = 0):
        return frame.rename_field(old_name, new_name, transformation_ctx, info, stageThreshold, totalThreshold)

    @classmethod
    def describeArgs(cls):
        arg1 = {"name": "frame",
                "type": "DynamicFrame",
                "description": "The DynamicFrame on which to rename a field",
                "optional": False,
                "defaultValue": None}
        arg2 = {"name": "old_name",
                "type": "String",
                "description": "Full path to the node to rename",
                "optional": False,
                "defaultValue": None}
        arg3 = {"name": "new_name",
                "type": "String",
                "description": "New name, including full path",
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

        return [arg1, arg2, arg3, arg4, arg5, arg6, arg7]

    @classmethod
    def describeTransform(cls):
        return "Rename a node within a DynamicFrame"

    @classmethod
    def describeErrors(cls):
        return []

    @classmethod
    def describeReturn(cls):
        return {"type": "DynamicFrame",
                "description": "new DynamicFrame with indicated field renamed"}


class DropFields(GlueTransform):
    """
    Drop fields within a DynamicFrame

    :param frame: DynamicFrame
    :param paths: List of Strings, each the full path to a node you want to drop
    :param info: String, any string to be associated with errors in this transformation.
    :param stageThreshold: Long, number of errors in the given transformation for which the processing needs to error out.
    :param totalThreshold: Long, total number of errors upto and including in this transformation
      for which the processing needs to error out.
    :return: DynamicFrame
    """

    def __call__(self, frame, paths, transformation_ctx = "", info = "", stageThreshold = 0, totalThreshold = 0):
        return frame.drop_fields(paths, transformation_ctx, info, stageThreshold, totalThreshold)

    @classmethod
    def describeArgs(cls):
        arg1 = {"name": "frame",
                "type": "DynamicFrame",
                "description": "The DynamicFrame from which to drop fields",
                "optional": False,
                "defaultValue": None}
        arg2 = {"name": "paths",
                "type": "List[String]",
                "description": "full paths corresponding to nodes to drop",
                "optional": False,
                "defaultValue": None}
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
        return "Drop fields from a DynamicFrame"

    @classmethod
    def describeErrors(cls):
        return []

    @classmethod
    def describeReturn(cls):
        return {"type": "DynamicFrame",
                "description": "new DynamicFrame without indicated fields"}


class SelectFields(GlueTransform):
    """
    Get fields within a DynamicFrame

    :param frame: DynamicFrame
    :param paths: List of Strings, each the full path to a node you want to get
    :param info: String, any string to be associated with errors in this transformation.
    :param stageThreshold: Long, number of errors in the given transformation for which the processing needs to error out.
    :param totalThreshold: Long, total number of errors upto and including in this transformation
      for which the processing needs to error out.
    :return: DynamicFrame
    """

    def __call__(self, frame, paths, transformation_ctx = "", info = "", stageThreshold = 0, totalThreshold = 0):
        return frame.select_fields(paths, transformation_ctx, info, stageThreshold, totalThreshold)

    @classmethod
    def describeArgs(cls):
        arg1 = {"name": "frame",
                "type": "DynamicFrame",
                "description": "The DynamicFrame from which to select fields",
                "optional": False,
                "defaultValue": None}
        arg2 = {"name": "paths",
                "type": "List[String]",
                "description": "full paths corresponding to nodes to select",
                "optional": False,
                "defaultValue": None}
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
        return "Select fields from a DynamicFrame"

    @classmethod
    def describeErrors(cls):
        return []

    @classmethod
    def describeReturn(cls):
        return {"type": "DynamicFrame",
                "description": "new DynamicFrame with only indicated fields"}


class SplitFields(GlueTransform):
    """
    Split fields within a DynamicFrame

    :param frame: DynamicFrame
    :param paths: List of Strings, each the full path to a node that you would like
      to split into a new frame
    :param info: String, any string to be associated with errors in this transformation.
    :param stageThreshold: Long, number of errors in the given transformation for which the processing needs to error out.
    :param totalThreshold: Long, total number of errors upto and including in this transformation
      for which the processing needs to error out.
    :return: DynamicFrameCollection with two Dynamic Frames, the first containing all the fields that you have
      split off, and the second containing the remaining fields
    """

    def __call__(self, frame, paths, name1 = None, name2 = None, transformation_ctx = "",  info = "", stageThreshold = 0, totalThreshold = 0):
        # Incorporate the existing DynamicFrame name into the new names.
        frame_name = frame.name if len(frame.name) > 0 else "frame"

        if name1 == None:
            name1 = frame_name + "1"
        if name2 == None:
            name2 = frame_name + "2"

        return frame.split_fields(paths, name1, name2, transformation_ctx, info, stageThreshold, totalThreshold)

    @classmethod
    def describeArgs(cls):
        arg1 = {"name": "frame",
                "type": "DynamicFrame",
                "description": "DynamicFrame from which to split fields",
                "optional": False,
                "defaultValue": None}
        arg2 = {"name": "paths",
                "type": "List[String]",
                "description": "full paths corresponding to nodes to split into new DynamicFrame",
                "optional": False,
                "defaultValue": None}
        arg3 = {"name": "frame1",
                "type": "String",
                "description": "name for the dynamic frame to be split off",
                "optional": True,
                "defaultValue": "frame1"}
        arg4 = {"name": "frame2",
                "type": "String",
                "description": "name for the dynamic frame remains on original",
                "optional": True,
                "defaultValue": "frame2"}
        arg5 = {"name": "transformation_ctx",
                "type": "String",
                "description": "A unique string that is used to identify stats / state information",
                "optional": True,
                "defaultValue": ""}
        arg6 = {"name": "info",
                "type": "String",
                "description": "Any string to be associated with errors in the transformation",
                "optional": True,
                "defaultValue": "\"\""}
        arg7 = {"name": "stageThreshold",
                "type": "Integer",
                "description": "Max number of errors in the transformation until processing will error out",
                "optional": True,
                "defaultValue": "0"}
        arg8 = {"name": "totalThreshold",
                "type": "Integer",
                "description": "Max number of errors total until processing will error out.",
                "optional": True,
                "defaultValue": "0"}

        return [arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8]

    @classmethod
    def describeTransform(cls):
        return "Split fields within a DynamicFrame"

    @classmethod
    def describeErrors(cls):
        return []

    @classmethod
    def describeReturn(cls):
        desc = "[new DynamicFrame with only indicated fields, new DynamicFrame without indicated fields]"
        return {"type": "DynamicFrameCollection",
                "description": desc}

class SplitRows(GlueTransform):
    """
    Split rows within a DynamicFrame

    :param frame: DynamicFrame
    :param comparison_dict: a dictionary where the key is the path to a column,
      the the value is another dictionary maping comparators to the value to which the column 
      will be compared, e.g. {"age": {">": 10, "<": 20}} will give back rows where age between 10 and 20
      exclusive split from rows that do not meet this criteria
    :param info: String, any string to be associated with errors in this transformation.
    :param stageThreshold: Long, number of errors in the given transformation for which the processing needs to error out.
    :param totalThreshold: Long, total number of errors upto and including in this transformation
      for which the processing needs to error out.
    :return: A DynamicFrameCollection with two Dynamic Frames, the first containing all the rows that you have
      split off, and the second containing the remaining rows
    """

    def __call__(self, frame, comparison_dict, name1 = "frame1", name2 = "frame2", transformation_ctx = "", info = None, stageThreshold = 0, totalThreshold = 0):
        info = info or ""
        return frame.split_rows(comparison_dict, name1, name2, transformation_ctx, info, stageThreshold, totalThreshold)

    @classmethod
    def describeArgs(cls):
        arg1 = {"name": "frame",
                "type": "DynamicFrame",
                "description": "DynamicFrame from which to split rows",
                "optional": False,
                "defaultValue": None}
        arg2 = {"name": "comparison_dict",
                "type": "Dictionary, {String 'path to node': {String 'operator': String or Integer 'value'}}",
                "description": "{paths to columns: {comparators: value to which each the column will be compared.}}\
                 Example: {'age': {'>': 10, '<': 20}} will give back rows where age is between 10 and 20 exclusive, \
                 and rows where this criteria is not met",
                "optional": False,
                "defaultValue": None}
        arg3 = {"name": "frame1",
                "type": "String",
                "description": "name for the dynamic frame to be split off",
                "optional": True,
                "defaultValue": "frame1"}
        arg4 = {"name": "frame2",
                "type": "String",
                "description": "name for the dynamic frame remains on original",
                "optional": True,
                "defaultValue": "frame2"}
        arg5 = {"name": "transformation_ctx",
                "type": "String",
                "description": "A unique string that is used to identify stats / state information",
                "optional": True,
                "defaultValue": ""}
        arg6 = {"name": "info",
                "type": "String",
                "description": "Any string to be associated with errors in the transformation",
                "optional": True,
                "defaultValue": None}
        arg7 = {"name": "stageThreshold",
                "type": "Integer",
                "description": "Max number of errors in the transformation until processing will error out",
                "optional": True,
                "defaultValue": "0"}
        arg8 = {"name": "totalThreshold",
                "type": "Integer",
                "description": "Max number of errors total until processing will error out.",
                "optional": True,
                "defaultValue": "0"}


        return [arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8]

    @classmethod
    def describeTransform(cls):
        return "Split rows within a DynamicFrame based on comparators"

    @classmethod
    def describeErrors(cls):
        return []

    @classmethod
    def describeReturn(cls):
        desc = "DynamicFrameCollection[new DynamicFrame with only indicated rows, new DynamicFrame without indicated rows]"
        return {"type": "DynamicFrameCollection",
                "description": desc}

class Join(GlueTransform):

    def __call__(self, frame1, frame2,  keys1, keys2, transformation_ctx = ""):
        return frame1.join(keys1, keys2, frame2)

    @classmethod
    def describeArgs(cls):
        arg1 = {"name": "frame1",
                "type": "DynamicFrame",
                "description": "join this DynamicFrame",
                "optional": False,
                "defaultValue": None}

        arg1 = {"name": "frame2",
                "type": "DynamicFrame",
                "description": "join with this DynamicFrame",
                "optional": False,
                "defaultValue": None}

        arg2 = {"name": "keys1",
                "type": "String",
                "description": "The keys to join on for the first frame",
                "optional": False,
                "defaultValue": None}

        arg3 = {"name": "keys2",
                "type": "String",
                "description": "The keys to join on for the second frame",
                "optional": False,
                "defaultValue": None}

        return [arg1, arg2, arg3, arg4]

    @classmethod
    def describeTransform(cls):
        return "equality join two dynamic frames DynamicFrames"

    @classmethod
    def describeErrors(cls):
        return []

    @classmethod
    def describeReturn(cls):
        return {"type": "DynamicFrame",
                "description": "DynamicFrame obtained by joining two frames"}
    

class Spigot(GlueTransform):

    def __call__(self, frame, path, options, transformation_ctx = ""):
        return frame.spigot(path,options,transformation_ctx)

    @classmethod
    def describeArgs(cls):
        arg1 = {"name": "frame",
                "type": "DynamicFrame",
                "description": "spigot this DynamicFrame",
                "optional": False,
                "defaultValue": None}

        arg2 = {"name": "path",
                "type": "string",
                "description": "file path to write spigot",
                "optional": False,
                "defaultValue": None}

        arg3 = {"name": "options",
                "type": "Json",
                "description": "topk -> first k records, prob -> probability of picking any record",
                "optional": True,
                "defaultValue": None}

        return [arg1, arg2, arg3]

    @classmethod
    def describeTransform(cls):
        return "write sample records to path destination mid transformation"

    @classmethod
    def describeErrors(cls):
        return []

    @classmethod
    def describeReturn(cls):
        return {"type": "DynamicFrame",
                "description": "DynamicFrame is the same as the infput dynamicFrame with an additional write step"}
