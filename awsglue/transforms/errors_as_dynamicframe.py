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

class ErrorsAsDynamicFrame(GlueTransform):

    def __call__(self, frame):
        """
        Returns a DynamicFrame which has error records leading up to the source DynmaicFrame, nested in the returned DynamicFrame.

        :param frame: Source dynamicFrame
        """
        return frame.errorsAsDynamicFrame()

    @classmethod
    def describeArgs(cls):
        arg1 = {"name": "frame",
                "type": "DynamicFrame",
                "description": "The DynamicFrame on which to call errorsAsDynamicFrame",
                "optional": False,
                "defaultValue": None}
        return [arg1]

    @classmethod
    def describeTransform(cls):
        return "Get error records leading up to the source DynmaicFrame"

    @classmethod
    def describeErrors(cls):
        return []

    @classmethod
    def describeReturn(cls):
        return {"type": "DynamicFrame",
                "description": "new DynamicFrame with error DynamicRecords"}
