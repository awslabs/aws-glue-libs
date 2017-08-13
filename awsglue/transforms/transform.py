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

class GlueTransform(object):
    """Base class for all Glue Transforms.

    All Glue transformations should inherit from GlueTransform and define a
    __call__ method. They can optionally override the name classmethod or use
    the default of the class name.
    """

    @classmethod
    def apply(cls, *args, **kwargs):
        transform = cls()
        return transform(*args, **kwargs)

    @classmethod
    def name(cls):
        return cls.__name__

    @classmethod
    def describeArgs(cls):
        '''
        Returns: a list of dictionaries, with each corresponding to
        an argument, in the following format:
                [{"name": "<name of argument>",
                 "type": "<type of argument>",
                 "description": "<description of argument>",
                 "optional": "<Boolean>",
                 "defaultValue": "<String default value or None>"}, ...]
        Raises: NotImplementedError if not implemented by Transform
        '''
        raise NotImplementedError("describeArgs method not implemented for Transform {}".format(cls.__name__))

    @classmethod
    def describeReturn(cls):
        '''
        Returns: A dictionary with information about the return type,
        in the following format:
                {"type": "<return type>",
                "description": "<description of output>"}
        Raises: NotImplementedError if not implemented by Transform
        '''
        raise NotImplementedError("describeReturn method not implemented for Transform {}".format(cls.__name__))

    @classmethod
    def describeTransform(cls):
        '''
        Returns: A string describing the transform, e.g.
                "Base class for all Glue Transforms"
        Raises: NotImplementedError if not implemented by Transform
        '''

        raise NotImplementedError("describeTransform method not implemented for Transform {}".format(cls.__name__))

    @classmethod
    def describeErrors(cls):
        '''
        Returns: A list of dictionaries, each describing possible errors thrown by
        this transform, in the following format:
                [{"type": "<type of error>",
                 "description": "<description of error>"}]
        Raises: NotImplementedError if not implemented by Transform
        '''
        raise NotImplementedError("describeErrors method not implemented for Transform {}".format(cls.__name__))

    @classmethod
    def describe(cls):
        return {"transform": {"name": cls.name(),
                "args": cls.describeArgs(),
                "returns": cls.describeReturn(),
                "description": cls.describeTransform(),
                "raises": cls.describeErrors(),
                "location": "internal"}}

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __repr__(self):
        return "<Transform: {}>".format(self.name())