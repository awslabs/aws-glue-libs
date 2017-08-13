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
from unbox import Unbox
from unnest_frame import UnnestFrame
from relationalize import Relationalize
from field_transforms import RenameField, DropFields, SelectFields, SplitFields, SplitRows, Join
from collection_transforms import SelectFromCollection, MapToCollection, FlatMap
from drop_nulls import DropNullFields
from apply_mapping import ApplyMapping
from resolve_choice import ResolveChoice
from errors_as_dynamicframe import ErrorsAsDynamicFrame
import json

ALL_TRANSFORMS = {Unbox, RenameField, DropFields, SplitFields, SelectFields, SplitRows,
                  UnnestFrame, Relationalize, SelectFromCollection,
                  MapToCollection, ErrorsAsDynamicFrame, FlatMap, DropNullFields, Join, ApplyMapping, ResolveChoice}

__all__ = [transform.__name__ for transform in ALL_TRANSFORMS]

def get_transforms():
    return {transform() for transform in ALL_TRANSFORMS}

def get_transform(name):
    transform, = [t for t in get_transforms() if t.name().lower() == name.lower()] or (None,)
    return transform

def describe_transform(name):
    transform = get_transform(name)
    description = transform.describe() if transform else {}
    return json.dumps(description, sort_keys=True, indent=4, separators=(',', ': '))
