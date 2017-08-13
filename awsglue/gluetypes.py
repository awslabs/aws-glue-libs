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

import json


class DataType(object):
    def __init__(self, properties={}):
        self.properties = properties

    def __eq__(self, other):
        return (isinstance(other, self.__class__) and
                self.__dict__ == other.__dict__)

    def __hash__(self):
        return hash(str(self.__class__))

    @classmethod
    def typeName(cls):
        return cls.__name__[:-4].lower()

    def jsonValue(self):
        return {"dataType": self.typeName(), "properties": self.properties}


# ---------------------------------------------------------------------------
# Atomic types
# ---------------------------------------------------------------------------

# Note we can't use singletons like Spark does because DataType instances can
# have properties.


class AtomicType(DataType):
    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.properties)

    @classmethod
    def fromJsonValue(cls, json_value):
        return cls(**{k: v for k, v in json_value.iteritems()
                      if k != "dataType"})


class BinaryType(AtomicType):
    pass


class BooleanType(AtomicType):
    pass


class ByteType(AtomicType):
    pass


class DateType(AtomicType):
    pass


class DecimalType(AtomicType):
    def __init__(self, precision=10, scale=2, properties={}):
        super(DecimalType, self).__init__(properties)
        self.precision = precision
        self.scale = scale

    def __repr__(self):
        return "DecimalType({}, {}, {})".format(self.precision,
                                                self.scale,
                                                self.properties)

    def jsonValue(self):
        return dict(super(DecimalType, self).jsonValue().items() +
                    [('precision', self.precision), ('scale', self.scale)])


class DoubleType(AtomicType):
    pass


class EnumType(AtomicType):
    def __init__(self, options, properties={}):
        super(EnumType, self).__init__(properties)
        DataType.__init__(self, properties)
        self.options = options

    def __repr__(self):
        options_str = ",".join(self.options[0:3])
        if len(self.options) > 3:
            options_str = options_str + ",..."
        return "EnumType([{}], {})".format(options_str, self.properties)

    def jsonValue(self):
        dict(super(EnumType, self).jsonValue().items() +
             [('options', list(self.options))])


class FloatType(AtomicType):
    pass


class IntegerType(AtomicType):
    @classmethod
    def typeName(cls):
        return "int"


class LongType(AtomicType):
    pass


class NullType(AtomicType):
    pass


class ShortType(AtomicType):
    pass


class StringType(AtomicType):
    pass


class TimestampType(AtomicType):
    pass


class UnknownType(AtomicType):
    pass


# ---------------------------------------------------------------------------
# Collection types
# ---------------------------------------------------------------------------

class ArrayType(DataType):

    def __init__(self, elementType=UnknownType(), properties={}):
        assert isinstance(elementType, DataType),\
            "elementType should be DataType. Got" + str(elementType.__class__)
        super(ArrayType, self).__init__(properties)
        self.elementType = elementType

    def __repr__(self):
        return "ArrayType({}, {})".format(self.elementType, self.properties)

    def jsonValue(self):
        return dict(super(ArrayType, self).jsonValue().items() +
                    [("elementType", self.elementType.jsonValue())])

    @classmethod
    def fromJsonValue(cls, json_value):
        element_type = _deserialize_json_value(json_value["elementType"])
        return cls(elementType=element_type,
                   properties=json_value.get('properties', {}))


class ChoiceType(DataType):

    def __init__(self, choices=[], properties={}):
        super(ChoiceType, self).__init__(properties)
        self.choices = {}
        for choice in choices:
            self.add(choice)

    def __repr__(self):
        sorted_values = sorted(self.choices.values(),
                               key = lambda x: x.typeName())
        choice_str = "[{}]".format(",".join([str(c) for c in sorted_values]))

        return "ChoiceType({}, {})".format(choice_str, self.properties)

    def add(self, new_choice):
        if new_choice.typeName() in self.choices:
            raise ValueError("Attempting to insert duplicate choice",
                             new_choice)
        self.choices[new_choice.typeName()] = new_choice

    def merge(self, new_choices):
        if not isinstance(new_choices, list):
            new_choices = [ new_choices ]
        for choice in new_choices:
            existing = self.choices.get(choice.typeName(), UnknownType())
            self.choices[choice.typeName()] = mergeDataTypes(existing, choice)

    def jsonValue(self):
        return dict(super(ChoiceType, self).jsonValue().items() +
                    [("choices", [v.jsonValue()
                                  for v in self.choices.values()])])

    @classmethod
    def fromJsonValue(cls, json_value):
        choices = [_deserialize_json_value(c) for c in json_value["choices"]]
        return cls(choices=choices, properties=json_value.get('properties', {}))


class MapType(DataType):

    def __init__(self, valueType=UnknownType(), properties={}):
        assert isinstance(valueType, DataType), "valueType should be DataType"
        super(MapType, self).__init__(properties)
        self.valueType = valueType

    def __repr__(self):
        return "MapType({}, {})".format(self.valueType, self.properties)

    def jsonValue(self):
        return dict(super(MapType, self).jsonValue().items() +
                    [("valueType", self.valueType.jsonValue())])

    @classmethod
    def fromJsonValue(cls, json_value):
        return cls(valueType=_deserialize_json_value(json_value["valueType"]),
                   properties=json_value.get('properties', {}))


class Field(object):

    def __init__(self, name, dataType, properties={}):
        assert isinstance(dataType, DataType),\
            "dataType should be DataType. Got " + str(dataType.__class__)
        if not isinstance(name, str):
            name = name.encode('utf-8')
        self.name = name
        self.dataType = dataType
        self.properties = properties

    def __eq__(self, other):
        return (self.name == other.name and
                self.dataType == other.dataType)

    def __repr__(self):
        return "Field({}, {}, {})".format(self.name, self.dataType,
                                          self.properties)

    def jsonValue(self):
        return {"name": self.name,
                "container": self.dataType.jsonValue(),
                "properties": self.properties}

    @classmethod
    def fromJsonValue(cls, json_value):
        return cls(json_value["name"],
                   _deserialize_json_value(json_value["container"]),
                   json_value.get("properties", {}))


class StructType(DataType):

    def __init__(self, fields=[], properties={}):
        super(StructType, self).__init__(properties)
        assert all(isinstance(f, Field) for f in fields),\
            "fields should be a list of Field"
        self.fields = fields
        self.field_map = {field.name: field for field in fields}

    def __iter__(self):
        return iter(self.fields)

    def __repr__(self):
        return "StructType([{}], {})".format(
            ",".join([str(f) for f in self.fields]), self.properties)

    def add(self, field):
        assert isinstance(field, Field), "field must be of type Field"
        self.fields.append(field)
        self.field_map[field.name] = field

    def hasField(self, field):
        if isinstance(field, Field):
            field = field.name
        return field in self.field_map

    def getField(self, field):
        if isinstance(field, Field):
            field = field.name
        return self.field_map[field]

    def jsonValue(self):
        return dict(super(StructType, self).jsonValue().items() +
                    [("fields", [f.jsonValue() for f in self.fields])])

    @classmethod
    def fromJsonValue(cls, json_value):
        return cls([Field.fromJsonValue(f) for f in json_value["fields"]],
                   json_value.get("properties", {}))


class EntityType(DataType):
    def __init__(self, entity, base_type, properties):
        raise NotImplementedError("EntityTypes not yet supported in Tape.")


# ---------------------------------------------------------------------------
# Utility methods
# ---------------------------------------------------------------------------

_atomic_types = [BinaryType, BooleanType, ByteType, DateType, DecimalType,
                 DoubleType, EnumType, FloatType, IntegerType, LongType, NullType,
                 ShortType, StringType, TimestampType, UnknownType]


_complex_types = [ArrayType, ChoiceType, MapType, StructType]


_atomic_type_map = dict((t.typeName(), t) for t in _atomic_types)


_complex_type_map = dict((t.typeName(), t) for t in _complex_types)


_all_type_map = dict((t.typeName(), t) for t in _atomic_types + _complex_types)


def _deserialize_json_string(json_str):
    return _deserialize_json_value(json.loads(json_str))


def _deserialize_json_value(json_val):
    assert isinstance(json_val, dict), "Json value must be dictionary"
    data_type = json_val["dataType"]
    return _all_type_map[data_type].fromJsonValue(json_val)


def _make_choice(s1, s2):
    if isinstance(s1, ChoiceType):
        left_types = s1.choices
    else:
        left_types = {s1.typeName(): s1}

    if isinstance(s2, ChoiceType):
        right_types = s2.choices
    else:
        right_types = {s2.typeName(): s2}

    for typecode, datatype in left_types.iteritems():
        if typecode in right_types:
            right_types[typecode] = mergeDataTypes(datatype,
                                                   right_types[typecode])
        else:
            right_types[typecode] = datatype

    return ChoiceType(right_types.values(), s1.properties)


# Simple Python merge implementation. This is less efficient than the Scala
# version and should be used primarily for interactive manipulation.
# Has similar limitations to the Scala version -- does not merge properties,
# for instance.
def mergeDataTypes(s1, s2):
    if isinstance(s1, UnknownType) or isinstance(s1, NullType):
        return s2
    elif isinstance(s2, UnknownType) or isinstance(s2, NullType):
        return s1
    elif isinstance(s1, ChoiceType) or isinstance(s2, ChoiceType):
        return _make_choice(s1, s2)
    elif type(s1) != type(s2):
        return _make_choice(s1, s2)
    else:
        if isinstance(s1, StructType):
            new_fields = []
            # Fields that are present in both s1 and s2.
            for field in s1:
                if s2.hasField(field):
                    new_fields.append(
                        Field(field.name,
                              mergeDataTypes(field.dataType,
                                              s2.getField(field).dataType),
                              field.properties))
                else:
                    # Fields in s1 that are not in s2.
                    new_fields.append(Field(field.name, field.dataType,
                                            field.properties))

            # Fields in s2 that are not in s1.
            new_fields.extend([Field(field.name, field.dataType,
                                     field.properties)
                               for field in s2 if not s1.hasField(field)])
            return StructType(new_fields, s1.properties)
        elif isinstance(s1, ArrayType):
            return ArrayType(mergeDataTypes(s1.elementType, s2.elementType))
        elif isinstance(s1, MapType):
            return MapType(mergeDataTypes(s1.valueType, s2.valueType))
        elif isinstance(s1, EnumType):
            return EnumType(s1.options + s2.options)
        else:
            return s1
