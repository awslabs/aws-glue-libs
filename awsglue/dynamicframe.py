# Copyright 2016-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
from __future__ import print_function

import json
from builtins import str, int
from awsglue.utils import makeOptions, callsite
from builtins import map, filter
from awsglue.gluetypes import _deserialize_json_string, _create_dynamic_record, _revert_to_dict, _serialize_schema
from awsglue.utils import _call_site, _as_java_list, _as_scala_option, _as_resolve_choiceOption
from pyspark.rdd import RDD, PipelinedRDD
from pyspark.sql.dataframe import DataFrame
from pyspark.serializers import PickleSerializer, BatchedSerializer


class ResolveOption(object):
    """
    ResolveOption is used for resolve ChoiceType while converting DynamicRecord to DataFrame
    option.action includes "Project", "KeepAsStruct" and "Cast".
    """
    def __init__(self, path, action, target=None):
        """
        :param path: string, path name to ChoiceType
        :param action: string,
        :param target: spark sql Datatype
        """
        self.path = path
        self.action = action
        self.target = target


class DynamicFrame(object):

    def __init__(self, jdf, glue_ctx, name=""):
        self._jdf = jdf
        self.glue_ctx = glue_ctx
        self._ssql_ctx = glue_ctx._ssql_ctx
        self._sc = glue_ctx and glue_ctx._sc
        self._schema = None
        self._lazy_rdd = None
        self.name = name

    @property
    def _rdd(self):
        if self._lazy_rdd is None:
            jrdd = self._jdf.javaToPython()
            self._lazy_rdd = RDD(jrdd, self._sc, BatchedSerializer(PickleSerializer()))
        return self._lazy_rdd

    def with_frame_schema(self, schema):
        """ Specify schema so we don't have to compute it """
        return DynamicFrame(self._jdf.pyWithFrameSchema(_serialize_schema(schema)), self.glue_ctx, self.name)

    def schema(self):
        if self._schema is None:
            try:
                self._schema = _deserialize_json_string(self._jdf.schema().toString())
            except AttributeError as e:
                raise Exception("Unable to parse datatype from schema. %s" % e)
        return self._schema

    def show(self, num_rows = 20):
        self._jdf.show(num_rows)

    def filter(self, f, transformation_ctx = "", info="", stageThreshold=0, totalThreshold=0):
        def wrap_dict_with_dynamic_records(x):
                rec = _create_dynamic_record(x["record"])
                try:
                    return f(rec)
                except Exception as E:
                    if isinstance(E, KeyError) or isinstance(E, ValueError) or isinstance(E, TypeError):
                        return False
                    x['isError'] = True
                    x['errorMessage'] = E.message
                    return True

        def func(iterator):
            return filter(wrap_dict_with_dynamic_records, iterator)
        return self.mapPartitions(func, True, transformation_ctx, info, stageThreshold, totalThreshold)

    def mapPartitions(self, f, preservesPartitioning=True, transformation_ctx="", info="", stageThreshold=0, totalThreshold=0):
        def func(s, iterator):
            return f(iterator)
        return self.mapPartitionsWithIndex(func, preservesPartitioning, transformation_ctx, info, stageThreshold, totalThreshold)

    def map(self, f, preservesPartitioning=False,transformation_ctx = "", info="", stageThreshold=0, totalThreshold=0):
        def wrap_dict_with_dynamic_records(x):
            rec = _create_dynamic_record(x["record"])
            try:
                result_record = _revert_to_dict(f(rec))
                if result_record:
                    x["record"] = result_record
                else:
                    x['isError'] = True
                    x['errorMessage'] = "User-specified function returned None instead of DynamicRecord"
                return x
            except Exception as E:
                x['isError'] = True
                x['errorMessage'] = E.message
                return x
        def func(_, iterator):
            return map(wrap_dict_with_dynamic_records, iterator)
        return self.mapPartitionsWithIndex(func, preservesPartitioning, transformation_ctx, info, stageThreshold, totalThreshold)

    def mapPartitionsWithIndex(self, f, preservesPartitioning=False, transformation_ctx = "", info = "", stageThreshold = 0,totalThreshold = 0):
        return DynamicFrame(self.glue_ctx._jvm.DynamicFrame.fromPythonRDD(self._jdf,
            PipelinedRDD(self._rdd, f, preservesPartitioning)._jrdd, self.glue_ctx._ssql_ctx, transformation_ctx, self.name,
                                            _call_site(self._sc, callsite(), info), int(stageThreshold),
                                            int(totalThreshold)), self.glue_ctx, self.name)

    def printSchema(self):
        print(self._jdf.schema().treeString())

    def toDF(self, options = None):
        """
        Please specify also target type if you choose Project and Cast action type.

        :param options: Must be list of options

        >>>toDF([ResolveOption("a.b.c", "KeepAsStruct")])
        >>>toDF([ResolveOption("a.b.c", "Project", DoubleType())])
        """
        if options is None: options = []
        scala_options = []

        for option in options:
            if option.action != "KeepAsStruct" and option.target is None:
                raise Exception("Missing target type for resolve action %s." % option.action)

            scala_options.append(self.glue_ctx.convert_resolve_option(option.path, option.action, option.target))

        return DataFrame(self._jdf.toDF(self.glue_ctx._jvm.PythonUtils.toSeq(scala_options)), self.glue_ctx)

    @classmethod
    def fromDF(cls, dataframe, glue_ctx, name):
        """
        Convert a DataFrame to a DynamicFrame by converting DynamicRecords to Rows
        :param dataframe: A spark sql DataFrame
        :param glue_ctx: the GlueContext object
        :param name: name of the result DynamicFrame
        :return: DynamicFrame
        """
        return DynamicFrame(glue_ctx._jvm.DynamicFrame.apply(dataframe._jdf, glue_ctx._ssql_ctx),
                            glue_ctx, name)


    def unbox(self, path, format, transformation_ctx="", info = "", stageThreshold = 0, totalThreshold = 0, **options):
        """
        unbox a string field

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
        :return: a new DynamicFrame with unboxed DynamicRecords

        >>>unbox("a.b.c", "csv", separator="|")
        """
        return DynamicFrame(self._jdf.unbox(path, format, json.dumps(options), transformation_ctx,
                                            _call_site(self._sc, callsite(), info), int(stageThreshold),
                                            int(totalThreshold)),
                            self.glue_ctx, self.name)

    def drop_fields(self, paths, transformation_ctx = "", info = "", stageThreshold = 0, totalThreshold = 0):
        """
        :param paths: List of strings, each the full path to a node you want to drop
        :param info: String, any string to be associated with errors in this transformation.
        :param stageThreshold: Int, number of errors in the given transformation for which the processing needs to error out.
        :param totalThreshold: Int, total number of errors upto and including in this transformation
          for which the processing needs to error out.
        :return: DynamicFrame
        """
        if isinstance(paths, str):
            paths = [paths]

        return DynamicFrame(self._jdf.dropFields(self.glue_ctx._jvm.PythonUtils.toSeq(paths), transformation_ctx,
                                                 _call_site(self._sc, callsite(), info), int(stageThreshold), int(totalThreshold)),
                            self.glue_ctx, self.name)

    def select_fields(self, paths, transformation_ctx = "", info = "", stageThreshold = 0, totalThreshold = 0):
        """
        :param paths: List of strings, each the full path to a node you want to get
        :param info: String, any string to be associated with errors in this transformation.
        :param stageThreshold: Int, number of errors in the given transformation for which the processing needs to error out.
        :param totalThreshold: Int, total number of errors upto and including in this transformation
          for which the processing needs to error out.
        :return: DynamicFrame
        """
        if isinstance(paths, str):
            paths = [paths]

        return DynamicFrame(self._jdf.selectFields(self.glue_ctx._jvm.PythonUtils.toSeq(paths), transformation_ctx,
                                                   _call_site(self._sc, callsite(), info), int(stageThreshold), int(totalThreshold)),
                            self.glue_ctx, self.name)

    def split_fields(self, paths, name1, name2, transformation_ctx = "", info = "", stageThreshold = 0, totalThreshold = 0):
        """
        :param paths: List of strings, each the full path to a node you want to split into a new DynamicFrame
        :param name1: name for the dynamic frame to be split off
        :param name2: name for the dynamic frame remains on original
        :param info: String, any string to be associated with errors in this transformation.
        :param stageThreshold: Int, number of errors in the given transformation for which the processing needs to error out.
        :param totalThreshold: Int, total number of errors upto and including in this transformation
          for which the processing needs to error out.
        :return: DynamicFrameCollection with two DynamicFrames, the first containing all the nodes that you have split off,
          the second containing the nodes remaining on the original.
        """
        if isinstance(paths, str):
            paths = [paths]

        jdfs = _as_java_list(self._sc, self._jdf.splitFields(self.glue_ctx._jvm.PythonUtils.toSeq(paths), transformation_ctx,
                                     _call_site(self._sc, callsite(), info), int(stageThreshold), int(totalThreshold)))
        return DynamicFrameCollection({name1 : DynamicFrame(jdfs[0], self.glue_ctx, name1), name2 : DynamicFrame(jdfs[1], self.glue_ctx, name2)}, self.glue_ctx)

    def split_rows(self, comparison_dict, name1, name2, transformation_ctx = "", info= "", stageThreshold = 0, totalThreshold = 0):
        """
        :param comparison_dict: a dictionary where the key is the path to a column, the the value is another
        dictionary maping comparators to the value to which the column will be compared.
        e.g. {"age": {">": 10, "<": 20}} will give back rows where age between 10 and 20 exclusive split from those
        that do not meet this criteria.
        :param name1: name for the dynamic frame to be split off
        :param name2: name for the dynamic frame remains on original
        :param info: String, any string to be associated with errors in this transformation.
        :param stageThreshold: Int, number of errors in the given transformation for which the processing needs to error out.
        :param totalThreshold: Int, total number of errors upto and including in this transformation
          for which the processing needs to error out.
        :return: DynamicFrameCollection with two DynamicFrames, the first containing all the nodes that you have split off,
          the second containing the nodes remaining on the original.
        """
        paths, values, operators = [], [], []

        for key, value in comparison_dict.items():
            paths.extend([key] * len(value))
            for k, v in value.items():
                operators.append(k)
                if isinstance(v, int):
                    values.append(int(v))
                else:
                    values.append(v)

        jdfs = _as_java_list(self._sc, self._jdf.splitRows(self.glue_ctx._jvm.PythonUtils.toSeq(paths),
                                                      self.glue_ctx._jvm.PythonUtils.toSeq(values),
                                                      self.glue_ctx._jvm.PythonUtils.toSeq(operators),
                                                      transformation_ctx, _call_site(self._sc, callsite(), info),
                                                      long(stageThreshold), long(totalThreshold)))
        return DynamicFrameCollection({name1 : DynamicFrame(jdfs[0], self.glue_ctx, name1), name2 : DynamicFrame(jdfs[1], self.glue_ctx, name2)}, self.glue_ctx)

    def rename_field(self, oldName, newName, transformation_ctx = "", info = "", stageThreshold = 0, totalThreshold = 0):
        """
        :param oldName: String, full path to the node you want to rename
        :param newName: String, new name including full path
        :param info: String, any string to be associated with errors in this transformation.
        :param stageThreshold: Int, number of errors in the given transformation for which the processing needs to error out.
        :param totalThreshold: Int, total number of errors upto and including in this transformation
          for which the processing needs to error out.
        :return: DynamicFrame
        """
        return DynamicFrame(self._jdf.renameField(oldName, newName, transformation_ctx, _call_site(self._sc, callsite(), info),
                                                  int(stageThreshold), int(totalThreshold)), self.glue_ctx, self.name)

    def write(self, connection_type, connection_options={},
              format=None, format_options={}, accumulator_size = 0):
        return self.glue_ctx.write_from_options(frame_or_dfc=self,
                                                connection_type=connection_type,
                                                connection_options=connection_options,
                                                format=format,
                                                format_options=format_options,
                                                accumulator_size=accumulator_size)

    def count(self):
        return self._jdf.count()

    def spigot(self, path, options={}, transformation_ctx = "", info = "", stageThreshold = 0, totalThreshold = 0):
        return DynamicFrame(self._jdf.spigot(path, makeOptions(self._sc, options), transformation_ctx,
                                             _call_site(self._sc, callsite(), info), int(stageThreshold),
                                             int(totalThreshold)),
                            self.glue_ctx, self.name)

    def join(self, paths1, paths2, frame2, transformation_ctx = "", info = "", stageThreshold = 0, totalThreshold = 0):
        if isinstance(paths1, str):
            paths1 = [paths1]
        if isinstance(paths2, str):
            paths2 = [paths2]

        return DynamicFrame(self._jdf.join(self.glue_ctx._jvm.PythonUtils.toSeq(paths1), self.glue_ctx._jvm.PythonUtils.toSeq(paths2), frame2._jdf, transformation_ctx, _call_site(self._sc, callsite(), info), long(stageThreshold), long(totalThreshold)), self.glue_ctx, self.name + frame2.name)

    def unnest(self, transformation_ctx = "", info = "", stageThreshold = 0, totalThreshold = 0):
        """
        unnest a dynamic frame. i.e. flattens nested objects to top level elements.
        It also generates joinkeys for array objects
        :param info: String, any string to be associated with errors in this transformation.
        :param stageThreshold: Int, number of errors in the given transformation for which the processing needs to error out.
        :param totalThreshold: Int, total number of errors upto and including in this transformation
          for which the processing needs to error out.
        :return: a new unnested dynamic frame

        >>>unnest()
        """
        return DynamicFrame(self._jdf.unnest(transformation_ctx, _call_site(self._sc, callsite(), info), int(stageThreshold), int(totalThreshold)), self.glue_ctx, self.name)

    def relationalize(self, root_table_name, staging_path, options={}, transformation_ctx = "", info = "", stageThreshold = 0, totalThreshold = 0):
        """
        Relationalizes a dynamic frame. i.e. produces a list of frames that are
        generated by unnesting nested columns and pivoting array columns. The
        pivoted array column can be joined to the root table using the joinkey
        generated in unnest phase
        :param root_table_name: name for the root table
        :param staging_path: path to store partitions of pivoted tables in csv format. Pivoted tables are read back from
            this path
        :param options: dict of optional parameters for relationalize
        :param transformation_ctx: context key to retrieve metadata about the current transformation
        :param info: String, any string to be associated with errors in this transformation.
        :param stageThreshold: Int, number of errors in the given transformation for which the processing needs to error out.
        :param totalThreshold: Int, total number of errors upto and including in this transformation
          for which the processing needs to error out.
        :return: DynamicFrameCollection
        """
        _rFrames = _as_java_list(self._sc, self._jdf.relationalize(root_table_name, staging_path,
                                                              makeOptions(self._sc, options),
                                                              transformation_ctx, _call_site(self._sc, callsite(), info),
                                                              int(stageThreshold), int(totalThreshold)))
        return DynamicFrameCollection(dict((df.getName(), DynamicFrame(df, self.glue_ctx, df.getName())) for df in _rFrames), self.glue_ctx)

    def applyMapping(self, *args, **kwargs):
        # In a previous version we passed args[1:] and in our tests we passed
        # the DynamicFrame as the first argument. This checks for that case
        # to avoid regressions.
        if len(args) > 0 and isinstance(args[0], DynamicFrame):
            return self.apply_mapping(*(args[1:]), **kwargs)
        else:
            return self.apply_mapping(*args, **kwargs)

    def apply_mapping(self, mappings, case_sensitive = False, transformation_ctx = "", info = "", stageThreshold = 0, totalThreshold = 0):
        def _to_java_mapping(mapping_tup):
            if not isinstance(mapping_tup, tuple):
                raise TypeError("Mapping must be specified as a tuple. Got " +
                                mapping_tup)

            tup2 = self.glue_ctx._jvm.scala.Tuple2
            tup3 = self.glue_ctx._jvm.scala.Tuple3
            tup4 = self.glue_ctx._jvm.scala.Tuple4
            java_cls = self.glue_ctx._jvm.MappingSpec

            if len(mapping_tup) == 2:
                return java_cls.apply(tup2.apply(mapping_tup[0], mapping_tup[1]))
            elif len(mapping_tup) == 3:
                return java_cls.apply(tup3.apply(mapping_tup[0], mapping_tup[1], mapping_tup[2]))
            elif len(mapping_tup) == 4:
                return java_cls.apply(tup4.apply(mapping_tup[0], mapping_tup[1], mapping_tup[2], mapping_tup[3]))
            else:
                raise ValueError("Mapping tuple must be of length 2, 3, or 4"
                                 "Got tuple of length " + str(len(mapping_tup)))

        if isinstance(mappings, tuple):
            mappings = [mappings]

        mappings_list = [ _to_java_mapping(m) for m in mappings ]

        new_jdf = self._jdf.applyMapping(
            self.glue_ctx._jvm.PythonUtils.toSeq(mappings_list),
            case_sensitive,
            transformation_ctx,
            _call_site(self._sc, callsite(), info), int(stageThreshold), int(totalThreshold))

        return DynamicFrame(new_jdf, self.glue_ctx, self.name)

    def resolveChoice(self, specs=None, choice="", database=None, table_name=None,
                      transformation_ctx="", info="", stageThreshold=0, totalThreshold=0):
        """
        :param specs: specification for choice type and corresponding resolve action,
                      if the specs is empty, then tape backend would go one round of the data
                      to get schema, and then based on the schema to resolve choice.
        :param choice: default option when choice type path found missing from specs
        :param database: Glue catalog database name, required for MATCH_CATALOG choice
        :param table_name: Glue catalog table name, required for MATCH_CATALOG choice
        :return: a new DynamicFrame
        """
        def _to_java_specs(specs_tup):
            path, action = specs_tup
            return self.glue_ctx._jvm.ResolveSpec.apply(path, action)

        if specs is None and not choice:
            raise Exception("Parameter specs and option are both missing, add one.")

        if specs is not None and choice:
            raise Exception("Parameter specs and option are both specified, choose one.")

        if specs is None:
            specs = []

        if isinstance(specs, tuple):
            specs = [specs]

        specs_list = [ _to_java_specs(m) for m in specs ]

        choice_option = _as_scala_option(self._sc, _as_resolve_choiceOption(self._sc, choice))
        database_option = _as_scala_option(self._sc, database)
        table_name_option = _as_scala_option(self._sc, table_name)

        new_jdf = self._jdf.resolveChoice(
            self.glue_ctx._jvm.PythonUtils.toSeq(specs_list),
            choice_option, database_option, table_name_option,
            transformation_ctx,
            _call_site(self._sc, callsite(), info), int(stageThreshold), int(totalThreshold))

        return DynamicFrame(new_jdf, self.glue_ctx, self.name)

    def errorsAsDynamicFrame(self):
        """
        Returns a DynamicFrame which has error records nested.
        :return: DynamicFrame
        """
        return DynamicFrame(self._jdf.errorsAsDynamicFrame(), self.glue_ctx, self.name)

    def errorsCount(self):
        """
        Returns the total error records in a DynamicFrames
        :return: Int
        """
        return self._jdf.errorsCount()

    def stageErrorsCount(self):
        """
        Returns the error generated in the transformation to this DynamicFrame
        :return: Int
        """
        return self._jdf.stageErrorsCount()

    def assertErrorThreshold(self):
        """
        Asserts for the errors in the transformations which yielded this DynamicFrame
        :return: Exception
        """
        return self._jdf.assertErrorThreshold()


class DynamicFrameCollection(object):

    def __init__(self, dynamic_frames, glue_ctx):
        """
        :param df_dict: a dictionary of dynamic frame
        """
        self._glue_ctx = glue_ctx
        if isinstance(dynamic_frames, list):
            self._df_dict = { df.name: df for df in dynamic_frames }
        elif isinstance(dynamic_frames, dict):
            self._df_dict = dynamic_frames
        else:
            raise TypeError("dynamic_frames must be list or dict.")

    def __getitem__(self, key):
        return self._df_dict[key]

    def __len__(self):
        return len(self._df_dict)

    def keys(self):
        return self._df_dict.keys()

    def values(self):
        return self._df_dict.values()

    def select(self, key, transformation_ctx = ""):
        """
        :param key: get dynamic frame of key
        :return: a dynamic frame
        """
        if key in self._df_dict:
            return self.__getitem__(key)
        else:
            return DynamicFrame(self._glue_ctx._jvm.DynamicFrame.emptyDynamicFrame(self._glue_ctx._glue_scala_context), self._glue_ctx, key)

    def map(self, callable, transformation_ctx = ""):
        """
        :param callable: pass in a callable to every DynamicFrame
        :return: a DynamicFrameCollection
        """
        new_dict = {}
        for k,v in self._df_dict.iteritems():
            res = callable(v, transformation_ctx+':'+k)
            if not isinstance(res, DynamicFrame):
                raise TypeError("callable must return a DynamicFrame. "\
                                "Got {}".format(str(type(res))))
            new_dict[k] = res

        return DynamicFrameCollection(new_dict, self._glue_ctx)

    def flatmap(self, f, transformation_ctx = ""):
        """
        :param f: A function that takes a DynamicFrame and returns a
                  DynamicFrame or a DynamicFrameCollection.
        :return: A DynamicFrameCollection
        """
        new_dict = {}

        for frame in self._df_dict.itervalues():
            res = f(frame, transformation_ctx+':'+frame.name)

            if isinstance(res, DynamicFrame):
                new_dict[res.name] = res
            elif isinstance(res, DynamicFrameCollection):
                new_dict.update(res)
            else:
                raise TypeError("Function argument to flatmap must return "\
                                "DynamicFrame or DynamicFrameCollection."\
                                " Got {}".format(str(type(res))))

        return DynamicFrameCollection(new_dict, self._glue_ctx)


class DynamicFrameReader(object):
    def __init__(self, glue_context):
        self._glue_context = glue_context

    def from_rdd(self, data, name, schema=None, sampleRatio=None):
        """Creates a DynamicFrame from an RDD.
        """
        return self._glue_context.create_dynamic_frame_from_rdd(data, name, schema, sampleRatio)

    def from_options(self, connection_type, connection_options={},
                     format=None, format_options={}, transformation_ctx="", push_down_predicate = "", **kwargs):
        """Creates a DynamicFrame with the specified connection and format.
        """
        return self._glue_context.create_dynamic_frame_from_options(connection_type,
                                                                    connection_options,
                                                                    format,
                                                                    format_options, transformation_ctx, push_down_predicate, **kwargs)

    def from_catalog(self, database = None, table_name = None, redshift_tmp_dir = "", transformation_ctx = "", push_down_predicate = "", additional_options = {}, catalog_id = None, **kwargs):
        """Creates a DynamicFrame with the specified catalog name space and table name.
        """
        if database is not None and "name_space" in kwargs:
            raise Exception("Parameter name_space and database are both specified, choose one.")
        elif database is None and "name_space" not in kwargs:
            raise Exception("Parameter name_space or database is missing.")
        elif "name_space" in kwargs:
            db = kwargs.pop("name_space")
        else:
            db = database

        if table_name is None:
            raise Exception("Parameter table_name is missing.")

        return self._glue_context.create_dynamic_frame_from_catalog(db, table_name, redshift_tmp_dir, transformation_ctx, push_down_predicate, additional_options, catalog_id, **kwargs)


class DynamicFrameWriter(object):
    def __init__(self, glue_context):
        self._glue_context = glue_context

    def from_options(self, frame, connection_type, connection_options={},
                       format=None, format_options={}, transformation_ctx=""):
        """Creates a DynamicFrame with the specified connection and format.
        """
        return self._glue_context.write_dynamic_frame_from_options(frame,
                                                                 connection_type,
                                                                 connection_options,
                                                                 format,
                                                                 format_options, transformation_ctx)

    def from_catalog(self, frame, database = None, table_name = None, redshift_tmp_dir = "", transformation_ctx = "", additional_options = {}, catalog_id = None, **kwargs):
        """Creates a DynamicFrame with the specified catalog name space and table name.
        """
        if database is not None and "name_space" in kwargs:
            raise Exception("Parameter name_space and database are both specified, choose one.")
        elif database is None and "name_space" not in kwargs:
            raise Exception("Parameter name_space or database is missing.")
        elif "name_space" in kwargs:
            db = kwargs.pop("name_space")
        else:
            db = database

        if table_name is None:
            raise Exception("Parameter table_name is missing.")

        return self._glue_context.write_dynamic_frame_from_catalog(frame, db, table_name, redshift_tmp_dir, transformation_ctx, additional_options, catalog_id)

    def from_jdbc_conf(self, frame, catalog_connection, connection_options={}, redshift_tmp_dir = "", transformation_ctx=""):
        """Creates a DynamicFrame with the specified JDBC connection information.
        """
        return self._glue_context.write_dynamic_frame_from_jdbc_conf(frame,
                                                              catalog_connection,
                                                              connection_options,
                                                              redshift_tmp_dir, transformation_ctx)
