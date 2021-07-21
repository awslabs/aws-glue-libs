# Copyright 2016-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
 
class DataFrameReader(object):
    def __init__(self, glue_context):
        self._glue_context = glue_context

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

        return self._glue_context.create_data_frame_from_catalog(db, table_name, redshift_tmp_dir, transformation_ctx, push_down_predicate, additional_options, catalog_id, **kwargs)

    def from_options(self, connection_type, connection_options={},
                     format=None, format_options={}, transformation_ctx="", push_down_predicate = "", **kwargs):
        """Creates a DataFrame with the specified connection and format.
        """
        return self._glue_context.create_data_frame_from_options(connection_type,
                                                                    connection_options,
                                                                    format,
                                                                    format_options, transformation_ctx, push_down_predicate, **kwargs)
