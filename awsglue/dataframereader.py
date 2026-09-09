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