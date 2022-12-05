class DataFrameWriter(object):
    def __init__(self, glue_context):
        self._glue_context = glue_context
    def from_catalog(self, frame, database=None, table_name=None, redshift_tmp_dir="", transformation_ctx="",
                     additional_options={}, catalog_id=None, **kwargs):
        """Writes a DataFrame with the specified catalog name space and table name.
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

        return self._glue_context.write_data_frame_from_catalog(frame, db, table_name, redshift_tmp_dir,
                                                                   transformation_ctx, additional_options, catalog_id)
