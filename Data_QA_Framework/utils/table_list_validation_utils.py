# utils/table_list_validation_utils.py

def validate_table_sources(table_sources):
    """
    Validates that each table source is a tuple (list_of_tables, source_name)
    where list_of_tables is a list of strings and source_name is a string.
    Raises ValueError if the structure is invalid.
    """
    for tables, source_name in table_sources:
        if isinstance(tables, str):
            raise ValueError(
                f"[❌] Expected a list of table names for source '{source_name}', but got a single string: {tables}"
            )
        if not isinstance(tables, list):
            raise ValueError(
                f"[❌] Expected a list of table names for source '{source_name}', but got: {type(tables).__name__}"
            )
        for table in tables:
            if not isinstance(table, str):
                raise ValueError(
                    f"[❌] Table name in source '{source_name}' must be a string, got: {type(table).__name__}"
                )
