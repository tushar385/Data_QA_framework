# from datetime import datetime
# from utils.snowflake_utils import snowflake_connection
# from utils.json_decorator_utils import print_decorated_json
# from utils.dump_results_to_csv_utils import dump_results_to_csv
# from decimal import Decimal

# def generate_mom_summary(*table_sources, date_column, date_column_type):
#     """
#     Generate month-over-month Unique COUNT and SUM for all columns grouped by month.
#     Text columns: Unique COUNT only, SUM null
#     Numeric columns: SUM only, Unique COUNT null
#     """
#     combined_tables = []
#     for tables, source_name in table_sources:
#         if tables:
#             combined_tables.extend([(table.strip(), source_name.strip()) for table in tables])

#     if not combined_tables:
#         return []

#     results = []

#     for table_fqn, source in combined_tables:
#         try:
#             database, schema, table = table_fqn.split('.')
#             cursor = snowflake_connection.cursor()

#             # Get column metadata
#             column_query = f'''
#                 SELECT COLUMN_NAME, DATA_TYPE
#                 FROM {database}.INFORMATION_SCHEMA.COLUMNS
#                 WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = '{schema}'
#             '''
#             cursor.execute(column_query)
#             columns = cursor.fetchall()
#             if not columns:
#                 continue

#             # Prepare month expression
#             if date_column_type.upper() == 'TEXT':
#                 month_expr = f"TO_DATE(LEFT({date_column}, 10))"
#             elif date_column_type.upper() == 'TIMESTAMP':
#                 month_expr = f"DATE_TRUNC('MONTH', {date_column})"
#             else:
#                 raise ValueError("date_column_type must be 'TEXT' or 'TIMESTAMP'")

#             # Build aggregations
#             agg_exprs = []
#             col_type_map = {}
#             for col, dtype in columns:
#                 col_quoted = f'"{col}"'
#                 dtype_upper = dtype.upper()
#                 col_type_map[col] = dtype_upper

#                 if any(x in dtype_upper for x in ['NUMBER', 'INT', 'FLOAT', 'DECIMAL']):
#                     agg_exprs.append(f'SUM({col_quoted}) AS {col}_sum')
#                 else:
#                     agg_exprs.append(f'COUNT(DISTINCT {col_quoted}) AS {col}_unique')

#             query = f'''
#                 SELECT
#                     {month_expr} AS month,
#                     {', '.join(agg_exprs)}
#                 FROM {table_fqn}
#                 GROUP BY 1
#                 ORDER BY 1
#             '''

#             cursor.execute(query)
#             rows = cursor.fetchall()
#             col_names = [desc[0] for desc in cursor.description]
#             timestamp = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

#             for row in rows:
#                 month_val = row[0].isoformat() if isinstance(row[0], datetime) else str(row[0])

#                 for idx, val in enumerate(row[1:], start=1):
#                     col_metric = col_names[idx]
#                     if '_' not in col_metric:
#                         continue
#                     col_name, metric = col_metric.rsplit('_', 1)

#                     dtype = col_type_map.get(col_name, "UNKNOWN")
#                     metric = metric.upper()
#                     value = float(val) if isinstance(val, Decimal) else val

#                     # Construct output dict based on data type
#                     if dtype in ['NUMBER', 'INT', 'FLOAT', 'DECIMAL']:
#                         # Numeric: Unique COUNT null, SUM value if metric=SUM
#                         if metric == 'SUM':
#                             results.append({
#                                 "source": source,
#                                 "table_name": table_fqn,
#                                 "column": col_name,
#                                 "Datatype": "NUMERIC",
#                                 "month": month_val,
#                                 "Unique COUNT": None,
#                                 "SUM": value,
#                                 "timestamp": timestamp
#                             })
#                     else:
#                         # Text/other: Unique COUNT value if metric=UNIQUE, SUM null
#                         if metric == 'UNIQUE':
#                             results.append({
#                                 "source": source,
#                                 "table_name": table_fqn,
#                                 "column": col_name,
#                                 "Datatype": "TEXT",
#                                 "month": month_val,
#                                 "Unique COUNT": value,
#                                 "SUM": None,
#                                 "timestamp": timestamp
#                             })

#         except Exception as e:
#             print(f"Error processing table {table_fqn}: {e}")
#         finally:
#             cursor.close()

#     print_decorated_json(results)
#     dump_results_to_csv(results, 'generate_mom_summary')
#     return results



from datetime import datetime
from utils.snowflake_utils import snowflake_connection
from utils.json_decorator_utils import print_decorated_json
from utils.dump_results_to_csv_utils import dump_results_to_csv
from decimal import Decimal

def generate_mom_summary(*table_sources_with_dates):
    """
    Generate month-over-month Unique COUNT and SUM for all columns grouped by month.
    Text columns: Unique COUNT only, SUM null
    Numeric columns: SUM only, Unique COUNT null

    Each item in table_sources_with_dates is a tuple:
        (table_list, source_name, date_column, date_column_type)
    """
    results = []

    for tables, source_name, date_column, date_column_type in table_sources_with_dates:
        if not tables:
            continue

        combined_tables = [(table.strip(), source_name.strip(), date_column, date_column_type) for table in tables]

        for table_fqn, source, date_col, date_col_type in combined_tables:
            try:
                database, schema, table = table_fqn.split('.')
                cursor = snowflake_connection.cursor()

                # Get column metadata
                column_query = f'''
                    SELECT COLUMN_NAME, DATA_TYPE
                    FROM {database}.INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = '{schema}'
                '''
                cursor.execute(column_query)
                columns = cursor.fetchall()
                if not columns:
                    continue

                # Prepare month expression per table's date column and type
                if date_col_type.upper() == 'TEXT':
                    month_expr = f"TO_DATE(LEFT({date_col}, 10))"
                elif date_col_type.upper() == 'TIMESTAMP':
                    month_expr = f"DATE_TRUNC('MONTH', {date_col})"
                else:
                    raise ValueError("date_column_type must be 'TEXT' or 'TIMESTAMP'")

                # Build aggregation expressions
                agg_exprs = []
                col_type_map = {}
                for col, dtype in columns:
                    col_quoted = f'"{col}"'
                    dtype_upper = dtype.upper()
                    col_type_map[col] = dtype_upper

                    if any(x in dtype_upper for x in ['NUMBER', 'INT', 'FLOAT', 'DECIMAL']):
                        agg_exprs.append(f'SUM({col_quoted}) AS {col}_sum')
                    else:
                        agg_exprs.append(f'COUNT(DISTINCT {col_quoted}) AS {col}_unique')

                query = f'''
                    SELECT
                        {month_expr} AS month,
                        {', '.join(agg_exprs)}
                    FROM {table_fqn}
                    GROUP BY 1
                    ORDER BY 1
                '''

                cursor.execute(query)
                rows = cursor.fetchall()
                col_names = [desc[0] for desc in cursor.description]
                timestamp = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

                for row in rows:
                    month_val = row[0].isoformat() if isinstance(row[0], datetime) else str(row[0])

                    for idx, val in enumerate(row[1:], start=1):
                        col_metric = col_names[idx]
                        if '_' not in col_metric:
                            continue
                        col_name, metric = col_metric.rsplit('_', 1)

                        dtype = col_type_map.get(col_name, "UNKNOWN")
                        metric = metric.upper()
                        value = float(val) if isinstance(val, Decimal) else val

                        # Construct output dict based on data type
                        if dtype in ['NUMBER', 'INT', 'FLOAT', 'DECIMAL']:
                            # Numeric: Unique COUNT null, SUM value if metric=SUM
                            if metric == 'SUM':
                                results.append({
                                    "source": source,
                                    "table_name": table_fqn,
                                    "column": col_name,
                                    "Datatype": "NUMERIC",
                                    "month": month_val,
                                    "Unique COUNT": None,
                                    "SUM": value,
                                    "timestamp": timestamp
                                })
                        else:
                            # Text/other: Unique COUNT value if metric=UNIQUE, SUM null
                            if metric == 'UNIQUE':
                                results.append({
                                    "source": source,
                                    "table_name": table_fqn,
                                    "column": col_name,
                                    "Datatype": "TEXT",
                                    "month": month_val,
                                    "Unique COUNT": value,
                                    "SUM": None,
                                    "timestamp": timestamp
                                })

            except Exception as e:
                print(f"Error processing table {table_fqn}: {e}")
            finally:
                cursor.close()

    print_decorated_json(results)
    dump_results_to_csv(results, 'generate_mom_summary')
    return results
