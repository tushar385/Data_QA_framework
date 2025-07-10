from datetime import datetime, date
from utils.snowflake_utils import snowflake_connection
from utils.json_decorator_utils import print_decorated_json
from utils.dump_results_to_csv_utils import dump_results_to_csv
from utils.infer_text_column_type import infer_text_column_type
from decimal import Decimal


def generate_eda_summary(*table_sources):
    """
    Generate EDA summary for multiple (table_list, source_name) inputs.
    Produces one flat row per column with standardized metrics.
    """
    combined_tables = []
    for tables, source_name in table_sources:
        if tables:
            combined_tables.extend([(table.strip(), source_name.strip()) for table in tables])

    if not combined_tables:
        return []

    results = []

    for table_fqn, source in combined_tables:
        try:
            database, schema, table = table_fqn.split('.')
            cursor = snowflake_connection.cursor()

            # Step 1: Fetch column metadata
            column_query = f'''
                SELECT COLUMN_NAME, DATA_TYPE
                FROM {database}.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = '{schema}'
            '''
            cursor.execute(column_query)
            columns = cursor.fetchall()

            if not columns:
                raise ValueError(f"No columns found for table {table_fqn}")

            # Step 2: Build EDA SQL
            agg_clauses = []
            column_info = {}

            for col_name, data_type in columns:
                col = f'"{col_name}"'
                clause = [f'COUNT(DISTINCT {col}) AS {col_name}_unique']
                metric_flags = {'unique': True, 'min': False, 'max': False, 'avg': False, 'sum': False}

                if any(x in data_type for x in ['NUMBER', 'INT', 'FLOAT', 'DECIMAL']):
                    clause += [
                        f'MIN({col}) AS {col_name}_min',
                        f'MAX({col}) AS {col_name}_max',
                        f'AVG({col}) AS {col_name}_avg',
                        f'SUM({col}) AS {col_name}_sum'
                    ]
                    metric_flags.update({k: True for k in ['min', 'max', 'avg', 'sum']})

                elif any(x in data_type for x in ['DATE', 'TIME']):
                    clause += [
                        f'MIN({col}) AS {col_name}_min',
                        f'MAX({col}) AS {col_name}_max'
                    ]
                    metric_flags.update({'min': True, 'max': True})

                elif data_type == 'TEXT':
                    inferred_type = infer_text_column_type(cursor, table_fqn, col_name)
                    if inferred_type == 'date':
                        clause += [
                            f'MIN(TRY_TO_DATE({col})) AS {col_name}_min',
                            f'MAX(TRY_TO_DATE({col})) AS {col_name}_max'
                        ]
                        metric_flags.update({'min': True, 'max': True})
                    elif inferred_type == 'numeric':
                        clause += [
                            f'MIN(TRY_TO_NUMBER({col})) AS {col_name}_min',
                            f'MAX(TRY_TO_NUMBER({col})) AS {col_name}_max',
                            f'AVG(TRY_TO_NUMBER({col})) AS {col_name}_avg',
                            f'SUM(TRY_TO_NUMBER({col})) AS {col_name}_sum'
                        ]
                        metric_flags.update({'min': True, 'max': True, 'avg': True, 'sum': True})

                agg_clauses.append(', '.join(clause))
                column_info[col_name] = {
                    'data_type': data_type,
                    'metrics': metric_flags
                }

            final_query = f'''SELECT {', '.join(agg_clauses)} FROM {table_fqn}'''

            # Step 3: Execute and parse result
            cursor.execute(final_query)
            row = cursor.fetchone()
            raw_columns = [desc[0] for desc in cursor.description]

            # Step 4: Build flat row-per-column output
            timestamp = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
            eda_summary = {}

            for col_metric, value in zip(raw_columns, row):
                if isinstance(value, Decimal):
                    value = float(value)
                elif isinstance(value, (datetime, date)):
                    value = value.isoformat()
                col_name, metric = col_metric.rsplit('_', 1)
                if col_name not in eda_summary:
                    eda_summary[col_name] = {
                        "UNIQUE": None,
                        "MIN": None,
                        "MAX": None,
                        "AVG": None,
                        "SUM": None
                    }
                eda_summary[col_name][metric.upper()] = value

            for col_name, metrics in eda_summary.items():
                flat_row = {
                    "source": source,
                    "table_name": table_fqn,
                    "column": col_name,
                    "data_type": column_info[col_name]['data_type'],
                    "timestamp": timestamp,
                    "UNIQUE": metrics.get("UNIQUE"),
                    "MIN": metrics.get("MIN"),
                    "MAX": metrics.get("MAX"),
                    "AVG": metrics.get("AVG"),
                    "SUM": metrics.get("SUM")
                }
                results.append(flat_row)

        except Exception as e:
            timestamp = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
            results.append({
                "source": source,
                "table_name": table_fqn,
                "column": None,
                "data_type": None,
                "timestamp": timestamp,
                "UNIQUE": None,
                "MIN": None,
                "MAX": None,
                "AVG": None,
                "SUM": None
            })
        finally:
            cursor.close()

    print_decorated_json(results)
    dump_results_to_csv(results, 'generate_eda_summary')
    return results
