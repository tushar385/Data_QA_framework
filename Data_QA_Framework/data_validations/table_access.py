# table_access.py
from datetime import datetime
import csv
import os
from utils.snowflake_utils import snowflake_connection
from utils.json_decorator_utils import print_decorated_json
from utils.dump_results_to_csv_utils import dump_results_to_csv


def table_access(*table_sources):

    combined_tables = []
    for tables, source_name in table_sources:
        if tables:
            combined_tables.extend([(table.strip(), source_name.strip()) for table in tables])

    if not combined_tables:
        return []

    status_sign_map = {
        "success": "✅",
        "failed": "❌",
        "no_records": "⚠️"
    }

    results = []

    for table_name, source in combined_tables:
        query = f"SELECT COUNT(*) FROM {table_name}"
        cursor = snowflake_connection.cursor()
        try:
            cursor.execute(query)
            result = cursor.fetchone()
            row_count = result[0] if result else 0
            status = "success" if row_count > 0 else "no_records"
            status_sign = status_sign_map[status]
            timestamp = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

            results.append({
                "source": source,
                "status_sign": status_sign,
                "status": status,
                "table_name": table_name,
                "row_count": row_count,
                "error_message": None,
                "timestamp": timestamp
            })
        except Exception as e:
            timestamp = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
            results.append({
                "source": source,
                "status_sign": status_sign_map["failed"],
                "status": "failed",
                "table_name": table_name,
                "row_count": None,
                "error_message": str(e),
                "timestamp": timestamp
            })
        finally:
            cursor.close()

    # Print the results using the print_decorated_json function
    print_decorated_json(results)

    # Dump results to CSV
    dump_results_to_csv(results, 'table_access')
    return results


