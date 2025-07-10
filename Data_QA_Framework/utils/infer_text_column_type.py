from datetime import datetime, date

def infer_text_column_type(cursor, table_fqn, column_name, sample_size=100):
    query = f'''
        SELECT "{column_name}" FROM {table_fqn}
        WHERE "{column_name}" IS NOT NULL
        LIMIT {sample_size}
    '''
    cursor.execute(query)
    rows = cursor.fetchall()
    samples = [row[0] for row in rows if row[0] is not None]

    date_count, numeric_count = 0, 0

    for val in samples:
        if not isinstance(val, str):
            continue
        try:
            _ = datetime.strptime(val, "%Y-%m-%d")
            date_count += 1
            continue
        except Exception:
            pass
        try:
            float(val)
            numeric_count += 1
        except Exception:
            pass

    if date_count >= sample_size * 0.6:
        return 'date'
    elif numeric_count >= sample_size * 0.6:
        return 'numeric'
    else:
        return None