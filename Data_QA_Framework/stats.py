from data_validations.table_access import table_access
from data_validations.eda import generate_eda_summary
from data_validations.mom_data_validation import generate_mom_summary

with open('tables_list.txt', 'r') as file: 
    exec(file.read())


# table_access(
#     (google_raw_tables, "raw"),
#     (google_intermedia_table, "intermedia")
# )


generate_eda_summary(
    (google_raw_tables, "raw"),
    (google_intermedia_table, "intermedia")
    # (test, "intermedia")
)


# generate_mom_summary(
#     (test, "intermedia"),
#     date_column='month_yr',
#     date_column_type='TEXT'
# )
