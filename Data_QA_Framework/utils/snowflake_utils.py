import snowflake.connector
from configs.snowflake_config import snowflake_credentials

snowflake_connection = snowflake.connector.connect(
    **snowflake_credentials
)