-- Load data into Snowflake by refreshing the pipe
ALTER PIPE BRONZE_DB.SALESFORCE.PIPE_SALES_ORDERS REFRESH;