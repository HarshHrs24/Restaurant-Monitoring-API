import mysql.connector

# Database configuration
db_config = {
    'host' :'127.0.0.1',        # Replace with your host name or IP address
    'database' :'restuarantMonitoring',# Replace with your database name
    'user' :'root',    # Replace with your database username
    'password':'root@8085' # Replace with your database password
}

def get_db_connection():
    return mysql.connector.connect(**db_config)
