import threading
import db_connector
import utils

# Function to start the report generation process asynchronously
def generate_report_async(report_id, report_tasks):
    # Create a new thread targeting the generate_report function
    thread = threading.Thread(target=generate_report, args=(report_id, report_tasks))
    thread.start() # Start the thread, thereby triggering report generation in the background

# Function that handles the actual report generation process
def generate_report(report_id, report_tasks):
    try:
        # Establish a connection to the database
        connection = db_connector.get_db_connection()
        cursor = connection.cursor(dictionary=True)
        print("Fetching data")

        # Fetch required data from the database using utility functions
        # This includes store statuses, business hours, and store timezones
        store_status, business_hours, store_timezones = utils.fetch_data(cursor)

        # Calculate the report based on the fetched data
        # This involves determining uptime and downtime for each store
        report = utils.calculate_report(store_status, business_hours, store_timezones)
        print('Complete')
        # Once the report is calculated, update the task status to 'Complete'
        # and store the report data in the report_tasks dictionary
        report_tasks[report_id]['status'] = 'Complete'
        report_tasks[report_id]['data'] = report

    except Exception as e:
        # In case of any exception during report generation, set the task status to 'Failed'
        # and store the exception message in the report_tasks dictionary
        report_tasks[report_id]['status'] = 'Failed'
        report_tasks[report_id]['data'] = str(e)
    finally:
        # Close the database cursor and connection
        cursor.close()
        connection.close()

