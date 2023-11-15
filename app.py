from flask import Flask, jsonify, request, Response
import report_generator
import uuid
import csv
import io

# Initialize the Flask application
app = Flask(__name__)

# Function to convert JSON data to a CSV string
def json_to_csv_string(json_data):

    # Check if the JSON data is empty
    if not json_data:
        return ""

    # Create a StringIO object to write CSV data
    output = io.StringIO()
    csv_writer = csv.writer(output)

    # Write the header (column names) to the CSV
    csv_writer.writerow(json_data[0].keys())

    # Write each data row to the CSV
    for entry in json_data:
        csv_writer.writerow(entry.values())

    # Retrieve the CSV data as a string
    return output.getvalue()

# Dictionary to keep track of report generation tasks
report_tasks = {}

# Route to trigger report generation
@app.route('/trigger_report', methods=['GET'])
def trigger_report():
    # Generate a unique report ID
    report_id = str(uuid.uuid4())

    print("Recieved get request- '/trigger_report")
    print("Report id :",report_id)
    # Initialize the task status as 'Running' and set data to None
    report_tasks[report_id] = {'status': 'Running'}

    # Start the report generation process asynchronously
    report_generator.generate_report_async(report_id, report_tasks)
    
    # Return the report ID as a JSON response
    return jsonify({'report_id': report_id})


# Route to get the status or the result of a report
@app.route('/get_report', methods=['POST'])
def get_report():
    # Extract the report ID from the request parameters
    report_id = request.args.get('report_id')

    # Check if the report ID exists in the report tasks
    if report_id in report_tasks:
        task = report_tasks[report_id]

        # If the report is complete, convert the data to CSV and send as a response
        if task['status'] == 'Complete':
            data = json_to_csv_string(task['data'])
            return Response(
            data,
            mimetype="text/csv",
            headers={"Content-disposition":
                    "attachment; filename=report.csv"})
        else:
            # If the report is not complete, return the current status
            return jsonify({'status': task['status']})
    else:
        # If the report ID is invalid, return an error message
        return jsonify({'error': 'Invalid report ID'}), 404
    


# Main function to run the Flask application
if __name__ == "__main__":
    app.run(port=8000,debug=True)
    







