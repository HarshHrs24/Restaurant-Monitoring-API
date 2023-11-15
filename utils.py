import pytz
from datetime import datetime, timedelta, time

# Converts local datetime to UTC datetime
def get_utc_from_local(local_dt, timezone_str):
    local_tz = pytz.timezone(timezone_str)
    local_dt = local_tz.localize(local_dt)
    return local_dt.astimezone(pytz.utc)


# Determine the status of a store at a given time based on start and end times and their respective statuses
def interpolate_status(start_time, end_time, start_status, end_status, check_time):
    if start_time <= check_time <= end_time:
        return start_status
    return end_status

# Get the business hours for a specific store on a specific day
def get_business_hours_for_day(store_id, day, business_hours):
     # Loop through the business hours data to find the matching store_id and day of the week
    for store in business_hours:
        if store == store_id:
            for i in business_hours[store]:
                 if i['dayOfWeek'] == day.weekday():
                    return i['start_time_local'], i['end_time_local']
    # Default return: assume open 24 hours if no specific hours are set for that day
    return time(0, 0), time(23, 59, 59)


# Generate a list of time ranges when the store is supposed to be open
def get_time_ranges_for_store(store_id, business_hours, store_timezones, start_date, end_date):
    time_ranges = []
     # Get the timezone for the store or default to 'America/Chicago'
    timezone_str = store_timezones.get(store_id, 'America/Chicago')
    current_date = start_date
    print("timezone :" ,timezone_str )
     # Iterate over each day within the specified date range
    while current_date < end_date:
         # Get the business hours for the current day
        start_time_local, end_time_local = get_business_hours_for_day(store_id, current_date, business_hours)
        
         # Combine the current date with the start and end times
        start_datetime_local = datetime.combine(current_date, start_time_local)
        end_datetime_local = datetime.combine(current_date, end_time_local)
        
        if end_datetime_local < start_datetime_local:
            # Handle overnight business hours
            end_datetime_local += timedelta(days=1)
        
        # Convert the times to UTC
        start_datetime_utc = get_utc_from_local(start_datetime_local, timezone_str)
        end_datetime_utc = get_utc_from_local(end_datetime_local, timezone_str)

        

        # Make start_date and end_date timezone-aware if they are not
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=pytz.utc)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=pytz.utc)
        
        # Adjust the range if it extends beyond the current date range
        if start_datetime_utc < start_date:
            start_datetime_utc = start_date
        if end_datetime_utc > end_date:
            end_datetime_utc = end_date
        print("date :", current_date, " Business hour starts at",start_datetime_utc," and ends at", end_datetime_utc)
        
        # Remove timezone info for comparison
        start_date = start_date.astimezone(pytz.utc).replace(tzinfo=None)
        end_date = end_date.astimezone(pytz.utc).replace(tzinfo=None)

        # Append the calculated time range to the list
        time_ranges.append((start_datetime_utc, end_datetime_utc))
        current_date += timedelta(days=1)
    return time_ranges

# Calculate the uptime and downtime for a store based on its status history and business hours
def calculate_report(store_status, business_hours, store_timezones):

    # Get the current time in UTC and calculate times for the last hour, day, and week
    # current_utc = datetime.utcnow()
    current_utc = datetime(2023, 1, 24, 23, 7, 9, 515535)
    # last_hour = current_utc - timedelta(hours=1)
    last_hour = datetime(2023, 1, 24, 22, 7, 9, 515535)
    # last_day = current_utc - timedelta(days=1)
    last_day = datetime(2023, 1, 23, 23, 7, 9, 515535)
    # last_week = current_utc - timedelta(weeks=1)
    last_week = datetime(2023, 1, 17, 23, 7, 9, 515535)

    print("current time :",current_utc)
    print("last hour :",last_hour)
    print("last day :",last_day)
    print("last week :",last_week)


    report = []

    # Iterate over each store
    for store_id in store_status:
        print("store id :",store_id)
        # Get the time ranges for the last hour, day, and week
        
        print("time_ranges_hour")
        time_ranges_hour = get_time_ranges_for_store(store_id, business_hours, store_timezones, last_hour, current_utc)
        print("time_ranges_day")
        time_ranges_day = get_time_ranges_for_store(store_id, business_hours, store_timezones, last_day, current_utc)     
        print("time_ranges_week")
        time_ranges_week = get_time_ranges_for_store(store_id, business_hours, store_timezones, last_week, current_utc)

        # # Calculate uptime and downtime for each period
        print('Calculating uptime_last_hour')
        uptime_hour, downtime_hour = calculate_uptime_downtime(store_id, store_status, time_ranges_hour,'h')
        print('Calculating uptime_last_day')
        uptime_day, downtime_day = calculate_uptime_downtime(store_id, store_status, time_ranges_day,'d')
        print('Calculating uptime_last_week')
        uptime_week, downtime_week = calculate_uptime_downtime(store_id, store_status, time_ranges_week,'w')

        print('uptime_last_hour', uptime_hour)
        print('uptime_last_day', uptime_day)
        print('uptime_last_week', uptime_week)
        print('downtime_last_hour', downtime_hour)
        print('downtime_last_day', downtime_day)
        print('downtime_last_week', downtime_week)
        print()
        print()
        print("---------------------X--------------")
        print()
        print()
        # Append the calculated data to the report
        report.append({
            'store_id': store_id,
            'uptime_last_hour': uptime_hour,
            'uptime_last_day': uptime_day,
            'uptime_last_week': uptime_week,
            'downtime_last_hour': downtime_hour,
            'downtime_last_day': downtime_day,
            'downtime_last_week': downtime_week
        })

    return report

# Calculate the uptime and downtime for a store within given time ranges
def calculate_uptime_downtime(store_id, store_status, time_ranges,tp):
    uptime = 0
    downtime = 0
    print("initial uptime",uptime)
    print("initial downtime",downtime)
    for start_time, end_time in time_ranges:
        print("for a time range :",start_time,"-", end_time)
        current_time = start_time
        print("initial current_time",current_time )
        aware_status_times = [(status_time.replace(tzinfo=pytz.utc), status) 
                                for (status_time, status) in store_status[store_id]]

        # Initialize last_status based on the first known status before start_time or default to 'inactive'
        last_status_entries = [(status_time, status) for status_time, status in aware_status_times if status_time < start_time]
        print("length of last_status",len(last_status_entries))
        last_status = last_status_entries[-1][1] if last_status_entries else 'inactive'
        print(last_status)

        if len(last_status_entries) == 0:
            continue
        elif tp=='h' and last_status_entries[-1][0] and (start_time-last_status_entries[-1][0]).total_seconds()>3600:
            print("no status had been recorded in the last one hour")        
            continue
        elif tp=='d' and last_status_entries[-1][0] and (start_time-last_status_entries[-1][0]).total_seconds()>86400:
            print("no status had been recorded in the last one day")  
            continue
        elif tp=='w' and last_status_entries[-1][0] and (start_time-last_status_entries[-1][0]).total_seconds()>604800:
            print("no status had been recorded in the last one week")  
            continue
        print("starting iteration within above given time range")
        while current_time < end_time:
            # Find the nearest status change or use the last known status
            next_status_changes = []
            for status_time, status in aware_status_times:
                if status_time > current_time:
                    next_status_changes.append([status_time, status])
            next_status_change = min(next_status_changes, default=[end_time, last_status], key=lambda x: x[0])
            # checker to ensure next_status_changes doesn't exceeds end_time
            if next_status_change[0]>end_time: 
                next_status_change[0]=end_time
            print("nearest status change :",next_status_change[0])
            # Calculate the time difference between the current time and the next status change
            time_diff = (next_status_change[0] - current_time).total_seconds()

            if last_status == 'active':
                uptime += time_diff
            else:  # Assuming any status other than 'active' implies 'inactive'
                downtime += time_diff
                
            print("updated uptime in seconds",uptime)
            print("updated downtime in seconds",downtime)

            # Update current_time and last_status for the next iteration
            current_time=next_status_change[0]
            last_status = next_status_change[1]
            print("updated current_time",current_time)
            print("updated last_status",last_status)



    # Convert seconds to hours
    return uptime / 3600, downtime / 3600

def fetch_data(cursor):
    # Fetch StoreStatus data
    query = "SELECT store_id, timestamp_utc, status FROM StoreStatus"
    cursor.execute(query)
    store_status_data = cursor.fetchall()

    # Transform store status data into a suitable format (e.g., dictionary)
    store_status = {}
    for row in store_status_data:
        store_id = row['store_id']
        if store_id not in store_status:
            store_status[store_id] = []
        store_status[store_id].append((row['timestamp_utc'], row['status']))

    # Fetch BusinessHours data
    query = "SELECT store_id, dayOfWeek, start_time_local, end_time_local FROM BusinessHours"
    cursor.execute(query)
    business_hours_data = cursor.fetchall()

    # Transform business hours data into a suitable format (e.g., dictionary)
    business_hours = {}
    for row in business_hours_data:
        if not isinstance(row['start_time_local'], time):
            row['start_time_local'] = (datetime.min + row['start_time_local']).time()
        if not isinstance(row['end_time_local'], time): 
            row['end_time_local'] = (datetime.min + row['end_time_local']).time()
        store_id = row['store_id']
        if store_id not in business_hours:
            business_hours[store_id] = []
        business_hours[store_id].append(row)

    # Fetch StoreTimezone data
    query = "SELECT store_id, timezone_str FROM StoreTimezone"
    cursor.execute(query)
    store_timezones_data = cursor.fetchall()

    # Transform store timezone data into a dictionary
    store_timezones = {row['store_id']: row['timezone_str'] for row in store_timezones_data}

    return store_status, business_hours, store_timezones
