import argparse
import json
import queue
from datetime import datetime, timedelta

# Define the time format for the input
timestamp_format = "%Y-%m-%d %H:%M:%S.%f"
print_format = "%Y-%m-%d %H:%M:%S"

# Add argument parsing
parser = argparse.ArgumentParser(description="Parse data stream")

parser.add_argument(
    "--input_file", type=argparse.FileType("r"), required=True, help="File to parse"
)

parser.add_argument("--window_size", type=int, default=10, help="Number of minutes")

args = parser.parse_args()

# Convert every line from the input file to a list
data_list = [json.loads(line) for line in args.input_file]


# Calculate the average duration for a queue
def calculate_window_average_duration(window):
    total_duration = 0
    valid_count = 0

    for value in window:
        if value["duration"] != -1:
            valid_count += 1
            total_duration += value["duration"]

    if valid_count == 0:
        return 0

    return total_duration / valid_count


def write_average_by_minute(data, current_date, queue):
    # If there is no more data, return
    if len(data) == 0:
        return

    # If the queue is full, remove the oldest element
    if queue.full():
        queue.get()

    current_date = datetime.strptime(current_date, timestamp_format)
    latest_data = datetime.strptime(data[0]["timestamp"], timestamp_format)

    # If the current date is the same as the latest data, add it to the queue
    if (
        current_date.date() == latest_data.date()
        and current_date.hour == latest_data.hour
        and current_date.minute == latest_data.minute
    ):
        queue.put(data.pop(0))
    else:
        queue.put(
            {"timestamp": current_date.strftime(timestamp_format), "duration": -1}
        )

    # Add one minute to the current date
    current_date = current_date + timedelta(minutes=1)

    # Write the average delivery time to the output file
    line = json.dumps(
        {
            "date": current_date.strftime(print_format),
            "average_delivery_time": calculate_window_average_duration(
                list(queue.queue)
            ),
        }
    )

    with open("output", "a") as file:
        file.write(line + "\n")

    current_date = current_date.strftime(timestamp_format)
    write_average_by_minute(data, current_date, queue)


# Create a queue to store the last n minutes
sliding_window = queue.Queue(maxsize=args.window_size)

# Get the starting date
starting_date = datetime.strptime(
    data_list[0]["timestamp"], timestamp_format
) - timedelta(minutes=1)

# Create a empty file to write the output
open("output", "w").close()

# Start the recursive function
write_average_by_minute(
    data_list, starting_date.strftime(timestamp_format), sliding_window
)
