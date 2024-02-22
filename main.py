import argparse
import json
import queue
from datetime import datetime, timedelta

# Define the time format for the input
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
PRINT_FORMAT = "%Y-%m-%d %H:%M"


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


def write_average_by_minute(data, current_date, queue, file):
    # If there is no more data, return
    if len(data) == 0:
        return

    # If the queue is full, remove the oldest element
    if queue.full():
        queue.get()

    latest_data = datetime.strptime(data[0]["timestamp"], TIMESTAMP_FORMAT)

    # If the current date is the same as the latest data, add it to the queue
    if (
        current_date.minute == latest_data.minute
        and current_date.hour == latest_data.hour
        and current_date.date() == latest_data.date()
    ):
        queue.put(data.pop(0))
    else:
        queue.put(
            {"timestamp": current_date.strftime(TIMESTAMP_FORMAT), "duration": -1}
        )

    # Add one minute to the current date
    current_date = current_date + timedelta(minutes=1)

    # Write the average delivery time to the output file
    line = json.dumps(
        {
            "date": current_date.strftime(PRINT_FORMAT),
            "average_delivery_time": calculate_window_average_duration(
                list(queue.queue)
            ),
        }
    )

    file.write(line + "\n")

    write_average_by_minute(data, current_date, queue, file)


if __name__ == "__main__":
    # Argument parsing
    parser = argparse.ArgumentParser(description="Parse data stream")

    parser.add_argument(
        "--input_file", type=argparse.FileType("r"), required=True, help="File to parse"
    )

    parser.add_argument("--window_size", type=int, default=10, help="Number of minutes")

    args = parser.parse_args()

    # Convert every line from the input file to a list
    data_list = [json.loads(line) for line in args.input_file]

    # Create a queue to store the last n minutes
    sliding_window = queue.Queue(maxsize=args.window_size)

    # We need to start the date one minute before the first data
    starting_date = datetime.strptime(
        data_list[0]["timestamp"], TIMESTAMP_FORMAT
    ) - timedelta(minutes=1)

    # Create a empty file to write the output or clear the file if it already exists
    open("output.json", "w").close()

    # Start the recursive function
    write_average_by_minute(
        data_list.copy(), starting_date, sliding_window, open("output.json", "a")
    )

    # Measure the time it takes to run the function 1000 times

    # import timeit

    # open("output_perf.json", "w").close()

    # result = timeit.timeit(
    #     lambda: write_average_by_minute(
    #         data_list.copy(),
    #         starting_date,
    #         queue.Queue(maxsize=args.window_size),
    #         open("output_perf.json", "a"),
    #     ),
    #     number=1000,
    # )

    # print("Time to run 1000 times: ", result)
