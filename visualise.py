from client import Database

from collections import defaultdict
from matplotlib import pyplot

import json
import time
import argparse


def get_datapoints(datapoint, timeframe) -> list[float]:
    client = Database()

    cursor = client.cursor()
    cursor.execute(
        """
        SELECT 
            DEVICE, DATA 
        FROM 
            GREENHOUSE_DATA
        WHERE 
            TIMESTAMP > ?
        ORDER BY TIMESTAMP ASC
    """,
        (timeframe,),
    )

    points = map(
        lambda row: {"device": row[0], "data": json.loads(row[1])}, cursor.fetchall()
    )

    try:
        return list(
            map(
                lambda point: {
                    "device": point["device"],
                    "value": float(point["data"][datapoint]),
                },
                points,
            )
        )
    except KeyError:
        print(f"Datapoint '{datapoint}' is not available")
        quit(-1)


def main(device, datapoint, timeframe):
    data = get_datapoints(datapoint, timeframe)

    if device != "*":
        data = filter(lambda point: point["device"] == device, data)



    plots = defaultdict(list)
    for point in data:
        plots[point["device"]].append(point["value"])

    for values in plots.values():
        pyplot.plot(range(len(values)), values)

    pyplot.title(f"{datapoint} Data")
    pyplot.ylabel(datapoint)

    pyplot.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pyplot Datapoint.")

    parser.add_argument(
        "--device",
        type=str,
        help="The device to retreive data from",
        metavar="device",
        default="*",
    )
    parser.add_argument(
        "--datapoint", type=str, help="The datapoint to display.", default="Temperature"
    )
    parser.add_argument(
        "--timeframe",
        type=str,
        help="The data timeframe: hourly, daily weekly.",
        default="hourly",
    )

    args = parser.parse_args()

    match args.timeframe.lower():
        case "hourly":
            timeframe = 3600
        case "daily":
            timeframe = 86400
        case "weekly":
            timeframe = 604800
        case _:
            timeframe = int(args.timeframe)

    main(args.device, args.datapoint, int(time.time() - timeframe))
