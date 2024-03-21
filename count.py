from client import Database

import time
import argparse


def main(timeframe):
    database = Database()
    cursor = database.cursor()

    if timeframe is not None:
        cursor.execute(
            "SELECT DEVICE, COUNT(*) FROM GREENHOUSE_DATA WHERE TIMESTAMP > ? GROUP BY DEVICE",
            (int(time.time() - timeframe),),
        )
    else:
        cursor.execute(
            "SELECT DEVICE, COUNT(*) FROM GREENHOUSE_DATA GROUP BY DEVICE",
        )

    for group in cursor.fetchall():
        print(f"{group[1]} for device {group[0]}")

    cursor.execute("SELECT COUNT (*) FROM GREENHOUSE_DATA")
    print(f"\nTotal Records: {cursor.fetchone()[0]}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pyplot Datapoint.")

    parser.add_argument(
        "--timeframe",
        type=str,
        help="The data timeframe: hourly, daily weekly.",
        default="none",
    )

    args = parser.parse_args()

    match args.timeframe.lower():
        case "none":
            timeframe = None
        case "hourly":
            timeframe = 3600
        case "daily":
            timeframe = 86400
        case "weekly":
            timeframe = 604800
        case _:
            timeframe = int(args.timeframe)

    main(timeframe)
