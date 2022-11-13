import asyncio
import aiohttp
import requests
import os
import time

# https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey=demo
# https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=IBM&apikey=demo
# https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=IBM&apikey=demo
# https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol=IBM&apikey=demo
# https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol=IBM&apikey=demo
# https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY_ADJUSTED&symbol=IBM&apikey=demo
# https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=IBM&apikey=demo

api_key = "demo"
url = "https://www.alphavantage.co/query?function={}&symbol=IBM&apikey={}"
symbols = [
    "TIME_SERIES_DAILY",
    "TIME_SERIES_DAILY_ADJUSTED",
    "TIME_SERIES_WEEKLY",
    "TIME_SERIES_WEEKLY_ADJUSTED",
    "TIME_SERIES_MONTHLY",
    "TIME_SERIES_MONTHLY_ADJUSTED",
    "GLOBAL_QUOTE",
    "TIME_SERIES_DAILY",
    "TIME_SERIES_DAILY_ADJUSTED",
    "TIME_SERIES_WEEKLY",
    "TIME_SERIES_WEEKLY_ADJUSTED",
    "TIME_SERIES_MONTHLY",
    "TIME_SERIES_MONTHLY_ADJUSTED",
    "GLOBAL_QUOTE",
    "TIME_SERIES_DAILY",
    "TIME_SERIES_DAILY_ADJUSTED",
    "TIME_SERIES_WEEKLY",
    "TIME_SERIES_WEEKLY_ADJUSTED",
    "TIME_SERIES_MONTHLY",
    "TIME_SERIES_MONTHLY_ADJUSTED",
    "GLOBAL_QUOTE",
    "TIME_SERIES_DAILY",
    "TIME_SERIES_DAILY_ADJUSTED",
    "TIME_SERIES_WEEKLY",
    "TIME_SERIES_WEEKLY_ADJUSTED",
    "TIME_SERIES_MONTHLY",
    "TIME_SERIES_MONTHLY_ADJUSTED",
    "GLOBAL_QUOTE",
    "TIME_SERIES_DAILY",
    "TIME_SERIES_DAILY_ADJUSTED",
    "TIME_SERIES_WEEKLY",
    "TIME_SERIES_WEEKLY_ADJUSTED",
    "TIME_SERIES_MONTHLY",
    "TIME_SERIES_MONTHLY_ADJUSTED",
    "GLOBAL_QUOTE",
]
results = []

# # old version
# async def get_symbols():
#     session = aiohttp.ClientSession()
#     for symbol in symbols:
#         response = aiohttp.get(url.format(symbol, api_key))
#         print(response)
#         results.append(response.json())
#         print(f"Got {symbol} ")
#     session.close()


def get_tasks(session):
    tasks = []
    for symbol in symbols:
        tasks.append(
            asyncio.create_task(
                session.get(url.format(symbol, api_key), ssl=False)))
    print(tasks)
    return tasks


async def get_symbols():
    async with aiohttp.ClientSession() as session:
        tasks = get_tasks(session)
        responses = await asyncio.gather(*tasks)


print("start timer")
start = time.time()

# loop = asyncio.get_event_loop()
# loop.run_until_complete(get_symbols())
# loop.close()
# bisa dipersingkat hanya jadi
asyncio.run(get_symbols())

end = time.time()

total_time = end - start
print("it took {} seconds to make {} api calls".format(total_time,
                                                       len(symbols)))
