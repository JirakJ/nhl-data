import asyncio
import json
import time
import aiohttp
from aiohttp.client import ClientSession
import pandas as pd
import os

data = []
async def download_link(url:str,session:ClientSession):
    async with session.get(url) as response:
        result = await response.text()
        data.append(result)
        # print(f'Read {len(result)} from {url}')

async def download_all(urls:list):
    my_conn = aiohttp.TCPConnector(limit=5)
    async with aiohttp.ClientSession(connector=my_conn) as session:
        tasks = []
        for url in urls:
            task = asyncio.ensure_future(download_link(url=url,session=session))
            tasks.append(task)
        await asyncio.gather(*tasks, return_exceptions=True) # the await must be nest inside of the session

url_list = []
for i in range(0, 33):
    url_list.append(f"https://api.nhle.com/stats/rest/en/team/summary?isAggregate=false&isGame=true&sort=%5B%7B%22property%22:%22points%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22wins%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22teamId%22,%22direction%22:%22ASC%22%7D%5D&start={i*50}&limit=50&factCayenneExp=gamesPlayed%3E=1&cayenneExp=gameDate%3C=%222022-01-19%2023%3A59%3A59%22%20and%20gameDate%3E=%221917-12-19%22%20and%20gameTypeId=2")

start = time.time()
loop = asyncio.get_event_loop()
task = loop.create_task(download_all(url_list))

try:
    loop.run_until_complete(task)
except asyncio.CancelledError:
    pass

end = time.time()
print(f'download {len(url_list)} links in {end - start} seconds')

updated_data = []
for obj in data:
    for row in json.loads(obj).get("data"):
        updated_data.append(row)


# updated_data = pd.read_csv("nhl-statistics-by-game.csv")

df = pd.DataFrame(updated_data)

df.drop_duplicates()



df.to_csv('Game/nhl-statistics-by-game.csv', mode="w+")
df.to_json('Game/nhl-statistics-by-game.json')
df.to_xml('Game/nhl-statistics-by-game.xml')
df.to_excel('Game/nhl-statistics-by-game.xlsx')
df.to_html('Game/nhl-statistics-by-game.html')
df.to_parquet('Game/nhl-statistics-by-game.parquet')
df.to_latex('Game/nhl-statistics-by-game.tex')

def split_years(dt):
    return [dt[dt['gameId'] == y] for y in dt['gameId'].unique()]

updated_dataframe = split_years(df)

print(updated_dataframe)
for df_data in updated_dataframe:
    for x in df_data["gameId"]:
        season = x
        break

    if season != None:
        directory = f"Game/{season}/"
        if not os.path.exists(directory):
            os.makedirs(directory)
        df_data.to_csv(f'{directory}/nhl-statistics-by-game-{season}.csv')
        df_data.to_json(f'{directory}/nhl-statistics-by-game-{season}.json')
        df_data.to_excel(f'{directory}/nhl-statistics-by-game-{season}.xlsx')
        df_data.to_html(f'{directory}/nhl-statistics-by-game-{season}.html')
        df_data.to_parquet(f'{directory}/nhl-statistics-by-game-{season}.parquet')
        df_data.to_latex(f'{directory}/nhl-statistics-by-game-{season}.tex')
