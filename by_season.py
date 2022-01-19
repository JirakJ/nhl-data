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
    url_list.append(f"https://api.nhle.com/stats/rest/en/team/summary?isAggregate=false&isGame=false&sort=%5B%7B%22property%22:%22points%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22wins%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22teamId%22,%22direction%22:%22ASC%22%7D%5D&start={i*50}&limit=50&factCayenneExp=gamesPlayed%3E=1&cayenneExp=gameTypeId=2%20and%20seasonId%3C=20212022%20and%20seasonId%3E=19171918")

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


# updated_data = pd.read_csv("nhl-statistics-by-season.csv")

df = pd.DataFrame(updated_data)

df.drop_duplicates()



df.to_csv('Season/nhl-statistics-by-season.csv', mode="w+")
df.to_json('Season/nhl-statistics-by-season.json')
df.to_xml('Season/nhl-statistics-by-season.xml')
df.to_excel('Season/nhl-statistics-by-season.xlsx')
df.to_html('Season/nhl-statistics-by-season.html')
df.to_parquet('Season/nhl-statistics-by-season.parquet')
df.to_latex('Season/nhl-statistics-by-season.tex')

def split_years(dt):
    return [dt[dt['seasonId'] == y] for y in dt['seasonId'].unique()]

updated_dataframe = split_years(df)

print(updated_dataframe)
for df_data in updated_dataframe:
    for x in df_data["seasonId"]:
        season = x
        break

    if season != None:
        directory = f"Season/{season}"
        if not os.path.exists(directory):
            os.makedirs(directory)
        df_data.to_csv(f'{directory}/nhl-statistics-by-season-{season}.csv')
        df_data.to_json(f'{directory}/nhl-statistics-by-season-{season}.json')
        df_data.to_excel(f'{directory}/nhl-statistics-by-season-{season}.xlsx')
        df_data.to_html(f'{directory}/nhl-statistics-by-season-{season}.html')
        df_data.to_parquet(f'{directory}/nhl-statistics-by-season-{season}.parquet')
        df_data.to_latex(f'{directory}/nhl-statistics-by-season-{season}.tex')
