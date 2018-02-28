import pandas as pd
import requests


# Found at https://codereview.stackexchange.com/a/156399

url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
response = requests.get(url)

df = pd.read_html(response.content)[0]
print(df)

