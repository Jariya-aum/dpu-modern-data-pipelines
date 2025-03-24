import requests


API_KEY = "ca802658-e8b7-4810-91e5-a304faf0f38c"
city = 'Bangkok'
state = 'Bangkok'
country = 'Thailand'

url = f"http://api.airvisual.com/v2/city?city={city}&state={state}&country={country}&key={API_KEY}"

payload = {}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)
data = response.json()
print(data)

