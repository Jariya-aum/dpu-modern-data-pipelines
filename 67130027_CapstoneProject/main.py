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
# print(data['data']['current']['pollution']['aqius'])

pm = data['data']['current']['pollution']['aqius']
print(str(pm) + "AQI Thailand")

if pm > 200:
    print('Air Pollution Level: very Unhealthy')
elif pm >= 101:
    print('Air Pollution Level: Unhealthy')
elif pm >= 51:
    print('Air Pollution Level: Moderate')
elif pm >= 26:
    print('Air Pollution Level: good')
else:
    print('Air Pollution Level: very good')
 