import requests
import re
import json

url = "https://shopee.com.br/Caixa-de-Som-Bluetooth-Potente-Com-4-Alto-Falantes-30W-Soundbar-PC-Notebook-TV-i.1351679961.21498150153"
match = re.search(r'-i\.(\d+)\.(\d+)', url)
if match:
    shopid = match.group(1)
    itemid = match.group(2)
    api_url = f"https://shopee.com.br/api/v4/item/get?itemid={itemid}&shopid={shopid}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Referer": url
    }
    res = requests.get(api_url, headers=headers)
    print("Status code:", res.status_code)
    try:
        data = res.json()
        print("Data keys:", data.keys())
        if 'data' in data and data['data']:
            print("Title:", data['data'].get('name'))
            print("Images:", data['data'].get('images')[:2])
            print("Video info:", data['data'].get('video_info_list'))
    except Exception as e:
        print("Error:", e)
        print("Response:", res.text[:200])
else:
    print("No match")
