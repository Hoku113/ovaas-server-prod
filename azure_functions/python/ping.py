import requests
import os

img = "./sample.jpg"
def run():
    with open(img, "rb") as f:
        files = {'image':('sample.jpg', f, 'image/jpeg', {})}
        r = requests.post("https://ovaas-backend.azurewebsites.net/api/humanpose", files=files)
        print(r.status_code)

    
print("started")
run()
print('End')
