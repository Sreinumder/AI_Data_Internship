from traceback import print_exception
import json
from urllib.request import urlopen


request_url="https://jsonplaceholder.typicode.com"
endpoint="/users"
try:
    with urlopen(request_url+endpoint, timeout=10) as res:
        if (res.status == 200):
            print("successfully fetched data!")
        users_data = json.loads(res.read().decode("utf-8"))
    # print(type(users_data))
    # print(type(users_data[0]))
    # print(users_data[0])
    for u in users_data:
        # print(u)
        print(f"name:{u["name"]}")
        print(f"email:{u["email"]}")
        print(f"city:{u["address"]["city"]}\n")
except:
    print_exception("failed to make an request")
    
