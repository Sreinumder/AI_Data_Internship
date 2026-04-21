from traceback import print_exception

import requests


request_url="https://jsonplaceholder.typicode.com"
endpoint="/users"
try:
    res = requests.get(request_url+endpoint)
    if (res.status_code == 200):
        print("successfully fetched data!")
    users_data = res.json()
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
    
