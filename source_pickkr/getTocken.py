import requests

def getToken(tanent, username, password):
    data = requests.api.get(url=f"https://{tanent}.unicommerce.com/oauth/token?grant_type=password&client_id=my-trusted-client&username={username}&password={password}").json()
    return data["access_token"]

def getTokenByRefreshToken(tenant, refresh_token):
    print("Testing the tocken")
    urlGlob = f"https://{tenant}.unicommerce.com/oauth/token?grant_type=refresh_token&client_id=my-trusted-client&refresh_token={refresh_token}"
    print("Get Token :", urlGlob)
    data = requests.api.get(url=urlGlob).json()
    #Conditions for 
    print("Another Testing :", data)
    if(data and "error" in data):
        return False
    print("This is just Testing 2 :", data)
    newtoken = data["access_token"]
    return newtoken