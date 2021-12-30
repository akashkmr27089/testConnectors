import requests

print("Testing Unicommerce")

def extractOrders(data):
    orders = []
    for i in range(len(data["elements"])):
        orderCode = data["elements"][i]['code']
        orders.append(orderCode)
    return orders

def getOrders(tanent, authCode, displayStart, displayLength):
    data = requests.api.post(url=f"https://{tanent}.unicommerce.com/services/rest/v1/oms/saleOrder/search", 
                    json={
                            "cashOnDelivery": False,
                                "returnStatuses": [
                                ],
                            "searchOptions": {
                                "getCount": True,
                                "displayStart": displayStart,
                                "displayLength": displayLength
                            },
                            "onHold": False
                            }, 
                    headers={"Authorization": "bearer " + authCode, "Content-Type": "application/json"}).json()
    return data

def getLoopTimes(totalRecords):
    loopTimes = 0
    if(totalRecords > 1000):
        loopTimes = totalRecords // 1000
        if(totalRecords%1000 != 0):
            loopTimes += 1
    else:
        loopTimes = 0
    return loopTimes

def getOrdersIds(tenant, authCode, displayStart, displayLength, limitFlag):
    data = getOrders(tenant, authCode, displayStart, displayLength)
    orders = extractOrders(data)
    totalRecords = data["totalRecords"]

    loops = getLoopTimes(totalRecords)

    # print("Total Records :", totalRecords, loops)
    if(limitFlag): 
        loops = 1
    if(loops > 0):
        for i in range(1, loops):
            displayStart = 1000*(i) + 1
            dataTemp = getOrders(tanent, authCode, displayStart, displayLength)
            print("Done With DataTemp with starting value {} and ending at {}".format(displayStart, displayStart + displayLength))
            orders += extractOrders(dataTemp)

    return orders

## Actual function to Extract the loop Occurance 
def getLoopTimesParent(totalCount, iteration, resultLimits):
    """
    # Arguments 
    totalCount : # Used for getting the total Count of the Records 
    iteration : # Current Ongoing iteration --> Globally Declared Variable Mentiaing the Values (Starts : 1)
    resultlimits : # this will hold how many records should be displayed (Non zero)

    # Returns 
    [displayStart, displayLength] // Offset Varialbe and Total Records per Interation 
    """
    offset = 0
    if(totalCount > resultLimits):
        totalIteration = totalCount//resultLimits
        if(totalIteration % resultLimits != 0):
            totalIteration += 1
        if(iteration <= totalIteration):
            if(iteration == 1):
                offset = 0
            else:
                offset = (iteration-1)*resultLimits
        else:
            offset = -1

    return [offset, resultLimits]


# Authenticator 
