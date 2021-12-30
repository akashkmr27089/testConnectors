'''
Slicing Service Will take the neeccessary parameter 
such as tracking id from the database and will pass on to api service
'''

class PickkerSlicingService():

    def __init__(self) -> None:
        pass

    def getSlicedData(self, type, data, params):
        response = {'error': False, 'ErrorObject': [], 'data': []}
        if type == 'snowflake':
            try:
                order = params["pos"]
                trackingId = data[order]
                response['data'] = trackingId
            except Exception as ex:
                response['error'] = True
                response['ErrorObject'] = ex
        return response
    
    def getSlicedDataList(self, type, data, params):
        response = {'error': False, 'ErrorObject': [], 'data': []}
        if type == 'snowflake':
            try:
                order = params["pos"]
                trackingId = []
                for doc in data:
                    trackingId.append({"code": doc[order]})
                response['data'] = trackingId
            except Exception as ex:
                response['error'] = True
                response['ErrorObject'] = ex
        return response
