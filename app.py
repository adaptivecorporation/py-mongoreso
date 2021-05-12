import requests
import sys
import time
import gc
import sys
from flask import Flask, jsonify
from time import sleep
import constants
import pymongo

mongoclient = pymongo.MongoClient("mongodb://10.10.10.200:27017/")
db = mongoclient["adaptive-reso"]
col = db["stellar-mls-residential"]




def gtTimestamp():
    msg = col.find().sort([('ModificationTimestamp', pymongo.DESCENDING)]).limit(1)
    for doc in msg:
        time = str(doc['ModificationTimestamp'])
        time1 = time[:10]
        time2 = time[11:19]
        time = time1 + "T" + time2 + "Z"
        print(time) 
        print('Greatest Timestamp: ' + str(doc['ModificationTimestamp']))
        return time

greatestTimestamp = gtTimestamp()
headers = {"Authorization": "Bearer {0}".format(constants.API_KEY), "User-Agent": "AdaptiveRESO 2.0.0"}
ImportUrl = 'https://api.mlsgrid.com/PropertyResi?$filter=ModificationTimestamp%20gt%20' + greatestTimestamp
#ImportUrl = 'https://api.mlsgrid.com/PropertyResi?$filter=MlgCanView%20eq%20true'
#&$top=5000'

sys.setrecursionlimit(10**6)

app = Flask(__name__)


@app.route('/gtTimestamp', methods=['GET'])
def greatestTS():
    return jsonify({'msg': gtTimestamp()})

def grabNextJson(nexturl):

    sleep(1)
    print("Running import...")
    gc.collect()
    time.sleep(1)
    r = requests.get(nexturl, headers=headers, stream=True)
    data = r.json()
    responseheaders = r.headers.get('content-length')
    r.encoding = r.apparent_encoding
    if data is None:
        print ('Connection Error! data is empty!')
        print(data, nexturl)
        exit()

    if '@odata.nextLink' not in data:
        print ('\n @odata.nextLink not found. Final Import Saving!')
        parseJson(data)
        print ('\n The import was successful.')
        ##DB INSERT
        print('Exiting.')
        exit()

    if '@odata.nextLink' in data:
        print ('\n Next link found. Inserting: ' + nexturl)
        ##DB INSERT
        parseJson(data)
        grabNextJson(data['@odata.nextLink'])

    else:
        parseJson(data) 
        print('Unknown error occurred, saving latest import.')
        return


def parseJson(data):

    for rec in data['value']:
        del rec['@odata.id']
        x = col.insert_one(rec)
        if 'UnparsedAddress' in rec: UnparsedAddress = rec['UnparsedAddress']
        else: UnparsedAddress = ''
        print('Document Inserted - ' + UnparsedAddress + ': ' + str(x.inserted_id))

           



@app.route('/run', methods=['GET'])   
def importData():
    #greatestTimestamp = gtTimestamp()
    #ImportUrl = 'https://api.mlsgrid.com/PropertyResi?$filter=ModificationTimestamp%20gt%20' + greatestTimestamp
    grabNextJson(ImportUrl)


if __name__ == '__main__':
      app.run(host='0.0.0.0', port=5000, debug=True)
