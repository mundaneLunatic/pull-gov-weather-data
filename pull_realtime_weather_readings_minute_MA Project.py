import requests
# TESTING GIT
from datetime import datetime,tzinfo
import time
import sys
import os
headers = {}
q_params = {}
# Credit: https://github.com/half0wl/datagovsg_api/
endpoints={'wind_speed':'https://api.data.gov.sg/v1/environment/wind-speed',
           'relative_humidity':'https://api.data.gov.sg/v1/environment/relative-humidity',
           'wind_direction':'https://api.data.gov.sg/v1/environment/wind-direction',
           'rainfall':'https://api.data.gov.sg/v1/environment/rainfall',
           'air_temperature':'https://api.data.gov.sg/v1/environment/air-temperature'}
other_metadata=['reading_type','reading_unit']
# Number of seconds between endpoint calls
time_interval=60.0

def pull_data():
    currentDT = datetime.now()
    fileName=str(currentDT.year)+'-'+str(currentDT.month).zfill(2)+'-'+str(currentDT.day).zfill(2)+'T_{0}.csv'
    for key in endpoints:
        if(os.path.exists(fileName.format(key))):
            print(key,'File already exists, skipping creation')
        else:
            out_File=open(fileName.format(key),mode='w+')
            # Generate metadata
            print("Pulling metadata for",key)
            try:
                r = requests.get(endpoints[key],headers = headers,params=q_params)
                metadata=r.json()['metadata']
                
                headerData=json_normalize(metadata['stations'][0])
                headerRow=','.join(headerData.keys())+"\r"
                out_File.write(headerRow)

                for station in metadata['stations']:
                    stationData=json_normalize(station)
                    metadataRow=','.join([str(val) for key,val in stationData.items()])+"\r"
                    out_File.write(metadataRow)

                for md in other_metadata:
                    out_File.write(md+','+metadata[md]+'\r')
                # Generate headers for main data
                out_File.write('timestamp,'+','.join(key for key in r.json()['items'][0]['readings'][0])+'\r')
                out_File.close()
                print("Successfully wrote metadata for",key,"at",datetime.now())
            except Exception as e:
                print(e)
                print('Getting Error for',key,'at',datetime.now())
                
    # Initialise last pulled array
    lastPulled={key:'0' for key in endpoints}
    # Check every minute for updated readings
    starttime=time.time()
    while True:
        time.sleep(time_interval - ((time.time() - starttime) % time_interval))
        print()
        for key in endpoints:
            try:
                r = requests.get(endpoints[key],headers = headers,params=q_params)
                data=r.json()['items'][0]
                timestamp=data['timestamp']
                
                # Pull updated reading
                if(timestamp!=lastPulled[key]):
                    lastPulled[key]=timestamp
                    out_File=open(fileName.format(key),mode='a+')
                    for reading in data['readings']:
                        dataRow=timestamp+','+','.join([str(val) for key,val in reading.items()])+"\r"
                        out_File.write(dataRow)
                    out_File.close()
                    print("Successfully wrote data for",key,"at",datetime.now())
                else:
                    print("No update for",key,"at",datetime.now())
            except Exception as e:
                print(e)
                print('Getting Error for',key,'at',datetime.now())

def json_normalize(json_object):
    normalized={}
    for k,v in json_object.items():
        if isinstance(v,dict):
            for k1,v1 in list(v.items()):
                normalized[k+'.'+str(k1)]=v1
        else:
            normalized[k]=v
    return normalized
if __name__=="__main__":
    print("Starting program: ",datetime.now())
    pull_data()
