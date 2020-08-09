#!/usr/bin/python
import psycopg2
from config import config
import requests
#from requests.auth import HTTPDigestAuth
import json
from psycopg2.extras import RealDictCursor
import sched
import time
from flask import Flask
import _thread

from apscheduler.schedulers.blocking import BlockingScheduler

sched1 = BlockingScheduler()

@sched1.scheduled_job('interval', minutes=15)
def timed_job():
    print ("%s: %s" % ( 'Processing', time.ctime(time.time()) ))
    process_trips()

#@sched1.scheduled_job('cron', day_of_week='mon-fri', hour=17)
def scheduled_job():
    print('This job is run every weekday at 5pm.')

app = Flask(__name__)

@app.route('/')
def hello():
    return 'Hello world!'

event_schedule = sched.scheduler(time.time, time.sleep)

def do_something():
    print ("%s: %s" % ( 'Processing', time.ctime(time.time()) ))
    process_trips()
    event_schedule.enter(900, 1, do_something )

def startProcess(delay):
    event_schedule.enter(1, 1, do_something)
    event_schedule.run()

def getLocationDetails(_long,_lat):
    headers = {
    'Accept': 'application/json, application/geo+json, application/gpx+xml, img/png; charset=utf-8',
    }
    call = requests.get('https://api.openrouteservice.org/geocode/reverse?api_key=5b3ce3597851110001cf6248e29e876eaf39498bb0b0b2ad0863e216&point.lon='+_long+'&point.lat='+ _lat + '&size=0', headers=headers)
    #print(call.status_code, call.reason)
    _resp = json.loads(call.text)
    if(len(_resp['features'])>0):
        return (_resp['features'][0]['properties']['label'] )
    else
        return 'Unknown'
	
def getDistance(locs):
    body = {"locations":locs,"metrics":["distance"],"units":"km"}
    print(locs)
    headers = {
        'Accept': 'application/json, application/geo+json, application/gpx+xml, img/png; charset=utf-8',
        'Authorization': '5b3ce3597851110001cf6248e29e876eaf39498bb0b0b2ad0863e216',
        'Content-Type': 'application/json; charset=utf-8'
    }
    call = requests.post('https://api.openrouteservice.org/v2/matrix/driving-car', json=body, headers=headers)

    print(call.status_code, call.reason)
    _resp = json.loads(call.text)
    distance = -1
    if(call.status_code == 200 and len(_resp['distances']) >0  ):
        alen = len(_resp['distances'][0])
        distance = _resp['distances'][0][alen-1]
    print(distance )
    return (distance)

def process_trips():
    """ query data from the logs table """
    conn = None
    #try:
    params = config()
    conn = psycopg2.connect(**params)
    cur = conn.cursor()
    cur2 = conn.cursor(cursor_factory=RealDictCursor)
    cur3 = conn.cursor()
    cur.execute("SELECT * from vw_pending_trips")

    row = cur.fetchone()

    while row is not None:
        #print(row[0])
        cur2.execute("SELECT * from vw_trip_details where tripid='" + row[0] + "'"  )
        rows = cur2.fetchall()
        Matrix = [[], []]
        if(len(rows) >1 ):
            #print(rows[0])
            _row = json.dumps(rows[0])
            #print(_row)
            _row = json.loads(_row)
            slat = _row['latitude']
            slong =_row['longitude']
            source= getLocationDetails(_row['longitude'],_row['latitude'])
            Matrix[0].append( _row['longitude'])  
            Matrix[0].append(_row['latitude'])
            print(_row['longitude'], '-' ,_row['latitude'] ,'->' ,source)      
            _row = json.dumps(rows[1]) 
            _row = json.loads(_row)
            destination= getLocationDetails(_row['longitude'],_row['latitude'])
            print(_row['longitude'], '-' ,_row['latitude'] ,'->' , destination)               
            Matrix[1].append( _row['longitude'])
            Matrix[1].append(_row['latitude'])
            distance = getDistance(Matrix)
            if (type(distance) is float):
                if (distance > 0):
                    cur3.execute('CALL add_trip(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', (row[0],slat,slong,_row['latitude'],_row['longitude'],_row['deviceid'],_row['userid'],distance,source,destination,1 ))
        row = cur.fetchone()
    conn.commit()
    cur2.close()   
    cur.close()
    cur3.close()
    #except (Exception, psycopg2.DatabaseError) as error:
    #    print(error)
    #finally:
    if conn is not None:
       conn.close()

sched1.start()
#_thread.start_new_thread(startProcess,(30,))
#if __name__ == '__main__':
    #getLocationDetails('55.396560661848646','25.27967442421421')
#    _thread.start_new_thread(startProcess,(30,))
    #getDistance([[76.94843,10.916860000000002],[76.6548,10.786738333333336]])
    #app.run(host='0.0.0.0')
    #process_trips()
