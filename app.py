#!/usr/bin/python
import psycopg2
from config import config
import requests
#from requests.auth import HTTPDigestAuth
import json
from psycopg2.extras import RealDictCursor
import sched
import time
from datetime import datetime
from flask import Flask
import _thread
import googlemaps
from apscheduler.schedulers.blocking import BlockingScheduler

sched1 = BlockingScheduler()

@sched1.scheduled_job('interval', minutes=15)
def timed_job():
    print ("%s: %s" % ( 'Processing', time.ctime(time.time()) ))
    #process_trips()
    process_goole_trips

#@sched1.scheduled_job('cron', day_of_week='mon-fri', hour=17)
def scheduled_job():
    print('This job is run every weekday at 5pm.')

app = Flask(__name__)

@app.route('/')
def hello():
    return 'Hello world!'

event_schedule = sched.scheduler(time.time, time.sleep)
gmaps = googlemaps.Client(key='AIzaSyAmbWmY2zPJMdn4CAoxq05zHshO8bKfW_E')

def do_something():
    print ("%s: %s" % ( 'Processing', time.ctime(time.time()) ))
    process_trips()
    event_schedule.enter(900, 1, do_something )

def startProcess(delay):
    event_schedule.enter(1, 1, do_something)
    event_schedule.run()

def getGoogleLocationDetails(src,dest,wayPoints):
    now = datetime.now()
    directions_result = gmaps.directions(origin=src,
                                     destination=dest,
                                     mode="driving",
                                     waypoints=wayPoints,
                                     departure_time=now)
    _resp = json.dumps(directions_result[0])
    #d = json.loads(])
    startAddress=directions_result[0]["legs"][0]['start_address']
    destAddress =  directions_result[0]["legs"][len(directions_result[0]["legs"])-1]['end_address']
    print( startAddress)
    print(destAddress)
    totlDistance = 0
    totDuration = 0
    for x in directions_result[0]["legs"]:
        print(x['distance']['value'])
        #print(x['duration']['value'])
        totlDistance = totlDistance + x['distance']['value']
        totDuration = totDuration + x['duration']['value']
    print('-------')
    print(totlDistance)
    #print(directions_result[0]['overview_polyline']['points'])
    return {"distance":totlDistance ,"duration":totDuration,"start":startAddress,"dest":destAddress,"polypoints":directions_result[0]['overview_polyline']['points'] }

def getLocationDetails(_long,_lat):
    headers = {
    'Accept': 'application/json, application/geo+json, application/gpx+xml, img/png; charset=utf-8',
    }
    call = requests.get('https://api.openrouteservice.org/geocode/reverse?api_key=5b3ce3597851110001cf6248e29e876eaf39498bb0b0b2ad0863e216&point.lon='+_long+'&point.lat='+ _lat + '&size=0', headers=headers)
    #print(call.status_code, call.reason)
    _resp = json.loads(call.text)
    if(len(_resp['features'])>0):
        return (_resp['features'][0]['properties']['label'] )
    else:
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

def process_goole_trips():
    """ query data from the logs table """
    conn = None
    #try:
    params = config()
    conn = psycopg2.connect(**params)
    cur = conn.cursor()
    cur2 = conn.cursor(cursor_factory=RealDictCursor)
    cur3 = conn.cursor()
    cur.execute("SELECT * from vw_pending_trips  ")

    row = cur.fetchone()

    while row is not None:
        #print(row[0])
        cur2.execute("SELECT * from gpslog where tripid='" + row[0] + "' order by time"  )
        rows = cur2.fetchall()
        wayPoints = ''
        i = 0
        skipper = 1
        src = rows[0]['latitude'] + ','+ rows[0]['longitude'] 
        dest = rows[len(rows)-1]['latitude'] + ','+ rows[len(rows)-1]['longitude'] 
        print(src)
        if(len(rows)>= 25):
            skipper = round(len(rows)/25) * 5
        if(len(rows)>= 250):
            skipper = round(len(rows)/25) * round(len(rows)/5)
        skipFirst = 0
        for _row in rows:
            if(i ==0 and skipFirst >0):
                wayPoints = wayPoints + _row['latitude'] + ','+ _row['longitude'] + '|'
                i = i +1
            if(i == skipper):
                i =0 
            skipFirst = 1
        res = getGoogleLocationDetails(src,dest,wayPoints)
        print(type(res['distance']))
        print(wayPoints)
        if (res['distance'] > 0):
               cur3.execute('CALL add_trips(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', (row[0],rows[0]['latitude'],rows[0]['longitude'],rows[len(rows)-1]['latitude'],rows[len(rows)-1]['longitude'],rows[0]['deviceid'],rows[0]['userid'],res['distance']/1000,res['start'],res['dest'], res['duration'],res['polypoints'] ))
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


def process_trips():
    """ query data from the logs table """
    conn = None
    #try:
    params = config()
    conn = psycopg2.connect(**params)
    cur = conn.cursor()
    cur2 = conn.cursor(cursor_factory=RealDictCursor)
    cur3 = conn.cursor()
    cur.execute("SELECT * from vw_pending_trips  ")

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
    #process_goole_trips()