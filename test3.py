import googlemaps
from datetime import datetime
import json

gmaps = googlemaps.Client(key='AIzaSyAmbWmY2zPJMdn4CAoxq05zHshO8bKfW_E')

# Geocoding an address
#geocode_result = gmaps.geocode('1600 Amphitheatre Parkway, Mountain View, CA')

# Look up an address with reverse geocoding
#reverse_geocode_result = gmaps.reverse_geocode((40.714224, -73.961452))
#print(reverse_geocode_result)
# Request directions via public transit
now = datetime.now()
 
directions_result = gmaps.directions(origin="10.180411, 76.394909",
                                     destination="10.740948, 76.653420",
                                     mode="driving",
                                     waypoints="10.320727, 76.326380|10.591530, 76.469471",
                                     departure_time=now)

_resp = json.dumps(directions_result[0])
#d = json.loads(])
print( directions_result[0]["legs"][0]['start_address'])
print( directions_result[0]["legs"][len(directions_result[0]["legs"])-1]['end_address'])
totlDistance = 0
totDuration = 0
for x in directions_result[0]["legs"]:
  print(x['distance']['value'])
  #print(x['duration']['value'])
  totlDistance = totlDistance + x['distance']['value']
  totDuration = totDuration + x['duration']['value']
print('-------')
print(totlDistance)
print(directions_result[0]['overview_polyline']['points'])