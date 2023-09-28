import json
from datetime import datetime
import time
import boto3


import folium
from folium import plugins
from folium.plugins import TimestampedGeoJson

dynamodb = boto3.resource('dynamodb', region_name='us-east-2')

# Open and parse the JSON file
with open('./data/all_pos.json', 'r') as json_file:
    data = json.load(json_file)


def run_dynamo_query(query, table_name):
    table = dynamodb.Table(table_name)
    nextToken = None
    result = []
    while True:
        if nextToken == None:
            response = table.meta.client.execute_statement(
                Statement=query
            )
        else:
            response = response = table.meta.client.execute_statement(
                Statement=query,
                NextToken=nextToken
            )
        result.extend(response['Items'])

        if response.get('NextToken'):
            nextToken = response['NextToken']
        else:
            break
    return result


qry = """SELECT * FROM "go_trains" where ts > 1695167679 """

res = run_dynamo_query(qry, 'go_trains')

qry = """SELECT * FROM "via_trains"  where ts > 1694535222"""

#res = run_dynamo_query(qry, 'via_trains')

def generate_via_features(data):
    features = []
    for item in data:
        feature = {
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [float(item['long']), float(item['lat'])]
            },
            'properties': {
                'time': datetime.utcfromtimestamp(item['ts']).isoformat(),
                'icon': 'marker',
                'iconstyle': {
                    'iconUrl': f"../go_train_icon.png",  # Path to your train icon image
                    'iconSize': [32, 32],  # Adjust the size as needed
                    'iconAnchor': [16, 16],  # Center of the icon
                },
                'popup': f'Train: {item["id"]} - FROM: {item.get("from")} TO: {item.get("to")}'  # Popup text
            },
        }
        features.append(feature)
    return features

def generate_go_features(data):
    features = []
    for item in data:
        feature = {
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [float(item['long']), float(item['lat'])]
            },
            'properties': {
                'time': datetime.utcfromtimestamp(item['ts']).isoformat(),
                'icon': 'marker',
                'iconstyle': {
                    'iconUrl': f"../icons/go_train_icon_{item['route'][0:2]}.png",  # Path to your train icon image
                    'iconSize': [32, 32],  # Adjust the size as needed
                    'iconAnchor': [16, 16],  # Center of the icon
                },
                'popup': f'Train: {item["id"]} - {item.get("route")}'  # Popup text
            },
        }
        features.append(feature)
    return features

feat = generate_go_features(res)
#feat = generate_via_features(res)

m = folium.Map(location=[43.76653, -79.472341], zoom_start=9)
#TimestampedGeoJson({'type': 'FeatureCollection', 'features': features}, period='PT5M', add_last_point=True).add_to(m)
timestamped_layer = TimestampedGeoJson(
    {'type': 'FeatureCollection', 'features': feat},
    period='PT5M',
    transition_time=50,
    add_last_point=True,
    duration='PT6M')
timestamped_layer.add_to(m)




# Save the map to an HTML file
m.save('train_map.html')

