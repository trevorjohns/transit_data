import folium
from folium.plugins import TimestampedGeoJson
import boto3

dynamodb = boto3.resource('dynamodb', region_name='us-east-2')

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

qry = """SELECT * FROM "via_trains"  where ts > 1695185679"""

data = run_dynamo_query(qry, 'via_trains')

# Create a Folium Map
m = folium.Map(location=[43.76653, -79.472341], zoom_start=9)
import folium
from folium.plugins import TimestampedGeoJson

# Create a Folium Map
m = folium.Map(location=[43.76653, -79.472341], zoom_start=9)

# Create a GeoJSON Feature Collection with timestamped data
geojson_data = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [train['long'], train['lat']],
            },
            "properties": {
                "time": train['ts'],  # Timestamp for animation
                "popup": f"train: {train['id']}",
            },
        }
        for train in data
    ],
}

# Function to filter data points based on the current time
def filter_data(timestamp):
    filtered_features = []
    for feature in geojson_data["features"]:
        if feature["properties"]["time"] <= timestamp:
            filtered_features.append(feature)
    return filtered_features

# Add the TimestampedGeoJson layer to the map with an empty dataset
timestamped_layer = TimestampedGeoJson([], period="PT1S", add_last_point=True)
timestamped_layer.add_to(m)

# Function to update the data displayed on the map based on the current time
def update_map(timestamp):
    filtered_data = filter_data(timestamp)
    timestamped_layer.data = {
        "type": "FeatureCollection",
        "features": filtered_data,
    }

# Create a time slider to control the animation
folium.plugins.TimestampedGeoJson(
    [],
    period="PT1S",
    add_last_point=True,
    transition_time=100,  # Transition time (adjust as needed, in milliseconds)
    auto_play=True,  # Auto-play the animation
    loop=True,  # Loop the animation
    duration="P1D",  # Total duration of animation (adjust as needed)
    on_timechange=update_map,  # Update map data when time changes
).add_to(m)

# Display the map
m.save("animated_train_location_map.html")
