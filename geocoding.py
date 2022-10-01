import json
import os

from OpenWeatherMap import OpenWeatherMap

openWeatherMap = OpenWeatherMap(api_key="2265d775d28c3c75d22edcb7126ca08f") # enter your api key here

# added some new cities and removed some others

cities = [
#     ('Aachen', 'DE'),
    ('Berlin', 'DE'),
#     ('Bonn', 'DE'),
    ('Dresden', 'DE'),
    ('Frankfurt am Main', 'DE'),
    ('Freiburg', 'DE'),
    ('Kiel', 'DE'),
    ('Köln', 'DE'),
    ('Leipzig', 'DE'),
#     ('Lübeck', 'DE'),
    ('Mannheim', 'DE'),
    ('München', 'DE'),
    ('Münster', 'DE'),
    ('Ravensburg', 'DE'),
    ('Rostock', 'DE'),
    ('Stuttgart', 'DE'),
    ('Nürnberg', 'DE'),
    ('Bremen', 'DE'),
    ('Hamburg', 'DE'),
    ('Hannover', 'DE'),
    ('Würzburg', 'DE'),
    ('Paderborn', 'DE'),
    ('Kassel', 'DE'),
    ('Saarbrücken', 'DE')
        ]


def save_locations(data: json) -> bool:
    filename = 'locations.json'
    temp_filename = f'{filename}.tmp'
    try:
        with open(temp_filename, mode='w') as f:
            json.dump(data, f, indent=4)
    except TypeError as te:
        print(f'        !!! could not write file: {te}')
        return False
    os.rename(temp_filename, filename)
    return True


def get_coordinates() -> None:
    locations = {}
    for city in cities:
        data = openWeatherMap.get_coordinates(city[0], country_code=city[1])
        locations[city[0]] = {}
        locations[city[0]]['latitude'] = data["lat"]
        locations[city[0]]['longitude'] = data["lon"]
    print(json.dumps(locations, indent=4))
    save_locations(locations)


get_coordinates()
