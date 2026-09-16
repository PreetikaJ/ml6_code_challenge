from math import radians, sin, cos, sqrt, atan2

def calculate_distance(lat1, lon1, lat2, lon2):
    # Radius of the Earth in km
    R = 6371.009

    # Convert latitude and longitude from degrees to radians
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None

    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)

    # Calculate the change in coordinates
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    # Haversine formula to calculate distance
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    # Calculate the distance
    distance = R * c

    return distance

d = calculate_distance(51.50311799, -0.153520935, 51.50645179, -0.170279555)
if d is not None:
    print('The distance in km is:', d)
    print('The total distance in km is:', d * 31508)
else:
    print('Invalid input: Latitude or longitude value is None')
