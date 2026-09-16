from geopy.distance import distance as geo_distance


def calculate_distance(lat1, lon1, lat2, lon2):
    distance = geo_distance(
                        (lat1, lon1),
                        (lat2, lon2)
                    ).kilometers

    return distance

d = calculate_distance(51.4981256, -0.1321021999999914, 51.51908011, -0.124678402)
if d is not None:
    print('The distance in km is:', d)
else:
    print('Invalid input: Latitude or longitude value is None')
