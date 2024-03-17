def top_10_cities_with_max_theatres(theatres):
    pipeline = [
        {"$group": {"_id": "$location.address.city", "total_theatres": {"$sum": 1}}},
        {"$sort": {"total_theatres": -1}},
        {"$limit": 10}
    ]
    top_cities = theatres.aggregate(pipeline)
    return list(top_cities)


def top_10_theatres_nearby_coordinates(theatres, longitude, latitude, max_distance_km=10, n=10):
    pipeline = [
        {
            "$geoNear": {
                "near": {
                    "type": "Point",
                    "coordinates": [longitude, latitude]
                },
                "distanceField": "distance",
                "maxDistance": max_distance_km * 1000,
                "spherical": True,
                "key": "location.geo"
            }
        },
        {"$limit": n}
    ]
    nearby_theatres = theatres.aggregate(pipeline)
    return list(nearby_theatres)
