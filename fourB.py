def top_n_movies_highest_imdb_rating(movies, n):
    pipeline = [
        {"$addFields": {
            "imdb.rating": {
                "$cond": {
                    "if": {"$eq": ["$imdb.rating", ""]},
                    "then": 0.0,
                    "else": {"$toDouble": "$imdb.rating"}
                }
            }
        }},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": n},
        {"$project": {"_id": 1, "imdb.rating": 1, "title": 1}}
    ]
    top_movies = movies.aggregate(pipeline)
    return list(top_movies)

def top_n_movies_highest_imdb_rating_given_year(movies, year, n):
    pipeline = [
        {"$match": {"year": year}},
        {"$addFields": {
            "imdb.rating": {
                "$cond": {
                    "if": {"$eq": ["$imdb.rating", ""]},
                    "then": 0.0,
                    "else": {"$toDouble": "$imdb.rating"}
                }
            }
        }},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": n},
        {"$project": {"_id": 1, "imdb.rating": 1, "title": 1}}
    ]
    top_movies = movies.aggregate(pipeline)
    return list(top_movies)

def top_n_movies_highest_imdb_rating_votes_gt_1000(movies, n):
    pipeline = [
        {"$match": {"imdb.votes": {"$gt": 1000}}},
        {"$addFields": {
            "imdb.rating": {
                "$cond": {
                    "if": {"$eq": ["$imdb.rating", ""]},
                    "then": 0.0,
                    "else": {"$toDouble": "$imdb.rating"}
                }
            },
            "imdb.votes": {
                "$cond": {
                    "if": {"$eq": ["$imdb.votes", ""]},
                    "then": 0,
                    "else": {"$toInt": "$imdb.votes"}
                }
            }
        }},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": n},
        {"$project": {"_id": 1, "imdb.rating": 1, "imdb.votes": 1, "title": 1}}
    ]
    top_movies = movies.aggregate(pipeline)
    return list(top_movies)


def top_n_movies_title_matching_pattern_sorted_by_tomatoes_ratings(movies, pattern, n):
    pipeline = [
        {"$match": {"title": {"$regex": pattern, "$options": "i"}}},
        {"$sort": {"tomatoes.viewer.rating": -1}},
        {"$limit": n},
        {"$project": {"_id": 1, "title": 1, "tomatoes.viewer.rating": 1}}
    ]
    top_movies = movies.aggregate(pipeline)
    return list(top_movies)


# ii. Find top N directors
def top_n_directors_max_movies(movies, n):
    pipeline = [
        {"$group": {"_id": "$directors", "total_movies": {"$sum": 1}}},
        {"$project": {"director": "$_id", "total_movies": 1, "_id": 0}},
        {"$sort": {"total_movies": -1}},
        {"$limit": n}
    ]
    top_directors = movies.aggregate(pipeline)
    return list(top_directors)


def top_n_directors_max_movies_given_year(movies, year, n):
    pipeline = [
        {"$match": {"year": year}},
        {"$group": {"_id": "$director", "total_movies": {"$sum": 1}}},
        {"$project": {"director": "$_id", "total_movies": 1, "_id": 0}},
        {"$sort": {"total_movies": -1}},
        {"$limit": n}
    ]
    top_directors = movies.aggregate(pipeline)
    return list(top_directors)


def top_n_directors_max_movies_given_genre(movies, genre, n):
    pipeline = [
        {"$unwind": "$genres"},
        {"$match": {"genres": genre}},
        {"$match": {"directors": {"$ne": None}}},
        {"$group": {"_id": "$directors", "total_movies": {"$sum": 1}}},
        {"$project": {"director": "$_id", "total_movies": 1, "_id": 0}},
        {"$sort": {"total_movies": -1}},
        {"$limit": n}
    ]
    top_directors = movies.aggregate(pipeline)
    return list(top_directors)



# iii. Find top N actors
def top_n_actors_max_movies(movies, n):
    pipeline = [
        {"$unwind": "$cast"},
        {"$group": {
            "_id": "$cast",
            "actor": {"$first": "$cast"},
            "total_movies": {"$sum": 1}
        }},
        {"$project": {"actor": 1, "total_movies": 1, "_id": 0}},
        {"$sort": {"total_movies": -1}},
        {"$limit": n}
    ]
    top_actors = movies.aggregate(pipeline)
    return list(top_actors)


def top_n_actors_max_movies_given_year(movies, year, n):
    pipeline = [
        {"$match": {"year": year}},
        {"$unwind": "$cast"},
        {"$group": {"_id": "$cast", "total_movies": {"$sum": 1}}},
        {"$project": {"actor": "$_id", "total_movies": 1, "_id": 0}},  # Projecting the actor field
        {"$sort": {"total_movies": -1}},
        {"$limit": n}
    ]
    top_actors = movies.aggregate(pipeline)
    return list(top_actors)


def top_n_actors_max_movies_given_genre(movies, genre, n):
    pipeline = [
        {"$unwind": "$cast"},
        {"$unwind": "$genres"},
        {"$match": {"genres": genre}},
        {"$group": {"_id": "$cast", "total_movies": {"$sum": 1}}},
        {"$project": {"actor": "$_id", "total_movies": 1, "_id": 0}},  # Projecting the actor field
        {"$sort": {"total_movies": -1}},
        {"$limit": n}
    ]
    top_actors = movies.aggregate(pipeline)
    return list(top_actors)


# iv. Find top N movies for each genre with the highest IMDB rating
def top_n_movies_per_genre_highest_imdb_rating(movies, n):
    pipeline = [
        {"$unwind": "$genres"},
        {"$sort": {"imdb.rating": -1}},
        {"$group": {"_id": "$genres", "top_movies": {"$push": "$$ROOT"}}},
        {"$project": {"_id": 0, "genre": "$_id", "top_movies": {"$slice": ["$top_movies", n]}}}
    ]
    top_movies_per_genre = movies.aggregate(pipeline)
    return list(top_movies_per_genre)

