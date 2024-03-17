from datetime import datetime


# i. Find top 10 users who made the maximum number of comments
def top_10_users_with_most_comments(comments):
    pipeline = [
        {"$group": {"_id": "$email", "total_comments": {"$sum": 1}}},
        {"$sort": {"total_comments": -1}},
        {"$limit": 10}
    ]
    top_users = comments.aggregate(pipeline)
    return list(top_users)

# ii. Find top 10 movies with most comments
def top_10_movies_with_most_comments(comments):
    pipeline = [
        {"$group": {"_id": "$movie_id", "total_comments": {"$sum": 1}}},
        {"$sort": {"total_comments": -1}},
        {"$limit": 10}
    ]
    top_movies = comments.aggregate(pipeline)
    return list(top_movies)

# iii. Given a year, find the total number of comments created each month in that year
def total_comments_per_month(year, comments):
    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 12, 31, 23, 59, 59)
    pipeline = [
        {"$match": {"date": {"$gte": start_date, "$lte": end_date}}},
        {"$group": {"_id": {"$month": "$date"}, "total_comments": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    comments_per_month = comments.aggregate(pipeline)
    return list(comments_per_month)