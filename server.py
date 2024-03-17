from mongoConnect import connect

from insertData import create_database
from insertData import insert_data_from_folder

from insertNewDoc import insert_comment
from insertNewDoc import insert_movie
from insertNewDoc import insert_theatre
from insertNewDoc import insert_user

from fourA import top_10_users_with_most_comments
from fourA import top_10_movies_with_most_comments
from fourA import total_comments_per_month

from fourB import top_n_movies_highest_imdb_rating
from fourB import top_n_movies_highest_imdb_rating_given_year
from fourB import top_n_movies_highest_imdb_rating_votes_gt_1000
from fourB import top_n_movies_title_matching_pattern_sorted_by_tomatoes_ratings

from fourB import top_n_directors_max_movies
from fourB import top_n_directors_max_movies_given_year
from fourB import top_n_directors_max_movies_given_genre

from fourB import top_n_actors_max_movies
from fourB import top_n_actors_max_movies_given_year
from fourB import top_n_actors_max_movies_given_genre

from fourB import top_n_movies_per_genre_highest_imdb_rating

from fourC import top_10_cities_with_max_theatres
from fourC import top_10_theatres_nearby_coordinates


def main():
    client = connect()  # Establish connection to MongoDB
    database_name = "sample_mflix"
    folder_path = "sample_mflix"
    db = client['sample_mflix']
    comments = db['comments']
    movies = db['movies']
    theatres = db['theaters']
    users = db['users']

    while True:
        print("\n2Q. Bulk load the JSON files")
        print("3Q. Insert new document")
        print("4Q. Execute queries")
        print("5. Exit")
        option = input("Select an option: ")

        if option == "2Q":
            create_database(client, database_name)
            insert_data_from_folder(client, database_name, folder_path)
        elif option == "3Q":
            print("3a. Insert new comment")
            print("3b. Insert new movie")
            print("3c. Insert new theatre")
            print("3d. Insert new user")
            sub_option = input("Select a sub-option: ")
            if sub_option == "3a":
                insert_comment(client, database_name, comments)
            elif sub_option == "3b":
                insert_movie(client, database_name, movies)
            elif sub_option == "3c":
                insert_theatre(client, database_name, theatres)
            elif sub_option == "3d":
                insert_user(client, database_name, users)
        elif option == "4Q":
            print("4a. Comments collection")
            print("4b. Movies collection")
            print("4c. Theatres collection")
            sub_option = input("Select a sub-option: ")
            if sub_option == "4a":
                print("i. Find top 10 users who made the maximum number of comments")
                print("ii. Find top 10 movies with most comments")
                print("iii. Find total number of comments created each month in a given year")
                sub_option_4a = input("Select the sub-option(string): ")
                if sub_option_4a == "i":
                    print(top_10_users_with_most_comments(comments))
                elif sub_option_4a == "ii":
                    print(top_10_movies_with_most_comments(comments))
                elif sub_option_4a == "iii":
                    year = int(input("Enter the year: "))
                    print(total_comments_per_month(year, comments))
                else:
                    print("Invalid option. Please try again")
            elif sub_option == "4b":
                print("i. Find top N movies: ")
                print("ii. Find top N directors: ")
                print("iii. Find top N actors: ")
                print("iv. Find top N movies for each genre with the highest IMDB rating")
                sub_option_4b = input("Select a sub-option(string): ")
                if sub_option_4b == "i":
                    # Implement further logic
                    print("1. with the highest IMDB rating: ")
                    print("2. with the highest IMDB rating in a given year: ")
                    print("3. with highest IMDB rating with number of votes >1000")
                    print("4. with title matching a given pattern sorted by highest tomatoes ratings")
                    sub_option_4b_i = int(input("Select a sub-option(int): "))
                    if sub_option_4b_i == 1:
                        n = int(input("Enter the value for n: "))
                        print(top_n_movies_highest_imdb_rating(movies,n))
                    elif sub_option_4b_i == 2:
                        n = int(input("Enter the value for n: "))
                        year = int(input("Enter the year: "))
                        print(top_n_movies_highest_imdb_rating_given_year(movies, year, n))
                    elif sub_option_4b_i == 3:
                        n = int(input("Enter the value for n: "))
                        print(top_n_movies_highest_imdb_rating_votes_gt_1000(movies, n))
                    elif sub_option_4b_i == 4:
                        n = int(input("Enter the value for n: "))
                        pattern = input("Enter the pattern: ")
                        print(top_n_movies_title_matching_pattern_sorted_by_tomatoes_ratings(movies, pattern, n))
                    else:
                        print("Invalid sub-option. Please try again")
                elif sub_option_4b == "ii":
                    print("1. who created the maximum number of movies")
                    print("2. who created the maximum number of movies in a given year")
                    print("3. who created the maximum number of movies for a given genre")
                    sub_option_4b_ii = int(input("Select a sub-option(int): "))
                    if sub_option_4b_ii == 1:
                        n = int(input("Enter the value for n: "))
                        print(top_n_directors_max_movies(movies, n))
                    elif sub_option_4b_ii == 2:
                        n = int(input("Enter the value for n: "))
                        year = int(input("Enter the year: "))
                        print(top_n_directors_max_movies_given_year(movies, year, n))
                    elif sub_option_4b_ii == 3:
                        n = int(input("Enter the value for n: "))
                        genre = input("Enter the genre: ")
                        print(top_n_directors_max_movies_given_genre(movies, genre, n))
                    else:
                        print("Invalid sub-option. Please try again")
                elif sub_option_4b == "iii":
                    print("1. who starred in the maximum number of movies")
                    print("2. who starred in the maximum number of movies in a given year")
                    print("3. who starred in the maximum number of movies for a given genre")
                    sub_option_4b_iii = int(input("Select a sub-option(int): "))
                    if sub_option_4b_iii == 1:
                        n = int(input("Enter the value for n: "))
                        print(top_n_actors_max_movies(movies, n))
                    elif sub_option_4b_iii == 2:
                        n = int(input("Enter the value for n: "))
                        year = int(input("Enter the year: "))
                        print(top_n_actors_max_movies_given_year(movies, year, n))
                    elif sub_option_4b_iii == 3:
                        n = int(input("Enter the value for n: "))
                        genre = input("Enter the genre: ")
                        print(top_n_actors_max_movies_given_genre(movies, genre, n))
                    else:
                        print("Invalid sub-option. Please try again")
                elif sub_option_4b == "iv":
                    n = int(input("Enter the value for n: "))
                    print(top_n_movies_per_genre_highest_imdb_rating(movies, n))
                else:
                    print("Invalid sub-option. Please try again.")
            elif sub_option == "4c":
                print("i. Find top 10 cities with the maximum number of theatres")
                print("ii. Find top 10 theatres nearby given coordinates")
                sub_option_4c = input("Enter the sub-option(string): ")
                if sub_option_4c == "i":
                    print(top_10_cities_with_max_theatres(theatres))
                elif sub_option_4c == "ii":
                    longitude = float(input("Enter the longitude: "))
                    latitude = float(input("Enter the latitude: "))
                    print(top_10_theatres_nearby_coordinates(theatres, longitude, latitude))
                else:
                    print("Invalid sub-option. Please try again")
            else:
                print("Invalid sub-option. Please try again.")
        elif option == "5":
            print("Exiting program...")
            break
        else:
            print("Invalid option. Please try again.")

if __name__ == "__main__":
    main()


