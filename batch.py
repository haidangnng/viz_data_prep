import psycopg2
import psycopg2.extras
import pandas as pd
from tqdm import tqdm
from datetime import datetime


def batch_import_movies(csv_file_path, db_config, batch_size=1000):
    """
    Imports movie data and related entities into a PostgreSQL database using batch processing.

    :param csv_file_path: Path to the CSV file containing the movie data.
    :param db_config: A dictionary containing database connection parameters (host, port, dbname, user, password).
    :param batch_size: Number of records to process in each batch.
    """
    df = pd.read_csv(csv_file_path)

    # Replace NaN in critical columns with None
    df["imdb_id"] = df["imdb_id"].replace({pd.NA: None, "NaN": None})
    df["title"] = df["title"].replace({pd.NA: None, "NaN": None})
    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")

    # Filter movies released in the last 30 years
    current_year = datetime.now().year
    min_year = current_year - 10
    df = df[df["release_date"].dt.year >= min_year]

    # Convert revenue and budget to millions
    df["revenue"] = df["revenue"] / 1_000_000_000
    df["budget"] = df["budget"] / 1_000_000_000

    try:
        connection = psycopg2.connect(
            host=db_config["host"],
            port=db_config["port"],
            dbname=db_config["dbname"],
            user=db_config["user"],
            password=db_config["password"],
        )
        cursor = connection.cursor()
        print("Connected to the database successfully.")
    except Exception as e:
        print(f"Failed to connect to the database: {e}")
        return

    def batch_insert_data(data, table, unique_column):
        """
        Batch insert related data into the database.

        :param data: A set of unique values to insert into the table.
        :param table: The table where data is stored.
        :param unique_column: The column to match the unique value.
        :return: A dictionary mapping values to their IDs.
        """
        id_mapping = {}
        try:
            # Resolve existing IDs
            cursor.execute(f"SELECT id, {unique_column} FROM {table}")
            for row in cursor.fetchall():
                id_mapping[row[1]] = row[0]

            # Insert new values and retrieve their IDs
            new_values = [(value,) for value in data if value not in id_mapping]
            if new_values:
                insert_query = f"""
                INSERT INTO "{table}" ({unique_column})
                VALUES %s
                ON CONFLICT ({unique_column}) DO NOTHING
                RETURNING id, {unique_column};
                """
                psycopg2.extras.execute_values(
                    cursor, insert_query, new_values, page_size=batch_size
                )
                connection.commit()
                for row in cursor.fetchall():
                    id_mapping[row[1]] = row[0]

        except Exception as e:
            print(f"Error during batch insert for {table}: {e}")
            connection.rollback()

        return id_mapping

    def batch_insert_movies(movies):
        """
        Batch insert movies into the database.

        :param movies: List of movie dictionaries to insert.
        """
        try:
            insert_query = """
            INSERT INTO movies (
                id, title, vote_average, vote_count, status, release_date, revenue, runtime, adult,
                budget, imdb_id, original_language, original_title, overview, popularity, tagline
            ) VALUES %s
            ON CONFLICT (imdb_id) DO NOTHING
            RETURNING id;
            """
            psycopg2.extras.execute_values(
                cursor, insert_query, movies, template=None, page_size=batch_size
            )
            connection.commit()

            # Fetch all successfully inserted movie IDs
            cursor.execute("SELECT id FROM movies")
            return {row[0] for row in cursor.fetchall()}

        except Exception as e:
            print(f"Error during batch insert for movies: {e}")
            connection.rollback()
            return set()

    def batch_insert_relationships(
        relationships, table, movie_column, related_column, valid_movie_ids
    ):
        """
        Batch insert many-to-many relationships into the join table.

        :param relationships: A list of tuples representing movie and related entity IDs.
        :param table: Name of the join table.
        :param movie_column: Column representing the movie ID in the join table.
        :param related_column: Column representing the related entity ID in the join table.
        :param valid_movie_ids: A set of valid movie IDs to ensure no invalid relationships are inserted.
        """
        try:
            # Filter relationships to include only valid movie IDs
            relationships = [rel for rel in relationships if rel[0] in valid_movie_ids]

            insert_query = f"""
            INSERT INTO "{table}" ({movie_column}, {related_column})
            VALUES %s
            ON CONFLICT DO NOTHING;
            """
            psycopg2.extras.execute_values(
                cursor, insert_query, relationships, template=None, page_size=batch_size
            )
            connection.commit()
        except Exception as e:
            print(f"Error during batch insert for {table}: {e}")
            connection.rollback()

    try:
        # Prepare related entity data
        all_genres = set()
        all_companies = set()
        all_countries = set()
        all_languages = set()
        all_keywords = set()
        genre_relationships = []
        company_relationships = []
        country_relationships = []
        language_relationships = []
        keyword_relationships = []

        movies = []

        for movie in tqdm(df.to_dict(orient="records"), desc="Processing movies"):
            if not movie["title"] or not movie["imdb_id"] or not movie["release_date"]:
                continue

            # Prepare movie data
            movies.append(
                (
                    movie["id"],
                    movie["title"],
                    movie["vote_average"],
                    movie["vote_count"],
                    movie["status"],
                    movie["release_date"],
                    movie["revenue"],
                    movie["runtime"],
                    movie["adult"],
                    movie["budget"],
                    movie["imdb_id"],
                    movie["original_language"],
                    movie["original_title"],
                    movie["overview"],
                    movie["popularity"],
                    movie["tagline"],
                )
            )

            # Collect related entities
            if pd.notna(movie["genres"]):
                all_genres.update(genre.strip() for genre in movie["genres"].split(","))
            if pd.notna(movie["production_companies"]):
                all_companies.update(
                    company.strip()
                    for company in movie["production_companies"].split(",")
                )
            if pd.notna(movie["production_countries"]):
                all_countries.update(
                    country.strip()
                    for country in movie["production_countries"].split(",")
                )
            if pd.notna(movie["spoken_languages"]):
                all_languages.update(
                    language.strip()
                    for language in movie["spoken_languages"].split(",")
                )
            if pd.notna(movie["keywords"]):
                all_keywords.update(
                    keyword.strip() for keyword in movie["keywords"].split(",")
                )

        # Insert movies
        valid_movie_ids = batch_insert_movies(movies)

        # Insert related entities and resolve IDs
        genre_ids = batch_insert_data(all_genres, "genres", "name")
        company_ids = batch_insert_data(all_companies, "companies", "name")
        country_ids = batch_insert_data(all_countries, "countries", "name")
        language_ids = batch_insert_data(all_languages, "languages", "code")
        keyword_ids = batch_insert_data(all_keywords, "keywords", "name")

        # Build relationships
        for movie in tqdm(
            df.to_dict(orient="records"), desc="Processing relationships"
        ):
            if pd.notna(movie["genres"]):
                genre_relationships.extend(
                    (movie["id"], genre_ids[genre.strip()])
                    for genre in movie["genres"].split(",")
                    if genre.strip() in genre_ids
                )
            if pd.notna(movie["production_companies"]):
                company_relationships.extend(
                    (movie["id"], company_ids[company.strip()])
                    for company in movie["production_companies"].split(",")
                    if company.strip() in company_ids
                )
            if pd.notna(movie["production_countries"]):
                country_relationships.extend(
                    (movie["id"], country_ids[country.strip()])
                    for country in movie["production_countries"].split(",")
                    if country.strip() in country_ids
                )
            if pd.notna(movie["spoken_languages"]):
                language_relationships.extend(
                    (movie["id"], language_ids[language.strip()])
                    for language in movie["spoken_languages"].split(",")
                    if language.strip() in language_ids
                )
            if pd.notna(movie["keywords"]):
                keyword_relationships.extend(
                    (movie["id"], keyword_ids[keyword.strip()])
                    for keyword in movie["keywords"].split(",")
                    if keyword.strip() in keyword_ids
                )

        # Insert relationships
        batch_insert_relationships(
            genre_relationships, "_MovieGenres", "movie_id", "genre_id", valid_movie_ids
        )
        batch_insert_relationships(
            company_relationships,
            "_MovieCompanies",
            "movie_id",
            "company_id",
            valid_movie_ids,
        )
        batch_insert_relationships(
            country_relationships,
            "_MovieCountries",
            "movie_id",
            "country_id",
            valid_movie_ids,
        )
        batch_insert_relationships(
            language_relationships,
            "_MovieLanguages",
            "movie_id",
            "language_id",
            valid_movie_ids,
        )
        batch_insert_relationships(
            keyword_relationships,
            "_MovieKeywords",
            "movie_id",
            "keyword_id",
            valid_movie_ids,
        )

        print("Data import completed successfully.")

    except Exception as e:
        print(f"Error during data import: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()
        print("Database connection closed.")


if __name__ == "__main__":
    csv_file_path = "imdb.csv"
    db_config = {
        "host": "localhost",
        "port": "5432",
        "dbname": "data-vis",
        "user": "postgres",
        "password": "basepwd",
    }
    batch_import_movies(csv_file_path, db_config)
