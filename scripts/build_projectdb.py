import psycopg2 as psql
import pandas as pd
import os
import re
from tqdm import tqdm
import unicodedata
from io import StringIO


def normalize_column_names(df):
    """Normalize column names to snake_case"""
    rename_map = {
        'Title': 'title',
        'description': 'description',
        'authors': 'authors',
        'image': 'image',
        'previewLink': 'preview_link',
        'publisher': 'publisher',
        'publishedDate': 'published_date',
        'infoLink': 'info_link',
        'categories': 'categories',
        'ratingsCount': 'ratings_count'
    }
    return df.rename(columns=rename_map)


def clean_author_name(author):
    """Clean individual author names with strict validation"""
    if pd.isna(author) or not str(author).strip():
        return None

    author = str(author)
    # Remove parenthetical content and special characters
    author = re.sub(r'\(.*?\)', '', author)  # Remove all (content)
    author = re.sub(r'[^\w\s\-\.\'&]', '', author)  # Keep only safe characters
    author = author.strip()

    # Normalize Unicode and handle special cases
    author = unicodedata.normalize('NFKD', author)
    author = ''.join(c for c in author if not unicodedata.combining(c))
    author = re.sub(r'\s+', ' ', author)  # Collapse multiple spaces

    return author if author else None


def clean_array_string(value):
    """Convert author strings to properly formatted PostgreSQL arrays"""
    if pd.isna(value) or not str(value).strip():
        return None

    # Handle both string representations and comma-separated strings
    if isinstance(value, str) and value.startswith('[') and value.endswith(']'):
        try:
            authors = eval(value)
        except:
            authors = [a.strip(" '\"") for a in value.strip("[]").split(",")]
    else:
        authors = [a.strip() for a in str(value).split(",")]

    # Clean each author and filter out empty/Nones
    cleaned_authors = []
    for author in authors:
        cleaned = clean_author_name(author)
        if cleaned:
            cleaned_authors.append(cleaned)

    # Remove duplicates while preserving order
    seen = set()
    unique_authors = []
    for author in cleaned_authors:
        if author not in seen:
            seen.add(author)
            unique_authors.append(author)

    return ",".join(unique_authors) if unique_authors else None


def convert_arrays(df):
    """Convert array columns to PostgreSQL format"""
    for col in ['authors', 'categories']:
        if col in df.columns:
            df[col] = (
                df[col].apply(clean_array_string).apply(lambda x: f"{{{x}}}" if x and x.strip() and not x.endswith(',') else None)
            )
    return df


def clean_numeric_columns(df):
    if 'ratings_count' in df.columns:
        df['ratings_count'] = (
            df['ratings_count'].replace(['', 'nan', 'NULL'], None)
            .apply(lambda x: pd.to_numeric(x, errors='coerce')))
    return df


def clean_and_prepare_books(books):
    """Clean and prepare the books dataset with strict validation"""
    books = normalize_column_names(books)
    books = books.dropna(subset=['title'])
    books = convert_arrays(books)
    books = clean_numeric_columns(books)
    return books


def clean_score(score):
    """Convert fraction scores to decimals (e.g., '10/10' -> 5.0)"""
    if pd.isna(score) or not str(score).strip():
        return None

    score = str(score).strip()


    # Handle fraction format (e.g., "10/10")
    if '/' in score:
        try:
            # Remove any spaces around the slash
            print(score)
            score = score.replace(' ', '')
            numerator, denominator = map(float, score.split('/'))
            result = round((numerator / denominator) * 5, 1)
            return result
        except Exception as e:
            print(f'Failed to convert fraction {score}: {e}')
            return None

    try:
        result = min(5.0, max(0.0, float(score)))
        #print(f'Converted numeric: {score} -> {result}')
        return result
    except Exception as e:
        print(f'Failed to convert numeric {score}: {e}')
        return None


def filter_reviews_chunk(chunk, valid_titles):
    """Filter a chunk of reviews with column rename"""
    chunk = chunk.rename(columns={
        'Id': 'book_id',
        'Title': 'book_title',
        'User_id': 'user_id',
        'profileName': 'profile_name',
        'review/helpfulness': 'helpfulness',
        'review/score': 'score',
        'review/time': 'review_time',
        'review/summary': 'summary',
        'review/text': 'review_text',
        'Price': 'price'
    })
    # chunk['score'] = chunk['score'].apply(clean_score)
    # chunk['score'] = pd.to_numeric(chunk['score'], errors='coerce')
    chunk['review_time'] = pd.to_datetime(chunk['review_time'], unit='s')
    return chunk[chunk['book_title'].isin(valid_titles)]


def filter_large_reviews_file(input_path, output_path, valid_titles):
    """Process large reviews file in chunks with progress tracking"""
    chunk_size = 100000
    first_chunk = True

    for chunk in tqdm(pd.read_csv(input_path, chunksize=chunk_size),
                      desc="Filtering reviews"):
        filtered = filter_reviews_chunk(chunk, valid_titles)
        filtered.to_csv(
            output_path,
            mode='w' if first_chunk else 'a',
            header=first_chunk,
            index=False,
            encoding='utf-8',
            na_rep='NULL'
        )
        first_chunk = False


def count_lines(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        return sum(1 for _ in f)


def main():
    # Path setup
    data_dir = os.path.join(os.path.expanduser('~'), 'data')
    secrets_dir = os.path.join(os.path.expanduser('~'), 'secrets')
    sql_dir = os.path.join(os.path.expanduser('~'), 'sql')

    # File paths
    books_csv = os.path.join(data_dir, "books_data.csv")
    reviews_csv = os.path.join(data_dir, "Books_rating.csv")
    books_processed = os.path.join(data_dir, "books_processed.csv")
    reviews_filtered = os.path.join(data_dir, "reviews_filtered.csv")

    # Read password
    with open(os.path.join(secrets_dir, "password.psql.pass"), "r") as f:
        password = f.read().strip()
        
    print("Processing books data")
    books = pd.read_csv(books_csv, encoding='utf-8')
    books = clean_and_prepare_books(books)

    # Validate array columns before saving
    books['authors'] = books['authors'].apply(
        lambda x: None if x and (x == '{}' or x.endswith(',}')) else x)

    # Save with NULL handling
    books.to_csv(books_processed, index=False, encoding='utf-8', na_rep='NULL')
    valid_titles = set(books['title'])

    # Filter reviews data
    print("Filtering reviews data")
    filter_large_reviews_file(reviews_csv, reviews_filtered, valid_titles)

    print("Loading data into PostgreSQL")
    try:
        with psql.connect(
                host="hadoop-04.uni.innopolis.ru",
                dbname="team27_projectdb",
                user="team27",
                password=password
        ) as conn:
            cur = conn.cursor()

            # Create tables
            with open(os.path.join(sql_dir, "create_tables.sql"), 'r', encoding='utf-8') as f:
                cur.execute(f.read())
                conn.commit()

            # Import data with additional validation
            with open(os.path.join(sql_dir, "import_data.sql"), 'r', encoding='utf-8') as f:
                commands = f.readlines()
                
            print("Importing books")
            with open(books_processed, 'r', encoding='utf-8') as f:
                # Skip header
                next(f)
                cur.copy_expert(commands[0], f)
                conn.commit()
            # Import reviews with progress bar
            print("Importing reviews")
            total_reviews = count_lines(reviews_filtered) - 1
            #print(total_reviews)
            with open(reviews_filtered, 'r', encoding='utf-8') as f:
                next(f)
                cur.copy_expert(commands[1], f)
                conn.commit()
            print("\nData imported successfully with all validations passed!")

    except Exception as e:
        print(f"Critical error during database operations: {e}")
        raise


if __name__ == "__main__":
    main()
