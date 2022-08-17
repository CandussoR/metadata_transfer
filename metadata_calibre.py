# This part of the script will prepare the data for its insertion in mysql.

import db_config as conf
import mysql.connector as ms
import json

def splitting_authors(books_list):
    '''For every book in the book list, splits the multiple authors 
    and set the author field as a list of every individual authors
    so that it's easier to replace their name by their database id.'''
    for book in books_list:
        for author in book['author']:
            if author.find(',') != -1:
                book['author'] = author.split(', ')
    return books_list

def create_set_from_list(books_list, field):
    return { item for book in books_list for item in book[field] }

# Switching to the mySql database
mysql_conn_params = {
    'host': conf.host,
    'user': conf.user,
    'password': conf.password,
    'database' : conf.db_name
}

# Queries I will have to use in DB
TITLE_EXISTENCE = "SELECT id, LOWER(title) FROM ebooks WHERE title IN (%s)"
AUTHOR_EXISTENCE = ("SELECT id, full_name FROM authors "
                    "WHERE full_name = %s")
PUBLISHER_EXISTENCE = ("SELECT publisher_id, publisher_name FROM publishers "
                       "WHERE publisher_name = %s")
GENRE_EXISTENCE = ("SELECT id, genre FROM genre "
                   "WHERE genre = %s")
INSERT_AUTHOR_NAME = "INSERT INTO authors (full_name) VALUES (%s)"
INSERT_PUBLISHER_NAME = "INSERT INTO publishers (publisher_name) VALUES (%s)"
INSERT_GENRE = "INSERT INTO genre (genre) VALUES (%s)"
INSERT_BOOK = "INSERT INTO ebooks (title, year_of_publication, publisher_id) VALUES (%s, %s, %s);"
GET_TITLE_ID = ("SELECT id FROM ebooks WHERE title = %s")
INSERT_EBOOK_GENRE = "INSERT INTO ebooks_genres (ebook_id, genre_id) VALUES (%s, %s)"
INSERT_EBOOK_AUTHOR = "INSERT INTO ebooks_authors (ebook_id, author_id) VALUES (%s, %s)"

def create_dictionary(connexion, query, iterable):
    '''
    Creates a dictionary out of the sets taken from the book list. The dictionary
    only contains values present in a database, and needs to be followed by an update
    with append_dictionary.
    '''
    dictionary = {}
    cursor = connexion.cursor(buffered=True)

    for value in iterable:
        cursor.execute(query, (value, ))
        try:
            for id, name in cursor:
                dictionary[name] = id
        except:
            return dictionary

    return dictionary 

def insert_unfound(connexion, query, iterable, dictionary):
    cursor = connexion.cursor(buffered=True)

    for value in iterable:
        if (value not in dictionary.keys()) or (not dictionary):
            try:
                cursor.execute(query, (value, ))
            except:
                print("Oops! Value already there !")

    connexion.commit()

def append_dictionary(connexion, query, iterable, dictionary):
    '''
    Updates create dictionary so every book data gets its id.
    '''
    cursor = connexion.cursor(buffered=True)

    new_iterable = list(filter(lambda x: x not in dictionary, iterable))
    
    for value in new_iterable:
        cursor.execute(query, (value, ))
        for id, name in cursor:
            dictionary[name] = id

    return dictionary 

def strings_to_id_lists(data_list, authors_dict, genres_dict, publishers_dict):
    '''
    Since only three fields (authors, tags and publisher) are to be modified,
    they are hard-coded.'''
    for obj in data_list:
        obj['author'] = [authors_dict[k] for k in obj['author']]
        obj['tags'] = [genres_dict[k] for k in obj['tags']]
        obj['publisher'] = publishers_dict[obj['publisher']]
    return data_list

# Last part : 
# Make the initial insertion of the book in ebooks, and get its id.
# Then we'll use this id in two new tables : ebook_authors, ebook_genres.

def title_check(connexion, query, iterable):
    cursor = connexion.cursor(buffered=True)

    already_there = {}
    for value in iterable:
        cursor.execute(query, (value,))
        for id, value in cursor:
            already_there[value] = id
    
    cursor.close()
    return already_there
            
def book_insert(connexion, book, query):
    with connexion.cursor(buffered=True) as cursor:
        cursor.execute(query, (book['title'], book['pubdate'], book['publisher']))
        connexion.commit()

def book_id(connexion, query, title):
    with connexion.cursor(buffered=True) as cursor:
        cursor.execute(query, (title,))
        for book_id in cursor:
            return book_id[0]

def create_book_genre_tuple(book_id, *id_list):
    return [(book_id, id) for id in id_list]

def insert_tuples(connexion, query, iterable):
    with connexion.cursor(buffered=True) as cursor:
        for couple in iterable:
            print(couple)
            cursor.execute(query, couple)
            connexion.commit()

def book_not_present(connexion, book_list):
    title_dict = title_check(connexion, 
                            TITLE_EXISTENCE, 
                            [book['title'].lower() for book in book_list])
    return list(filter(lambda x: x['title'].lower() not in title_dict, book_list))

def prepare_data_for_insertion(connexion, data_file):
    with open(data_file) as file:
        books_init = json.load(file)

    if (books := book_not_present(connexion, books_init)):
        # Individualise authors initially grouped by book
        books = splitting_authors(books)

        # Creating sets for following dictionary constitution with mysql
        authors_set = create_set_from_list(books, 'author')
        genre_set = create_set_from_list(books, 'tags')
        publishers_set = { book['publisher'] for book in books }

        #Creating dict, inserting, appending to dicts
        a_d = create_dictionary(connexion, AUTHOR_EXISTENCE, authors_set)
        insert_unfound(connexion, INSERT_AUTHOR_NAME, authors_set, a_d)
        append_dictionary(connexion, AUTHOR_EXISTENCE, authors_set, a_d)

        g_d = create_dictionary(connexion, GENRE_EXISTENCE, genre_set)
        insert_unfound(connexion, INSERT_GENRE, genre_set, g_d)
        append_dictionary(connexion, GENRE_EXISTENCE, genre_set, g_d)

        p_d = create_dictionary(connexion, PUBLISHER_EXISTENCE, publishers_set)
        insert_unfound(connexion, INSERT_PUBLISHER_NAME, publishers_set, p_d)
        append_dictionary(connexion, PUBLISHER_EXISTENCE, authors_set, p_d)

        # Reverting values in the list of books
        strings_to_id_lists(books, a_d, g_d, p_d)
        
        return books

def data_insertion(connexion, data):
    for book in data:
        book_insert(connexion, book, INSERT_BOOK)
        id = book_id(connexion, GET_TITLE_ID, book['title'])
        book_genre_tuples = create_book_genre_tuple(id, *book['tags'])
        print(book_genre_tuples)
        insert_tuples(connexion, INSERT_EBOOK_GENRE, book_genre_tuples)
        book_author_tuples = create_book_genre_tuple(id, *book['author'])
        print(book_author_tuples)
        insert_tuples(connexion, INSERT_EBOOK_AUTHOR, book_author_tuples)


connexion = ms.connect(**mysql_conn_params)
prep_data = prepare_data_for_insertion(connexion, 'json_books.json')
data_insertion(connexion, prep_data)
connexion.close()