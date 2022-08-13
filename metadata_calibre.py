# This part of the script will prepare the data for its insertion in mysql.

import db_config as conf
import mysql.connector as ms
import json

with open('json_books.json') as file:
    books = json.load(file)

def book_authors_initial(books_list):
    '''
    Gets the unmutated author. Books with more than one author will find all the
    authors in the same string : we might use this when we check the existence of 
    a folder when moving the files after data have been inserted in the database.
    '''
    return set([ author for book in books_list 
                        for author in book['author'] ])

def splitting_authors(books_list):
    '''For every book in the book list, splits the multiple authors 
    and set the author field as a list of every individual authors
    so that it's easier to replace their name by their database id.'''
    for book in books_list:
        for author in book['author']:
            if author.find(',') != -1:
                book['author'] = author.split(', ')
    return books_list

def create_genre_set(books_list):
    return { genre for book in books_list for genre in book['tags']}

# Get the sets to create dictionaries and prepare data before it's insertion
# So that it limitates the number of requests necessary
initial_authors_set = book_authors_initial(books)

authors_splitted_books = splitting_authors(books)

authors_set = { author 
                for book in splitting_authors(books) 
                for author in book['author'] }

publishers_set = { book['publisher'] for book in books }

genre_set_funct = create_genre_set(books)

# Switching to the mySql database
mysql_conn_params = {
    'host': conf.host,
    'user': conf.user,
    'password': conf.password,
    'database' : conf.db_name
}

# Queries I will have to use in DB
AUTHOR_EXISTENCE = ("SELECT id, full_name FROM authors "
                    "WHERE full_name = %s")
PUBLISHER_EXISTENCE = ("SELECT publisher_id, publisher_name FROM publishers "
                       "WHERE publisher_name = %s")
GENRE_EXISTENCE = ("SELECT id, genre FROM genre "
                   "WHERE genre = %s")
INSERT_AUTHOR_NAME = "INSERT INTO authors (full_name) VALUES (%s)"
INSERT_PUBLISHER_NAME = "INSERT INTO publishers (publisher_name) VALUES (%s)"
INSERT_GENRE = "INSERT INTO authors (full_name) VALUES (%s)"
INSERT_BOOK = "INSERT INTO ebooks (title, year_of_publication, publisher_id) VALUES (%s, %s, %s);"

def insert_unfound(query, iterable, dict):
    connexion =  ms.connect(**mysql_conn_params)
    cursor = connexion.cursor(buffered=True)

    for value in iterable:
        if value not in dict.keys():
            try:
                cursor.execute(query, (value, ))
            except:
                print("Oops! Value already there !")

    connexion.commit()
    connexion.close()


def create_dictionary(query, iterable):
    dictionary = {}
    connexion =  ms.connect(**mysql_conn_params)
    cursor = connexion.cursor(buffered=True)

    for value in iterable:
        cursor.execute(query, (value, ))
        for id, name in cursor:
            dictionary[name] = id

    connexion.close()
    return dictionary 

authors_dict = create_dictionary(AUTHOR_EXISTENCE, authors_set)
genres_dict = create_dictionary(GENRE_EXISTENCE, genre_set_funct)
publishers_dict = create_dictionary(PUBLISHER_EXISTENCE, publishers_set)

def strings_to_id_lists(data_list):
    for obj in data_list:
        obj['author'] = [authors_dict[k] for k in obj['author']]
        obj['tags'] = [genres_dict[k] for k in obj['tags']]
        obj['publisher'] = publishers_dict[obj['publisher']]
    return data_list

data_prepared = strings_to_id_lists(authors_splitted_books)
print(data_prepared)
