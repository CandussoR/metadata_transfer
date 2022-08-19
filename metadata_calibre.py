# This part of the script will prepare the data for its insertion in mysql.

from db_config import mysql_conn_params
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
        for id, name in cursor:
            try:
                dictionary[name] = id
            except:
                pass
    return dictionary 

def insert_unfound(connexion, query, iterable, dictionary):
    cursor = connexion.cursor(buffered=True)
    # Filters value in dictionary to avoid futher if.
    print(f"dictionary: {dictionary}")
    for value in list(filter(lambda x: x not in dictionary, iterable)):
        cursor.execute(query, (value, ))
        connexion.commit()

def append_dictionary(connexion, query, iterable, dictionary):
    '''
    Updates the dictionary created so every book data gets its id.
    '''
    cursor = connexion.cursor(buffered=True)
    if ( filtered := list(filter(lambda x: x not in dictionary, iterable)) ):
        for value in filtered:
            cursor.execute(query, (value, ))
            for id, name in cursor:
                dictionary[name] = id
            return dictionary 

def strings_to_id_lists(data_list, authors_dict, genres_dict, publishers_dict):
    '''
    Since there are only three fields (authors, tags and publisher) are to be modified,
    they are hard-coded.'''
    for obj in data_list:
        obj['author'] = [authors_dict[k] for k in obj['author']]
        obj['tags'] = [genres_dict[k] for k in obj['tags']]
        obj['publisher'] = publishers_dict[obj['publisher']]
    return data_list

# Last part : 
# Make the initial insertion of the book in ebooks, and get its id.
# Then we'll use this id in two new tables : ebook_authors, ebook_genres.

def title_check(connexion, iterable):
    cursor = connexion.cursor(buffered=True)

    query = "SELECT id, LOWER(title) FROM ebooks WHERE title IN (%s)"

    already_there = {}
    for value in iterable:
        cursor.execute(query, (value,))
        for id, value in cursor:
            already_there[value] = id
    
    cursor.close()
    return already_there
            
def book_insert(connexion, book):
    query = ("INSERT INTO ebooks (title, year_of_publication, publisher_id) "
            "VALUES (%s, %s, %s);")
    with connexion.cursor(buffered=True) as cursor:
        cursor.execute(query, (book['title'], book['pubdate'], book['publisher']))
        connexion.commit()

def book_id(connexion, title):
    query = ("SELECT id FROM ebooks WHERE title = %s")
    with connexion.cursor(buffered=True) as cursor:
        cursor.execute(query, (title,))
        for book_id in cursor:
            return book_id[0]

def create_tuple(book_id, *id_list):
    return [(book_id, id) for id in id_list]

def insert_tuples(connexion, relation, iterable):
    if relation == "ebook_genre":
        query = "INSERT INTO ebooks_genres (ebook_id, genre_id) VALUES (%s, %s)"
    elif relation == "ebook_author":
         query = "INSERT INTO ebooks_authors (ebook_id, author_id) VALUES (%s, %s)"
    with connexion.cursor(buffered=True) as cursor:
        for couple in iterable:
            cursor.execute(query, couple)
            connexion.commit()

def book_not_present(connexion, book_list):
    title_dict = title_check(connexion, [book['title'].lower() for book in book_list])
    return list(filter(lambda x: x['title'].lower() not in title_dict, book_list))

def return_complete_dict(connexion, field, field_set):
    '''Field is a string, field_set a set. Procedure extracted to shorten prepare_data_for_insertion. It creates a first dictionary from a set, insert what isn't in the dictionary then completes it.'''

    existence_query, insert_query = select_queries(field)
    field_dict = create_dictionary(connexion, existence_query, field_set)

    if not field_set.issubset(set(field_dict.keys())):
        try: 
            insert_unfound(connexion, insert_query, field_set, field_dict)
            append_dictionary(connexion, existence_query, field_set, field_dict)
        except:
            print(f"Quelque chose cloche dans les métadonnées. Le set et le dict devraient probablement être identiques mais ne le sont pas.\n\
                Le set: {field_set}\n\
                Le dict: {set(field_dict.keys())}")

    return field_dict
    
def select_queries(field):
    if field == "author":
        existence = "SELECT id, full_name FROM authors WHERE full_name = %s"
        insert = "INSERT INTO authors (full_name) VALUES (%s)"
        return existence, insert
    elif field == "tags":
        existence = "SELECT id, genre FROM genre WHERE genre = %s"
        insert = "INSERT INTO genre (genre) VALUES (%s)"
        return existence, insert
    elif field == "publisher":
        existence = ("SELECT publisher_id, publisher_name FROM publishers "
                        "WHERE publisher_name = %s")
        insert = "INSERT INTO publishers (publisher_name) VALUES (%s)"
        return existence, insert
    else:
        print("This field doesn't exist.")

def prepare_data_for_insertion(connexion, data_file):
    with open(data_file) as file:
        books_init = json.load(file)

        print(books_init)

    if (books := book_not_present(connexion, books_init)):
        # Individualise authors initially grouped by book
        books = splitting_authors(books)

        # Creating sets for following dictionary constitution with mysql
        authors_set = create_set_from_list(books, 'author')
        genres_set = create_set_from_list(books, 'tags')
        publishers_set = { book['publisher'] for book in books }

        #Creating dicts
        authors_dict = return_complete_dict(connexion, 'author', authors_set)
        genres_dict = return_complete_dict(connexion, 'tags', genres_set)
        publishers_dict = return_complete_dict(connexion, 'publisher', publishers_set)

        # Swapping data in the list of books
        print(f"authors: {authors_dict}\ngenres: {genres_dict}\npublishers: {publishers_dict}")
        strings_to_id_lists(books, authors_dict, genres_dict, publishers_dict)
        
        return books

def data_insertion(connexion, data):
    for book in data:
        book_insert(connexion, book)
        id = book_id(connexion, book['title'])
        book_genre_tuples = create_tuple(id, *book['tags'])
        insert_tuples(connexion, "ebook_genre", book_genre_tuples)
        book_author_tuples = create_tuple(id, *book['author'])
        insert_tuples(connexion, "ebook_author", book_author_tuples)


connexion = ms.connect(**mysql_conn_params)
prep_data = prepare_data_for_insertion(connexion, 'json_books.json')
print(prep_data)
data_insertion(connexion, prep_data)
print("C'est dans la db!")
connexion.close()