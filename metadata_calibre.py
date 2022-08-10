import sqlite3
import db_config as conf
import mysql.connector as ms

calibre_request = '''
            SELECT title,
                    (SELECT name FROM books_authors_link AS bal 
                        JOIN authors ON author = authors.id 
                        WHERE book = books.id) authors,
                    (SELECT name FROM publishers 
                        WHERE publishers.id IN (
                            SELECT publisher from books_publishers_link 
                            WHERE book=books.id)) publisher,
                    (SELECT GROUP_CONCAT(DISTINCT name) FROM tags 
                        WHERE tags.id IN (
                            SELECT tag from books_tags_link 
                            WHERE book=books.id)) tags,
                    (SELECT format FROM data 
                        WHERE data.book=books.id) formats,
                    pubdate
        FROM books
        GROUP BY title;
        '''
calibre_connexion = sqlite3.connect(conf.path_db_cal)

calibre_db = calibre_connexion.execute(calibre_request).fetchall()

print(calibre_db)
