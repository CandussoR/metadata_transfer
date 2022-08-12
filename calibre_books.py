from calibre.library import db
import db_config as conf
import json

calibre_db = db(conf.cal_folder).new_api

book_ids = calibre_db.all_book_ids()

books = []

for book in book_ids:
    title = calibre_db.field_for("title", book)
    authors = calibre_db.field_for("authors", book)
    tags = calibre_db.field_for("tags", book)
    publishers = calibre_db.field_for("publisher", book)
    pub_date = calibre_db.field_for("pubdate", book).year
    books.append( {
        'title': title,
        'author': authors,
        'tags': tags,
        'publisher': publishers,
        'pubdate': pub_date
    })

with open('json_books.json', 'w') as outfile:
    json.dump(books, outfile, ensure_ascii=False)
