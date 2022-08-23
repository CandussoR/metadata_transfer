from calibre.library import db
import config as conf
import json

calibre_db = db(conf.cal_folder).new_api

book_ids = calibre_db.all_book_ids()

books = []

for book in book_ids:
    books.append( {
        'title': calibre_db.field_for("title", book),
        'author': calibre_db.field_for("authors", book),
        'tags': calibre_db.field_for("tags", book),
        'publisher': calibre_db.field_for("publisher", book),
        'pubdate': calibre_db.field_for("pubdate", book).year,
        'path': conf.cal_folder + "/" + calibre_db.field_for("path", book) + "/"
    })

with open('json_books.json', 'w') as outfile:
    json.dump(books, outfile, ensure_ascii=False)
