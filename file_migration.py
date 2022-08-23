import os
from sys import exit
from shutil import copy2
from unicodedata import normalize
from metadata_calibre import load_file, splitting_authors
from config import dst_path

books = load_file('json_books.json')

if not os.path.exists(dst_path):
    print("Le disque dur n'est pas branché ou le chemin de destination n'existe pas.")
    exit()

# Normalize to avoid errors with diacritic characters
normalized_listdir = [normalize('NFC', file) for file in os.listdir(dst_path)]
file_name = ""

for book in splitting_authors(books):
    for filename in os.listdir(book["path"]):
        if filename.endswith( ('.epub', '.pdf', '.djvu') ):
            file_name = filename

    if len(book['author']) == 1:
        author = normalize('NFC', book["author"][0])
        _, nom = author.rsplit(' ', 1)
        p_n = author.replace(' ', '_') if author.find('.') == -1 else author.replace('. ', '_')
        if (p_n or nom) not in os.listdir(dst_path):
            os.mkdir(dst_path + p_n)

        if p_n in os.listdir(dst_path):
            copy2(book["path"] + file_name, dst_path + p_n + "/" + file_name)
        elif nom in os.listdir(dst_path):
            copy2(book["path"] + file_name, dst_path + nom + "/" + file_name)
    
    else:
        author = ', '.join( [author.rsplit(' ', 1)[1] for author in book['author']] )
        if (author) not in os.listdir(dst_path):
            os.mkdir(dst_path + author)
            copy2(book["path"] + file_name, dst_path + author + "/" + file_name)