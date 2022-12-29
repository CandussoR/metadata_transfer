import os
from sys import exit
from shutil import copy2
from unicodedata import normalize
from metadata_calibre import load_file, splitting_authors
from config import dst_path

def main():
    books = load_file('json_books.json')

    for book in splitting_authors(books):
        file_name = get_src_ebook_file_name(book["path"])
        normalized_listdir = [normalized_name(folder) for folder in os.listdir(dst_path)]

        if len(book['author']) == 1:
            author = book["author"][0]
            last_name = normalized_name(extract_author_last_name(author))
            first_last = normalized_name(create_folder_name(author))

            if (first_last or last_name) not in normalized_listdir:
                try:
                    print(f"Creating {dst_path + first_last}.")
                    os.mkdir(dst_path + first_last)
                except FileExistsError:
                    print("Il y a probablement une erreur dans la normalisation des noms.")

            if first_last in normalized_listdir:
                print("Copying book.")
                copy2(book["path"] + file_name, dst_path + first_last + "/" + file_name)
            elif last_name in normalized_listdir:
                print("Copying book.")
                copy2(book["path"] + file_name, dst_path + last_name + "/" + file_name)
        
        else:
            authors_list = [normalized_name(author) for author in book["author"]]
            author = ', '.join( [extract_author_last_name(author) for author in authors_list] )
            if (normalized_name(author)) not in normalized_listdir:
                os.mkdir(dst_path + normalized_name(author))
            try:
                copy2(book["path"] + file_name, dst_path + normalized_name(author) + "/" + file_name)
            except:
                print("Couldn't copy.")

def normalized_name(name):
    return normalize('NFC', name)

def create_folder_name(author):
    if author.find('.') == -1:
        return author.replace(' ', '_')
    else:
        return author.replace('. ', '_').replace(' ', '_')

def extract_author_last_name(author):
    return author.rsplit(' ', 1)[1] if author.find(' ') != -1 else author

def check_destination_path(dst):
    if not os.path.exists(dst):
        print("Le disque dur n'est pas branché ou le chemin de destination n'existe pas.")
        exit()

def get_src_ebook_file_name(src):
    for filename in os.listdir(src):
        if filename.endswith( ('.epub', '.pdf', '.djvu') ):
            return filename



if __name__ == "__main__":
    main()