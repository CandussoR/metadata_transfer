import os
from sys import exit
from shutil import copy2
from unicodedata import normalize
from metadata_calibre import load_file, splitting_authors
from config import dst_path

def main():
    books = load_file('json_books.json')
    # Normalize to avoid errors with diacritic characters
    file_name = ""

    for book in splitting_authors(books):
        file_name = get_src_ebook_file_name(book["path"])
        normalized_listdir = [normalize('NFC', folder) for folder in os.listdir(dst_path)]

        if len(book['author']) == 1:
            author = book["author"][0]
            nom = extract_author_last_name(author)
            p_n = create_folder_name(author)

            if (normalized_name(p_n) or normalized_name(nom)) not in normalized_listdir:
                try:
                    os.mkdir(dst_path + normalized_name(p_n))
                except FileExistsError:
                    print("Il y a probablement une erreur dans la normalisation des noms.")

            if (n_n := normalized_name(p_n)) in normalized_listdir:
                copy2(book["path"] + file_name, dst_path + n_n + "/" + file_name)
            elif (norm_nom := normalized_name(nom)) in normalized_listdir:
                copy2(book["path"] + file_name, dst_path + norm_nom + "/" + file_name)
        
        else:
            authors = [normalize('NFC', auth) for auth in book["author"]]
            author = ', '.join( [author.rsplit(' ', 1)[1] for author in authors] )
            if (normalized_name(author)) not in normalized_listdir:
                os.mkdir(dst_path + normalized_name(author))
            copy2(book["path"] + file_name, dst_path + normalized_name(author) + "/" + file_name)
    
    print("Fichiers copiés.")


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