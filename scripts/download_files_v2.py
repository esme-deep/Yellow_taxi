import os
import random
import requests
from urllib.parse import urlparse
# Assurez-vous que le script find_parquet_links est accessible
from scripts.find_parquet_links import find_parquet_links, TLC_DATA_PAGE_URL
import shutil
import json
from datetime import datetime

# --- CONFIGURATION ---
DOWNLOAD_DIR = "data/random_samples"
NUMBER_OF_FILES_TO_DOWNLOAD = 3
LOG_FILE = "download_log.json" # Fichier pour tracer les téléchargements

# --- NOUVELLES FONCTIONS : GESTION DU LOG ET DE LA TAILLE ---

def load_log():
    """Charge le contenu du fichier log. S'il n'existe pas, retourne un dictionnaire vide."""
    if not os.path.exists(LOG_FILE):
        return {}
    try:
        with open(LOG_FILE, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return {}

def update_log(filename, size, log_data):
    """Met à jour le log avec les informations d'un nouveau fichier téléchargé."""
    log_data[filename] = {
        "size": size,
        "download_date": datetime.now().isoformat()
    }
    with open(LOG_FILE, 'w') as f:
        json.dump(log_data, f, indent=4)
    print(f"INFO: Log mis à jour pour '{filename}'.")

def get_remote_file_size(url):
    """Récupère la taille d'un fichier distant en octets sans le télécharger."""
    try:
        response = requests.head(url, allow_redirects=True, timeout=10)
        response.raise_for_status()
        size = int(response.headers.get('content-length', 0))
        return size
    except requests.exceptions.RequestException as e:
        print(f"   ERREUR: Impossible de récupérer la taille pour {url}. Erreur: {e}")
        return None

# --- VOS FONCTIONS, MODIFIÉES ---

def download_files(url_list: list, destination_folder: str):
    """
    Télécharge une liste de fichiers après avoir vérifié s'ils existent déjà
    et si leur taille a changé.
    """
    print(f"\n--- Début du processus de téléchargement pour {len(url_list)} fichier(s) ---")
    
    # Charge le log existant au début de l'opération.
    download_log = load_log()
    os.makedirs(destination_folder, exist_ok=True)
    
    for url in url_list:
        file_name = os.path.basename(urlparse(url).path)
        print(f"\n--- Traitement du fichier: {file_name} ---")

        # 1. Vérifier la taille du fichier sur le serveur.
        remote_size = get_remote_file_size(url)
        if remote_size is None:
            continue # Passe au fichier suivant si on ne peut pas obtenir la taille.

        # 2. Vérifier si le fichier est dans le log et si la taille correspond.
        if file_name in download_log:
            logged_size = download_log[file_name].get("size")
            if remote_size == logged_size:
                print(f"IGNORÉ: Le fichier '{file_name}' existe déjà et sa taille est identique.")
                continue
            else:
                print(f"MISE À JOUR: La taille de '{file_name}' a changé. Re-téléchargement...")
        else:
            print(f"NOUVEAU: Le fichier '{file_name}' va être téléchargé.")

        # 3. Si la vérification passe, procéder au téléchargement.
        try:
            # Votre logique de création de sous-dossiers (améliorée avec os.path.join)
            type_folder = file_name.split('_')[0]
            year_folder = file_name.split('_')[-1].split('-')[0]
            sub_folder = os.path.join(destination_folder, type_folder, year_folder)
            os.makedirs(sub_folder, exist_ok=True)
            local_path = os.path.join(sub_folder, file_name)
            
            print(f"Téléchargement de '{file_name}'...")
            
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            with open(local_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            print(f" -> Fichier sauvegardé dans : {local_path}")
            # 4. Mettre à jour le log après un téléchargement réussi.
            update_log(file_name, remote_size, download_log)
            
        except requests.exceptions.RequestException as e:
            print(f"   ERREUR lors du téléchargement de {url} : {e}")

# Le reste de vos fonctions (download_files_sample, empty_folder) reste inchangé.
# ... (vous pouvez les garder ici si vous en avez besoin) ...
def download_files_sample(all_links: list, num_files: int, destination_folder: str):
    """
    en fonction de la liste des urls, telecharcge un sample de x PARQUET
    """
    if not all_links or len(all_links) < num_files:
        print(f"Pas assez de liens trouvés pour télécharger un échantillon de {num_files} fichier(s).")
        return
    print(f"\nSélection de {num_files} liens au hasard parmi {len(all_links)} trouvés.")
    links_to_download = random.sample(all_links, k=num_files)
    os.makedirs(destination_folder, exist_ok=True)
    print("--- Début du téléchargement ---")
    for url in links_to_download:
        try:
            file_name = os.path.basename(urlparse(url).path)
            sub_folder = os.path.join(destination_folder, file_name.split('_')[0], file_name.split('_')[-1].split('-')[0])
            os.makedirs(sub_folder, exist_ok=True)
            local_path = os.path.join(sub_folder, file_name)
            response = requests.get(url, stream=True)
            response.raise_for_status()
            with open(local_path, "wb") as f:
                f.write(response.content) # Attention, .content peut consommer bcp de mémoire
            print(f"Téléchargé : {file_name}")
        except requests.exceptions.RequestException as e:
            print(f"   ERREUR lors du téléchargement de {url} : {e}")


def empty_folder(folder_path: str):
    """Vide complètement un dossier de tous ses fichiers et sous-dossiers."""
    print(f"Tentative de vidage du dossier : '{folder_path}'")
    if not os.path.isdir(folder_path):
        print(f"Le dossier n'existe pas. Rien à faire.")
        return
    project_root = os.path.abspath(os.path.curdir)
    folder_to_empty_abs = os.path.abspath(folder_path)
    if not folder_to_empty_abs.startswith(project_root) or folder_to_empty_abs == project_root:
        print(f"ERREUR DE SÉCURITÉ : Tentative de vider un dossier en dehors ou à la racine du projet.")
        return
    for item_name in os.listdir(folder_path):
        item_path = os.path.join(folder_path, item_name)
        try:
            if os.path.isfile(item_path) or os.path.islink(item_path):
                os.unlink(item_path)
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)
        except Exception as e:
            print(f"Erreur lors de la suppression de {item_path}. Raison : {e}")
    print("Dossier vidé avec succès.")


if __name__ == "__main__":
    # Votre logique principale reste la même, elle appelle maintenant la fonction améliorée.
    all_links = find_parquet_links(TLC_DATA_PAGE_URL)
    
    if all_links and len(all_links) >= NUMBER_OF_FILES_TO_DOWNLOAD:
        print(f"\n{len(all_links)} liens trouvés au total.")
        
        print(f"Sélection de {NUMBER_OF_FILES_TO_DOWNLOAD} liens au hasard...")
        randomly_selected_links = random.sample(all_links, k=NUMBER_OF_FILES_TO_DOWNLOAD)
        
        print("Liens sélectionnés pour le traitement :")
        for link in randomly_selected_links:
            print(f"- {link}")
        
        # L'appel à download_files lance maintenant tout le processus de vérification.
        download_files(randomly_selected_links, DOWNLOAD_DIR)
        
        print("\n--- Opération terminée ---")
        
    elif not all_links:
        print("Aucun lien n'a été trouvé, le script s'arrête.")
    else:
        print(f"Moins de {NUMBER_OF_FILES_TO_DOWNLOAD} liens ont été trouvés.")