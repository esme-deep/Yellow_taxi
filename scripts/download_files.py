import os
import random
import requests
from urllib.parse import urlparse
from scripts.find_parquet_links import find_parquet_links,TLC_DATA_PAGE_URL
import os
import shutil


found_links = find_parquet_links(TLC_DATA_PAGE_URL)
    

DOWNLOAD_DIR = "data/random_samples"

# Le nombre de fichiers que nous voulons télécharger au hasard.
NUMBER_OF_FILES_TO_DOWNLOAD = 3

def download_files(url_list: list, destination_folder: str):  #!!!!downloads ALL parquet!!!
    """
    Télécharge une liste de fichiers depuis leurs URLs vers un dossier de destination.
    
    :param url_list: La liste des URLs des fichiers à télécharger.
    :param destination_folder: Le dossier où sauvegarder les fichiers.
    """
    print(f"\n--- Début du téléchargement de {len(url_list)} fichier(s) ---")
    
    # S'assurer que le dossier de destination existe.
    os.makedirs(destination_folder, exist_ok=True)
    
    for url in url_list:
        try:
            # Extrait le nom du fichier depuis l'URL (ex: yellow_tripdata_2024-01.parquet)
            file_name = os.path.basename(urlparse(url).path)
            sub_folder = destination_folder + '\\'+ file_name.split('_')[0] + '\\' + file_name.split('_')[-1].split('-')[0]
            os.makedirs(sub_folder, exist_ok=True)
            local_path = os.path.join(sub_folder, file_name)
            
            print(f"Téléchargement de '{file_name}'...")
            
            response = requests.get(url, stream=True)
            response.raise_for_status() # Lève une erreur si le téléchargement échoue
            
            with open(local_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    
            print(f" -> Fichier sauvegardé dans : {local_path}")
            
        except requests.exceptions.RequestException as e:
            print(f"   ERREUR lors du téléchargement de {url} : {e}")




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
            sub_folder = destination_folder + '\\'+ file_name.split('_')[0] + '\\' + file_name.split('_')[-1].split('-')[0]
            os.makedirs(sub_folder, exist_ok=True)
            local_path = os.path.join(sub_folder, file_name)
            
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            with open(local_path, "wb") as f:
                f.write(response.content)
            
            print(f"Téléchargé : {file_name}")
            
        except requests.exceptions.RequestException as e:
            print(f"   ERREUR lors du téléchargement de {url} : {e}")



def empty_folder(folder_path: str):
    """
    Vide complètement un dossier de tous ses fichiers et sous-dossiers.
    Ne supprime pas le dossier lui-même.
    Inclut des sécurités pour éviter de vider des dossiers importants.
    """
    print(f"Tentative de vidage du dossier : '{folder_path}'")

    # --- Sécurité 1 : Vérifier si le dossier existe ---
    if not os.path.isdir(folder_path):
        print(f"Le dossier n'existe pas. Rien à faire.")
        return

    # --- Sécurité 2 : Empêcher la suppression en dehors du projet ---
    project_root = os.path.abspath(os.path.curdir)
    folder_to_empty_abs = os.path.abspath(folder_path)

    if not folder_to_empty_abs.startswith(project_root) or folder_to_empty_abs == project_root:
        print(f"ERREUR DE SÉCURITÉ : Tentative de vider un dossier en dehors ou à la racine du projet.")
        print(f"Opération annulée pour le chemin : {folder_to_empty_abs}")
        return

    # --- Logique de suppression ---
    for item_name in os.listdir(folder_path):
        item_path = os.path.join(folder_path, item_name)
        try:
            if os.path.isfile(item_path) or os.path.islink(item_path):
                os.unlink(item_path) # Supprime fichier ou lien symbolique
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path) # Supprime un dossier et tout son contenu
        except Exception as e:
            print(f"Erreur lors de la suppression de {item_path}. Raison : {e}")
    
    print("Dossier vidé avec succès.")

if __name__ == "__main__":
    # 1. Obtenir la liste complète de tous les liens Parquet disponibles.
    all_links = find_parquet_links(TLC_DATA_PAGE_URL)
    
    if all_links and len(all_links) >= NUMBER_OF_FILES_TO_DOWNLOAD:
        print(f"\n{len(all_links)} liens trouvés au total.")
        
        # 2. Sélectionner 3 liens au hasard dans cette liste.
        print(f"Sélection de {NUMBER_OF_FILES_TO_DOWNLOAD} liens au hasard...")
        randomly_selected_links = random.sample(all_links, k=NUMBER_OF_FILES_TO_DOWNLOAD)
        
        # Affiche les liens qui ont été choisis.
        print("Liens sélectionnés pour le téléchargement :")
        for link in randomly_selected_links:
            print(f"- {link}")
        
        # 3. Lancer le téléchargement pour ces 3 liens.
        download_files(randomly_selected_links, DOWNLOAD_DIR)
        
        print("\n--- Opération terminée ---")
        print(f"Vous pouvez retrouver vos fichiers dans le dossier '{DOWNLOAD_DIR}'.")
        
    elif not all_links:
        print("Aucun lien n'a été trouvé, le script s'arrête.")
    else:
        print(f"Moins de {NUMBER_OF_FILES_TO_DOWNLOAD} liens ont été trouvés, impossible de sélectionner un échantillon.")