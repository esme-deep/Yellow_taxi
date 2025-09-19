import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

TLC_DATA_PAGE_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

def find_parquet_links(url: str):
   
    print(f"Connexion à la page : {url}...")
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        print("Page téléchargée avec succès.")

        # Parser le HTML avec BeautifulSoup
        soup = BeautifulSoup(response.text, 'lxml')

        # Trouver tous les liens (les balises <a>).
        all_links = soup.find_all('a')
        
        parquet_links = set() # On utilise un 'set' pour éviter les doublons automatiquement.

        for link_tag in all_links:
            # On récupère l'attribut 'href', qui contient l'URL du lien.
            href = link_tag.get('href')
            
            # On vérifie si le lien existe et se termine bien par ".parquet". #ICI IL FAUT MODIFIER
            if 'parquet' in href: #ici ca me fou des errer et je recupere pas le meme nombre de parquet
                # Certains liens peuvent être relatifs (ex: /path/to/file).
                # urljoin permet de construire une URL absolue.
                full_url = urljoin(url, href)
                parquet_links.add(full_url)
        
        if not parquet_links:
            print("AVERTISSEMENT : Aucun lien Parquet n'a été trouvé sur la page.")
            return []
            
        # On retourne une liste triée pour un affichage propre.
        return sorted(list(parquet_links))

    except requests.exceptions.RequestException as e:
        print(f"ERREUR : Impossible de télécharger la page. {e}")
        return []
    except Exception as e:
        print(f"Une erreur inattendue est survenue : {e}")
        return []
    

if __name__ == "__main__":
    # Point d'entrée du script.
    found_links = find_parquet_links(TLC_DATA_PAGE_URL)
    
    if found_links:
        print(f"\n--- {len(found_links)} LIENS PARQUET TROUVÉS ---")
        for link in found_links:
            print(link)
    