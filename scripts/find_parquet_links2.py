import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# L'URL de la page à analyser reste la même.
TLC_DATA_PAGE_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

def find_parquet_links_by_section(url: str):
    """
    Trouve les liens vers les fichiers Parquet en scannant les sections de la page
    contenant les données de voyage.
    """
    print(f"Connexion à la page : {url}...")
    
    try:
        # Étape 1 : Télécharger le contenu de la page web.
        response = requests.get(url)
        response.raise_for_status()  # Vérifie si la requête a réussi.
        print("Page téléchargée avec succès.")

        # Étape 2 : Analyser le HTML de la page avec BeautifulSoup.
        soup = BeautifulSoup(response.text, 'lxml')

        # Étape 3 : Trouver toutes les sections qui contiennent les liens.
        # En inspectant la page, on voit que chaque groupe de liens (pour chaque année)
        # est dans une balise <div> avec la classe "faq-answers".
        data_sections = soup.find_all('div', class_='faq-answers')
        
        if not data_sections:
            print("AVERTISSEMENT : Aucune section de données ('faq-answers') n'a été trouvée.")
            return []

        # On utilise un 'set' pour s'assurer que chaque URL est unique.
        parquet_links = set()

        # Étape 4 : Parcourir chaque section pour en extraire les liens.
        print(f"Analyse de {len(data_sections)} sections de données trouvées...")
        for section in data_sections:
            # Pour chaque section, on cherche toutes les balises de lien <a>.
            links_in_section = section.find_all('a')
            
            for link_tag in links_in_section:
                href = link_tag.get('href')
                
                # On vérifie que le lien existe et qu'il mène bien à un fichier .parquet.
                #if href and href.endswith('.parquet'):
                    # On transforme les liens relatifs en liens absolus.
                full_url = urljoin(url, href)
                parquet_links.add(full_url)
        
        if not parquet_links:
            print("AVERTISSEMENT : Aucun lien Parquet n'a été trouvé dans les sections analysées.")
            return []
            
        # On retourne une liste triée pour un affichage plus clair.
        return sorted(list(parquet_links))

    except requests.exceptions.RequestException as e:
        print(f"ERREUR : Impossible de télécharger la page. {e}")
        return []
    except Exception as e:
        print(f"Une erreur inattendue est survenue : {e}")
        return []

# --- Point d'entrée du script ---
if __name__ == "__main__":
    found_links = find_parquet_links_by_section(TLC_DATA_PAGE_URL)
    
    if found_links:
        print(f"\n--- {len(found_links)} LIENS PARQUET TROUVÉS ---")
        for link in found_links:
            print(link)