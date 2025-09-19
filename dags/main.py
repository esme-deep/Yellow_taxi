from scripts.find_parquet_links import find_parquet_links,TLC_DATA_PAGE_URL
from scripts.download_files import download_files #!!!!downloads ALL parquet!!!
from scripts.download_files_v2 import download_files_sample #ceci download un sample de parquet
from scripts.download_files import empty_folder
import os
from urllib.parse import urlparse
import re
#from scripts.find_parquet_links2 import find_parquet_links_by_section


found_links = find_parquet_links(TLC_DATA_PAGE_URL)


  

#print(len(found_links))


#empty_folder('data\staging')

download_files_sample(found_links,5,'data\staging')  


















ANNEES_A_ANALYSER = ["2023"]
annee = "2015"
c=0  
if found_links:
    print(f"\n--- {len(found_links)} LIENS PARQUET TROUVÉS ---")
    for link in found_links:
        if annee in link : 
            #print(link)
            c+=1
        if 'parquet' not in link : 
            print(link)
        

print(c)