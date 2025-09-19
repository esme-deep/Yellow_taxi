import polars as pl
import polars_st as st
import geopandas as gpd
from typing import List
from polars._typing import SchemaDict



def add_location_ids(data_lazy, zones_df):
    """
    Vérifie la présence des colonnes de coordonnées et les transforme en LocationID.
    """
    # Étape 1 : Vérification de la présence des colonnes (seule addition)
    required_cols = {"Start_Lon", "Start_Lat", "End_Lon", "End_Lat"}
    if not required_cols.issubset(data_lazy.columns):
        #missing = required_cols - set(data_lazy.columns)
        #raise ValueError(f"Le LazyFrame d'entrée est invalide. Colonnes manquantes : {missing}")
        print('pas de coords longitude/ latitude')
        return data_lazy

    # Étape 2 : Application de votre code de transformation exact
    final_lazy_df = (
        data_lazy.with_columns(
            geometry=st.point(pl.concat_arr(pl.col("Start_Lon"), pl.col("Start_Lat")))
        )
        .st.sjoin(zones_df.lazy(), predicate="contains", how="left", left_on="geometry", right_on="geometry")
        .rename({"LocationID": "PULocationID"})
        .drop("geometry", "geometry_right")
        .with_columns(
            geometry=st.point(pl.concat_arr(pl.col("End_Lon"), pl.col("End_Lat")))
        )
        .st.sjoin(zones_df.lazy(), predicate="contains", how="left", left_on="geometry", right_on="geometry")
        .rename({"LocationID": "DOLocationID"})
        .drop("geometry", "geometry_right")
        .select(
            pl.all().exclude(["Start_Lon", "Start_Lat", "End_Lon", "End_Lat"])
        )
    )
    
    return final_lazy_df



VENDOR_STRING_TO_ID_MAP = {
    "CMT": 1,
    "VTS": 2,
    "DDS": 3,
}

def reconcile_vendor_column(ldf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Cette fonction réconcilie la colonne du fournisseur, qu'elle s'appelle
    'vendor_name' (String) ou 'VendorID' (Int).
    """
    input_columns = ldf.collect_schema().names()

    if "vendor_name" in input_columns:
        return ldf.with_columns(
            pl.col("vendor_name")
              .replace_strict(VENDOR_STRING_TO_ID_MAP, default=None) 
              .alias("VendorID") # On crée la colonne cible 'VendorID'
        ).drop("vendor_name") # On supprime l'ancienne colonne

    elif "VendorID" in input_columns:
        # --- SCÉNARIO 2 : Fichier récent avec des IDs numériques ---
        print("Détection de 'VendorID'. Standardisation du type.")
        # On s'assure juste que le type est correct
        return ldf.with_columns(
            pl.col("VendorID").cast(pl.Int64)
        )
        
    else:
        # --- SCÉNARIO 3 : Aucune colonne fournisseur trouvée ---
        print("Aucune colonne de fournisseur trouvée. Ajout d'une colonne 'VendorID' vide.")
        # On crée une colonne vide pour que le schéma final soit cohérent
        return ldf.with_columns(
            pl.lit(None, dtype=pl.Int32).alias("VendorID")
        )



"""RENAME_MAP = {
        "Trip_Pickup_DateTime": "tpep_pickup_datetime",
        "Trip_Dropoff_DateTime": "tpep_dropoff_datetime",
        "Passenger_Count": "passenger_count",
        "Trip_Distance": "trip_distance",
        "Rate_Code": "RatecodeID",
        "store_and_forward": "store_and_fwd_flag",
        "Payment_Type": "payment_type",
        "Fare_Amt": "fare_amount",
        "surcharge": "extra",
        "Tip_Amt": "tip_amount",
        "Tolls_Amt": "tolls_amount",
        "Total_Amt": "total_amount"
    }"""
RENAME_MAP = {
    "Trip_Pickup_DateTime": "tpep_pickup_datetime",
    "pickup_datetime": "tpep_pickup_datetime", 

    "Trip_Dropoff_DateTime": "tpep_dropoff_datetime",
    "dropoff_datetime": "tpep_dropoff_datetime",

    "Passenger_Count": "passenger_count",
    "Trip_Distance": "trip_distance",
    
    "Rate_Code": "RatecodeID",
    "rate_code": "RatecodeID",
    
    "store_and_forward": "store_and_fwd_flag",
    "Payment_Type": "payment_type",
    "Fare_Amt": "fare_amount",
    "surcharge": "extra",
    "Tip_Amt": "tip_amount",
    "Tolls_Amt": "tolls_amount",
    "Total_Amt": "total_amount",
    "vendor_id": "VendorID",
    "pickup_longitude": "Start_Lon",
    "pickup_latitude": "Start_Lat",
    "dropoff_longitude": "End_Lon",
    "dropoff_latitude": "End_Lat",
    "airport_fee": "Airport_fee"
    }

def col_rename(lazy_df: pl.LazyFrame) -> pl.LazyFrame:
    """Renomme les colonnes pour correspondre au schéma de 2024."""
    rename_map ={}
    
    for col,desc in RENAME_MAP.items() :
        if col in lazy_df.collect_schema() :
            rename_map[col] = desc 
            print(col,desc)

    
     

    
    return_lazy = lazy_df.rename(rename_map)
    print(return_lazy.collect_schema())
    
    return return_lazy



def type_convert(lazy_df: pl.LazyFrame) -> pl.LazyFrame:
    """Vérifie le type de chaque colonne et ne convertit que si nécessaire."""
    print("debut de conversion")
    
    schema = lazy_df.collect_schema()
    expressions_de_conversion = []

    # Vérification pour la date de début
    if schema.get("tpep_pickup_datetime") == pl.String:
        print("  - Conversion de 'tpep_pickup_datetime'...")
        expressions_de_conversion.append(pl.col("tpep_pickup_datetime").str.to_datetime())

    # Vérification pour la date de fin
    if schema.get("tpep_dropoff_datetime") == pl.String:
        print("  - Conversion de 'tpep_dropoff_datetime'...")
        expressions_de_conversion.append(pl.col("tpep_dropoff_datetime").str.to_datetime())

    # Vérification pour le RatecodeID
    if schema.get("RatecodeID") != pl.Int64:
        print("  - Conversion de 'RatecodeID'...")
        expressions_de_conversion.append(pl.col("RatecodeID").cast(pl.Int64))

    # Si on a trouvé des colonnes à convertir, on les applique toutes en une fois
    if expressions_de_conversion:
        return lazy_df.with_columns(expressions_de_conversion)
    else:
        print("  - Tous les types sont déjà corrects.")
        return lazy_df
    


def enforce_schema_types(lazy_df: pl.LazyFrame, target_schema: dict) -> pl.LazyFrame:
    """
    Dynamically converts column types in a LazyFrame to match a target schema.
    """
    print("-> Dynamically checking and converting column types to match target schema...")
    
    current_schema = lazy_df.collect_schema()
    conversion_expressions = []

    # Iterate over every column in the target schema
    for col_name, target_type in target_schema.items():
        # Check if the column exists in our current dataframe
        if col_name in current_schema:
            current_type = current_schema[col_name]

            # If the types don't match, prepare a conversion
            if current_type != target_type:
                print(f"  - Mismatch found for '{col_name}': Current is {current_type}, Target is {target_type}. Converting...")
                
                # Special handling for String -> Datetime conversion
                if current_type == pl.String and target_type == pl.Datetime:
                    expression = pl.col(col_name).str.to_datetime()
                # For all other conversions, a standard .cast() is sufficient
                else:
                    expression = pl.col(col_name).cast(target_type)
                
                conversion_expressions.append(expression)

    if conversion_expressions:
        return lazy_df.with_columns(conversion_expressions)
    else:
        print("  - All column types already match the target schema.")
        return lazy_df

"""
def values_map(lazy_df: pl.LazyFrame) -> pl.LazyFrame:
    # Traduit les anciennes valeurs codées (texte) en nouvelles valeurs (numérique). de store_andfwd_flag et payment_type
    print("Mapping started")
    
    return lazy_df.with_columns(
        # Mappage pour store_and_fwd_flag
        pl.when(pl.col("store_and_fwd_flag") == 1.0)
          .then(pl.lit("Y"))
          .otherwise(pl.lit("N"))
          .alias("store_and_fwd_flag"),
        # Mappage pour payment_type 
        pl.when(pl.col("payment_type").str.to_uppercase().str.contains("CRE"))
          .then(pl.lit(1)) # 1 = Credit card
          .when(pl.col("payment_type").str.to_uppercase().str.contains("CAS"))
          .then(pl.lit(2)) # 2 = Cash
          .when(pl.col("payment_type").str.to_uppercase().str.contains("NO"))
          .then(pl.lit(3)) # 3 = No charge
          .when(pl.col("payment_type").str.to_uppercase().str.contains("DIS"))
          .then(pl.lit(4)) # 4 = Dispute
          .otherwise(pl.lit(5)) # 5 = Unknown
          .cast(pl.Int64)
          .alias("payment_type")
    )
"""

VENDOR_STRING_TO_ID_MAP = {
    "CMT": 1,
    "VTS": 2,
    "DDS": 3,
}

def values_map(lazy_df: pl.LazyFrame) -> pl.LazyFrame:
    print("Mapping started (robust version)...")
    
    current_schema = lazy_df.collect_schema()
    
    if "vendor_name" in current_schema:
        processed_df = lazy_df.with_columns(
            pl.col("vendor_name")
              .replace_strict(VENDOR_STRING_TO_ID_MAP, default=None)
              .alias("VendorID")
        ).drop("vendor_name")
    elif "VendorID" in current_schema and current_schema["VendorID"] != pl.Int64:
        if  current_schema["VendorID"] != pl.Int32 : 
            processed_df = lazy_df.with_columns(
            pl.col("VendorID")
              .replace_strict(VENDOR_STRING_TO_ID_MAP, default=None)
            )
        else : 
            processed_df = lazy_df.with_columns(
                pl.col("VendorID").cast(pl.Int64)
        )
    elif "VendorID" not in current_schema:
         processed_df = lazy_df.with_columns(
            pl.lit(None, dtype=pl.Int32).alias("VendorID")
        )
    else:
        processed_df = lazy_df

    current_schema = processed_df.collect_schema()
    expressions = []

    if "store_and_fwd_flag" in current_schema and current_schema["store_and_fwd_flag"] != pl.String:
        print("  - Converting 'store_and_fwd_flag'...")
        expressions.append(
            pl.when(pl.col("store_and_fwd_flag") == 1.0)
              .then(pl.lit("Y"))
              .otherwise(pl.lit("N"))
              .alias("store_and_fwd_flag")
        )

    if "payment_type" in current_schema and current_schema["payment_type"] == pl.String:
        print("  - Converting 'payment_type'...")
        expressions.append(
            pl.when(pl.col("payment_type").str.to_uppercase().str.contains("CRE"))
              .then(pl.lit(1))
              .when(pl.col("payment_type").str.to_uppercase().str.contains("FLE"))
              .then(pl.lit(0))
              .when(pl.col("payment_type").str.to_uppercase().str.contains("CAS"))
              .then(pl.lit(2))
              .when(pl.col("payment_type").str.to_uppercase().str.contains("NO"))
              .then(pl.lit(3))
              .when(pl.col("payment_type").str.to_uppercase().str.contains("DIS"))
              .then(pl.lit(4))
              .otherwise(pl.lit(5))
              .cast(pl.Int64)
              .alias("payment_type")
        )

    if expressions:
        return processed_df.with_columns(expressions)
    else:
        print("  - No value mapping needed for store_and_fwd_flag or payment_type.")
        return processed_df



def matching_schemas(
    lazy_df1: pl.LazyFrame, 
    lazy_df2: pl.LazyFrame, 
    afficher_differences: bool = False
) -> bool:
    """
    Vérifie si les schémas de deux LazyFrames sont identiques.

    Args:
        lazy_df1: Le premier LazyFrame à comparer.
        lazy_df2: Le deuxième LazyFrame à comparer.
        afficher_differences: Si True, affiche les différences détaillées
                              en cas de non-correspondance.

    Returns:
        True si les schémas sont identiques (noms, types et ordre des colonnes),
        False sinon.
    """

    # On utilise .collect_schema() qui est la méthode optimisée pour les LazyFrames
    schema1 = lazy_df1.collect_schema()
    schema2 = lazy_df2.collect_schema()

    # Comparaison directe des deux schémas
    sont_identiques = (schema1 == schema2)

    if sont_identiques:
        print(" Les schémas sont identiques.")
        return True
    
    # Si les schémas ne sont pas identiques et que l'utilisateur veut des détails
    if not sont_identiques and afficher_differences:
        print("Les schémas ne sont PAS identiques. Voici le détail des différences :")
        
        colonnes1 = list(schema1.keys())
        colonnes2 = list(schema2.keys())

        # Vérification du nombre, des noms et de l'ordre des colonnes
        if colonnes1 != colonnes2:
            print("\n1. Les noms ou l'ordre des colonnes diffèrent :")
            print(f"   - Schéma 1 ({len(colonnes1)} cols): {colonnes1}")
            print(f"   - Schéma 2 ({len(colonnes2)} cols): {colonnes2}")
            
            diff_cols = set(colonnes1).symmetric_difference(set(colonnes2))
            if diff_cols:
                print(f"   - Colonnes non partagées : {list(diff_cols)}")

        # Vérification des types pour les colonnes en commun
        print("\n2. Vérification des types de données pour les colonnes communes :")
        colonnes_communes = sorted(list(set(colonnes1).intersection(set(colonnes2))))
        
        aucune_diff_type = True
        for col in colonnes_communes:
            if schema1[col] != schema2[col]:
                print(f"   - Colonne '{col}':")
                print(f"     -> Type Schéma 1 = {schema1[col]}")
                print(f"     -> Type Schéma 2 = {schema2[col]}")
                aucune_diff_type = False
        
        if aucune_diff_type:
            print("   - Aucun type de données ne diffère pour les colonnes communes.")

    elif not sont_identiques:
        print(" Les schémas ne sont pas identiques.")
        
    return False

from polars._typing import SchemaDict

def add_missing_columns(
    lazy_df: pl.LazyFrame,
    target_schema: SchemaDict
) -> pl.LazyFrame:
    """Compares the LazyFrame to a target schema and adds any missing columns."""
    

    current_columns = set(lazy_df.columns)
    target_columns = set(target_schema.keys())
    
    columns_to_add = target_columns - current_columns

    if not columns_to_add:
        print("  - No columns to add.")
        return lazy_df

    print(f"  - Missing columns detected and will be added: {sorted(list(columns_to_add))}")

    add_expressions = []
    for column_name in sorted(list(columns_to_add)):
        target_dtype = target_schema[column_name]
        expression = pl.lit(None, dtype=target_dtype).alias(column_name)
        add_expressions.append(expression)
            
    return lazy_df.with_columns(add_expressions)




def order_columns_by_schema(
    lazy_df: pl.LazyFrame,
    target_schema: SchemaDict
) -> pl.LazyFrame:
    """
    Selects and reorders columns of a LazyFrame to match a target schema.
    This will also drop any columns that are not in the target schema.
    """
    print("-> Step 5: Ordering columns to match the final schema...")

    target_column_order = list(target_schema.keys())
    
    return lazy_df.select(target_column_order)