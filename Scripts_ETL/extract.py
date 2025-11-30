import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from typing import List, Tuple

SELECTED_COLUMNS = [
    'VendorID',
    'tpep_pickup_datetime',
    'tpep_dropoff_datetime',
    'passenger_count',
    'trip_distance',
    'PULocationID',
    'DOLocationID',
    'RatecodeID',
    'payment_type',
    'fare_amount',
    'total_amount'
]

def extract_data(required_cols: List[str] = SELECTED_COLUMNS):
    """Extrait et transforme les données depuis un fichier Parquet"""
    
    current_dir = os.path.dirname(__file__)
    input_path = os.path.abspath(os.path.join(current_dir, '..', 'data_source', 'echantillon.parquet'))
    output_dir = os.path.abspath(os.path.join(current_dir, '..', 'output', 'extract'))
    output_path = os.path.join(output_dir, 'extracted_data.parquet')

    print("\n=== Démarrage de l'extraction ===")
    print(f"📂 Source : {input_path}")
    print(f"🎯 Cible : {output_path}")

    try:
        # Lecture avec PyArrow
        table = pq.read_table(input_path)
        df = table.to_pandas()

        # Sélection des colonnes
        df = df[required_cols]

        # Conversion des dates
        date_cols = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.tz_localize(None)

        # Nettoyage des données
        df = df.dropna(subset=date_cols)
        df = df.convert_dtypes()

        # Conversion des types numériques
        int_cols = ['VendorID', 'passenger_count', 'PULocationID', 'DOLocationID']
        df[int_cols] = df[int_cols].fillna(0).astype('int16')
        df['payment_type'] = df['payment_type'].astype('category')

        # Validation finale
        if df.empty:
            raise ValueError("Aucune donnée valide après traitement")

        # Sauvegarde
        os.makedirs(output_dir, exist_ok=True)
        df.to_parquet(
            output_path,
            engine='pyarrow',
            compression='snappy',
            index=False
        )

        print(f"\n✅ Succès ! {len(df)} lignes sauvegardées")
        return True

    except Exception as e:
        print(f"\n❌ ERREUR : {str(e)}")
        print("🔧 Actions recommandées :")
        print("1. Vérifier le format des dates dans le fichier source")
        print("2. Vérifier les types de colonnes avec df.info()")
        return False

if __name__ == "__main__":
    success = extract_data()
    status = "SUCCÈS" if success else "ÉCHEC"
    print(f"\n=== Statut : {status} ===")