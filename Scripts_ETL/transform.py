import pandas as pd
import numpy as np
import os
import json
from datetime import datetime
from typing import Tuple
import logging

# Configuration du logger
logging.basicConfig(
    filename='transform.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class DataTransformer:
    def __init__(self):
        self.anomalies = pd.DataFrame()
        self.stats = {}
        self.log = []
        self.df = pd.DataFrame()

    def load_data(self, input_path: str) -> pd.DataFrame:
        """Charge les données extraites"""
        try:
            self.df = pd.read_parquet(input_path)
            logging.info(f"Données chargées : {len(self.df)} lignes")
            return self.df
        except Exception as e:
            logging.error(f"Erreur de chargement : {str(e)}")
            raise

    def calculate_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calcule les métriques de transformation"""
        df['trip_duration'] = (
            df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']
        ).dt.total_seconds() / 60
        
        df['avg_speed'] = df['trip_distance'] / (df['trip_duration']/60)
        df['avg_speed'] = df['avg_speed'].round(2)
        
        return df

    def handle_anomalies(self, df: pd.DataFrame) -> pd.DataFrame:
        """Détecte et gère les anomalies avec des raisons spécifiques"""
        # Création de masques pour chaque type d'anomalie
        conditions = {
            'Durée invalide': (df['trip_duration'] <= 0),
            'Distance hors limites': ~df['trip_distance'].between(0.1, 100),
            'Montant de course invalide': (df['fare_amount'] <= 0),
            'Passagers invalides': (df['passenger_count'] <= 0),
            'Vitesse irréaliste': ~df['avg_speed'].between(1, 100)
        }

        # Création d'une colonne de raison par défaut
        df['rejection_reason'] = np.nan

        # Application des masques avec priorité
        for reason, mask in conditions.items():
            anomaly_mask = mask & df['rejection_reason'].isna()
            df.loc[anomaly_mask, 'rejection_reason'] = reason

        # Séparation des anomalies
        anomalies = df[df['rejection_reason'].notna()].copy()
        valid_data = df[df['rejection_reason'].isna()].drop(columns=['rejection_reason'])

        # Enregistrement des anomalies
        if not anomalies.empty:
            self.anomalies = pd.concat([self.anomalies, anomalies])
            logging.warning(f"Anomalies détectées : {len(anomalies)} lignes")
            logging.info(f"Détail des anomalies :\n{anomalies['rejection_reason'].value_counts().to_string()}")

        return valid_data

    def add_time_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ajoute les caractéristiques temporelles"""
        df['pickup_hour'] = df['tpep_pickup_datetime'].dt.hour
        df['day_of_week'] = df['tpep_pickup_datetime'].dt.day_name()
        
        conditions = [
            df['pickup_hour'].between(0, 5),
            df['pickup_hour'].between(6, 11),
            df['pickup_hour'].between(12, 17),
            df['pickup_hour'].between(18, 23)
        ]
        choices = ['Nuit', 'Matin', 'Après-midi', 'Soir']
        
        df['time_period'] = np.select(conditions, choices, default='Inconnu')
        return df

    def encode_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Encode les caractéristiques catégorielles"""
        payment_labels = {
            1: 'Carte de crédit',
            2: 'Espèces',
            3: 'Gratuit',
            4: 'Conflit'
        }
        df['payment_label'] = df['payment_type'].map(payment_labels)
        
        ratecode_labels = {
            1: 'Standard',
            2: 'Aéroport JFK',
            3: 'Aéroport Newark',
            4: 'Aéroport LaGuardia',
            5: 'Course partagée',
            6: 'Location'
        }
        df['ratecode_label'] = df['RatecodeID'].map(ratecode_labels)
        
        return df

    def save_artifacts(self, df: pd.DataFrame, output_dir: str):
        """Sauvegarde les résultats"""
        os.makedirs(output_dir, exist_ok=True)
        
        # Sauvegarde des données transformées
        df.to_parquet(
            os.path.join(output_dir, 'transformed_data.parquet'),
            compression='snappy'
        )
        
        # Sauvegarde des anomalies
        if not self.anomalies.empty:
            anomaly_path = os.path.join(output_dir, f"anomalies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet")
            self.anomalies.to_parquet(anomaly_path)
            logging.info(f"Anomalies sauvegardées : {anomaly_path}")

    def generate_report(self, output_dir: str):
        """Génère le rapport de transformation détaillé"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'total_processed': len(self.anomalies) + len(self.df),
            'anomaly_rate': f"{len(self.anomalies)/(len(self.anomalies)+len(self.df))*100:.2f}%",
            'anomaly_details': self.anomalies['rejection_reason'].value_counts().to_dict(),
            'data_quality_metrics': {
                'avg_trip_duration': self.df['trip_duration'].mean(),
                'avg_speed': self.df['avg_speed'].mean(),
                'total_fare_amount': self.df['fare_amount'].sum()
            }
        }
        
        with open(os.path.join(output_dir, 'transformation_report.json'), 'w') as f:
            json.dump(report, f, indent=4, default=str)

def transform_data():
    """Pipeline principal de transformation"""
    transformer = DataTransformer()
    
    try:
        # Configuration des chemins
        current_dir = os.path.dirname(__file__)
        input_path = os.path.abspath(os.path.join(current_dir, '..', 'output', 'extract', 'extracted_data.parquet'))
        output_dir = os.path.abspath(os.path.join(current_dir, '..', 'output', 'transform'))
        
        print("\n=== Démarrage de la transformation ===")
        print(f"📥 Entrée : {input_path}")
        print(f"📤 Sortie : {output_dir}")

        # Chargement des données
        df = transformer.load_data(input_path)
        
        # Pipeline de transformation
        df = transformer.calculate_metrics(df)
        df = transformer.handle_anomalies(df)
        df = transformer.add_time_features(df)
        df = transformer.encode_features(df)
        
        # Post-traitement
        df = df.convert_dtypes()
        df = df.drop_duplicates(subset=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])
        
        # Sauvegarde
        transformer.save_artifacts(df, output_dir)
        transformer.generate_report(output_dir)
        
        print(f"\n✅ Transformation réussie !")
        print(f"📝 Rapport généré : {os.path.join(output_dir, 'transformation_report.json')}")
        return True

    except Exception as e:
        logging.error(f"Échec de la transformation : {str(e)}")
        print(f"\n❌ ERREUR : {str(e)}")
        return False

if __name__ == "__main__":
    success = transform_data()
    status = "TERMINÉ AVEC SUCCÈS" if success else "TERMINÉ AVEC DES ERREURS"
    print(f"\n=== Statut : {status} ===")