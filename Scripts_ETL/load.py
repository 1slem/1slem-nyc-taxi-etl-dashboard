import pandas as pd
import os
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import MetaData, Table

def create_star_schema(engine):
    """Crée le schéma en étoile optimisé pour les données taxi"""
    
    with engine.connect() as conn:
        # Suppression des tables existantes
        conn.execute(sa.text("DROP TABLE IF EXISTS fact_trips CASCADE"))
        conn.execute(sa.text("DROP TABLE IF EXISTS dim_time CASCADE"))
        conn.execute(sa.text("DROP TABLE IF EXISTS dim_location CASCADE"))
        conn.execute(sa.text("DROP TABLE IF EXISTS dim_payment CASCADE"))
        conn.commit()

        # Création des tables de dimension
        conn.execute(sa.text("""
            CREATE TABLE dim_time (
                time_pk SERIAL PRIMARY KEY,
                datetime TIMESTAMP UNIQUE NOT NULL,
                hour SMALLINT NOT NULL CHECK (hour BETWEEN 0 AND 23),
                day_of_week VARCHAR(9) NOT NULL,
                time_period VARCHAR(20) NOT NULL
            )
        """))
        
        conn.execute(sa.text("""
            CREATE TABLE dim_location (
                location_pk SERIAL PRIMARY KEY,
                location_id INTEGER UNIQUE NOT NULL,
                borough VARCHAR(50) NOT NULL DEFAULT 'Inconnu'
            )
        """))
        
        conn.execute(sa.text("""
            CREATE TABLE dim_payment (
                payment_pk SERIAL PRIMARY KEY,
                payment_type VARCHAR(20) UNIQUE NOT NULL
            )
        """))
        
        # Création de la table de faits avec optimisations
        conn.execute(sa.text("""
            CREATE TABLE fact_trips (
                trip_id BIGSERIAL PRIMARY KEY,
                time_pk INTEGER NOT NULL REFERENCES dim_time(time_pk),
                pickup_loc_pk INTEGER NOT NULL REFERENCES dim_location(location_pk),
                dropoff_loc_pk INTEGER NOT NULL REFERENCES dim_location(location_pk),
                payment_pk INTEGER NOT NULL REFERENCES dim_payment(payment_pk),
                passenger_count SMALLINT CHECK (passenger_count > 0),
                trip_distance NUMERIC(8,2) CHECK (trip_distance > 0),
                fare_amount NUMERIC(8,2) CHECK (fare_amount > 0),
                total_amount NUMERIC(8,2) CHECK (total_amount > 0),
                duration_min NUMERIC(8,2) CHECK (duration_min > 0),
                avg_speed NUMERIC(8,2) CHECK (avg_speed > 0)
            )
        """))
        
        # Création d'index pour les requêtes courantes
        conn.execute(sa.text("""
            CREATE INDEX idx_fact_time ON fact_trips(time_pk)
        """))
        conn.execute(sa.text("""
            CREATE INDEX idx_fact_payment ON fact_trips(payment_pk)
        """))
        
        conn.commit()

def load_to_dw():
    """Charge les données transformées dans le Data Warehouse"""
    
    DB_CONFIG = {
        'user': 'postgres',
        'password': 'admin',
        'host': 'localhost',
        'port': '5432',
        'database': 'db_taxi'
    }
    
    input_path = os.path.join(
        os.path.dirname(__file__), 
        '..', 
        'output', 
        'transform', 
        'transformed_data.parquet'
    )
    
    try:
        # Configuration de la connexion
        engine = sa.create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}",
            echo=False
        )
        
        # Vérification de la connexion
        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
            
        # Création du schéma
        create_star_schema(engine)
        print("✅ Schéma en étoile créé avec succès")

        # Chargement des données transformées
        df = pd.read_parquet(input_path)
        print(f"📊 Données transformées chargées : {len(df)} lignes")

        with engine.begin() as connection:
            metadata = MetaData()

            # Chargement de la dimension Temps
            time_df = df[['tpep_pickup_datetime', 'day_of_week', 'time_period']].copy()
            time_df = time_df.rename(columns={'tpep_pickup_datetime': 'datetime'})
            time_df['hour'] = time_df['datetime'].dt.hour.astype('int16')
            time_df = time_df[['datetime', 'hour', 'day_of_week', 'time_period']].drop_duplicates()
            
            dim_time = Table('dim_time', metadata, autoload_with=engine)
            if not time_df.empty:
                stmt = insert(dim_time).values(time_df.to_dict('records'))
                stmt = stmt.on_conflict_do_nothing(index_elements=['datetime'])
                connection.execute(stmt)
                print(f"🕒 Dim Temps : {len(time_df)} lignes insérées")

            # Chargement de la dimension Localisation
            locations = pd.concat([
                df['PULocationID'].rename('location_id'),
                df['DOLocationID'].rename('location_id')
            ]).drop_duplicates().to_frame()
            
            dim_location = Table('dim_location', metadata, autoload_with=engine)
            if not locations.empty:
                stmt = insert(dim_location).values(locations.to_dict('records'))
                stmt = stmt.on_conflict_do_nothing(index_elements=['location_id'])
                connection.execute(stmt)
                print(f"📍 Dim Localisation : {len(locations)} lignes insérées")

            # Chargement de la dimension Paiement
            payments = df[['payment_label']].drop_duplicates().rename(columns={'payment_label': 'payment_type'})
            
            dim_payment = Table('dim_payment', metadata, autoload_with=engine)
            if not payments.empty:
                stmt = insert(dim_payment).values(payments.to_dict('records'))
                stmt = stmt.on_conflict_do_nothing(index_elements=['payment_type'])
                connection.execute(stmt)
                print(f"💳 Dim Paiement : {len(payments)} lignes insérées")

            # Récupération des clés étrangères
            time_keys = pd.read_sql("SELECT time_pk, datetime FROM dim_time", connection)
            loc_keys = pd.read_sql("SELECT location_pk, location_id FROM dim_location", connection)
            payment_keys = pd.read_sql("SELECT payment_pk, payment_type FROM dim_payment", connection)

            # Préparation des données de faits
            fact_data = df.merge(
                time_keys,
                left_on='tpep_pickup_datetime',
                right_on='datetime'
            ).merge(
                loc_keys,
                left_on='PULocationID',
                right_on='location_id'
            ).merge(
                loc_keys,
                left_on='DOLocationID',
                right_on='location_id',
                suffixes=('_pu', '_do')
            ).merge(
                payment_keys,
                left_on='payment_label',
                right_on='payment_type'
            )

            # Sélection et formatage des colonnes
            fact_data = fact_data[[
                'time_pk', 
                'location_pk_pu', 
                'location_pk_do', 
                'payment_pk',
                'passenger_count', 
                'trip_distance', 
                'fare_amount', 
                'total_amount',
                'trip_duration',
                'avg_speed'
            ]].rename(columns={
                'location_pk_pu': 'pickup_loc_pk',
                'location_pk_do': 'dropoff_loc_pk',
                'trip_duration': 'duration_min'
            })

            # Conversion des types
            fact_data = fact_data.astype({
                'passenger_count': 'int16',
                'trip_distance': 'float32',
                'fare_amount': 'float32',
                'total_amount': 'float32',
                'duration_min': 'float32',
                'avg_speed': 'float32'
            })

            # Insertion des données de faits
            if not fact_data.empty:
                fact_data.to_sql(
                    'fact_trips', 
                    connection, 
                    if_exists='append', 
                    index=False,
                    dtype={
                        'time_pk': sa.Integer(),
                        'pickup_loc_pk': sa.Integer(),
                        'dropoff_loc_pk': sa.Integer(),
                        'payment_pk': sa.Integer(),
                        'passenger_count': sa.SmallInteger(),
                        'trip_distance': sa.Numeric(8,2),
                        'fare_amount': sa.Numeric(8,2),
                        'total_amount': sa.Numeric(8,2),
                        'duration_min': sa.Numeric(8,2),
                        'avg_speed': sa.Numeric(8,2)
                    }
                )
                print(f"🚕 Faits Taxi : {len(fact_data)} lignes insérées")

        return True

    except Exception as e:
        print(f"❌ Erreur critique : {str(e)}")
        return False

if __name__ == "__main__":
    success = load_to_dw()
    status = "SUCCÈS" if success else "ÉCHEC"
    print(f"\n=== CHARGEMENT TERMINÉ : {status} ===")