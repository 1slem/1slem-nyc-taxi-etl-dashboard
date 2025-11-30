# NYC Taxi ETL & Dashboard

Short description
This project contains an ETL pipeline for a NYC taxi sample dataset and instructions to reproduce a Power BI dashboard shown in the screenshot below.

Screenshot

![NYC Taxi Dashboard](Dashboard/nyc%20taxi.png)


## Requirements

The project requires the following Python packages:


pandas>=2.2.0
numpy>=1.24.0
pyarrow>=19.0.0
python-dateutil>=2.8.0
pytz>=2025.0


Architecture (high level)
- Extract: read raw CSV from `data_source/echantillon.csv` and write extraction artifacts to `output/extract/`.
- Transform: clean and enrich data in `Scripts_ETL/transform.py`; results go to `output/transform/`.
- Load: persist transformed data (files / DB) in `Scripts_ETL/load.py`.
- Visualization: create Power BI report using the transformed dataset.

How to reproduce the Power BI dashboard (quick steps)
1. Prepare data
   - Ensure `data_source/echantillon.csv` is present and cleaned by running:
     - `python Scripts_ETL/extract.py`
     - `python Scripts_ETL/transform.py`
   - Place the final CSV or parquet in a known path (e.g., `output/transform/cleaned_trips.csv`).

2. Open Power BI Desktop
   - Get Data → Text/CSV → choose `output/transform/cleaned_trips.csv`.
   - Use Power Query to:
     - Parse datetime columns (pickup_datetime, dropoff_datetime).
     - Create derived columns: trip_duration (minutes), pickup_hour, pickup_date, borough (if coordinates → geocoding or spatial join).
     - Remove duplicates and invalid fares (fare_amount <= 0).

3. Model & Measures (DAX examples)
   - Total Trips:
     ```
     Total Trips = COUNTROWS('trips')
     ```
   - Total Revenue:
     ```
     Total Revenue = SUM('trips'[fare_amount]) + SUM('trips'[tip_amount])
     ```
   - Average Fare:
     ```
     Average Fare = AVERAGE('trips'[fare_amount])
     ```
   - Trips per Day:
     ```
     Trips per Day = CALCULATE([Total Trips], ALLEXCEPT('trips','trips'[pickup_date]))
     ```

4. Recommended visuals to match screenshot
   - KPI cards: Total Trips, Total Revenue, Average Fare.
   - Time series (line) of trips by day or hour.
   - Bar chart: trips by borough or payment type.
   - Map visual: pickup/dropoff density (use ArcGIS/Map or built-in map visual).
   - Filters/slicers: date range, borough, payment type.
