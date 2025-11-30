# NYC Taxi ETL & Dashboard

Short description
This project contains an ETL pipeline for a NYC taxi sample dataset and instructions to reproduce a Power BI dashboard shown in the screenshot below.

Screenshot

![NYC Taxi Dashboard](`C:\Users\Islem\OneDrive\Desktop\nyc-taxi-etl-dashboard\Taxi_dw\Dashboard\nyc taxi.png`)

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

5. Styling & layout
   - Use consistent color palette, set titles and tooltips, format currency for revenue.
   - Add a small textbox with data refresh instructions (e.g., run ETL scripts before refresh).

Where to put the screenshot
- Create an `images/` folder at the project root and copy `nyc taxi.png` into it, renaming to `nyc_taxi.png` is recommended.
- If you keep the original name, update the image link in this README accordingly:
  `![NYC Taxi Dashboard](images/nyc%20taxi.png)`

Next steps (optional)
- Add a requirements.txt (pandas, pyarrow, etc.).
- Add a small sample PBIX file or Power BI template (.pbit) with the visuals.
- Add automation to export cleaned data for Power BI refresh (e.g., scheduled task or API).