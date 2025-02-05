# aww_data
Query data from SNIRH for the AWW app.

## Essentials

1. `get_data_SNIRH.ipynb` gets data from SNIRH for stations which are in the table in the inputs.
2. Outputs (`aww_output`) contains two important parts
    * `algarve_stations.csv` which contains metadata for all the sampling stations. That is how we can even build the map
    * `single_station_data` folder, which contains two column (`date, value`) time series for each sampling point.
3. How to connect stations to single files.
    * Option 1: single data filenames are values in column `marker site` of the `algarve_stations`.
    * Option 2: second column name of the single data is `site_id` in the `algarve_stations`.
4. `station_list_clean` is simpler sites table, but not used currently.

All this can change if we need it.
 