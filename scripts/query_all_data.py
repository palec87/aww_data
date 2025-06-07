"""
All data means water level data.
This script is used to query all data from the SNIRH and saves it in tmp file

Second script will assert correctness with the old data and update new values.

TODO: clarify the longitude and lat systems between local and stations DF
"""

import os
import sys
import pandas as pd
import requests
import io
import random
from time import sleep

from tqdm import tqdm
from datetime import datetime


# Function to get the representative piezometer name, from the dict
def get_key(d: dict, val: str):
    for key, value in d.items():  # give name of piezometer, get code
        if val == value:
            return key

    return "key doesn't exist"


def get_station_list(folder, save=False):
    """
    Get the list of stations from the SNIRH website.
    """
    df = pd.read_csv(f"{folder}/station_list.csv")
    df = df["<markers>"].str.split('"', expand=True)

    # cols to drop
    cols_drop = [0, 2, 3, 4, 5, 6, 8, 10, 11, 12, 14, 16, 17, 18, 19, 20]
    df.drop(df.columns[cols_drop], axis=1, inplace=True)

    # Change the column names
    df.columns = ["marker site", "latitude", "longitude", "estacao3", "estacao"]

    # marker site is the hyperlink in the website SNIRH to reach each station

    # clean the estacao3 and estacao fields
    df["estacao3"] = df["estacao3"].str.replace("&amp;#9632; ", "")
    df["estacao"] = df["estacao"].str.replace("■ ", "")
    df.drop(["estacao3"], axis=1, inplace=True)
    return df


def get_bacias(
    stations: pd.DataFrame, region_name: str = "algarve", save: bool = False
) -> pd.DataFrame:
    """
    Get the bacias for the given region. Bacias for algarve are
    'RIBEIRAS DO ALGARVE', 'GUADIANA', 'ARADE'.

    Args:
        stations (pd.DataFrame): DataFrame with the stations information.
        region_name (str): Name of the region to filter the stations.
        save (bool): Whether to save the resulting DataFrame to a CSV file.

    Returns:
        pd.DataFrame: DataFrame with the filtered stations for the specified region.
    """
    if region_name == "algarve":
        bacias = ["RIBEIRAS DO ALGARVE", "GUADIANA", "ARADE"]
        sites = stations.loc[stations["BACIA"].isin(bacias)]

        # remove duplicate columns or columns with no value
        sites = sites.drop(["_merge", "CÓDIGO"], axis=1)
        sites = sites.rename(columns={"estacao": "site_id"})

        # save and show
    else:
        raise NotImplementedError(
            "Region not recognized. So far only 'algarve' implemented"
        )
    if save:
        # save the sites to a CSV file
        sites.to_csv(f"aww_output/{region_name}_sites.csv", index=False)
    return sites


def merge_stations(stations_df, local_df):
    """
    Merge the stations dataframe with the local dataframe.
    """
    merged_df = pd.merge(
        local_df,
        stations_df,
        left_on="CÓDIGO",
        right_on="estacao",
        how="outer",
        indicator=True,
    )  # merge the df with hyperlink - station relation, with the df with info on the stations

    merged_df = merged_df.iloc[:-1, :]  # drop the last row which is uneeded info
    merged_df = merged_df[
        merged_df["_merge"] != "left_only"
    ]  # to remove obs point 607/884 that was added to the list, but was abandanoed, and it does not show up in the XML list
    return merged_df


if __name__ == "__main__":
    # define root as a parent directory of the current script
    root = sys.path[0] + "/.."
    input_folder = os.path.join(root, "input")
    tmp_folder = os.path.join(root, "tmp")
    df_stations = get_station_list(folder=input_folder, save=False)

    # get the list of stations
    df_local = pd.read_csv(
        os.path.join(
            root,
            "input/rede_Piezometria.csv",
        ),
        encoding="ISO-8859-1", skiprows=3, index_col=False
    )
    # merge the two dataframes
    df_stations = merge_stations(df_stations, df_local)

    # get the sites for the algarve region
    sites_algarve = get_bacias(df_stations, region_name="algarve")
    # print(sites_algarve)
    # dictionary with obs points in Sites_Algarve
    station_Algarve_dict = dict(
        zip(sites_algarve["marker site"], sites_algarve["site_id"])
    )

    # dictionary with obs points code and belonging aquifer
    station_aquif_Algarve_dict = dict(
        zip(sites_algarve["marker site"], sites_algarve["SISTEMA AQUÍFERO"])
    )

    # Define filter Parameters in this case hydrological years 1990 until 2025
    iDate = "01/10/1990"  # Initial date
    eDate = datetime.today().strftime("%d/%m/%Y")  # End date as today
    par = "2277"  # depth to groundwater level
    print(
        f"number of stations to be saved (1 csv per station): {len(list(station_aquif_Algarve_dict.keys()))}"
    )

    # for requests
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/118.0",
    }

    for key in tqdm(
        list(station_aquif_Algarve_dict.keys())
    ):  # for all obs points within the selected aquifers
        PiezometerName = station_Algarve_dict[key]
        PiezometerCode = get_key(station_Algarve_dict, PiezometerName)

        url = ("https://snirh.apambiente.pt/snirh/_dadosbase/site/paraCSV/dados_csv.php?sites=" + key + "&pars=" + par + "&tmin=" + iDate + "&tmax=" + eDate + "&formato=csv")  # fmt: off
        r = requests.get(
            url, headers=headers, allow_redirects=True
        ).content  # getting the file
        df = pd.read_csv(
            io.StringIO(r.decode("ISO-8859-1")),
            skiprows=3,
            dayfirst=True,
            parse_dates=[0],
            index_col=[0],
            usecols=[0, 1],
            header=0,
            skipfooter=1,
            names=["Date", PiezometerName],
            engine="python",
        )

        # temp can be rewritten
        # check if folder exists, if not create it
        out_folder = os.path.join(tmp_folder, "single_station_data/")
        if not os.path.exists(out_folder):
            os.makedirs(out_folder)
        filepath = os.path.join(out_folder, f"{PiezometerCode}.csv")
        df.to_csv(filepath, header=True)
        sleep(random.uniform(0.3, 0.7))
