"""
This module contains all IT-specific data loading and data cleaning routines.
"""
import requests
import pandas as pd
import numpy as np

idx = pd.IndexSlice


def get_raw_covid_data_it():
    url = "https://raw.githubusercontent.com/pcm-dpc/COVID-19/master/dati-regioni/dpc-covid19-ita-regioni.csv"
    #url = "../git_dpc/COVID-19/dati-regioni/dpc-covid19-ita-regioni.csv"
    data = pd.read_csv(url)
    return data


def process_covid_data_it(data: pd.DataFrame, run_date: pd.Timestamp):
    data = data.rename(columns={"denominazione_regione": "region"})
    data = data.rename(columns={"totale_casi": "positive"})
    data = data.rename(columns={"tamponi": "total"})
    data = data.rename(columns={"deceduti": "dead"})
    data["date"] = pd.to_datetime(data["data"], format="%Y-%m-%d").dt.date
    data = data.set_index(["region", "date"]).sort_index()
    data = data[["positive", "total", "dead"]]
    data = data.astype(float)
    
    # Data Clean
    data.loc[idx["Abruzzo", pd.Timestamp("2020-07-26")], "total"] = 124891
    data.loc[idx["Abruzzo", pd.Timestamp("2020-06-19")], "positive"] = 3281
    data.loc[idx["Abruzzo", pd.Timestamp("2020-07-22")], "positive"] = 3344
    
    data.loc[idx["Calabria", pd.Timestamp("2020-04-17")], "positive"] = 1010
    
    data.loc[idx["Marche", pd.Timestamp("2020-05-18")], "positive"] = 6671
    data.loc[idx["Marche", pd.Timestamp("2020-07-18")], "positive"] = 6811
    
    data.loc[idx["Basilicata", pd.Timestamp("2020-05-03")], "positive"] = 380
    data.loc[idx["Basilicata", pd.Timestamp("2020-05-04")], "positive"] = 380
    data.loc[idx["Basilicata", pd.Timestamp("2020-05-05")], "positive"] = 380
    data.loc[idx["Basilicata", pd.Timestamp("2020-05-06")], "positive"] = 380
    data.loc[idx["Basilicata", pd.Timestamp("2020-05-07")], "positive"] = 380
    data.loc[idx["Basilicata", pd.Timestamp("2020-07-11")], "positive"] = 405
    data.loc[idx["Basilicata", pd.Timestamp("2020-07-12")], "positive"] = 405
    data.loc[idx["Basilicata", pd.Timestamp("2020-07-13")], "positive"] = 405
    data.loc[idx["Basilicata", pd.Timestamp("2020-07-14")], "positive"] = 405
    data.loc[idx["Basilicata", pd.Timestamp("2020-08-15")], "positive"] = 485
    
    data.loc[idx["Sardegna", pd.Timestamp("2020-05-03")], "positive"] = 1315
    data.loc[idx["Sardegna", pd.Timestamp("2020-05-20")], "positive"] = 1354
    data.loc[idx["Sardegna", pd.Timestamp("2020-05-21")], "positive"] = 1354
    data.loc[idx["Sardegna", pd.Timestamp("2020-05-22")], "positive"] = 1354
    data.loc[idx["Sardegna", pd.Timestamp("2020-05-23")], "positive"] = 1354
    data.loc[idx["Sardegna", pd.Timestamp("2020-05-24")], "positive"] = 1354
    data.loc[idx["Sardegna", pd.Timestamp("2020-07-27")], "positive"] = 1388
    
    data.loc[idx["Campania", pd.Timestamp("2020-05-12")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-05-13")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-05-14")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-05-15")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-05-16")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-05-17")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-05-18")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-05-19")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-05-20")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-05-21")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-05-22")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-05-23")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-05-24")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-05-25")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-05-26")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-05-27")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-05-28")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-05-29")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-05-30")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-05-31")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-06-01")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-06-02")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-06-03")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-06-04")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-06-05")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-06-06")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-06-07")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-06-08")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-06-09")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-06-10")], "positive"] = 4608
    data.loc[idx["Campania", pd.Timestamp("2020-06-11")], "positive"] = 4608
    
    data.loc[idx["Emilia-Romagna", pd.Timestamp("2020-03-28")], "total"] = 48619
    data.loc[idx["Emilia-Romagna", pd.Timestamp("2020-03-29")], "total"] = 49439
    
    data.loc[idx["Friuli Venezia Giulia", pd.Timestamp("2020-03-19")], "total"] = 4958
    
    data.loc[idx["Lombardia", pd.Timestamp("2020-02-25")], "total"] = 2336
    
    data.loc[idx["Sicilia", pd.Timestamp("2020-04-27")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-04-28")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-04-29")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-04-30")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-01")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-02")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-03")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-04")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-05")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-06")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-07")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-08")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-09")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-10")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-11")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-12")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-13")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-14")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-15")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-16")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-17")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-18")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-19")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-20")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-21")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-22")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-23")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-24")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-25")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-26")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-27")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-28")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-29")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-30")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-05-31")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-06-01")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-06-02")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-06-03")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-06-04")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-06-05")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-06-06")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-06-07")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-06-08")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-06-09")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-06-10")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-06-11")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-06-12")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-06-13")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-06-14")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-06-15")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-06-16")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-06-17")], "positive"] = 3070
    data.loc[idx["Sicilia", pd.Timestamp("2020-06-18")], "positive"] = 3070
    
    data.loc[idx["Liguria", pd.Timestamp("2020-02-29")], "positive"] = 20
    data.loc[idx["Liguria", pd.Timestamp("2020-03-01")], "positive"] = 21
    
    data.loc[idx["P.A. Bolzano", pd.Timestamp("2020-06-14")], "positive"] = 2610
    data.loc[idx["P.A. Bolzano", pd.Timestamp("2020-07-28")], "positive"] = 2700
    data.loc[idx["P.A. Bolzano", pd.Timestamp("2020-07-29")], "positive"] = 2700
    
    data.loc[idx["Piemonte", pd.Timestamp("2020-02-27")], "positive"] = 3
    data.loc[idx["Piemonte", pd.Timestamp("2020-03-09")], "positive"] = 360
    
    data.loc[idx["Sardegna", pd.Timestamp("2020-06-06")], "positive"] = 1360
    data.loc[idx["Sardegna", pd.Timestamp("2020-06-07")], "positive"] = 1360
    data.loc[idx["Sardegna", pd.Timestamp("2020-06-08")], "positive"] = 1360
    data.loc[idx["Sardegna", pd.Timestamp("2020-06-09")], "positive"] = 1360
    data.loc[idx["Sardegna", pd.Timestamp("2020-06-10")], "positive"] = 1360
    data.loc[idx["Sardegna", pd.Timestamp("2020-06-11")], "positive"] = 1360
    data.loc[idx["Sardegna", pd.Timestamp("2020-06-12")], "positive"] = 1360
    data.loc[idx["Sardegna", pd.Timestamp("2020-06-13")], "positive"] = 1360
    data.loc[idx["Sardegna", pd.Timestamp("2020-06-14")], "positive"] = 1360
    data.loc[idx["Sardegna", pd.Timestamp("2020-06-15")], "positive"] = 1360
    data.loc[idx["Sardegna", pd.Timestamp("2020-06-16")], "positive"] = 1360
    data.loc[idx["Sardegna", pd.Timestamp("2020-06-17")], "positive"] = 1360
    data.loc[idx["Sardegna", pd.Timestamp("2020-06-18")], "positive"] = 1360
    data.loc[idx["Sardegna", pd.Timestamp("2020-06-19")], "positive"] = 1360
    data.loc[idx["Sardegna", pd.Timestamp("2020-06-20")], "positive"] = 1360
    data.loc[idx["Sardegna", pd.Timestamp("2020-06-21")], "positive"] = 1360
    data.loc[idx["Sardegna", pd.Timestamp("2020-06-22")], "positive"] = 1360
    
    data.loc[idx["Sicilia", pd.Timestamp("2020-03-02")], "positive"] = 9
    data.loc[idx["Sicilia", pd.Timestamp("2020-03-03")], "positive"] = 9
    
    data.loc[idx["P.A. Trento", pd.Timestamp("2020-06-24") :], "positive"] -= 385
    
    data.loc[idx["Puglia", pd.Timestamp("2020-06-26")], "positive"] = 4530
    data.loc[idx["Puglia", pd.Timestamp("2020-06-27")], "positive"] = 4530
    data.loc[idx["Puglia", pd.Timestamp("2020-06-28")], "positive"] = 4530
    data.loc[idx["Puglia", pd.Timestamp("2020-06-29")], "positive"] = 4530
    data.loc[idx["Puglia", pd.Timestamp("2020-06-30")], "positive"] = 4530
    data.loc[idx["Puglia", pd.Timestamp("2020-07-07")], "positive"] = 4536
    data.loc[idx["Puglia", pd.Timestamp("2020-07-19")], "positive"] = 4556
    data.loc[idx["Puglia", pd.Timestamp("2020-07-20")], "positive"] = 4556
    
    data.loc[idx["Valle d'Aosta", pd.Timestamp("2020-03-14")], "total"] = 230
    data.loc[idx["Valle d'Aosta", pd.Timestamp("2020-06-22")], "total"] = 17491
    
    date_index = data.index.get_level_values(1).unique()
    
    # Add - Trentino Alto Adige
    trentino_alto_adige_index = pd.MultiIndex.from_product([["Trentino Alto Adige"], date_index], names=["region", "date"])
    trentino_alto_adige = pd.DataFrame(index=trentino_alto_adige_index).sort_index()
    trentino_alto_adige["positive"] = data.loc["P.A. Trento"]["positive"].values + data.loc["P.A. Bolzano"]["positive"].values
    trentino_alto_adige["total"] = data.loc["P.A. Trento"]["total"].values + data.loc["P.A. Bolzano"]["total"].values
    trentino_alto_adige["dead"] = data.loc["P.A. Trento"]["dead"].values + data.loc["P.A. Bolzano"]["dead"].values
    data = data.append(trentino_alto_adige)
    
    # Add - Nord Italia
    nord_italia_index = pd.MultiIndex.from_product([["Nord Italia"], date_index], names=["region", "date"])
    nord_italia = pd.DataFrame(index=nord_italia_index).sort_index()
    nord_italia["positive"] = data.loc["Valle d\'Aosta"]["positive"].values + data.loc["Piemonte"]["positive"].values + data.loc["Liguria"]["positive"].values + data.loc["Lombardia"]["positive"].values + data.loc["P.A. Trento"]["positive"].values + data.loc["P.A. Bolzano"]["positive"].values + data.loc["Veneto"]["positive"].values + data.loc["Friuli Venezia Giulia"]["positive"].values + data.loc["Emilia-Romagna"]["positive"].values
    nord_italia["total"] = data.loc["Valle d\'Aosta"]["total"].values + data.loc["Piemonte"]["total"].values + data.loc["Liguria"]["total"].values + data.loc["Lombardia"]["total"].values + data.loc["P.A. Trento"]["total"].values + data.loc["P.A. Bolzano"]["total"].values + data.loc["Veneto"]["total"].values + data.loc["Friuli Venezia Giulia"]["total"].values + data.loc["Emilia-Romagna"]["total"].values
    nord_italia["dead"] = data.loc["Valle d\'Aosta"]["dead"].values + data.loc["Piemonte"]["dead"].values + data.loc["Liguria"]["dead"].values + data.loc["Lombardia"]["dead"].values + data.loc["P.A. Trento"]["dead"].values + data.loc["P.A. Bolzano"]["dead"].values + data.loc["Veneto"]["dead"].values + data.loc["Friuli Venezia Giulia"]["dead"].values + data.loc["Emilia-Romagna"]["dead"].values
    data = data.append(nord_italia)
    
    # Add - Centro Italia
    centro_italia_index = pd.MultiIndex.from_product([["Centro Italia"], date_index], names=["region", "date"])
    centro_italia = pd.DataFrame(index=centro_italia_index).sort_index()
    centro_italia["positive"] = data.loc["Toscana"]["positive"].values + data.loc["Marche"]["positive"].values + data.loc["Umbria"]["positive"].values + data.loc["Lazio"]["positive"].values
    centro_italia["total"] = data.loc["Toscana"]["total"].values + data.loc["Marche"]["total"].values + data.loc["Umbria"]["total"].values + data.loc["Lazio"]["total"].values
    centro_italia["dead"] = data.loc["Toscana"]["dead"].values + data.loc["Marche"]["dead"].values + data.loc["Umbria"]["dead"].values + data.loc["Lazio"]["dead"].values
    data = data.append(centro_italia)
    
    # Add - Sud Italia
    sud_italia_index = pd.MultiIndex.from_product([["Sud Italia"], date_index], names=["region", "date"])
    sud_italia = pd.DataFrame(index=sud_italia_index).sort_index()
    sud_italia["positive"] = data.loc["Abruzzo"]["positive"].values + data.loc["Molise"]["positive"].values + data.loc["Campania"]["positive"].values + data.loc["Basilicata"]["positive"].values + data.loc["Puglia"]["positive"].values + data.loc["Calabria"]["positive"].values + data.loc["Sicilia"]["positive"].values + data.loc["Sardegna"]["positive"].values
    sud_italia["total"] = data.loc["Abruzzo"]["total"].values + data.loc["Molise"]["total"].values + data.loc["Campania"]["total"].values + data.loc["Basilicata"]["total"].values + data.loc["Puglia"]["total"].values + data.loc["Calabria"]["total"].values + data.loc["Sicilia"]["total"].values + data.loc["Sardegna"]["total"].values
    sud_italia["dead"] = data.loc["Abruzzo"]["dead"].values + data.loc["Molise"]["dead"].values + data.loc["Campania"]["dead"].values + data.loc["Basilicata"]["dead"].values + data.loc["Puglia"]["dead"].values + data.loc["Calabria"]["dead"].values + data.loc["Sicilia"]["dead"].values + data.loc["Sardegna"]["dead"].values
    data = data.append(sud_italia)
    
    # Add - Italia
    italia_index = pd.MultiIndex.from_product([["Italia"], date_index], names=["region", "date"])
    italia = pd.DataFrame(index=italia_index).sort_index()
    italia["positive"] = data.loc["Nord Italia"]["positive"].values + data.loc["Centro Italia"]["positive"].values + data.loc["Sud Italia"]["positive"].values
    italia["total"] = data.loc["Nord Italia"]["total"].values + data.loc["Centro Italia"]["total"].values + data.loc["Sud Italia"]["total"].values
    italia["dead"] = data.loc["Nord Italia"]["dead"].values + data.loc["Centro Italia"]["dead"].values + data.loc["Sud Italia"]["dead"].values
    data = data.append(italia)
    
    all_cases = data['positive']
    data = data.diff().fillna(0).clip(0, None).sort_index()
    data['all_cases'] = all_cases
    
    return data.loc[idx[:, :run_date], ["positive", "total", "dead", "all_cases"]]
    return data


def get_and_process_covid_data_it(run_date: pd.Timestamp):
    """ Helper function for getting and processing COVIDTracking data at once """
    data = get_raw_covid_data_it()
    data = process_covid_data_it(data, run_date)
    return data
