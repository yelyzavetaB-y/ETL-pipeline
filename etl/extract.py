import os
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from textblob import TextBlob


def load_config():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, "config", "config.json")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at: {config_path}")

    with open(config_path, "r") as f:
        return json.load(f)


# WORLD BANK 
class WorldBankExtractor:

    def __init__(self):
        config = load_config()
        self.config = config
        self.countries = config["countries"]
        self.base_url = config["api_urls"]["worldbank"]
        self.indicators = config["indicators"]["worldbank"]

    def get_indicator(self, indicator_key, value_name):
        indicator_code = self.indicators[indicator_key]
        url = self.base_url.format(
            countries=";".join(self.countries),
            indicator=indicator_code
        )

        print(f"📡 Fetching {indicator_key} ({indicator_code}) from World Bank...")
        response = requests.get(url)

        if response.status_code != 200:
            print(f"Error {response.status_code} fetching {indicator_code}")
            return pd.DataFrame()

        data = r.json()[1]
        df = pd.DataFrame(data)[["country", "countryiso3code", "date", "value"]]
        df["country"] = df["country"].apply(lambda x: x["value"])
        df.rename(columns={"value": value_name}, inplace=True)
        df.dropna(inplace=True)
        df["date"] = df["date"].astype(int)
        return df

    def get_all_data(self):
        df_gdp = self.get_indicator("gdp_total", "GDP_total")
        df_gdp_pc = self.get_indicator("gdp_per_capita", "GDP_per_capita")
        df_unemp = self.get_indicator("unemployment", "Unemployment_Rate")
        df_fx = self.get_indicator("exchange_rate", "Exchange_Rate_LC")
        df_polstab = self.get_indicator("political_stability", "Political_Stability")

        df = (
            df_gdp
            .merge(df_gdp_pc, on=["country", "countryiso3code", "date"], how="inner")
            .merge(df_unemp, on=["country", "countryiso3code", "date"], how="left")
            .merge(df_fx, on=["country", "countryiso3code", "date"], how="left")
            .merge(df_polstab, on=["country", "countryiso3code", "date"], how="left")
        )

        current_year = datetime.now().year
        year_min = current_year - 3
        df["date"] = df["date"].astype(int)
        df_filtered = df[df["date"].between(year_min, current_year)]

        df_filtered["GDP_total_Billion"] = (df_filtered["GDP_total"] / 1e9).round(2)
        df_filtered["GDP_per_capita"] = df_filtered["GDP_per_capita"].round(2)
        df_filtered["Unemployment_Rate"] = df_filtered["Unemployment_Rate"].round(2)
        df_filtered["Exchange_Rate_LC"] = df_filtered["Exchange_Rate_LC"].round(2)
        df_filtered["Political_Stability"] = df_filtered["Political_Stability"].round(2)

        return df_filtered[
            [
                "country",
                "countryiso3code",
                "date",
                "GDP_total_Billion",
                "GDP_per_capita",
                "Unemployment_Rate",
                "Exchange_Rate_LC",
                "Political_Stability",
            ]
        ]


#EUROSTAT
class EurostatExtractor:

    def __init__(self):
        config = load_config()
        self.countries = config["countries"]
        self.base_url = config["api_urls"]["eurostat"]

        hicp_cfg = config["indicators"]["eurostat"]["hicp_inflation"]
        self.dataset = hicp_cfg["dataset"]
        self.unit = hicp_cfg["unit"]
        self.coicop = hicp_cfg["coicop"]
        self.years = hicp_cfg["years"]

    def get_hicp(self):
        all_records = []

        for country in self.countries:
            url = (
                self.base_url.format(dataset=self.dataset)
                + f"?geo={country}&unit={self.unit}&coicop={self.coicop}"
            )
            response = requests.get(url)
            if response.status_code != 200:
                print(f"Error {response.status_code} for {country}")
                continue

            data = response.json()

            if not data.get("value") or "dimension" not in data or "geo" not in data["dimension"]:
                print(f" No valid HICP data found for {country}")
                continue

            geo_labels = data["dimension"]["geo"]["category"]["label"]
            time_labels = data["dimension"]["time"]["category"]["label"]

            if not geo_labels or not time_labels:
                print(f"Missing geo/time labels for {country}")
                continue

            geo = list(geo_labels.values())[0]
            time_values = list(time_labels.values())

            for key, val in data["value"].items():
                time_idx = int(key.split(":")[-1])
                if time_idx < len(time_values):
                    year = int(time_values[time_idx])
                    if year in self.years:
                        all_records.append({
                            "country": geo,
                            "year": year,
                            "HICP_inflation_rate": float(val)
                        })

        df = pd.DataFrame(all_records)
        if df.empty:
            print("No valid HICP data found for any country.")
            return pd.DataFrame(columns=["country", "year", "HICP_inflation_rate"])

        df.sort_values(["country", "year"], inplace=True)
        print("HICP data extracted successfully.")
        return df


if __name__ == "__main__":
    euro = EurostatExtractor()
    df_hicp = euro.get_hicp()
    print("\n Final HICP Data:")
    print(df_hicp)
