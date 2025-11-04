import requests
import json
import pandas as pd

class WorldBankExtractor:
    """
    Handles data extraction from the World Bank API for multiple indicators and countries.
    Config-driven: reads URLs and indicator codes from config.json
    """

    def __init__(self, config_path="ETL-Pipeline/config/config.json"):
        # Load configuration
        with open(config_path) as f:
            config = json.load(f)

        self.config = config
        self.countries = config["countries"]
        self.base_url = config["api_urls"]["worldbank"]
        self.indicators = config["indicators"]["worldbank"]

    def get_indicator(self, indicator_key, value_name):
        """
        Fetch a World Bank indicator (defined in config) and return a DataFrame.
        """
        indicator_code = self.indicators[indicator_key]
        url = self.base_url.format(
            countries=";".join(self.countries), indicator=indicator_code
        )

        print(f"📡 Fetching {indicator_key} ({indicator_code}) from World Bank...")
        response = requests.get(url)

        if response.status_code != 200:
            print(f"❌ Error {response.status_code} fetching {indicator_code}")
            return pd.DataFrame()

        data = response.json()[1]
        df = pd.DataFrame(data)[["country", "countryiso3code", "date", "value"]]
        df["country"] = df["country"].apply(lambda x: x["value"])
        df.rename(columns={"value": value_name}, inplace=True)
        df.dropna(inplace=True)
        df["date"] = df["date"].astype(int)
        return df

    def get_all_data(self):
        """
        Fetch GDP, GDP per capita, unemployment, and exchange rate data (from config).
        """
        df_gdp = self.get_indicator("gdp_total", "GDP_total_USD")
        df_gdp_pc = self.get_indicator("gdp_per_capita", "GDP_per_capita_USD")
        df_unemp = self.get_indicator("unemployment", "Unemployment_Rate")
        df_fx = self.get_indicator("exchange_rate", "Exchange_Rate_LC_per_USD")

        # Merge all datasets
        df = (
            df_gdp
            .merge(df_gdp_pc, on=["country", "countryiso3code", "date"], how="inner")
            .merge(df_unemp, on=["country", "countryiso3code", "date"], how="left")
            .merge(df_fx, on=["country", "countryiso3code", "date"], how="left")
        )

        # Keep latest year per country
        df_latest = df.sort_values("date", ascending=False).drop_duplicates("country")

        # Format values
        df_latest["GDP_total_Billion_USD"] = (df_latest["GDP_total_USD"] / 1e9).round(2)
        df_latest["GDP_per_capita_USD"] = df_latest["GDP_per_capita_USD"].round(2)
        df_latest["Unemployment_Rate"] = df_latest["Unemployment_Rate"].round(2)
        df_latest["Exchange_Rate_LC_per_USD"] = df_latest["Exchange_Rate_LC_per_USD"].round(2)

        return df_latest[
            [
                "country",
                "countryiso3code",
                "date",
                "GDP_total_Billion_USD",
                "GDP_per_capita_USD",
                "Unemployment_Rate",
                "Exchange_Rate_LC_per_USD",
            ]
        ]


if __name__ == "__main__":
    extractor = WorldBankExtractor()
    df_latest = extractor.get_all_data()
    print(df_latest)



class EurostatExtractor:
    def __init__(self, config_path="ETL-Pipeline/config/config.json"):
        with open(config_path) as f:
            config = json.load(f)

        self.config = config
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

            print(f"📡 Fetching HICP for {country} from Eurostat...")
            response = requests.get(url)
            if response.status_code != 200:
                print(f"❌ Error {response.status_code} for {country}")
                continue

            data = response.json()
            values = data["value"]
            geo_labels = data["dimension"]["geo"]["category"]["label"]
            time_labels = data["dimension"]["time"]["category"]["label"]

            geo = list(geo_labels.values())[0]  # one country
            for key, val in values.items():
                parts = key.split(":")
                time_idx = int(parts[-1])
                year = int(list(time_labels.values())[time_idx])
                all_records.append({"country": geo, "year": year, "HICP_inflation_rate": float(val)})

        df = pd.DataFrame(all_records)
        df = df[df["year"].isin(self.years)]
        df.sort_values(["country", "year"], inplace=True)
        return df


if __name__ == "__main__":
    euro = EurostatExtractor()
    df_hicp = euro.get_hicp()
    print("\n✅ Final HICP Data:")
    print(df_hicp)