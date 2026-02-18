import os
import json
import math
import pandas as pd
import requests

def load_config():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, "config", "config.json")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f" Config not found at {config_path}")

    with open(config_path, "r") as f:
        return json.load(f)

class NotionLoader:
    def __init__(self):
        config = load_config()
        notion_cfg = config["notion"]

        self.token = notion_cfg["token"]
        self.database_id = notion_cfg["databases"]["economic_state"]

        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Notion-Version": "2025-09-03"
        }

    def safe_number(self, val):
        if val is None:
            return None
        if isinstance(val, (float, int)):
            if math.isnan(val) or math.isinf(val):
                return None
            return float(val)
        try:
            return float(val)
        except Exception:
            return None

    def create_page(self, row):
        url = "https://api.notion.com/v1/pages"

        data = {
            "parent": {"database_id": self.database_id},
            "properties": {
                "Country": {"title": [{"text": {"content": str(row.get("country", ""))}}]},
                "Year": {"number": int(row.get("date", 0))},
                "GDP_total_Billion": {"number": self.safe_number(row.get("GDP_total_Billion"))},
                "GDP_per_capita": {"number": self.safe_number(row.get("GDP_per_capita"))},
                "Unemployment_Rate": {"number": self.safe_number(row.get("Unemployment_Rate"))},
                "Exchange_Rate_LC": {"number": self.safe_number(row.get("Exchange_Rate_LC"))},
                "Political_Stability": {"number": self.safe_number(row.get("Political_Stability"))},
                "HICP_inflation_rate": {"number": self.safe_number(row.get("HICP_inflation_rate"))},
                "News_Sentiment": {"number": self.safe_number(row.get("News_Sentiment"))},
                "Economic_Stability_Score": {"number": self.safe_number(row.get("Economic_Stability_Score"))}
                


            }
        }

        try:
            response = requests.post(url, headers=self.headers, json=data)
            if response.status_code == 200:
                print(f" Added {row['country']} ({row['date']})")
            else:
                print(f" Failed for {row['country']}: {response.text}")
        except Exception as e:
            print(f"Request error for {row.get('country', '?')}: {e}")




    def upload_dataframe(self, df):

        df = df.replace([pd.NA, float('inf'), -float('inf')], None)
        df = df.where(pd.notnull(df), None).reset_index(drop=True)

        for _, row in df.iterrows():
            print(f"→ Uploading {row['country']} ({row['date']})...")  # debug print
            self.create_page(row)

        print(" Upload complete!")





if __name__ == "__main__":

    
    from etl.extract import WorldBankExtractor, EurostatExtractor, NewsExtractor
    from etl.transform import DataTransformer

    wb = WorldBankExtractor()
    euro = EurostatExtractor()
    news = NewsExtractor()

    df_wb = wb.get_all_data()
    df_hicp = euro.get_hicp()
    df_news = news.get_country_sentiment(["Czechia", "Hungary", "Poland", "Romania", "Ukraine"])

    
    transformer = DataTransformer(df_wb, df_hicp, df_news)
    df_final = transformer.transform()
    print("\n✅ Final Data:")
    print(df_final.head())

    
    loader = NotionLoader()
    loader.upload_dataframe(df_final)

    print("\n ETL Pipeline finished !")