import pandas as pd


class DataTransformer:

    def __init__(self, df_wb, df_hicp, df_news=None):
        self.df_wb = df_wb
        self.df_hicp = df_hicp
        self.df_news = df_news  

    def transform(self):

        df = self.df_wb.merge(
            self.df_hicp,
            left_on=["country", "date"],
            right_on=["country", "year"],
            how="left"
        ).drop(columns=["year"], errors="ignore")

        if self.df_news is not None and not self.df_news.empty:
            df = df.merge(
                self.df_news,
                on="country",
                how="left"
            )

        df["GDP_ratio"] = (
            (df["GDP_per_capita"] / df["GDP_total_Billion"])
            .replace([float("inf"), -float("inf")], None)
        ).round(4)

        df["Economic_Stability_Score"] = (
            (df["Political_Stability"].fillna(df["Political_Stability"].mean()) * 0.3)
            + ((df["GDP_per_capita"] / df["GDP_per_capita"].max()).fillna(0) * 0.2)
            + ((1 - df["Unemployment_Rate"].fillna(df["Unemployment_Rate"].mean()) / 100) * 0.2)
            + ((1 - df["HICP_inflation_rate"].fillna(df["HICP_inflation_rate"].mean()) / 100) * 0.1)
            + (((df["News_Sentiment"].fillna(0)) + 1) / 2 * 0.2)
        ).round(3)

        df["Data_Quality_Flag"] = df[["Political_Stability", "HICP_inflation_rate"]].isna().any(axis=1)

        df = df.sort_values(["country", "date"], ascending=[True, False])
        df.reset_index(drop=True, inplace=True)

        if "News_Sentiment_x" in df.columns:
            df.rename(columns={"News_Sentiment_x": "News_Sentiment"}, inplace=True)
        if "News_Sentiment_y" in df.columns:
            df.drop(columns=["News_Sentiment_y"], inplace=True, errors="ignore")

        print("Data transformation complete.")
        return df



if __name__ == "__main__":
    from extract import WorldBankExtractor, EurostatExtractor, NewsExtractor

    wb = WorldBankExtractor()
    euro = EurostatExtractor()
    news = NewsExtractor()

    df_wb = wb.get_all_data()
    df_hicp = euro.get_hicp()
    df_news = news.get_country_sentiment(["PL", "CZ", "HU", "RO", "UA"])

    transformer = DataTransformer(df_wb, df_hicp, df_news)
    df_final = transformer.transform()

    print("\n Final transformed dataset:")
    print(df_final.head())
