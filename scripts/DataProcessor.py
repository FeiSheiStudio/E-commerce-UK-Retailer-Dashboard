# ---------------------------
# DataProcessor.py
# ---------------------------
import polars as pl
from datetime import datetime

class DataProcessor:
    def __init__(self, filename: str):
        self.filename = filename
        self.df_lazy = self._read_csv()

        self._returns_only = None
        self._sales_only = None

    # -------------------------
    # Load CSV + parse dates robustly
    # -------------------------
    def _read_csv(self):
        # Lazy scan with correct schema
        df_scan = pl.scan_csv(
            self.filename,
            schema_overrides={
                "InvoiceNo": pl.Utf8,   # <- treat InvoiceNo as string
                "CustomerID": pl.Utf8    # <- treat CustomerID as string
            },
            encoding="utf8-lossy",
            ignore_errors=True,         # ignore rows with parsing issues
        ).drop_nulls()
    
        # Parse InvoiceDate robustly
        invoice_dates = df_scan.select("InvoiceDate").collect()["InvoiceDate"].to_list()
        parsed_dates = []
        for val in invoice_dates:
            dt = None
            for fmt in ("%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M:%S"):
                try:
                    dt = datetime.strptime(val, fmt)
                    break
                except:
                    continue
            parsed_dates.append(dt)
    
        dates_df = pl.DataFrame({
            "InvoiceDate": invoice_dates,
            "DateTime": parsed_dates,
            "Date": [d.date() if d else None for d in parsed_dates]
        })
    
        df_lazy = df_scan.join(dates_df.lazy(), on="InvoiceDate", how="left")

    
        # Compute Revenue
        df_lazy = df_lazy.with_columns((pl.col("Quantity") * pl.col("UnitPrice")).alias("Revenue"))
    
        return df_lazy


    # -------------------------
    # Unique countries
    # -------------------------
    @property
    def unique_countries(self):
        return (
            self.df_lazy.select(pl.col("Country").unique())
            .sort("Country")
            .collect()
            .to_series()
            .to_list()
        )

    # -------------------------
    # Unique products for a country
    # -------------------------
    def unique_products(self, country: str):
        return (
            self.sales_only
            .filter(pl.col("Country") == country)
            .select("Description")
            .unique()
            .sort("Description")
            .collect()
            .to_series()
            .to_list()
        )

    # -------------------------
    # Returns dataset
    # -------------------------
    @property
    def returns_only(self):
        if self._returns_only is None:
            self._returns_only = (
                self.df_lazy
                .filter(pl.col("InvoiceNo").str.starts_with("C"))
                .with_columns(
                    pl.col("InvoiceNo").str.replace("^C", "").alias("OriginalInvoiceNo")
                )
                .select(["OriginalInvoiceNo", "StockCode", "CustomerID"])
            )
        return self._returns_only

    # -------------------------
    # Sales excluding returns
    # -------------------------
    @property
    def sales_only(self):
        if self._sales_only is None:
            self._sales_only = (
                self.df_lazy
                .filter(~pl.col("InvoiceNo").str.starts_with("C"))
                .join(
                    self.returns_only,
                    left_on=["InvoiceNo", "StockCode", "CustomerID"],
                    right_on=["OriginalInvoiceNo", "StockCode", "CustomerID"],
                    how="anti"
                )
            )
        return self._sales_only

    # -------------------------
    # Revenue per article per country
    # -------------------------
    def revenue_per_article_per_country(self, country: str):
        return (
            self.sales_only
            .filter(pl.col("Country") == country)
            .group_by("Description")
            .agg(pl.col("Revenue").sum())
            .sort("Revenue", descending=True)
            .head(15)
            .collect()
        )

    # -------------------------
    # Total revenue vs country
    # -------------------------
    def total_revenue_vs_country(self, country: str):
        total_revenue = self.df_lazy.select(pl.col("Revenue").sum()).collect().to_series()[0]
        country_revenue = self.df_lazy.filter(pl.col("Country") == country).select(pl.col("Revenue").sum()).collect().to_series()[0]
        country_perc = f"{(country_revenue / total_revenue * 100):.2f}"

        df = pl.DataFrame({
            "Country": ["Total Global Revenue", country],
            "Revenue": [total_revenue, country_revenue],
            "% of total": ["", country_perc]
        })
        return df.to_pandas()

    # -------------------------
    # Quantity per article for country
    # -------------------------
    def get_country_quantity(self, country: str, product: str):
        return (
            self.sales_only
            .filter((pl.col("Country") == country) & (pl.col("Description") == product))
            .group_by("Date")
            .agg(pl.col("Quantity").sum().alias("Quantity"))
            .sort("Date")
            .collect()
        )

    # -------------------------
    # Quantity per article for global (excluding selected country)
    # -------------------------
    def get_global_quantity(self, country: str, product: str):
        country_set = str(country)
        country_df = self.get_country_quantity(country, product)
        country_dates = country_df.select("Date").to_series().to_list()

        return (
            self.sales_only
            .filter((pl.col("Country") != country_set) & (pl.col("Description") == product) & pl.col("Date").is_in(country_dates))
            .group_by("Date")
            .agg(pl.col("Quantity").sum().alias("Quantity"))
            .sort("Date")
            .collect()
        )
