import streamlit as st
import altair as alt
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# ---------------------------
# Dashboard Class
# ---------------------------
class Dashboard:
    def __init__(self, processor):
        self.processor = processor
        self.selected_country = None
        self.tab1_selected_article = None

    # ---------------------------
    # Page config
    # ---------------------------
    def initialize_dashboard(self):
        st.set_page_config(
            page_title="Ecommerce Dashboard",
            page_icon="📊",
            layout="wide",
            initial_sidebar_state="expanded",
        )

    # ---------------------------
    # Main header + sidebar
    # ---------------------------
    def main_comp(self):
        st.title("Dashboard E-commerce")
        self.countries = sorted(self.processor.unique_countries)
        self.selected_country = st.sidebar.selectbox(
            "Select Country", options=self.countries
        )

    # ---------------------------
    # Tabs
    # ---------------------------
    def sub_tabs(self):
        tab1, tab2 = st.tabs(["Article Information", "Sales"])

        with tab1:
            if not self.selected_country:
                return

            st.subheader(f"Top Articles in {self.selected_country}")

            col1, col2 = st.columns(2)

            # ---------------------
            # Column 1: Bar chart
            # ---------------------
            with col1:
                data = self.processor.revenue_per_article_per_country(self.selected_country)
                data = data.with_columns(
                    data["Description"].str.slice(0, 40).alias("DescShort")
                )

                chart = (
                    alt.Chart(data.to_pandas())
                    .mark_bar()
                    .encode(
                        x=alt.X("Revenue:Q", title="Revenue (USD)"),
                        y=alt.Y("DescShort:N", sort="-x", title="Product"),
                        tooltip=["Description", alt.Tooltip("Revenue:Q", format=",.2f")],
                    )
                    .properties(height=350)
                    .configure_axis(labelFontSize=10, titleFontSize=12)
                    .configure_title(fontSize=14)
                )
                st.altair_chart(chart, use_container_width=True)

                # Show total revenue dataframe
                total_revenue = self.processor.total_revenue_vs_country(self.selected_country)
                st.dataframe(total_revenue)

            # ---------------------
            # Column 2: World map highlighting the selected country
            # ---------------------
            with col2:
                df_map = pd.DataFrame({
                    "country": self.countries,
                    "highlight": [1 if c == self.selected_country else 0 for c in self.countries]
                })
                fig_map = px.choropleth(
                    df_map,
                    locations="country",
                    locationmode="country names",
                    color="highlight",
                    color_continuous_scale=["lightgray", "blue"],
                    range_color=[0, 1],
                )
                fig_map.update_geos(
                    visible=False,
                    showcountries=True,
                    fitbounds="locations",
                    projection_type="natural earth"
                )
                fig_map.update_layout(coloraxis_showscale=False, width=700, height=550)
                st.plotly_chart(fig_map, use_container_width=True)

            # ---------------------
            # Full-row: Article Quantity Timeseries
            # ---------------------
            self.tab1_articles = self.processor.unique_products(self.selected_country)
            if self.tab1_articles:
                self.tab1_selected_article = st.selectbox(
                    "Select Article", options=self.tab1_articles
                )

                # Smaller subheader
                st.markdown(
                    f"<h5>Quantity Over Time — {self.tab1_selected_article}</h5>",
                    unsafe_allow_html=True,
                )

                ts_data = self.plot_article_timeseries(
                    self.selected_country, self.tab1_selected_article
                )

                if ts_data["y_max"] > 0:
                    fig_ts = go.Figure()
                    fig_ts.add_trace(
                        go.Scatter(
                            x=ts_data["country_dates"],
                            y=ts_data["country_qty"],
                            mode="lines+markers",
                            name=self.selected_country,
                            line=dict(color="red", width=3),
                            marker=dict(size=6),
                        )
                    )
                    fig_ts.add_trace(
                        go.Scatter(
                            x=ts_data["global_dates"],
                            y=ts_data["global_qty"],
                            mode="lines+markers",
                            name="Global",
                            line=dict(color="black", width=2),
                            marker=dict(size=4),
                        )
                    )
                    fig_ts.update_layout(
                        title=f"{self.selected_country} vs Global Quantity Over Time",
                        xaxis=dict(range=[ts_data["first_date"], ts_data["extended_date"]]),
                        yaxis=dict(range=[-ts_data["y_max"]*0.05, ts_data["y_max"]*1.1]),
                        height=350,
                    )
                    st.plotly_chart(fig_ts, use_container_width=True)
            else:
                st.info("No articles sold in this country.")

        with tab2:
            st.write("Sales analytics coming soon...")

    # ---------------------------
    # Prepare time series data
    # ---------------------------
    def plot_article_timeseries(self, country: str, product: str):
        country_df = self.processor.get_country_quantity(country, product)
        global_df = self.processor.get_global_quantity(country, product)

        def to_datetime_safe(val):
            if val is None:
                return datetime(2000, 1, 1)
            if isinstance(val, datetime):
                return val
            return datetime.combine(val, datetime.min.time())

        country_dates = [to_datetime_safe(d) for d in country_df["Date"].to_list()]
        country_qty = country_df["Quantity"].to_list()

        global_dates = [to_datetime_safe(d) for d in global_df["Date"].to_list()]
        global_qty = global_df["Quantity"].to_list()

        all_dates = country_dates + global_dates
        first_date = min(all_dates) if all_dates else datetime.today()
        last_date = max(all_dates) if all_dates else datetime.today()
        extended_date = last_date + timedelta(days=int((last_date - first_date).days * 0.1))

        y_max = max(country_qty + global_qty) if (country_qty + global_qty) else 0

        return {
            "country_dates": country_dates,
            "country_qty": country_qty,
            "global_dates": global_dates,
            "global_qty": global_qty,
            "first_date": first_date,
            "extended_date": extended_date,
            "y_max": y_max,
        }

    # ---------------------------
    # Run
    # ---------------------------
    def run(self):
        self.initialize_dashboard()
        self.main_comp()
        self.sub_tabs()


# ---------------------------
# Main
# ---------------------------
def main():
    from DataProcessor import DataProcessor  # your preprocessing class
    filename = r"C:\Users\jarmo\Documents\Business\Projects\UK ECommerce Data\data\data.csv"
    processor = DataProcessor(filename)
    dashboard = Dashboard(processor)
    dashboard.run()


if __name__ == "__main__":
    main()
