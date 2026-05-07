import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import snowflake.connector

from dotenv import load_dotenv


load_dotenv()


def create_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )


def load_data(query):
    connection = create_snowflake_connection()
    dataframe = pd.read_sql(query, connection)
    connection.close()
    return dataframe


def clean_plotly_chart(chart, height=420):
    chart.update_layout(
        template="plotly_white",
        height=height,
        font=dict(size=13, color="#111827"),
        title_font=dict(size=19, color="#111827"),
        paper_bgcolor="white",
        plot_bgcolor="white",
        margin=dict(l=30, r=30, t=60, b=40),
        showlegend=True
    )
    return chart


st.set_page_config(
    page_title="Nashville Urban Operations Dashboard",
    layout="wide"
)


st.markdown(
    """
    <style>
    .stApp {
        background-color: #f8fafc;
        color: #111827;
    }

    .block-container {
        padding-top: 1.5rem;
        padding-left: 2.5rem;
        padding-right: 2.5rem;
    }

    h1, h2, h3, p, label {
        color: #111827 !important;
    }

    [data-testid="stMetric"] {
        background-color: white;
        padding: 18px;
        border-radius: 14px;
        border: 1px solid #e5e7eb;
        box-shadow: 0 2px 8px rgba(0,0,0,0.05);
    }

    [data-testid="stMetricLabel"] {
        color: #6b7280 !important;
        font-size: 14px;
    }

    [data-testid="stMetricValue"] {
        color: #111827 !important;
        font-size: 25px;
        font-weight: 700;
    }

    .dashboard-title {
        font-size: 34px;
        font-weight: 800;
        color: #111827;
        margin-bottom: 2px;
    }

    .dashboard-subtitle {
        font-size: 15px;
        color: #6b7280;
        margin-bottom: 22px;
    }
    </style>
    """,
    unsafe_allow_html=True
)


requests_by_type = load_data("""
SELECT REQUEST_TYPE, TOTAL_REQUESTS
FROM GOLD_311_REQUESTS_BY_TYPE
WHERE REQUEST_TYPE IS NOT NULL
ORDER BY TOTAL_REQUESTS DESC
LIMIT 6;
""")

requests_by_status = load_data("""
SELECT STATUS, TOTAL_REQUESTS
FROM GOLD_311_REQUESTS_BY_STATUS
WHERE STATUS IS NOT NULL
ORDER BY TOTAL_REQUESTS DESC
LIMIT 5;
""")

requests_by_year = load_data("""
SELECT REQUEST_YEAR, TOTAL_REQUESTS
FROM GOLD_311_REQUESTS_BY_YEAR
WHERE REQUEST_YEAR IS NOT NULL
ORDER BY REQUEST_YEAR;
""")

property_violations_by_city = load_data("""
SELECT CITY, TOTAL_REQUESTS
FROM GOLD_PROPERTY_VIOLATIONS_BY_CITY
WHERE CITY IS NOT NULL
  AND TRIM(CITY) <> ''
ORDER BY TOTAL_REQUESTS DESC
LIMIT 6;
""")

property_violations_by_type = load_data("""
SELECT SUBREQUEST_TYPE, TOTAL_REQUESTS
FROM GOLD_PROPERTY_VIOLATIONS_BY_SUBREQUEST_TYPE
WHERE SUBREQUEST_TYPE IS NOT NULL
  AND TRIM(SUBREQUEST_TYPE) <> ''
ORDER BY TOTAL_REQUESTS DESC
LIMIT 6;
""")

housing_by_city = load_data("""
SELECT PROPERTY_CITY, TOTAL_PROPERTIES, AVERAGE_APPRAISED_VALUE
FROM GOLD_HOUSING_BY_CITY
WHERE PROPERTY_CITY IS NOT NULL
  AND TRIM(PROPERTY_CITY) <> ''
ORDER BY TOTAL_PROPERTIES DESC
LIMIT 6;
""")

housing_by_land_use = load_data("""
SELECT LAND_USE_DESCRIPTION, TOTAL_PROPERTIES, AVERAGE_APPRAISED_VALUE
FROM GOLD_HOUSING_BY_LAND_USE
WHERE LAND_USE_DESCRIPTION IS NOT NULL
  AND TRIM(LAND_USE_DESCRIPTION) <> ''
ORDER BY TOTAL_PROPERTIES DESC
LIMIT 6;
""")

housing_value_by_city = load_data("""
SELECT PROPERTY_CITY, TOTAL_PROPERTIES, AVERAGE_APPRAISED_VALUE
FROM GOLD_HOUSING_BY_CITY
WHERE PROPERTY_CITY IS NOT NULL
  AND TRIM(PROPERTY_CITY) <> ''
ORDER BY AVERAGE_APPRAISED_VALUE DESC
LIMIT 6;
""")


year_options = requests_by_year["REQUEST_YEAR"].astype(int).tolist()

st.sidebar.header("Filters")

selected_year = st.sidebar.selectbox(
    "Year",
    year_options
)

st.sidebar.caption("Showing Gold layer analytics only.")


selected_year_row = requests_by_year[
    requests_by_year["REQUEST_YEAR"].astype(int) == selected_year
]

selected_year_requests = (
    int(selected_year_row["TOTAL_REQUESTS"].iloc[0])
    if not selected_year_row.empty
    else 0
)

total_requests = int(requests_by_year["TOTAL_REQUESTS"].sum())
top_request_type = requests_by_type.iloc[0]["REQUEST_TYPE"]
top_violation_city = property_violations_by_city.iloc[0]["CITY"]
top_property_city = housing_by_city.iloc[0]["PROPERTY_CITY"]


st.markdown(
    "<div class='dashboard-title'>Nashville Urban Operations Dashboard</div>",
    unsafe_allow_html=True
)

st.markdown(
    "<div class='dashboard-subtitle'>Focused analytics for 311 requests, property violations, and housing activity.</div>",
    unsafe_allow_html=True
)


metric_one, metric_two, metric_three, metric_four = st.columns(4)

metric_one.metric(
    f"311 Requests in {selected_year}",
    f"{selected_year_requests:,}"
)

metric_two.metric(
    "All 311 Requests",
    f"{total_requests:,}"
)

metric_three.metric(
    "Top Request Category",
    top_request_type
)

metric_four.metric(
    "Largest Property City",
    top_property_city
)


tab_summary, tab_operations, tab_property, tab_housing = st.tabs(
    [
        "Executive Summary",
        "311 Operations",
        "Property Violations",
        "Housing Market"
    ]
)


with tab_summary:
    left_column, right_column = st.columns(2)

    with left_column:
        summary_bar = px.bar(
            requests_by_type,
            x="REQUEST_TYPE",
            y="TOTAL_REQUESTS",
            text="TOTAL_REQUESTS",
            title="Highest Volume 311 Categories",
            color="TOTAL_REQUESTS",
            color_continuous_scale="Blues"
        )

        summary_bar.update_traces(textposition="outside")

        summary_bar.update_layout(
            xaxis_title="",
            yaxis_title="Requests",
            showlegend=False
        )

        st.plotly_chart(
            clean_plotly_chart(summary_bar),
            use_container_width=True,
            key="summary_request_categories"
        )

    with right_column:
        housing_bubble = px.scatter(
            housing_by_city,
            x="TOTAL_PROPERTIES",
            y="AVERAGE_APPRAISED_VALUE",
            size="TOTAL_PROPERTIES",
            color="PROPERTY_CITY",
            hover_name="PROPERTY_CITY",
            title="Property Concentration vs. Appraised Value"
        )

        housing_bubble.update_layout(
            xaxis_title="Total Properties",
            yaxis_title="Average Appraised Value"
        )

        st.plotly_chart(
            clean_plotly_chart(housing_bubble),
            use_container_width=True,
            key="summary_housing_bubble"
        )

    trend_chart = px.area(
        requests_by_year,
        x="REQUEST_YEAR",
        y="TOTAL_REQUESTS",
        title="311 Request Volume by Year",
        markers=True
    )

    trend_chart.update_traces(
        line_color="#2563eb",
        fillcolor="rgba(37, 99, 235, 0.18)"
    )

    trend_chart.update_layout(
        xaxis_title="Year",
        yaxis_title="Total Requests"
    )

    st.plotly_chart(
        clean_plotly_chart(trend_chart, height=360),
        use_container_width=True,
        key="summary_year_trend"
    )


with tab_operations:
    left_column, right_column = st.columns([1.4, 1])

    with left_column:
        category_chart = px.treemap(
            requests_by_type,
            path=["REQUEST_TYPE"],
            values="TOTAL_REQUESTS",
            title="311 Request Category Share",
            color="TOTAL_REQUESTS",
            color_continuous_scale="Blues"
        )

        st.plotly_chart(
            clean_plotly_chart(category_chart, height=500),
            use_container_width=True,
            key="operations_treemap"
        )

    with right_column:
        status_chart = px.pie(
            requests_by_status,
            names="STATUS",
            values="TOTAL_REQUESTS",
            hole=0.55,
            title="Request Status Distribution"
        )

        status_chart.update_traces(
            textposition="inside",
            textinfo="percent+label"
        )

        st.plotly_chart(
            clean_plotly_chart(status_chart, height=500),
            use_container_width=True,
            key="operations_status_donut"
        )


with tab_property:
    metric_a, metric_b = st.columns(2)

    metric_a.metric(
        "Top Violation City",
        top_violation_city
    )

    metric_b.metric(
        "Top Violation Type",
        property_violations_by_type.iloc[0]["SUBREQUEST_TYPE"]
    )

    left_column, right_column = st.columns(2)

    with left_column:
        violation_city_chart = px.bar(
            property_violations_by_city,
            x="CITY",
            y="TOTAL_REQUESTS",
            text="TOTAL_REQUESTS",
            title="Property Violations by City",
            color="TOTAL_REQUESTS",
            color_continuous_scale="Reds"
        )

        violation_city_chart.update_traces(textposition="outside")

        violation_city_chart.update_layout(
            xaxis_title="",
            yaxis_title="Violations",
            showlegend=False
        )

        st.plotly_chart(
            clean_plotly_chart(violation_city_chart),
            use_container_width=True,
            key="property_city_chart"
        )

    with right_column:
        violation_type_chart = px.bar(
            property_violations_by_type,
            x="SUBREQUEST_TYPE",
            y="TOTAL_REQUESTS",
            text="TOTAL_REQUESTS",
            title="Property Violation Types",
            color="TOTAL_REQUESTS",
            color_continuous_scale="Oranges"
        )

        violation_type_chart.update_traces(textposition="outside")

        violation_type_chart.update_layout(
            xaxis_title="",
            yaxis_title="Violations",
            showlegend=False
        )

        st.plotly_chart(
            clean_plotly_chart(violation_type_chart),
            use_container_width=True,
            key="property_type_chart"
        )


with tab_housing:
    left_column, right_column = st.columns(2)

    with left_column:
        land_use_chart = px.treemap(
            housing_by_land_use,
            path=["LAND_USE_DESCRIPTION"],
            values="TOTAL_PROPERTIES",
            title="Property Land Use Composition",
            color="TOTAL_PROPERTIES",
            color_continuous_scale="Greens"
        )

        st.plotly_chart(
            clean_plotly_chart(land_use_chart, height=500),
            use_container_width=True,
            key="housing_land_use_treemap"
        )

    with right_column:
        value_chart = px.bar(
            housing_value_by_city,
            x="PROPERTY_CITY",
            y="AVERAGE_APPRAISED_VALUE",
            text="AVERAGE_APPRAISED_VALUE",
            title="Average Appraised Value by City",
            color="AVERAGE_APPRAISED_VALUE",
            color_continuous_scale="Purples"
        )

        value_chart.update_traces(
            texttemplate="$%{text:,.0f}",
            textposition="outside"
        )

        value_chart.update_layout(
            xaxis_title="",
            yaxis_title="Average Appraised Value",
            showlegend=False
        )

        st.plotly_chart(
            clean_plotly_chart(value_chart, height=500),
            use_container_width=True,
            key="housing_value_chart"
        )

    with st.expander("View Gold Table Samples"):
        st.dataframe(requests_by_type, use_container_width=True)
        st.dataframe(property_violations_by_city, use_container_width=True)
        st.dataframe(housing_by_city, use_container_width=True)
        