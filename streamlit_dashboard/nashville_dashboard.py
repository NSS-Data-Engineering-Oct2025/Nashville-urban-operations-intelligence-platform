import os
import pandas as pd
import plotly.express as px
import streamlit as st
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


def create_connection():
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
    connection = create_connection()
    dataframe = pd.read_sql(query, connection)
    connection.close()
    return dataframe


def safe_city(city):
    return city.replace("'", "''")


def bar_chart(dataframe, x_column, y_column, title, x_title, color):
    chart = px.bar(
        dataframe.sort_values(x_column),
        x=x_column,
        y=y_column,
        orientation="h",
        text=x_column,
        title=title,
        template="plotly_white"
    )

    chart.update_traces(
        marker_color=color,
        textposition="outside",
        texttemplate="%{text:,}",
        textfont=dict(
            size=15,
            color="#111827"
        ),
        cliponaxis=False
    )

    chart.update_layout(
        height=430,
        font=dict(
            color="#111827",
            size=15
        ),
        title_font=dict(
            color="#111827",
            size=22
        ),
        paper_bgcolor="white",
        plot_bgcolor="white",
        xaxis=dict(
            title=x_title,
            title_font=dict(size=16, color="#111827"),
            tickfont=dict(size=14, color="#111827"),
            gridcolor="#e5e7eb",
            zerolinecolor="#9ca3af"
        ),
        yaxis=dict(
            title="",
            tickfont=dict(size=15, color="#111827")
        ),
        margin=dict(l=40, r=90, t=70, b=45),
        showlegend=False
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
        background-color: #f4f7fb !important;
        color: #111827 !important;
    }

    .block-container {
        padding-top: 1rem;
        padding-left: 2rem;
        padding-right: 2rem;
    }

    /* SIDEBAR */

    [data-testid="stSidebar"] {
        background-color: #ffffff !important;
        border-right: 1px solid #e5e7eb;
    }

    [data-testid="stSidebar"] * {
        color: #111827 !important;
    }

    [data-testid="stSidebarNav"] {
        background: #ffffff !important;
    }

    /* TITLE */

    .title {
        font-size: 34px;
        font-weight: 800;
        color: #111827 !important;
        margin-bottom: 6px;
    }

    .subtitle {
        font-size: 16px;
        color: #4b5563 !important;
        margin-bottom: 24px;
    }

    /* METRIC CARDS */

    .metric-card {
        background: white;
        border: 1px solid #e5e7eb;
        border-radius: 18px;
        padding: 22px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.05);
        min-height: 120px;
    }

    .metric-label {
        font-size: 14px;
        font-weight: 700;
        color: #4b5563 !important;
        margin-bottom: 8px;
    }

    .metric-value {
        font-size: 30px;
        font-weight: 800;
        color: #111827 !important;
    }

    .metric-note {
        font-size: 13px;
        color: #6b7280 !important;
        margin-top: 6px;
    }

    /* CHARTS */

    div[data-testid="stPlotlyChart"] {
        background: white;
        border-radius: 18px;
        border: 1px solid #e5e7eb;
        padding: 10px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.05);
    }

    /* SELECT BOX */

    .stSelectbox label {
        font-size: 15px !important;
        font-weight: 700 !important;
        color: #111827 !important;
    }

    div[data-baseweb="select"] {
        background-color: white !important;
        border-radius: 12px !important;
        border: 1px solid #d1d5db !important;
    }

    div[data-baseweb="select"] * {
        color: #111827 !important;
        background-color: white !important;
    }

    /* REMOVE DARK COLORS */

    header {
        background: #f4f7fb !important;
    }

    /* TABS */

    button[data-baseweb="tab"] {
        color: #374151 !important;
        font-weight: 600 !important;
    }

    button[data-baseweb="tab"][aria-selected="true"] {
        color: #2563eb !important;
    }

    </style>
    """,
    unsafe_allow_html=True
)


city_options_query = """
SELECT CITY FROM GOLD_311_REQUESTS_BY_CITY_TYPE
UNION
SELECT CITY FROM GOLD_PROPERTY_VIOLATIONS_BY_CITY
UNION
SELECT PROPERTY_CITY AS CITY FROM GOLD_HOUSING_BY_CITY
ORDER BY CITY;
"""

city_options_dataframe = load_data(city_options_query)

city_options = ["All Cities"] + city_options_dataframe["CITY"].dropna().tolist()

st.sidebar.markdown("## Filters")

selected_city = st.sidebar.selectbox(
    "Select City",
    city_options
)

st.sidebar.markdown("---")

st.sidebar.markdown("## Focus")

st.sidebar.markdown("311 Requests")
st.sidebar.markdown("Property Violations")
st.sidebar.markdown("Housing / Property")


if selected_city == "All Cities":

    requests_by_type_query = """
    SELECT REQUEST_TYPE, TOTAL_REQUESTS
    FROM GOLD_311_REQUESTS_BY_TYPE
    ORDER BY TOTAL_REQUESTS DESC
    LIMIT 6;
    """

    requests_by_status_query = """
    SELECT STATUS, TOTAL_REQUESTS
    FROM GOLD_311_REQUESTS_BY_STATUS
    ORDER BY TOTAL_REQUESTS DESC
    LIMIT 6;
    """

    property_violations_query = """
    SELECT CITY, TOTAL_REQUESTS
    FROM GOLD_PROPERTY_VIOLATIONS_BY_CITY
    ORDER BY TOTAL_REQUESTS DESC
    LIMIT 6;
    """

    property_violations_type_query = """
    SELECT SUBREQUEST_TYPE, TOTAL_REQUESTS
    FROM GOLD_PROPERTY_VIOLATIONS_BY_SUBREQUEST_TYPE
    ORDER BY TOTAL_REQUESTS DESC
    LIMIT 6;
    """

    housing_city_query = """
    SELECT PROPERTY_CITY, TOTAL_PROPERTIES, AVERAGE_APPRAISED_VALUE
    FROM GOLD_HOUSING_BY_CITY
    ORDER BY TOTAL_PROPERTIES DESC
    LIMIT 6;
    """

    housing_land_use_query = """
    SELECT LAND_USE_DESCRIPTION, TOTAL_PROPERTIES, AVERAGE_APPRAISED_VALUE
    FROM GOLD_HOUSING_BY_LAND_USE
    ORDER BY TOTAL_PROPERTIES DESC
    LIMIT 6;
    """

else:

    city = safe_city(selected_city)

    requests_by_type_query = f"""
    SELECT REQUEST_TYPE, SUM(TOTAL_REQUESTS) AS TOTAL_REQUESTS
    FROM GOLD_311_REQUESTS_BY_CITY_TYPE
    WHERE CITY = '{city}'
    GROUP BY REQUEST_TYPE
    ORDER BY TOTAL_REQUESTS DESC
    LIMIT 6;
    """

    requests_by_status_query = f"""
    SELECT STATUS, SUM(TOTAL_REQUESTS) AS TOTAL_REQUESTS
    FROM GOLD_311_REQUESTS_BY_CITY_STATUS
    WHERE CITY = '{city}'
    GROUP BY STATUS
    ORDER BY TOTAL_REQUESTS DESC
    LIMIT 6;
    """

    property_violations_query = f"""
    SELECT CITY, TOTAL_REQUESTS
    FROM GOLD_PROPERTY_VIOLATIONS_BY_CITY
    WHERE CITY = '{city}'
    ORDER BY TOTAL_REQUESTS DESC
    LIMIT 6;
    """

    property_violations_type_query = f"""
    SELECT SUBREQUEST_TYPE, SUM(TOTAL_REQUESTS) AS TOTAL_REQUESTS
    FROM GOLD_PROPERTY_VIOLATIONS_BY_CITY_TYPE
    WHERE CITY = '{city}'
    GROUP BY SUBREQUEST_TYPE
    ORDER BY TOTAL_REQUESTS DESC
    LIMIT 6;
    """

    housing_city_query = f"""
    SELECT PROPERTY_CITY, TOTAL_PROPERTIES, AVERAGE_APPRAISED_VALUE
    FROM GOLD_HOUSING_BY_CITY
    WHERE PROPERTY_CITY = '{city}'
    ORDER BY TOTAL_PROPERTIES DESC
    LIMIT 6;
    """

    housing_land_use_query = f"""
    SELECT LAND_USE_DESCRIPTION,
           SUM(TOTAL_PROPERTIES) AS TOTAL_PROPERTIES,
           AVG(AVERAGE_APPRAISED_VALUE) AS AVERAGE_APPRAISED_VALUE
    FROM GOLD_HOUSING_BY_CITY_LAND_USE
    WHERE PROPERTY_CITY = '{city}'
    GROUP BY LAND_USE_DESCRIPTION
    ORDER BY TOTAL_PROPERTIES DESC
    LIMIT 6;
    """


requests_by_type = load_data(requests_by_type_query)
requests_by_status = load_data(requests_by_status_query)
property_violations = load_data(property_violations_query)
property_violations_type = load_data(property_violations_type_query)
housing_city = load_data(housing_city_query)
housing_land_use = load_data(housing_land_use_query)


total_requests = (
    int(requests_by_type["TOTAL_REQUESTS"].sum())
    if not requests_by_type.empty else 0
)

total_property_violations = (
    int(property_violations["TOTAL_REQUESTS"].sum())
    if not property_violations.empty else 0
)

total_properties = (
    int(housing_city["TOTAL_PROPERTIES"].sum())
    if not housing_city.empty else 0
)

top_land_use = (
    housing_land_use.iloc[0]["LAND_USE_DESCRIPTION"]
    if not housing_land_use.empty else "No data"
)


st.markdown(
    "<div class='title'>Nashville Urban Operations Dashboard</div>",
    unsafe_allow_html=True
)

st.markdown(
    f"<div class='subtitle'>City-level analytics for <b>{selected_city}</b></div>",
    unsafe_allow_html=True
)


metric_one, metric_two, metric_three, metric_four = st.columns(4)

with metric_one:
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">311 Requests</div>
            <div class="metric-value">{total_requests:,}</div>
            <div class="metric-note">Filtered by selected city</div>
        </div>
        """,
        unsafe_allow_html=True
    )

with metric_two:
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">Property Violations</div>
            <div class="metric-value">{total_property_violations:,}</div>
            <div class="metric-note">Code-related request activity</div>
        </div>
        """,
        unsafe_allow_html=True
    )

with metric_three:
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">Properties</div>
            <div class="metric-value">{total_properties:,}</div>
            <div class="metric-note">Housing/property records</div>
        </div>
        """,
        unsafe_allow_html=True
    )

with metric_four:
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">Top Land Use</div>
            <div class="metric-value">{top_land_use[:22]}</div>
            <div class="metric-note">Most common property use</div>
        </div>
        """,
        unsafe_allow_html=True
    )


st.markdown("## Operations Summary")

top_left, top_right = st.columns(2)

with top_left:
    st.plotly_chart(
        bar_chart(
            requests_by_type,
            "TOTAL_REQUESTS",
            "REQUEST_TYPE",
            "Top 311 Request Categories",
            "Requests",
            "#2563eb"
        ),
        use_container_width=True,
        key="request_categories_chart"
    )

with top_right:
    st.plotly_chart(
        bar_chart(
            requests_by_status,
            "TOTAL_REQUESTS",
            "STATUS",
            "311 Request Status",
            "Requests",
            "#059669"
        ),
        use_container_width=True,
        key="request_status_chart"
    )


bottom_left, bottom_right = st.columns(2)

with bottom_left:

    y_column = (
        "CITY"
        if selected_city == "All Cities"
        else "SUBREQUEST_TYPE"
    )

    title = (
        "Top Property Violation Cities"
        if selected_city == "All Cities"
        else "Property Violation Types"
    )

    dataframe = (
        property_violations
        if selected_city == "All Cities"
        else property_violations_type
    )

    st.plotly_chart(
        bar_chart(
            dataframe,
            "TOTAL_REQUESTS",
            y_column,
            title,
            "Violations",
            "#dc2626"
        ),
        use_container_width=True,
        key="property_violations_chart"
    )

with bottom_right:
    st.plotly_chart(
        bar_chart(
            housing_land_use,
            "TOTAL_PROPERTIES",
            "LAND_USE_DESCRIPTION",
            "Top Housing Land Uses",
            "Properties",
            "#7c3aed"
        ),
        use_container_width=True,
        key="housing_land_use_chart"
    )


with st.expander("View Filtered Gold Data"):
    st.dataframe(requests_by_type, use_container_width=True)
    st.dataframe(requests_by_status, use_container_width=True)
    st.dataframe(property_violations, use_container_width=True)
    st.dataframe(housing_land_use, use_container_width=True)