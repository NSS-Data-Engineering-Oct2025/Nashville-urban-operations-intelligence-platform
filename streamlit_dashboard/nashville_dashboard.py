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


def safe_sql(value):
    return value.replace("'", "''")


def build_bar_chart(dataframe, x_column, y_column, title, color):
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
        texttemplate="%{text:,.0f}",
        textposition="outside",
        textfont=dict(size=14, color="#111827"),
        cliponaxis=False
    )

    chart.update_layout(
        height=410,
        paper_bgcolor="rgba(255,255,255,0.96)",
        plot_bgcolor="rgba(255,255,255,0.96)",
        font=dict(size=14, color="#111827"),
        title_font=dict(size=21, color="#111827"),
        margin=dict(l=30, r=100, t=65, b=35),
        xaxis=dict(title="", tickfont=dict(size=13, color="#111827"), gridcolor="#e5e7eb"),
        yaxis=dict(title="", tickfont=dict(size=14, color="#111827")),
        showlegend=False
    )

    return chart


def metric_card(label, value, note):
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
            <div class="metric-note">{note}</div>
        </div>
        """,
        unsafe_allow_html=True
    )


st.set_page_config(
    page_title="Nashville Urban Operations Dashboard",
    layout="wide"
)


st.markdown(
    """
    <style>
    .stApp {
        background:
            radial-gradient(circle at top left, rgba(59,130,246,0.08), transparent 28%),
            radial-gradient(circle at bottom right, rgba(124,58,237,0.08), transparent 30%),
            linear-gradient(180deg, #f8fafc 0%, #eef2f7 100%) !important;
        color: #111827 !important;
    }

    .block-container {
        padding-top: 3rem;
        padding-left: 2rem;
        padding-right: 2rem;
    }

    section[data-testid="stSidebar"] {
        background: #ffffff !important;
        border-right: 1px solid #dbe3ec;
        box-shadow: 4px 0 18px rgba(15, 23, 42, 0.05);
    }

    section[data-testid="stSidebar"] * {
        color: #111827 !important;
    }

    .title {
        font-size: 42px;
        font-weight: 900;
        color: #0f172a !important;
        margin-bottom: 10px;
    }

    .subtitle {
        font-size: 16px;
        color: #475569 !important;
        margin-bottom: 28px;
    }

    .metric-card {
        background: rgba(255,255,255,0.95);
        border: 1px solid #dce4ee;
        border-radius: 20px;
        padding: 24px;
        box-shadow: 0 10px 24px rgba(15,23,42,0.05);
        min-height: 125px;
    }

    .metric-label {
        font-size: 14px;
        font-weight: 700;
        color: #64748b !important;
        margin-bottom: 10px;
    }

    .metric-value {
        font-size: 32px;
        font-weight: 900;
        color: #0f172a !important;
    }

    .metric-note {
        font-size: 13px;
        color: #64748b !important;
        margin-top: 8px;
    }

    div[data-testid="stPlotlyChart"] {
        background: rgba(255,255,255,0.96);
        border: 1px solid #dce4ee;
        border-radius: 20px;
        padding: 12px;
        box-shadow: 0 8px 22px rgba(15,23,42,0.05);
    }

    div[data-baseweb="select"] > div {
        background: #ffffff !important;
        border: 1px solid #cbd5e1 !important;
        border-radius: 12px !important;
        min-height: 48px !important;
    }

    div[data-baseweb="select"] span,
    div[data-baseweb="select"] input {
        color: #111827 !important;
        background-color: #ffffff !important;
    }

    div[data-baseweb="popover"] {
        background-color: #ffffff !important;
    }

    ul {
        background: #ffffff !important;
        border-radius: 14px !important;
        border: 1px solid #dce4ee !important;
        box-shadow: 0 12px 30px rgba(15,23,42,0.12) !important;
    }

    li {
        background: #ffffff !important;
        color: #111827 !important;
        font-size: 15px !important;
        font-weight: 500 !important;
        padding: 12px 16px !important;
    }

    li:hover {
        background: #eff6ff !important;
        color: #2563eb !important;
    }

    li[aria-selected="true"] {
        background: #dbeafe !important;
        color: #1d4ed8 !important;
        font-weight: 700 !important;
    }

    header {
        background: transparent !important;
    }
    </style>
    """,
    unsafe_allow_html=True
)


city_summary = load_data("""
SELECT
    CITY,
    TOTAL_311_REQUESTS,
    TOTAL_PROPERTY_VIOLATIONS,
    TOTAL_PROPERTIES,
    AVERAGE_APPRAISED_VALUE
FROM GOLD_CITY_OPERATIONS_SUMMARY
ORDER BY TOTAL_311_REQUESTS DESC;
""")

city_options = ["ALL CITIES"] + city_summary["CITY"].dropna().tolist()

st.sidebar.markdown("## Filters")

selected_city = st.sidebar.selectbox(
    "City",
    city_options,
    index=0
)

st.sidebar.markdown("---")
st.sidebar.markdown("## Dashboard")
st.sidebar.markdown("311 Service Requests")
st.sidebar.markdown("Property Appraisals")
st.sidebar.markdown("Housing Analytics")


if selected_city == "ALL CITIES":
    selected_summary = pd.DataFrame({
        "TOTAL_311_REQUESTS": [city_summary["TOTAL_311_REQUESTS"].sum()],
        "TOTAL_PROPERTY_VIOLATIONS": [city_summary["TOTAL_PROPERTY_VIOLATIONS"].sum()],
        "TOTAL_PROPERTIES": [city_summary["TOTAL_PROPERTIES"].sum()],
        "AVERAGE_APPRAISED_VALUE": [city_summary["AVERAGE_APPRAISED_VALUE"].mean()]
    })

    requests_by_type_query = """
    SELECT REQUEST_TYPE, TOTAL_REQUESTS
    FROM GOLD_311_REQUESTS_BY_TYPE
    ORDER BY TOTAL_REQUESTS DESC
    LIMIT 7;
    """

    property_appraisals_query = """
    SELECT PROPERTY_CITY, AVERAGE_APPRAISED_VALUE
    FROM GOLD_HOUSING_BY_CITY
    ORDER BY AVERAGE_APPRAISED_VALUE DESC
    LIMIT 7;
    """

    housing_land_use_query = """
    SELECT LAND_USE_DESCRIPTION, TOTAL_PROPERTIES
    FROM GOLD_HOUSING_BY_LAND_USE
    ORDER BY TOTAL_PROPERTIES DESC
    LIMIT 7;
    """

    city_comparison_query = """
    SELECT CITY, TOTAL_311_REQUESTS
    FROM GOLD_CITY_OPERATIONS_SUMMARY
    ORDER BY TOTAL_311_REQUESTS DESC
    LIMIT 10;
    """

else:
    city = safe_sql(selected_city)

    selected_summary = city_summary[city_summary["CITY"] == selected_city]

    requests_by_type_query = f"""
    SELECT REQUEST_TYPE, SUM(TOTAL_REQUESTS) AS TOTAL_REQUESTS
    FROM GOLD_311_REQUESTS_BY_CITY_TYPE
    WHERE CITY = '{city}'
    GROUP BY REQUEST_TYPE
    ORDER BY TOTAL_REQUESTS DESC
    LIMIT 7;
    """

    property_appraisals_query = f"""
    SELECT PROPERTY_CITY, AVERAGE_APPRAISED_VALUE
    FROM GOLD_HOUSING_BY_CITY
    WHERE PROPERTY_CITY = '{city}'
    ORDER BY AVERAGE_APPRAISED_VALUE DESC;
    """

    housing_land_use_query = f"""
    SELECT LAND_USE_DESCRIPTION, SUM(TOTAL_PROPERTIES) AS TOTAL_PROPERTIES
    FROM GOLD_HOUSING_BY_CITY_LAND_USE
    WHERE PROPERTY_CITY = '{city}'
    GROUP BY LAND_USE_DESCRIPTION
    ORDER BY TOTAL_PROPERTIES DESC
    LIMIT 7;
    """

    city_comparison_query = f"""
    SELECT CITY, TOTAL_311_REQUESTS
    FROM GOLD_CITY_OPERATIONS_SUMMARY
    WHERE CITY = '{city}';
    """


requests_by_type = load_data(requests_by_type_query)
property_appraisals = load_data(property_appraisals_query)
housing_land_use = load_data(housing_land_use_query)
city_comparison = load_data(city_comparison_query)


total_requests = int(selected_summary["TOTAL_311_REQUESTS"].iloc[0]) if not selected_summary.empty else 0
total_property_violations = int(selected_summary["TOTAL_PROPERTY_VIOLATIONS"].iloc[0]) if not selected_summary.empty else 0
total_properties = int(selected_summary["TOTAL_PROPERTIES"].iloc[0]) if not selected_summary.empty else 0
average_appraised_value = float(selected_summary["AVERAGE_APPRAISED_VALUE"].iloc[0]) if not selected_summary.empty else 0


st.markdown("<div class='title'>Nashville Urban Operations Dashboard</div>", unsafe_allow_html=True)
st.markdown(
    f"<div class='subtitle'>City operations summary for <b>{selected_city}</b></div>",
    unsafe_allow_html=True
)


metric_one, metric_two, metric_three, metric_four = st.columns(4)

with metric_one:
    metric_card(
        "311 Requests",
        f"{total_requests:,}",
        "Service request activity"
    )

with metric_two:
    metric_card(
        "Property Violations",
        f"{total_property_violations:,}",
        "Property-related activity"
    )

with metric_three:
    metric_card(
        "Property Records",
        f"{total_properties:,}",
        "Housing/property records"
    )

with metric_four:
    metric_card(
        "Average Appraised Value",
        f"${average_appraised_value:,.0f}",
        "Property valuation average"
    )


st.markdown("## Operations Summary")

top_left, top_right = st.columns(2)

with top_left:
    if requests_by_type.empty:
        st.info("No 311 request data available for this city.")
    else:
        st.plotly_chart(
            build_bar_chart(
                requests_by_type,
                "TOTAL_REQUESTS",
                "REQUEST_TYPE",
                "Top 311 Request Categories",
                "#2563eb"
            ),
            use_container_width=True
        )

with top_right:
    if property_appraisals.empty:
        st.info("No property appraisal data available.")
    else:
        st.plotly_chart(
            build_bar_chart(
                property_appraisals,
                "AVERAGE_APPRAISED_VALUE",
                "PROPERTY_CITY",
                "Average Property Appraisals",
                "#059669"
            ),
            use_container_width=True
        )


st.markdown("## Housing and City Analytics")

bottom_left, bottom_right = st.columns(2)

with bottom_left:
    if city_comparison.empty:
        st.info("No city comparison data available.")
    else:
        st.plotly_chart(
            build_bar_chart(
                city_comparison,
                "TOTAL_311_REQUESTS",
                "CITY",
                "311 Requests by City",
                "#dc2626"
            ),
            use_container_width=True
        )

with bottom_right:
    if housing_land_use.empty:
        st.info("No housing land use data available.")
    else:
        st.plotly_chart(
            build_bar_chart(
                housing_land_use,
                "TOTAL_PROPERTIES",
                "LAND_USE_DESCRIPTION",
                "Housing Land Use",
                "#7c3aed"
            ),
            use_container_width=True
        )


with st.expander("View filtered data"):
    st.write("City Summary")
    st.dataframe(selected_summary, use_container_width=True)

    st.write("311 Request Categories")
    st.dataframe(requests_by_type, use_container_width=True)

    st.write("Property Appraisals")
    st.dataframe(property_appraisals, use_container_width=True)

    st.write("Housing Land Use")
    st.dataframe(housing_land_use, use_container_width=True)