import streamlit as st
import pandas as pd
import snowflake
import snowflake.connector

# Set the title and favicon that appear in the Browser's tab bar.
st.set_page_config(
    page_title='Code Challenge',
    page_icon=':chart_with_upwards_trend:', # This is an emoji shortcode. Could be a URL too.
)

# -----------------------------------------------------------------------------
# Declare some useful functions.

def _get_snowflake_connection():
    credentials = {
"user": "guest_R8FNL6AING1Q",
"password": "IST1100740Jo@o",
"account": "ui76830.west-europe.azure",
"database": "CODE_CHALLENGE_R8FNL6AING1Q",
"schema": "source",
"warehouse": "guest_code_challenge_R8FNL6AING1Q",
"role": "guest_code_challenge_R8FNL6AING1Q",
    }
    return snowflake.connector.connect(**credentials)

@st.cache_data
def fetch_data(query):
    conn = _get_snowflake_connection()
    with conn.cursor() as cur:
        cur.execute(query)
        return pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])


# -----------------------------------------------------------------------------
# Draw the actual page

# Set the title that appears at the top of the page.
'''
# :chart_with_upwards_trend: Stock Market Dashboard

Dashboard made for Code Challenge with three widgets:\n
-Top 10 sectors by position in most recent date available,\n
-Information on the top 25% companies with the largest average position (USD) in the last year\n
-Visualize a given company's stock price time series\n
'''

# Add some spacing
''
''

date = fetch_data("SELECT MAX(date) FROM position")
most_recent_date = date["MAX(DATE)"].values[0]


st.header(f'Top 10 sectors by position in {most_recent_date}', divider='gray')
''
''
query = (
    "SELECT TOP 10 "
    "c.sector_name, "
    "p.date, "
    "SUM(p.shares) AS SECTOR_POSITION "
    "FROM company c "
    "JOIN position p ON c.id = p.company_id "
    "JOIN price m ON (p.company_id = m.company_id AND p.date = m.date) "
    "WHERE m.date = (SELECT MAX(date) FROM price) "
    "GROUP BY c.sector_name, p.date "
    "ORDER BY SECTOR_POSITION DESC;"
)
data = fetch_data(query)
st.bar_chart(data,x="SECTOR_NAME",x_label="Sector Position",y="SECTOR_POSITION", y_label="Sector Name",horizontal=True)

cols = st.columns(4)
for i,sector in enumerate(data['SECTOR_NAME']):
    col = cols[i % len(cols)]
    with col:
        st.metric(
                label=f'{i+1} : {sector}',
                value=f'{data["SECTOR_POSITION"][i]/1000000:,.0f} M',
        )

''
''

st.header(f'Top 25% companies with the largest average position (USD) in the last year', divider='gray')
''
''
ranked_averages_cte = (
    "WITH RankedAverages AS ("
    "SELECT "
    "c.ticker, "
    "c.sector_name, "
    "AVG(m.close_usd * p.shares) AS Average, "
    "NTILE(4) OVER (ORDER BY AVG(m.close_usd * p.shares) DESC) AS Quartile "
    "FROM "
    "price m "
    "JOIN position p "
    "ON m.company_id = p.company_id AND m.date = p.date "
    "JOIN company c "
    "ON m.company_id = c.id "
    "WHERE m.date BETWEEN '2024-01-01' AND '2024-12-31' "
    "GROUP BY c.ticker, c.sector_name"
    "), "
)

daily_info_cte = (
    "DailyInfo AS ("
    "SELECT "
    "c.ticker, "
    "p.shares, "
    "m.close_usd "
    "FROM "
    "price m "
    "JOIN position p "
    "ON m.company_id = p.company_id AND m.date = p.date "
    "JOIN company c "
    "ON m.company_id = c.id "
    "WHERE m.date = (SELECT MAX(date) FROM price) "
    "GROUP BY c.ticker, p.shares, m.close_usd"
    ") "
)

final_query = (
    "SELECT "
    "RankedAverages.ticker, "
    "sector_name, "
    "DailyInfo.shares, "
    "close_usd, "
    "Average "
    "FROM RankedAverages "
    "JOIN DailyInfo "
    "ON RankedAverages.ticker = DailyInfo.ticker "
    "WHERE Quartile = 1 "
    "ORDER BY Average DESC;"
)
query = ranked_averages_cte + daily_info_cte + final_query

data = fetch_data(query)
st.dataframe(data,
             column_config={
        "CLOSE_USD": st.column_config.NumberColumn(
            "Last Close",
            help="Last Close Price (in USD)",
            min_value=0,
            format="$ %f",
        ),
        "TICKER": st.column_config.TextColumn(
            "Ticker"
        ),
        "SECTOR_NAME": st.column_config.TextColumn(
            "Sector Name"
        ),
        "SHARES": st.column_config.NumberColumn(
            "Position Shares",
            help="Share Quantity",
            min_value=0,
        ),
        "AVERAGE": st.column_config.NumberColumn(
            "Last Year Average",
            help="Average Position of Last Year"
        )
    },
    hide_index=True)

''
''

st.header(f'Company daily close price chart', divider='gray')
''
''
query="SELECT DISTINCT IDENTIFIER FROM price;"
identifiers = fetch_data(query)

companies = identifiers['IDENTIFIER'].unique()

company = st.selectbox(
    "Which company would you like to see?",
    companies,
    index=None,
    placeholder="Select company",
)

if company != None:
    query="SELECT * FROM price WHERE IDENTIFIER = '"+ company +"' ORDER BY DATE DESC;"
    data = fetch_data(query)

    st.line_chart(
        data,
        x='DATE',
        x_label="Date",
        y='CLOSE_USD',
        y_label="Close Price in USD",
    )
