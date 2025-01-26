import streamlit as st
import pandas as pd
import math
import snowflake
import snowflake.connector
import matplotlib.pyplot as plt
from pathlib import Path

# Set the title and favicon that appear in the Browser's tab bar.
st.set_page_config(
    page_title='Code Challenge',
    page_icon=':earth_americas:', # This is an emoji shortcode. Could be a URL too.
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
# :earth_americas: GDP dashboard

Browse GDP data from the [World Bank Open Data](https://data.worldbank.org/) website. As you'll
notice, the data only goes to 2022 right now, and datapoints for certain years are often missing.
But it's otherwise a great (and did I mention _free_?) source of data.
'''

# Add some spacing
''
''

date = fetch_data("SELECT MAX(date) FROM position")
most_recent_date = date["MAX(DATE)"].values[0]


st.header(f'Top 10 sectors by position in {most_recent_date}', divider='gray')
query = "SELECT TOP 10 c.sector_name, p.date,SUM(p.shares) AS SECTOR_POSITION,FROM company c JOIN position p ON c.id = p.company_id JOIN price m ON (p.company_id=m.company_id AND p.date=m.date) WHERE m.date = (SELECT MAX(date) FROM price) GROUP BY c.sector_name,p.date ORDER BY SECTOR_POSITION DESC;"
data = fetch_data(query)
st.bar_chart(data["SECTOR_POSITION"], horizontal=True)

cols = st.columns(3)
for i,sector in enumerate(data['SECTOR_NAME']):
    col = cols[i % len(cols)]
    with col:
        st.metric(
                label=f'{sector}',
                value=f'{data["SECTOR_POSITION"][i]/1000000:,.0f} M',
        )

st.header(f'Top 25% companies with the largest average position (USD) in the last year.', divider='gray')
query="SELECT TOP 25 c.ticker, AVG(m.close_usd*p.shares) AS Average FROM company c JOIN position p ON c.id = p.company_id JOIN price m ON (p.company_id=m.company_id AND p.date=m.date) WHERE m.date BETWEEN '1/01/2024' AND '12/31/2024' GROUP BY c.ticker ORDER BY Average DESC;"
data = fetch_data(query)
st.table(data)


st.header(f'Company daily close price chart.', divider='gray')

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
        y='CLOSE_USD',
        y_label="Close Price in USD",
    )
