import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from pathlib import Path

# Path to your DuckDB file
DB_PATH = Path("./cfb_analytics.duckdb")

st.set_page_config(page_title="CFB Analytics Explorer", layout="wide")
st.title("🏈 College Football Analytics Dashboard")

# --- Check if DB exists ---
if not DB_PATH.exists():
    st.error("❌ No cfb_analytics.duckdb file found. Run your pipeline first.")
    st.stop()

# --- Connect to DuckDB ---
conn = duckdb.connect(str(DB_PATH), read_only=True)

# --- Get all available tables ---
tables_df = conn.execute("""
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema NOT IN ('information_schema','sqlmesh','sqlmesh__cfb')
""").fetchdf()

if tables_df.empty:
    st.warning("No tables found in the database.")
    st.stop()

# Remove DLT internal tables
tables_df = tables_df[~tables_df["table_name"].str.startswith("_dlt")]

# --- Schema selector ---
schemas = sorted(tables_df["table_schema"].unique().tolist())
selected_schema = st.selectbox("Select a schema:", schemas)

# --- Table selector (filtered by schema) ---
filtered_tables = tables_df[tables_df["table_schema"] == selected_schema]
selected_table = st.selectbox(
    "Select a table to explore:",
    sorted(filtered_tables["table_name"].tolist())
)

# --- Load selected table ---
if selected_table:
    full_table_name = f'{selected_schema}.{selected_table}'
    try:
        df = conn.execute(f'SELECT * FROM {full_table_name} LIMIT 1000').fetchdf()

        st.subheader(f"📋 Preview of `{full_table_name}`")
        st.dataframe(df)

        # --- Basic visualization ---
        numeric_cols = df.select_dtypes(include="number").columns.tolist()
        if len(numeric_cols) > 1:
            st.subheader("📊 Quick Visualization")
            x_col = st.selectbox("X Axis", numeric_cols)
            y_col = st.selectbox("Y Axis", numeric_cols, index=min(1, len(numeric_cols) - 1))

            fig = px.scatter(df, x=x_col, y=y_col, title=f"{y_col} vs {x_col}")
            st.plotly_chart(fig, use_container_width=True)

        # --- Custom SQL Query ---
        st.subheader("🧠 Run Custom SQL Query")
        query = st.text_area("Enter your SQL query:", f"""-- AP Top 25 Matchup Predictions
WITH rankings AS (
    SELECT *
    FROM cfb.cfb_rankings
    WHERE poll = 'AP Top 25'
)
SELECT 
    CONCAT('Week ', week,', ',season, ' ',DAYNAME(cg.start_date),' @',CASE WHEN DATEPART('HOUR', cg.start_date) > 12 THEN DATEPART('HOUR', cg.start_date) - 12 ELSE DATEPART('HOUR', cg.start_date) END, 'pm') AS game_details,
    CONCAT(CASE WHEN hr.rank NOT NULL THEN CONCAT(hr.rank,' ')END,cg.home_team, ' vs. ',CASE WHEN ar.rank NOT NULL THEN CONCAT(ar.rank,' ')END, cg.away_team) AS matchup,
    CASE WHEN cp.home_win_pred = 1 THEN cg.home_team ELSE cg.away_team END AS predicted_winner,
    CASE WHEN cp.home_win_pred = 1 THEN ROUND(cp.home_win_prob * 100, 0) ELSE ROUND((1-cp.home_win_prob)*100,0) END AS win_pred_prob,
    ai_recommendation AS bet_recommendation,
    ROUND(cp.point_spread_pred,0) AS point_spread_pred,
    cg.home_points - cg.away_points AS point_spread_actual,
    CASE WHEN (cg.home_points > cg.away_points AND cp.home_win_pred = 1) OR (cg.home_points < cg.away_points AND cp.home_win_pred = 0) THEN TRUE ELSE FALSE END AS win_pred_correct,
FROM cfb.cfb_games cg
LEFT JOIN cfb.cfb_predictions cp
    USING(season, week, home_id, away_id)
LEFT JOIN rankings hr
    ON cg.home_id = hr.team_id
    AND cg.season = hr.season
    AND cg.season_type = hr.season_type
    AND cg.week = hr.week
LEFT JOIN rankings ar
    ON cg.away_id = ar.team_id
    AND cg.season = ar.season
    AND cg.season_type = ar.season_type
    AND cg.week = ar.week
LEFT JOIN cfb.ai_best_bets bb
   ON bb.season = cg.season
   AND bb.week = cg.week
   AND bb.home_id = cg.home_id
   AND bb.away_id = cg.away_id
WHERE season = 2025 
    AND week = 9 
    --AND (cg.home_conference = 'ACC' OR cg.away_conference = 'ACC')
    AND (ar.rank IS NOT NULL OR hr.rank IS NOT NULL)
GROUP BY cg.start_date, season, week, hr.rank,cg.home_team, ar.rank,cg.away_team, cp.home_win_pred, cp.home_win_prob,  bb.ai_recommendation, cp.point_spread_pred, home_points, away_points
ORDER BY cg.start_date, hr.rank, ar.rank, cg.home_team, cg.away_team ASC""", height = 600)
        if st.button("Run Query"):
            try:
                result = conn.execute(query).fetchdf()
                st.write(result)
            except Exception as e:
                st.error(f"Query failed: {e}")

    except Exception as e:
        st.error(f"❌ Error loading table `{selected_table}`: {e}")
else:
    st.info("Please select a table to display.")

conn.close()
