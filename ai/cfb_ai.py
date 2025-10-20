# cfb_ai.py
"""
Full pipeline:
- Connect to DuckDB
- Load cfb_games, cfb_rankings, cfb_drives, cfb_plays
- Normalize columns
- Aggregate drive & play metrics per (game_id, offense)
- Assign those aggregates to home/away using is_home_offense
- Merge those features into games (merge on games.id == team_perf.game_id)
- Compute recent rolling stats
- Train LightGBM models (home_win classifier, spread regressor, total points regressor)
- Save predictions back to DuckDB (cfb.cfb_predictions)
- Evaluate model performance and store results
"""

import duckdb
import pandas as pd
import numpy as np
from datetime import datetime
import warnings
from sklearn.metrics import accuracy_score, roc_auc_score, log_loss
import lightgbm as lgb

warnings.filterwarnings("ignore")

# -------------------------
# Step 1: Connect to DuckDB
# -------------------------
con = duckdb.connect(database='cfb_analytics.duckdb', read_only=False)

# -------------------------
# Step 2: Load tables
# -------------------------
games = con.execute("SELECT * FROM cfb.cfb_games").df()
rankings = con.execute("SELECT * FROM cfb.cfb_rankings").df()
drives = con.execute("SELECT * FROM cfb.cfb_drives").df()
plays = con.execute("SELECT * FROM cfb.cfb_plays").df()

# -------------------------
# Step 3: Normalize column names
# -------------------------
def normalize(df):
    df = df.copy()
    df.columns = df.columns.str.lower().str.strip()
    return df

games = normalize(games)
rankings = normalize(rankings)
drives = normalize(drives)
plays = normalize(plays)

# -------------------------
# Step 4: Prepare rankings (AP Top 25)
# -------------------------
if 'poll' in rankings.columns:
    rankings['poll'] = rankings['poll'].astype(str).str.lower()
    rankings_ap = rankings[rankings['poll'] == 'ap top 25'].copy()
else:
    rankings_ap = pd.DataFrame(columns=['team_id', 'season', 'week', 'rank'])

if not rankings_ap.empty and {'team_id', 'season', 'week', 'rank'}.issubset(rankings_ap.columns):
    games = games.merge(
        rankings_ap[['team_id', 'season', 'week', 'rank']],
        left_on=['home_id', 'season', 'week'],
        right_on=['team_id', 'season', 'week'],
        how='left'
    ).rename(columns={'rank': 'home_rank'}).drop(columns=['team_id'], errors='ignore')
    games = games.merge(
        rankings_ap[['team_id', 'season', 'week', 'rank']],
        left_on=['away_id', 'season', 'week'],
        right_on=['team_id', 'season', 'week'],
        how='left'
    ).rename(columns={'rank': 'away_rank'}).drop(columns=['team_id'], errors='ignore')
else:
    games['home_rank'] = 25
    games['away_rank'] = 25

# -------------------------
# Step 5: Base game-level features
# -------------------------
games['home_pregame_elo'] = games.get('home_pregame_elo', 1500).fillna(1500)
games['away_pregame_elo'] = games.get('away_pregame_elo', 1500).fillna(1500)
games['home_rank'] = games['home_rank'].fillna(25)
games['away_rank'] = games['away_rank'].fillna(25)

games['elo_diff'] = games['home_pregame_elo'] - games['away_pregame_elo']
games['rank_diff'] = games['home_rank'] - games['away_rank']

games['completed'] = games.get('completed', False)
games['home_points'] = games.get('home_points', pd.NA)
games['away_points'] = games.get('away_points', pd.NA)

games['home_win'] = (
    (games['completed']) &
    (games['home_points'].notna()) &
    (games['away_points'].notna()) &
    (games['home_points'] > games['away_points'])
).astype(int)

games['point_spread'] = games['home_points'].fillna(0) - games['away_points'].fillna(0)
games['total_points'] = games['home_points'].fillna(0) + games['away_points'].fillna(0)

# -------------------------
# Step 6: Drive-level aggregation
# -------------------------
for col in ['yards', 'plays', 'scoring']:
    if col not in drives.columns:
        drives[col] = 0
if 'is_home_offense' not in drives.columns:
    drives['is_home_offense'] = 0

drive_summary = drives.groupby(['game_id', 'offense'], dropna=False).agg(
    drives_run=('drive_number', 'count'),
    drives_scoring=('scoring', 'sum'),
    total_drive_yards=('yards', 'sum'),
    total_drive_plays=('plays', 'sum'),
    is_home_offense=('is_home_offense', 'max')
).reset_index()

drive_summary['drive_scoring_rate'] = (drive_summary['drives_scoring'] / drive_summary['drives_run']).fillna(0)
drive_summary['avg_drive_yards'] = (drive_summary['total_drive_yards'] / drive_summary['drives_run']).fillna(0)
drive_summary['avg_drive_plays'] = (drive_summary['total_drive_plays'] / drive_summary['drives_run']).fillna(0)

# -------------------------
# Step 7: Play-level aggregation
# -------------------------
for col in ['yards_gained', 'ppa', 'scoring']:
    if col not in plays.columns:
        plays[col] = 0

play_summary = plays.groupby(['game_id', 'offense'], dropna=False).agg(
    total_plays=('id', 'count'),
    total_yards=('yards_gained', 'sum'),
    avg_yards_per_play=('yards_gained', 'mean'),
    scoring_plays=('scoring', 'sum'),
    total_ppa=('ppa', 'sum'),
    avg_ppa=('ppa', 'mean'),
).reset_index()

success_rate = plays.groupby(['game_id', 'offense'], dropna=False)['ppa'].apply(lambda x: (x > 0).mean()).reset_index(name='success_rate')
play_summary = play_summary.merge(success_rate, on=['game_id', 'offense'], how='left').fillna(0)

# -------------------------
# Step 8: Combine drive + play summaries
# -------------------------
team_perf = pd.merge(drive_summary, play_summary, on=['game_id', 'offense'], how='outer').fillna(0)
team_perf['is_home_offense'] = team_perf['is_home_offense'].astype(int)
max_yds = team_perf['avg_yards_per_play'].max() or 1.0
team_perf['offense_efficiency'] = (
    0.5 * team_perf['drive_scoring_rate'] +
    0.3 * (team_perf['avg_yards_per_play'] / max_yds) +
    0.2 * team_perf['success_rate']
).fillna(0)

# -------------------------
# Step 9: Merge team_perf into games
# -------------------------
def prefix(df, pre):
    rename = {c: f"{pre}{c}" for c in df.columns if c not in ['game_id', 'offense']}
    return df.rename(columns=rename)

home_perf = prefix(team_perf[team_perf['is_home_offense'] == 1], 'home_')
away_perf = prefix(team_perf[team_perf['is_home_offense'] == 0], 'away_')

games = games.merge(home_perf, left_on='id', right_on='game_id', how='left')
games = games.merge(away_perf, left_on='id', right_on='game_id', how='left', suffixes=('', '_away'))
games = games.fillna(0)

# -------------------------
# Step 10: Rolling averages (3-game window)
# -------------------------
def rolling_stats(df, team_col, scored_col, allowed_col):
    df[f'{team_col}_recent_scored'] = df.groupby(team_col)[scored_col].transform(
        lambda x: x.shift(1).rolling(3, min_periods=1).mean()
    ).fillna(0)
    df[f'{team_col}_recent_allowed'] = df.groupby(team_col)[allowed_col].transform(
        lambda x: x.shift(1).rolling(3, min_periods=1).mean()
    ).fillna(0)
    return df

games = rolling_stats(games, 'home_id', 'home_points', 'away_points')
games = rolling_stats(games, 'away_id', 'away_points', 'home_points')

# -------------------------
# Step 11: Feature selection
# -------------------------
features = [
    'elo_diff', 'rank_diff',
    'home_id_recent_scored', 'home_id_recent_allowed',
    'away_id_recent_scored', 'away_id_recent_allowed',
    'home_drive_scoring_rate', 'away_drive_scoring_rate',
    'home_avg_yards_per_play', 'away_avg_yards_per_play',
    'home_offense_efficiency', 'away_offense_efficiency',
    'home_avg_ppa', 'away_avg_ppa'
]
features = [f for f in features if f in games.columns]
print("Using feature columns:", features)

# -------------------------
# Step 12: Train on completed games
# -------------------------
train_data = games[games['completed'] == True].copy()
X_train = train_data[features].fillna(0)
y_train = train_data['home_win']

clf = lgb.LGBMClassifier(n_estimators=200, learning_rate=0.05)
clf.fit(X_train, y_train)

spread_model = lgb.LGBMRegressor(n_estimators=200, learning_rate=0.05)
spread_model.fit(X_train, train_data['point_spread'])

total_model = lgb.LGBMRegressor(n_estimators=200, learning_rate=0.05)
total_model.fit(X_train, train_data['total_points'])

# -------------------------
# Step 13: Predict on all games
# -------------------------
X_all = games[features].fillna(0)
games['home_win_prob'] = clf.predict_proba(X_all)[:, 1]
games['home_win_pred'] = (games['home_win_prob'] >= 0.5).astype(int)
games['point_spread_pred'] = -spread_model.predict(X_all)
games['total_points_pred'] = total_model.predict(X_all)

# -------------------------
# Step 14: Save predictions (only future/incomplete games)
# -------------------------
future_games = games[games['completed'] == False].copy()

pred_cols = [
    'season', 'week', 'home_id', 'away_id',
    'home_win_pred', 'home_win_prob', 'point_spread_pred', 'total_points_pred'
]
preds = future_games[pred_cols].copy()

preds = preds.sort_values(['season', 'week']).drop_duplicates(
    subset=['season', 'week', 'home_id', 'away_id'], keep='last'
)

con.register('___preds', preds)

con.execute("""
CREATE TABLE IF NOT EXISTS cfb.cfb_predictions (
    season INTEGER,
    week INTEGER,
    home_id INTEGER,
    away_id INTEGER,
    home_win_pred DOUBLE,
    home_win_prob DOUBLE,
    point_spread_pred DOUBLE,
    total_points_pred DOUBLE
)
""")

# Only merge future predictions — past games stay untouched
con.execute("""
MERGE INTO cfb.cfb_predictions AS t
USING (SELECT * FROM ___preds) AS s
ON t.season = s.season
   AND t.week = s.week
   AND t.home_id = s.home_id
   AND t.away_id = s.away_id
WHEN MATCHED THEN UPDATE SET
    home_win_pred = s.home_win_pred,
    home_win_prob = s.home_win_prob,
    point_spread_pred = s.point_spread_pred,
    total_points_pred = s.total_points_pred
WHEN NOT MATCHED THEN INSERT (
    season, week, home_id, away_id,
    home_win_pred, home_win_prob, point_spread_pred, total_points_pred
)
VALUES (
    s.season, s.week, s.home_id, s.away_id,
    s.home_win_pred, s.home_win_prob, s.point_spread_pred, s.total_points_pred
)
""")

con.unregister('___preds')
print(f"✅ Merged {len(preds)} future predictions into cfb.cfb_predictions")

# -------------------------
# Step 15: Evaluate model (on completed games)
# -------------------------
if len(y_train.unique()) > 1:
    acc = accuracy_score(y_train, train_data['home_win_pred'] if 'home_win_pred' in train_data else clf.predict(X_train))
    auc = roc_auc_score(y_train, train_data['home_win_prob'] if 'home_win_prob' in train_data else clf.predict_proba(X_train)[:,1])
    loss = log_loss(y_train, train_data['home_win_prob'] if 'home_win_prob' in train_data else clf.predict_proba(X_train)[:,1])
    print(f"\n🏈 Model evaluation:\nAccuracy: {acc:.3f}\nAUC: {auc:.3f}\nLog Loss: {loss:.3f}")
else:
    acc, auc, loss = np.nan, np.nan, np.nan
    print("\n⚠️ Only one class in training — metrics not computed.")

eval_df = pd.DataFrame({
    "timestamp": [datetime.now()],
    "accuracy": [acc],
    "auc": [auc],
    "log_loss": [loss],
    "rows_trained": [len(train_data)]
})
con.register("___eval", eval_df)
con.execute("CREATE OR REPLACE TABLE cfb.model_eval AS SELECT * FROM ___eval")
con.unregister("___eval")
print("✅ Evaluation saved in DuckDB (cfb.model_eval)")

# -------------------------
# Step 16: Evaluate betting edges
# -------------------------
BETTING_PROVIDER = "DraftKings"

# Pull lines
lines = con.execute(f"""
    SELECT
        home_team_id,
        home_team,
        away_team_id,
        away_team,
        spread AS vegas_spread,
        week,
        season
    FROM cfb.cfb_lines
    WHERE provider = '{BETTING_PROVIDER}'
""").df()

# Pull predictions
future_preds = con.execute("SELECT * FROM cfb.cfb_predictions").df()

# Merge predictions with lines
future_preds = future_preds.merge(
    lines,
    left_on=['home_id', 'away_id', 'week'],
    right_on=['home_team_id', 'away_team_id', 'week'],
    how='left',
    suffixes=('', '_line')
)

# Generate AI recommendation
def recommend_cover(row):
    predicted_margin = row['point_spread_pred']  # negative = home favored
    vegas_margin = row['vegas_spread']          # negative = home favored

    if pd.isna(predicted_margin) or pd.isna(vegas_margin):
        return None

    if predicted_margin < vegas_margin - 1:
        # Home expected to cover
        covering_team = row['home_team']
        spread_to_show = vegas_margin  # keep Vegas sign
    elif predicted_margin > vegas_margin + 1:
        # Away expected to cover
        covering_team = row['away_team']
        spread_to_show = -vegas_margin  # flip sign to show away covering
    else:
        return "Too close to call"

    return f"{covering_team} covers {spread_to_show:+.1f}"

future_preds['ai_recommendation'] = future_preds.apply(recommend_cover, axis=1)

# Keep only relevant columns
best_bets_cols = [
    'season', 'week', 'home_id', 'away_id',
    'home_team', 'away_team',
    'point_spread_pred', 'vegas_spread',
    'ai_recommendation'
]
best_bets = future_preds[best_bets_cols]

# Save to DuckDB
con.register("___best_bets", best_bets)
con.execute("""
CREATE OR REPLACE TABLE cfb.ai_best_bets AS
SELECT *
FROM ___best_bets
""")
con.unregister("___best_bets")

print("✅ AI best bets saved in DuckDB (cfb.ai_best_bets)")
