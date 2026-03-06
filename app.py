import streamlit as st
import pandas as pd
import numpy as np
import datetime
import pandas_gbq
from google.oauth2 import service_account
from sofascore_metrics import METRICS_CONFIG, get_metric_info
import json

# --- Configuration ---
st.set_page_config(page_title="Sofascore Analytics", layout="wide", page_icon="📈")
PROJECT_ID = 'betterbet-467621'
DATASET_ID = 'sofascore'

# --- CSS Styling ---
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 20px;
        text-align: center;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
    }
    .metric-value {
        font-size: 24px;
        font-weight: bold;
        color: #1f77b4;
    }
    .metric-label {
        font-size: 14px;
        color: #555;
    }
</style>
""", unsafe_allow_html=True)

# --- Helper Functions (BigQuery) ---
# Authentication setup
@st.cache_resource
def get_bq_credentials():
    # Check if running on Streamlit Cloud (has st.secrets)
    try:
        if "gcp_service_account" in st.secrets:
            # Create credentials from Streamlit secrets
            creds_dict = dict(st.secrets["gcp_service_account"])
            return service_account.Credentials.from_service_account_info(creds_dict)
    except Exception:
        pass
        
    # Fallback to local Application Default Credentials (ADC) or credentials.json
    # Since local machine works currently, pandas_gbq will automatically 
    # use the active gcloud authenticated user if we return None.
    return None

credentials = get_bq_credentials()

@st.cache_data(ttl=3600*24)
def load_base_data():
    q_tournaments = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.tournaments`"
    df_tournaments = pandas_gbq.read_gbq(q_tournaments, project_id=PROJECT_ID, credentials=credentials).drop_duplicates(subset=['unique_tournament_id', 'season_id'])
    
    q_matches = f"SELECT match_id, tournament_id, season_id, round_id, match_date, home_team_id, away_team_id, home_team_name, away_team_name, round_name, round_slug FROM `{PROJECT_ID}.{DATASET_ID}.matches`"
    df_matches = pandas_gbq.read_gbq(q_matches, project_id=PROJECT_ID, credentials=credentials).drop_duplicates(subset=['match_id'])
    df_matches['match_date'] = pd.to_datetime(df_matches['match_date'], unit='s')
    
    q_clubs = f"SELECT team_id, name FROM `{PROJECT_ID}.{DATASET_ID}.clubs`"
    df_clubs = pandas_gbq.read_gbq(q_clubs, project_id=PROJECT_ID, credentials=credentials).drop_duplicates(subset=['team_id'])
    
    q_players = f"SELECT player_id, team_id, name, position FROM `{PROJECT_ID}.{DATASET_ID}.players`"
    df_players = pandas_gbq.read_gbq(q_players, project_id=PROJECT_ID, credentials=credentials).drop_duplicates(subset=['player_id'])
    
    return df_tournaments, df_matches, df_clubs, df_players

@st.cache_data(ttl=3600*24)
def fetch_club_general_stats_sql(metric_key, metric_source, match_ids, club_stat_type, agg_scope, calc_mode, sort_order, top_n, valid_team_ids=None):
    if not match_ids: return pd.DataFrame()
    match_ids_str = ",".join(map(str, match_ids))
    
    team_filter = ""
    if valid_team_ids:
        team_ids_str = ",".join(map(str, valid_team_ids))
        team_filter = f"AND team_id IN ({team_ids_str})"
        
    part_tourn = ", m.tournament_id as CampID, m.season_id as TempID, t.name as Comp, t.season_year as Temp" if agg_scope == 'Separado por Temporada' else ""
    join_tourn = f"JOIN `{PROJECT_ID}.{DATASET_ID}.tournaments` t ON m.tournament_id = t.unique_tournament_id AND m.season_id = t.season_id" if agg_scope == 'Separado por Temporada' else ""
    group_tourn = ", m.tournament_id, m.season_id, t.name, t.season_year" if agg_scope == 'Separado por Temporada' else ""
    
    if metric_source == 'match':
        val_col = "home_value" if club_stat_type == "Feito (Pró)" else "away_value"
        val_col_opp = "away_value" if club_stat_type == "Feito (Pró)" else "home_value"
        
        query = f"""
        WITH unpivoted AS (
            SELECT m.match_id, m.home_team_id as team_id, m.home_team_name as Club, 
                   CAST(ms.{val_col} AS FLOAT64) as Value {part_tourn}
            FROM `{PROJECT_ID}.{DATASET_ID}.matches` m JOIN `{PROJECT_ID}.{DATASET_ID}.match_stats_log` ms ON m.match_id = ms.match_id
            {join_tourn}
            WHERE ms.metric_key = '{metric_key}' AND m.match_id IN ({match_ids_str})
            
            UNION ALL
            
            SELECT m.match_id, m.away_team_id as team_id, m.away_team_name as Club, 
                   CAST(ms.{val_col_opp} AS FLOAT64) as Value {part_tourn}
            FROM `{PROJECT_ID}.{DATASET_ID}.matches` m JOIN `{PROJECT_ID}.{DATASET_ID}.match_stats_log` ms ON m.match_id = ms.match_id
            {join_tourn}
            WHERE ms.metric_key = '{metric_key}' AND m.match_id IN ({match_ids_str})
        )
        SELECT Club {", Comp, Temp" if agg_scope == 'Separado por Temporada' else ""}, 
               COUNT(Value) as Jogos, SUM(Value) as sum_val, AVG(Value) as avg_val 
        FROM unpivoted
        WHERE Value IS NOT NULL {team_filter}
        GROUP BY Club {", Comp, Temp" if agg_scope == 'Separado por Temporada' else ""}
        """
    else: 
        if club_stat_type == "Feito (Pró)":
            team_condition = "AND psl.team_id = unp.team_id"
        else:
            team_condition = "AND psl.team_id != unp.team_id"
            
        query = f"""
        WITH unpivoted AS (
            SELECT m.match_id, m.home_team_id as team_id, m.home_team_name as Club {part_tourn}
            FROM `{PROJECT_ID}.{DATASET_ID}.matches` m {join_tourn} WHERE m.match_id IN ({match_ids_str})
            UNION ALL
            SELECT m.match_id, m.away_team_id as team_id, m.away_team_name as Club {part_tourn}
            FROM `{PROJECT_ID}.{DATASET_ID}.matches` m {join_tourn} WHERE m.match_id IN ({match_ids_str})
        )
        SELECT unp.Club {", unp.Comp, unp.Temp" if agg_scope == 'Separado por Temporada' else ""},
               COUNT(DISTINCT unp.match_id) as Jogos,
               SUM(CASE WHEN psl.metric_key = '{metric_key}' THEN CAST(psl.value AS FLOAT64) ELSE 0 END) as sum_val
        FROM unpivoted unp
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.player_stats_log` psl 
          ON psl.match_id = unp.match_id {team_condition} AND psl.metric_key = '{metric_key}'
        WHERE 1=1 {team_filter.replace('team_id', 'unp.team_id')}
        GROUP BY unp.Club {", unp.Comp, unp.Temp" if agg_scope == 'Separado por Temporada' else ""}
        HAVING sum_val IS NOT NULL
        """
        
    df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials)
    if df.empty: return df
    
    if metric_source == 'player':
        df['Valor'] = df['sum_val'] if calc_mode == 'Total' else (df['sum_val'] / df['Jogos'])
    else:
        df['Valor'] = df['sum_val'] if calc_mode == 'Total' else df['avg_val']
        
    ascending = False if sort_order == "DESC" else True
    return df.sort_values(by='Valor', ascending=ascending).head(top_n).drop(columns=['sum_val', *(['avg_val'] if metric_source == 'match' else [])])

@st.cache_data(ttl=3600*24)
def fetch_player_general_stats_sql(metric_key, match_ids, club_stat_type, agg_scope, calc_mode, sort_order, top_n, min_games, min_minutes, valid_team_ids=None, valid_player_ids=None, valid_positions=None):
    if not match_ids: return pd.DataFrame()
    match_ids_str = ",".join(map(str, match_ids))

    filters = []
    if valid_team_ids:
        team_ids_str = ",".join(map(str, valid_team_ids))
        if club_stat_type == "Feito (Pró)":
            filters.append(f"psl.team_id IN ({team_ids_str})")
        else:
            filters.append(f"psl.team_id NOT IN ({team_ids_str})")
            
    if valid_player_ids:
        p_ids_str = ",".join(map(str, valid_player_ids))
        filters.append(f"psl.player_id IN ({p_ids_str})")
        
    if valid_positions:
        pos_str = ",".join(f"'{p}'" for p in valid_positions)
        filters.append(f"p.position IN ({pos_str})")
        
    where_clause = " AND ".join(filters)
    if where_clause: where_clause = " AND " + where_clause
    
    part_tourn = ", t.name as Comp, t.season_year as Temp" if agg_scope == 'Separado por Temporada' else ""
    join_tourn = f"JOIN `{PROJECT_ID}.{DATASET_ID}.tournaments` t ON m.tournament_id = t.unique_tournament_id AND m.season_id = t.season_id" if agg_scope == 'Separado por Temporada' else ""
    group_tourn = ", t.name, t.season_year" if agg_scope == 'Separado por Temporada' else ""

    query = f"""
    SELECT 
        p.name as Jogador, 
        STRING_AGG(DISTINCT CASE WHEN psl.team_id = m.home_team_id THEN m.home_team_name ELSE m.away_team_name END, ', ') as Clube,
        MAX(p.position) as Pos
        {part_tourn},
        COUNT(DISTINCT psl.match_id) as Jogos,
        SUM(CASE WHEN psl.metric_key = 'minutesPlayed' THEN CAST(psl.value AS FLOAT64) ELSE 0 END) as Minutos,
        SUM(CASE WHEN psl.metric_key = '{metric_key}' THEN CAST(psl.value AS FLOAT64) ELSE 0 END) as Valor_Total
    FROM `{PROJECT_ID}.{DATASET_ID}.player_stats_log` psl
    JOIN `{PROJECT_ID}.{DATASET_ID}.matches` m ON m.match_id = psl.match_id
    JOIN `{PROJECT_ID}.{DATASET_ID}.players` p ON p.player_id = psl.player_id
    {join_tourn}
    WHERE psl.match_id IN ({match_ids_str}) 
      AND psl.metric_key IN ('{metric_key}', 'minutesPlayed')
      {where_clause}
    GROUP BY psl.player_id, p.name {group_tourn}
    HAVING Jogos >= {min_games} AND Minutos >= {min_minutes}
    """
    df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials)
    if df.empty: return df

    if calc_mode == "Total":
        df['Valor'] = df['Valor_Total']
    elif calc_mode == "Média por Jogo":
        df['Valor'] = df['Valor_Total'] / df['Jogos']
    else: # Per 90
        df['Valor'] = np.where(df['Minutos'] > 0, (df['Valor_Total'] * 90.0) / df['Minutos'], 0)
        
    ascending = False if sort_order == "DESC" else True
    return df.sort_values(by='Valor', ascending=ascending).head(top_n).drop(columns=['Valor_Total'])

@st.cache_data(ttl=3600*24)
def fetch_club_single_match_sql(metric_key, metric_source, match_ids, club_stat_type, sort_order, top_n, valid_team_ids=None):
    if not match_ids: return pd.DataFrame()
    match_ids_str = ",".join(map(str, match_ids))
    
    team_filter = ""
    if valid_team_ids:
        team_ids_str = ",".join(map(str, valid_team_ids))
        team_filter = f"AND team_id IN ({team_ids_str})"
        
    if metric_source == 'match':
        val_col = "home_value" if club_stat_type == "Feito (Pró)" else "away_value"
        val_col_opp = "away_value" if club_stat_type == "Feito (Pró)" else "home_value"
        
        query = f"""
        WITH unpivoted AS (
            SELECT m.match_id, m.match_date, m.home_team_id as team_id, m.home_team_name as Clube, 
                   CONCAT(m.home_team_name, ' vs ', m.away_team_name) as Jogo,
                   CAST(ms.{val_col} AS FLOAT64) as Valor
            FROM `{PROJECT_ID}.{DATASET_ID}.matches` m JOIN `{PROJECT_ID}.{DATASET_ID}.match_stats_log` ms ON m.match_id = ms.match_id
            WHERE ms.metric_key = '{metric_key}' AND m.match_id IN ({match_ids_str})
            
            UNION ALL
            
            SELECT m.match_id, m.match_date, m.away_team_id as team_id, m.away_team_name as Clube, 
                   CONCAT(m.home_team_name, ' vs ', m.away_team_name) as Jogo,
                   CAST(ms.{val_col_opp} AS FLOAT64) as Valor
            FROM `{PROJECT_ID}.{DATASET_ID}.matches` m JOIN `{PROJECT_ID}.{DATASET_ID}.match_stats_log` ms ON m.match_id = ms.match_id
            WHERE ms.metric_key = '{metric_key}' AND m.match_id IN ({match_ids_str})
        )
        SELECT TIMESTAMP_SECONDS(CAST(match_date AS INT64)) as Data, Jogo, Clube, Valor
        FROM unpivoted
        WHERE Valor IS NOT NULL {team_filter}
        ORDER BY Valor {"DESC" if sort_order == "DESC" else "ASC"}
        LIMIT {top_n}
        """
    else: 
        if club_stat_type == "Feito (Pró)":
            team_condition = "AND psl.team_id = unp.team_id"
        else:
            team_condition = "AND psl.team_id != unp.team_id"
            
        query = f"""
        WITH unpivoted AS (
            SELECT m.match_id, m.match_date, m.home_team_id as team_id, m.home_team_name as Clube, CONCAT(m.home_team_name, ' vs ', m.away_team_name) as Jogo
            FROM `{PROJECT_ID}.{DATASET_ID}.matches` m WHERE m.match_id IN ({match_ids_str})
            UNION ALL
            SELECT m.match_id, m.match_date, m.away_team_id as team_id, m.away_team_name as Clube, CONCAT(m.home_team_name, ' vs ', m.away_team_name) as Jogo
            FROM `{PROJECT_ID}.{DATASET_ID}.matches` m WHERE m.match_id IN ({match_ids_str})
        )
        SELECT TIMESTAMP_SECONDS(CAST(unp.match_date AS INT64)) as Data, 
               unp.Jogo, unp.Clube, 
               SUM(CAST(psl.value AS FLOAT64)) as Valor
        FROM unpivoted unp
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.player_stats_log` psl 
          ON psl.match_id = unp.match_id {team_condition} AND psl.metric_key = '{metric_key}'
        WHERE 1=1 {team_filter.replace('team_id', 'unp.team_id')}
        GROUP BY unp.match_id, unp.team_id, unp.Clube, unp.Jogo, Data
        HAVING Valor IS NOT NULL
        ORDER BY Valor {"DESC" if sort_order == "DESC" else "ASC"}
        LIMIT {top_n}
        """
        
    df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials)
    if not df.empty:
        df['Data'] = pd.to_datetime(df['Data']).dt.strftime('%d/%m/%Y')
    return df

@st.cache_data(ttl=3600*24)
def fetch_combined_match_sql(metric_key, metric_source, match_ids, sort_order, top_n, valid_team_ids=None):
    if not match_ids: return pd.DataFrame()
    match_ids_str = ",".join(map(str, match_ids))
    
    team_filter = ""
    team_filter_unp = ""
    if valid_team_ids:
        team_ids_str = ",".join(map(str, valid_team_ids))
        team_filter = f"AND (m.home_team_id IN ({team_ids_str}) OR m.away_team_id IN ({team_ids_str}))"
        team_filter_unp = f"AND (unp.home_team_id IN ({team_ids_str}) OR unp.away_team_id IN ({team_ids_str}))"
        
    if metric_source == 'match':
        query = f"""
        SELECT 
            TIMESTAMP_SECONDS(CAST(m.match_date AS INT64)) as Data,
            CONCAT(m.home_team_name, ' vs ', m.away_team_name) as Jogo,
            CAST(ms.home_value AS FLOAT64) + CAST(ms.away_value AS FLOAT64) as Valor
        FROM `{PROJECT_ID}.{DATASET_ID}.match_stats_log` ms
        JOIN `{PROJECT_ID}.{DATASET_ID}.matches` m ON m.match_id = ms.match_id
        WHERE ms.metric_key = '{metric_key}' AND ms.match_id IN ({match_ids_str}) {team_filter}
        ORDER BY Valor {"DESC" if sort_order == "DESC" else "ASC"}
        LIMIT {top_n}
        """
    else: 
        query = f"""
        WITH base_matches AS (
            SELECT m.match_id, m.match_date, m.home_team_id, m.away_team_id,
                   CONCAT(m.home_team_name, ' vs ', m.away_team_name) as Jogo
            FROM `{PROJECT_ID}.{DATASET_ID}.matches` m WHERE m.match_id IN ({match_ids_str})
        )
        SELECT TIMESTAMP_SECONDS(CAST(unp.match_date AS INT64)) as Data, 
               unp.Jogo,
               SUM(CAST(psl.value AS FLOAT64)) as Valor
        FROM base_matches unp
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.player_stats_log` psl 
          ON psl.match_id = unp.match_id AND psl.metric_key = '{metric_key}'
        WHERE 1=1 {team_filter_unp}
        GROUP BY unp.match_id, unp.Jogo, Data
        HAVING Valor IS NOT NULL
        ORDER BY Valor {"DESC" if sort_order == "DESC" else "ASC"}
        LIMIT {top_n}
        """
        
    df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials)
    if not df.empty:
        df['Data'] = pd.to_datetime(df['Data']).dt.strftime('%d/%m/%Y')
    return df

@st.cache_data(ttl=3600*24)
def fetch_player_match_sql(metric_key, match_ids, club_stat_type, sort_order, top_n, valid_team_ids=None, valid_player_ids=None, valid_positions=None):
    if not match_ids: return pd.DataFrame()
    match_ids_str = ",".join(map(str, match_ids))

    filters = []
    if valid_team_ids:
        team_ids_str = ",".join(map(str, valid_team_ids))
        if club_stat_type == "Feito (Pró)":
            filters.append(f"psl.team_id IN ({team_ids_str})")
        else:
            filters.append(f"psl.team_id NOT IN ({team_ids_str})")
            
    if valid_player_ids:
        p_ids_str = ",".join(map(str, valid_player_ids))
        filters.append(f"psl.player_id IN ({p_ids_str})")
        
    if valid_positions:
        pos_str = ",".join(f"'{p}'" for p in valid_positions)
        filters.append(f"p.position IN ({pos_str})")
        
    where_clause = " AND ".join(filters)
    if where_clause: where_clause = " AND " + where_clause
    
    query = f"""
    SELECT 
        p.name as Jogador,
        CASE WHEN psl.team_id = m.home_team_id THEN m.home_team_name ELSE m.away_team_name END as Clube,
        CONCAT(m.home_team_name, ' vs ', m.away_team_name) as Jogo,
        TIMESTAMP_SECONDS(CAST(m.match_date AS INT64)) as Data,
        CAST(psl.value AS FLOAT64) as Valor
    FROM `{PROJECT_ID}.{DATASET_ID}.player_stats_log` psl
    JOIN `{PROJECT_ID}.{DATASET_ID}.matches` m ON m.match_id = psl.match_id
    JOIN `{PROJECT_ID}.{DATASET_ID}.players` p ON p.player_id = psl.player_id
    WHERE psl.metric_key = '{metric_key}' AND psl.match_id IN ({match_ids_str}) {where_clause}
    ORDER BY Valor {"DESC" if sort_order == "DESC" else "ASC"}
    LIMIT {top_n}
    """
    df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials)
    if not df.empty:
        df['Data'] = pd.to_datetime(df['Data']).dt.strftime('%d/%m/%Y')
    return df

@st.cache_data(ttl=3600*24)
def fetch_player_streaks_sql(metric_key, match_ids, club_stat_type, valid_team_ids=None, valid_player_ids=None, valid_positions=None):
    if not match_ids: return pd.DataFrame()
    match_ids_str = ",".join(map(str, match_ids))

    filters = []
    if valid_team_ids:
        team_ids_str = ",".join(map(str, valid_team_ids))
        if club_stat_type == "Feito (Pró)":
            filters.append(f"psl.team_id IN ({team_ids_str})")
        else:
            filters.append(f"psl.team_id NOT IN ({team_ids_str})")
            
    if valid_player_ids:
        p_ids_str = ",".join(map(str, valid_player_ids))
        filters.append(f"psl.player_id IN ({p_ids_str})")
        
    if valid_positions:
        pos_str = ",".join(f"'{p}'" for p in valid_positions)
        filters.append(f"p.position IN ({pos_str})")
        
    where_clause = " AND ".join(filters)
    if where_clause: where_clause = " AND " + where_clause
    
    query = f"""
    WITH PlayerGames AS (
        SELECT 
            p.name as Entity,
            psl.player_id,
            m.match_id,
            m.match_date,
            m.home_team_name,
            m.away_team_name,
            psl.team_id,
            CAST(psl.value AS FLOAT64) as minutes
        FROM `{PROJECT_ID}.{DATASET_ID}.player_stats_log` psl
        JOIN `{PROJECT_ID}.{DATASET_ID}.matches` m ON m.match_id = psl.match_id
        JOIN `{PROJECT_ID}.{DATASET_ID}.players` p ON p.player_id = psl.player_id
        WHERE psl.match_id IN ({match_ids_str})
          AND psl.metric_key = 'minutesPlayed'
          {where_clause}
    )
    SELECT 
        pg.Entity,
        pg.player_id,
        pg.match_date,
        pg.home_team_name,
        pg.away_team_name,
        pg.team_id,
        pg.minutes,
        COALESCE(CAST(MAX(psl_metric.value) AS FLOAT64), 0.0) as value
    FROM PlayerGames pg
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.player_stats_log` psl_metric
      ON psl_metric.match_id = pg.match_id 
     AND psl_metric.player_id = pg.player_id
     AND psl_metric.metric_key = '{metric_key}'
    GROUP BY pg.Entity, pg.player_id, pg.match_date, pg.home_team_name, pg.away_team_name, pg.team_id, pg.minutes
    ORDER BY pg.Entity, pg.match_date
    """
    df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials)
    return df

# Load into memory
df_tournaments, df_matches, df_clubs, df_players = load_base_data()

# --- Sidebar: Filter Engine ---
st.sidebar.title("🔍 Configuração de Análise")

# 1. Competitions & Seasons
if df_tournaments.empty:
    st.error("Base de dados vazia. Execute o crawler primeiro.")
    st.stop()

with st.sidebar.expander("🏆 Competição e Temporada", expanded=True):
    # Tournament Selection
    all_tournaments = df_tournaments['name'].unique().tolist()
    sel_tournaments = st.multiselect("Competição", all_tournaments, default=all_tournaments[:1])

    # Season Selection (Contextual)
    if sel_tournaments:
        available_seasons = df_tournaments[df_tournaments['name'].isin(sel_tournaments)]['season_year'].unique().tolist()
        available_seasons.sort(reverse=True)
        sel_seasons = st.multiselect("Temporada", available_seasons, default=available_seasons)
    else:
        sel_seasons = []

    if not sel_tournaments or not sel_seasons:
        st.warning("Selecione Competição e Temporada para começar.")
        st.stop()

# Resolve IDs for Selected Context
selected_configs = df_tournaments[
    (df_tournaments['name'].isin(sel_tournaments)) & 
    (df_tournaments['season_year'].isin(sel_seasons))
]
valid_tournament_ids = selected_configs['unique_tournament_id'].unique().tolist()
valid_season_ids = selected_configs['season_id'].unique().tolist()

if not valid_tournament_ids:
    st.warning("Combinação inválida de Competição/Temporada.")
    st.stop()

# Pre-filter matches by tournament and season for min/max
context_matches = df_matches[
    df_matches['tournament_id'].isin(valid_tournament_ids) &
    df_matches['season_id'].isin(valid_season_ids)
]

with st.sidebar.expander("📅 Período e Rodadas", expanded=False):
    # 2. Date Range
    if not context_matches.empty:
        db_min_date = context_matches['match_date'].min().date()
        db_max_date = context_matches['match_date'].max().date()
        
        db_min_round = int(context_matches['round_id'].min()) if pd.notnull(context_matches['round_id'].min()) else 1
        db_max_round = int(context_matches['round_id'].max()) if pd.notnull(context_matches['round_id'].max()) else 38
    else:
        db_min_date = datetime.date(2020, 1, 1)
        db_max_date = datetime.date.today()
        db_min_round, db_max_round = 1, 38

    sel_dates = st.date_input(
        "Período", 
        [db_min_date, db_max_date], 
        min_value=datetime.date(2010,1,1), 
        max_value=max(datetime.datetime.now().date(), db_max_date)
    )

    # 3. Rounds (Hybrid Strategy)
    sel_rounds_slider = None
    sel_extra_knockouts = []
    
    if not context_matches.empty:
        # Separate numeric-like rounds from disparate ones
        unique_rounds = sorted(context_matches['round_id'].dropna().unique())
        
        # Determine continuous phase (Fase de Grupos)
        continuous_rounds = []
        if unique_rounds and unique_rounds[0] == 1:
            for r in unique_rounds:
                if len(continuous_rounds) == 0 or r == continuous_rounds[-1] + 1:
                    continuous_rounds.append(int(r))
                else:
                    break
                    
        extra_rounds = [int(r) for r in unique_rounds if r not in continuous_rounds]
        
        # Checkbox + Slider for Continuous
        if len(continuous_rounds) > 1:
            use_slider = st.checkbox("Incluir Fase de Grupos", value=True)
            if use_slider:
                sel_rounds_slider = st.slider(
                    "Fase Contínua (Rodadas)", 
                    min(continuous_rounds), 
                    max(continuous_rounds), 
                    (min(continuous_rounds), max(continuous_rounds))
                )
        elif len(continuous_rounds) == 1:
            # Only one round in continuous, just add it to extra
            extra_rounds.insert(0, continuous_rounds[0])
            continuous_rounds = []

        # Multi-select for Knockouts/Disparate
        if extra_rounds:
            # Map IDs to actual names, fallback to ID if no name
            extra_mapping = {}
            for r_id in extra_rounds:
                r_name_row = context_matches[context_matches['round_id'] == r_id]['round_name'].dropna().unique()
                display_name = r_name_row[0] if len(r_name_row) > 0 else f"Fase {r_id}"
                extra_mapping[display_name] = r_id
                
            sel_extra_names = st.multiselect("Fases Extras / Mata-Mata", list(extra_mapping.keys()), default=list(extra_mapping.keys()))
            sel_extra_knockouts = [extra_mapping[n] for n in sel_extra_names]

with st.sidebar.expander("🛡️ Clubes e Jogadores", expanded=False):
    # 4. Clubs Filter
    clubs_in_matches = pd.concat([context_matches['home_team_id'], context_matches['away_team_id']]).unique()
    context_clubs = df_clubs[df_clubs['team_id'].isin(clubs_in_matches)].sort_values('name')

    all_clubs = context_clubs['name'].tolist()
    sel_clubs = st.multiselect("Clubes (Alvo principal)", all_clubs, default=[])
    selected_team_ids = []
    if sel_clubs:
        selected_team_ids = context_clubs[context_clubs['name'].isin(sel_clubs)]['team_id'].tolist()

    # 5. Players Filter
    sel_players = []
    if selected_team_ids:
        context_players = df_players[df_players['team_id'].isin(selected_team_ids)].sort_values('name')
        sel_players = st.multiselect("Jogadores (Opcional)", context_players['name'].tolist(), default=[])

    # 6. Position Filter
    sel_pos = st.multiselect("Posição", ["G", "D", "M", "F"], default=["G", "D", "M", "F"])

st.sidebar.divider()

# --- Analysis Configuration ---
st.sidebar.subheader("⚙️ Parâmetros de Análise")

# Metric Selection (Source of Truth)
all_metrics_list = sorted(list(METRICS_CONFIG.keys()))
sel_metric = st.sidebar.selectbox("Métrica Principal", all_metrics_list, index=0)

# Retrieve Keys
metric_key, metric_source = get_metric_info(sel_metric)
metric_category = METRICS_CONFIG[sel_metric]['category']

st.sidebar.caption(f"Categoria: {metric_category}")
st.sidebar.caption(f"Fonte de Dados: {metric_source.capitalize()}")

st.sidebar.divider()
view_mode = st.sidebar.radio("Visualização (Aba Recordes)", ["Médias e Totais", "Recordes em uma Única Partida"])


with st.sidebar.expander("Modos e Escopos", expanded=False):
    # Calculation Mode
    calc_mode = st.radio("Modo de Cálculo", ["Total", "Média por Jogo", "Por 90 min"])

    # Row Limit
    top_n = st.selectbox("Quantidade de Resultados (Top N)", [3, 5, 10, 20, 50, 100], index=3)

    # Conditional Filters for Averages
    min_games = 0
    min_minutes = 0

    if calc_mode in ["Média por Jogo", "Por 90 min"]:
        c1, c2 = st.columns(2)
        min_games = c1.number_input("Mín. Partidas", min_value=0, value=5, step=1)
        min_minutes = c2.number_input("Mín. Minutos", min_value=0, value=90, step=45)

    # Record Type
    record_type = st.radio("Tipo de Ordenação", ["Maior (Positivo)", "Menor (Negativo)"])

    # Aggregation Scope
    agg_scope = st.radio("Escopo de Agregação", ["Acumulado (Tudo)", "Separado por Temporada"])

    # Club Stat Type (For/Against)
    st.markdown("### Perspectiva do Clube")
    club_stat_type = st.radio("Filtro de Clube se aplica a:", ["Feito (Pró)", "Sofrido (Contra)"], help=" 'Feito': Gols marcados pelo clube.\n'Sofrido': Gols sofridos pelo clube (estatística do adversário).")

# --- Filter Logic Implementation ---
if len(sel_dates) == 2:
    start_date = pd.to_datetime(sel_dates[0])
    end_date = pd.to_datetime(sel_dates[1]).replace(hour=23, minute=59, second=59)
else:
    start_date = pd.to_datetime(sel_dates[0])
    end_date = pd.to_datetime(datetime.date.today()).replace(hour=23, minute=59, second=59)

# Base pandas filter for dates
mask_matches = context_matches['match_date'].between(start_date, end_date)

# Round Filtering
if sel_rounds_slider is not None and sel_extra_knockouts:
    # Both active
    mask_matches &= (
        context_matches['round_id'].between(sel_rounds_slider[0], sel_rounds_slider[1]) |
        context_matches['round_id'].isin(sel_extra_knockouts)
    )
elif sel_rounds_slider is not None:
    # Only slider active
    mask_matches &= context_matches['round_id'].between(sel_rounds_slider[0], sel_rounds_slider[1])
elif sel_extra_knockouts:
    # Only knockouts active
    mask_matches &= context_matches['round_id'].isin(sel_extra_knockouts)
else:
    # Both empty/disabled = block all
    mask_matches &= (context_matches['round_id'] == -1)

if selected_team_ids:
    mask_matches &= (
        context_matches['home_team_id'].isin(selected_team_ids) | 
        context_matches['away_team_id'].isin(selected_team_ids)
    )

filtered_matches = context_matches[mask_matches]
match_count = len(filtered_matches)

# --- Tabs ---
st.title(f"{sel_metric}")
st.caption(f"Analisando **{match_count}** partidas com Google BigQuery.")

tab_records, tab_streaks = st.tabs(["🏆 Recordes & Rankings", "🔥 Forma & Sequências"])

# Sort Order
sort_order = "DESC" if record_type.startswith("Maior") else "ASC"
agg_func = "SUM" # Default
if calc_mode == "Média por Jogo" or calc_mode == "Por 90 min":
    agg_func = "AVG" 

with tab_records:
    # ---------------------------------------------------------
    # VIEW: MÉDIAS E TOTAIS (General Performance)
    # ---------------------------------------------------------
    if view_mode == "Médias e Totais":
        col_c, col_p = st.columns(2)
        
        # --- CLUB GENERAL STATS ---
        with col_c:
            st.subheader("🛡️ Clubes")
            if metric_key:
                # Logic for Club Stats (For vs Against)
                if club_stat_type == "Feito (Pró)":
                    val_home = "home_value"
                    val_away = "away_value"
                    suffix = ""
                else:
                    val_home = "away_value"
                    val_away = "home_value"
                    suffix = " (Sofrido)"
                    
                st.markdown(f"#### Desempenho Geral{suffix}")
            
            if metric_key and not filtered_matches.empty:
                match_ids_list = tuple(filtered_matches['match_id'].tolist())
                
                df_c_agg = fetch_club_general_stats_sql(
                    metric_key=metric_key,
                    metric_source=metric_source,
                    match_ids=match_ids_list,
                    club_stat_type=club_stat_type,
                    agg_scope=agg_scope,
                    calc_mode=calc_mode,
                    sort_order=sort_order,
                    top_n=top_n,
                    valid_team_ids=selected_team_ids if selected_team_ids else None
                )
                
                if not df_c_agg.empty:
                    if "Posse" in sel_metric or "%" in sel_metric:
                        st.dataframe(df_c_agg.style.format({"Valor": "{:.1f}%"}), width="stretch", hide_index=True)
                    else:
                        st.dataframe(df_c_agg.style.format({"Valor": "{:.2f}"}), width="stretch", hide_index=True)
                else:
                    st.info("Sem dados")
            else:
                 st.info("Sem dados")

        # --- PLAYER GENERAL STATS ---
        with col_p:
            st.subheader("🏃 Jogadores")
            if metric_key and metric_source == 'player':
                pos_tuple = tuple(sel_pos) if sel_pos else ('X',)
                player_clause = ""
                if sel_players:
                     p_names = "','".join(sel_players)
                     player_clause = f"AND p.name IN ('{p_names}')"

                st.markdown("#### Desempenho Geral")
            
            if metric_key and metric_source == 'player' and not filtered_matches.empty:
                match_ids_list = tuple(filtered_matches['match_id'].tolist())
                
                p_id_list = selected_team_ids if selected_team_ids else None
                if sel_players:
                    p_name_filtered = df_players[df_players['name'].isin(sel_players)]['player_id'].tolist()
                else:
                    p_name_filtered = None
                    
                df_p_agg = fetch_player_general_stats_sql(
                    metric_key=metric_key,
                    match_ids=match_ids_list,
                    club_stat_type=club_stat_type,
                    agg_scope=agg_scope,
                    calc_mode=calc_mode,
                    sort_order=sort_order,
                    top_n=top_n,
                    min_games=min_games,
                    min_minutes=min_minutes,
                    valid_team_ids=p_id_list,
                    valid_player_ids=p_name_filtered,
                    valid_positions=sel_pos if sel_pos else None
                )
                
                if not df_p_agg.empty:
                    cols_to_show = ['Jogador', 'Clube', 'Pos']
                    if agg_scope == "Separado por Temporada":
                        cols_to_show += ['Comp', 'Temp']
                    cols_to_show += ['Jogos', 'Minutos', 'Valor']
                    
                    st.dataframe(df_p_agg[cols_to_show].style.format({"Valor": "{:.2f}", "Minutos": "{:.0f}"}), width="stretch", hide_index=True)
                else:
                    st.info("A métrica não retornou dados com os filtros atuais.")
            else:
                st.info(f"A métrica '{sel_metric}' não está disponível para Jogadores (Métrica Coletiva Pura).")

    # ---------------------------------------------------------
    # VIEW: RECORDES EM UMA ÚNICA PARTIDA
    # ---------------------------------------------------------
    elif view_mode == "Recordes em uma Única Partida":
        col_m1, col_m2, col_m3 = st.columns(3)
        
        with col_m1:
            st.markdown("#### ⚔️ Somatório do Jogo (2 Clubes)")
            if metric_key and not filtered_matches.empty:
                match_ids_list = tuple(filtered_matches['match_id'].tolist())
                df_combined = fetch_combined_match_sql(
                    metric_key=metric_key,
                    metric_source=metric_source,
                    match_ids=match_ids_list,
                    sort_order=sort_order,
                    top_n=top_n,
                    valid_team_ids=selected_team_ids if selected_team_ids else None
                )
                    
                if not df_combined.empty:
                    if "Posse" in sel_metric or "%" in sel_metric:
                         st.dataframe(df_combined.style.format({"Valor": "{:.1f}%"}), width="stretch", hide_index=True)
                    else:
                         st.dataframe(df_combined, width="stretch", hide_index=True)
                else:
                    st.info("Sem dados")
            else:
                st.info(f"Métrica indisponível.")

        with col_m2:
            if metric_key and not filtered_matches.empty:
                if club_stat_type == "Feito (Pró)":
                    suffix = ""
                else:
                    suffix = " (Sofrido)"
                    
                st.markdown(f"#### 🛡️ Clube em Partida{suffix}")
                
                match_ids_list = tuple(filtered_matches['match_id'].tolist())
                df_c_match = fetch_club_single_match_sql(
                    metric_key=metric_key,
                    metric_source=metric_source,
                    match_ids=match_ids_list,
                    club_stat_type=club_stat_type,
                    sort_order=sort_order,
                    top_n=top_n,
                    valid_team_ids=selected_team_ids if selected_team_ids else None
                )
                    
                if not df_c_match.empty:
                    if "Posse" in sel_metric or "%" in sel_metric:
                         st.dataframe(df_c_match.style.format({"Valor": "{:.1f}%"}), width="stretch", hide_index=True)
                    else:
                         st.dataframe(df_c_match, width="stretch", hide_index=True)
                else:
                    st.info("Sem dados")
            else:
                 st.info(f"Métrica indisponível.")

        with col_m3:
            st.markdown("#### 🏃 Jogador em Partida")
            if metric_key and metric_source == 'player' and not filtered_matches.empty:
                match_ids_list = tuple(filtered_matches['match_id'].tolist())
                
                p_id_list = selected_team_ids if selected_team_ids else None
                if sel_players:
                    p_name_filtered = df_players[df_players['name'].isin(sel_players)]['player_id'].tolist()
                else:
                    p_name_filtered = None
                    
                df_p_match = fetch_player_match_sql(
                    metric_key=metric_key,
                    match_ids=match_ids_list,
                    club_stat_type=club_stat_type,
                    sort_order=sort_order,
                    top_n=top_n,
                    valid_team_ids=p_id_list,
                    valid_player_ids=p_name_filtered,
                    valid_positions=sel_pos if sel_pos else None
                )
                    
                if not df_p_match.empty:
                    st.dataframe(df_p_match, width="stretch", hide_index=True)
                else:
                    st.info("Sem dados")
            else:
                 st.info(f"Métrica indisponível para Jogadores (Métrica Coletiva Pura).")


with tab_streaks:
    st.subheader(f"🔥 Forma e Sequências: {sel_metric}")
    
    # Configuration Row
    c_seq1, c_seq2 = st.columns([1, 1])
    target_val = c_seq1.number_input("Alvo", value=1.0, step=0.5, format="%.1f")
    op = c_seq2.selectbox("Condição", [">=", ">", "<=", "<", "="])
    
    if metric_key and metric_source == 'player':
        st.markdown("### Jogadores")
        st.markdown("### Jogadores")
        
        if metric_key and not filtered_matches.empty:
            match_ids_list = tuple(filtered_matches['match_id'].tolist())
            
            p_id_list = selected_team_ids if selected_team_ids else None
            if sel_players:
                p_name_filtered = df_players[df_players['name'].isin(sel_players)]['player_id'].tolist()
            else:
                p_name_filtered = None
                
            df_ts = fetch_player_streaks_sql(
                metric_key=metric_key,
                match_ids=match_ids_list,
                club_stat_type=club_stat_type,
                valid_team_ids=p_id_list,
                valid_player_ids=p_name_filtered,
                valid_positions=sel_pos if sel_pos else None
            )
        else:
            df_ts = pd.DataFrame()
        
        if not df_ts.empty:
            # Pandas Calculation for Streaks
            if op == ">=": mask = df_ts['value'] >= target_val
            elif op == ">": mask = df_ts['value'] > target_val
            elif op == "<=": mask = df_ts['value'] <= target_val
            elif op == "<": mask = df_ts['value'] < target_val
            else: mask = df_ts['value'] == target_val
            
            df_ts['hit'] = mask
            
            results = []
            
            results = []
            
            # Group by ID and Name
            for (pid, entity), group in df_ts.groupby(['player_id', 'Entity']):
                group = group.sort_values('match_date')
                
                # --- 1. Filter Logic ---
                total_games = len(group)
                total_minutes = group['minutes'].sum()
                
                if total_games < min_games:
                    continue
                if total_minutes < min_minutes:
                    continue

                # --- 2. Streak Calculation ---
                hits = group['hit'].sum()
                pct = (hits / total_games) * 100
                
                max_streak_len = 0
                current_streak_len = 0
                current_fail_streak = 0
                
                # Iterate chronologically
                for hit_status in group['hit']:
                    if hit_status:
                        current_streak_len += 1
                        current_fail_streak = 0
                        
                        if current_streak_len > max_streak_len:
                            max_streak_len = current_streak_len
                    else:
                        current_streak_len = 0
                        current_fail_streak += 1
                        
                # Determine active status based on the VERY LAST game played
                is_hit = group.iloc[-1]['hit']
                
                # If they are on a positive streak, show the positive count
                # If they are on a negative streak, show the negative count
                display_current_streak = current_streak_len if is_hit else current_fail_streak
                
                status_icon = "✅" if is_hit else "❌"
                status_str = "Sim" if is_hit else "Não"
                
                player_team_ids = group['team_id'].unique()
                club_names = df_clubs[df_clubs['team_id'].isin(player_team_ids)]['name'].tolist()
                club_name = ", ".join(club_names) if club_names else "Desconhecido"
                
                # --- 3. Extract Details (Basic only, DataFrames deferred) ---
                results.append({
                    "Jogador": entity,
                    "Clube": club_name,
                    "Jogos": total_games,
                    "Minutos": total_minutes,
                    "% Sucesso": pct,
                    "Sequência Atual": display_current_streak,
                    "Status Atual": status_str,
                    "Icon": status_icon,
                    "Maior Seq. (Sucesso)": max_streak_len,
                    "player_id": pid  # Need to keep this for deferred lookup
                })
                
            if not results:
                st.info("Nenhum jogador encontrado com os filtros selecionados.")
            else:
                df_res = pd.DataFrame(results)
                # Sort: 1. Current Streak Desc, 2. Hit Rate Desc
                df_res = df_res.sort_values(by=['Sequência Atual', '% Sucesso'], ascending=[False, False])
                
                # Main Table Display (Exclude nested DataFrames)
                cols_to_show = ["Jogador", "Clube", "Jogos", "Minutos", "% Sucesso", "Sequência Atual", "Status Atual", "Maior Seq. (Sucesso)"]
                
                st.dataframe(
                    df_res[cols_to_show].style.format({"% Sucesso": "{:.1f}%", "Minutos": "{:.0f}"}),
                    width="stretch",
                    hide_index=True
                )
                
                st.divider()
                st.subheader("🔎 Detalhes da Sequência")
                
                show_details = st.checkbox("Carregar detalhes por jogador", value=False)
                
                if show_details:
                    # Selector for Drill-down
                    player_list = df_res['Jogador'].tolist()
                    sel_player_detail = st.selectbox("Selecione um Jogador para ver os jogos:", player_list)
                    
                    if sel_player_detail:
                        # Retrieve original row stats
                        row = df_res[df_res['Jogador'] == sel_player_detail].iloc[0]
                        p_id = row['player_id']
                        
                        st.markdown(f"**{row['Jogador']}** | Seq. Atual: **{row['Sequência Atual']}** ({row['Status Atual']}) | Maior: **{row['Maior Seq. (Sucesso)']}**")
                        
                        # Deferred DataFrame Generation
                        c_cur, c_best = st.columns(2)
                        
                        # Re-obtain the group for this specific player
                        group = df_ts[df_ts['player_id'] == p_id].copy()
                        group = group.sort_values('match_date')
                        group['streak_id'] = (group['hit'] != group['hit'].shift()).cumsum()
                        
                        def format_matches(sub_df):
                            if sub_df.empty: return pd.DataFrame()
                            out = sub_df.copy()
                            out['Data'] = pd.to_datetime(out['match_date'], unit='s').dt.strftime('%d/%m/%Y')
                            out['Jogo'] = out['home_team_name'] + ' vs ' + out['away_team_name']
                            return out[['Data', 'Jogo', 'value']].rename(columns={'value': 'Valor'})
    
                        last_streak_id = group.iloc[-1]['streak_id']
                        current_seq_df = group[group['streak_id'] == last_streak_id]
                        df_current_matches = format_matches(current_seq_df)
    
                        streaks = group[group['hit'] == True].groupby('streak_id').size()
                        df_best_matches = pd.DataFrame()
                        if not streaks.empty:
                            best_streak_id = streaks.idxmax()
                            best_seq_df = group[group['streak_id'] == best_streak_id]
                            df_best_matches = format_matches(best_seq_df)
    
                        with c_cur:
                            st.caption(f"Sequência Atual ({row['Sequência Atual']} jogos)")
                            if not df_current_matches.empty:
                                st.dataframe(df_current_matches, hide_index=True, width="stretch")
                            else:
                                st.write("-")
    
                        with c_best:
                            st.caption(f"Melhor Sequência ({row['Maior Seq. (Sucesso)']} jogos)")
                            if not df_best_matches.empty:
                                st.dataframe(df_best_matches, hide_index=True, width="stretch")
                            else:
                                st.write("Nenhuma sequência positiva.")
        else:
            st.info("Sem dados para calcular sequências.")
    else:
         st.info("Métrica indisponível para sequência de jogadores.")
