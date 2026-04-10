import streamlit as st
import pandas as pd
import numpy as np
import datetime
import pandas_gbq
from google.oauth2 import service_account
from sofascore_metrics import METRICS_CONFIG, get_metric_info

# --- Configuration ---
st.set_page_config(page_title="Sofascore Analytics", layout="wide", page_icon="📈")
PROJECT_ID = 'betterbet-467621'
DATASET_ID = 'sofascore'

# --- CSS Styling (Premium Dark/Glassmorphism Base) ---
st.markdown("""
<style>
    /* Global Styling */
    .stApp {
        background-color: #0e1117;
        color: #e0e6ed;
    }
    
    /* Headers */
    h1, h2, h3 {
        font-family: 'Inter', sans-serif;
    }
    .title-gradient {
        background: linear-gradient(90deg, #1f77b4 0%, #00d4ff 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 800;
        margin-bottom: 20px;
    }
    
    /* Dataframes Styling overrides */
    .stDataFrame {
        border-radius: 12px;
        overflow: hidden;
        box-shadow: 0 4px 15px rgba(0,0,0,0.3);
    }
</style>
""", unsafe_allow_html=True)

CATEGORIES_UI_PT = {
    "Finalização": [
        "Gols", "xG (Gols Esperados)", 
        "Finalizações Totais", "Finalizações no Alvo", "% Acerto Finalização",
        "xGoT (No Alvo)", "Grandes Chances Convertidas", "Grandes Chances Perdidas",
        "Chutes pra Fora", "Chutes Bloqueados", "Na Trave", 
        "Pênalti Sofrido", "Gols Contra"
    ],
    "Passes": [
        "Assistências", "xA (Assistências Esperadas)", "Grandes Chances Criadas", "Passes Decisivos", 
        "Passes Totais", "Passes Certos", "% Acerto Passes",
        "Passes Totais Campo Rival", "Passes Certos Campo Rival", "% Passes C. Rival",
        "Passes Certos Campo Defesa", "Passes em Profundidade Certos",
        "Bolas Longas Totais", "Bolas Longas Certas", "% Bolas Longas", 
        "Cruzamentos Totais", "Cruzamentos Certos", "% Cruzamentos"
    ],
    "Posse de Bola": [
        "Posse de Bola (%)", "Ações com a Bola", "Toques na Área Rival", 
        "Conduções", "Conduções Progressivas", "Domínios Errados", "Perda de Posse", 
        "Faltas Sofridas", "Impedimentos"
    ],
    "Duelos": [
         "Duelos Ganhos", "Duelos Perdidos", "% Duelos Ganhos",
         "Duelos Aéreos Ganhos", "Duelos Aéreos Perdidos", "% Aéreos Ganhos",
         "Dribles Totais", "Dribles Certos", "% Acerto Dribles",
         "Dribles Sofridos", "Desarmado"
    ],
    "Defesa": [
        "Desarmes Totais", "Desarmes Certos", "% Acerto Desarmes",
        "Interceptações", "Bolas Recuperadas", "Cortes", "Cortes na Linha", "Bloqueios", 
        "Erro Capital (Gol)", "Erro Capital (Chute)", "Pênalti Cometido", "Faltas Cometidas"
    ],
    "Goleiro": [
        "Defesas", "Defesas Difíceis", "Defesas Dentro da Área", "Socos",
        "Saídas Líbero Certas", "Pênaltis Defendidos", "Gols Evitados"
    ]
}

PAIRS_PERCENTAGE = [
    ("Finalizações Totais", "Finalizações no Alvo", "% Acerto Finalização"),
    ("Passes Totais", "Passes Certos", "% Acerto Passes"),
    ("Bolas Longas Totais", "Bolas Longas Certas", "% Bolas Longas"),
    ("Cruzamentos Totais", "Cruzamentos Certos", "% Cruzamentos"),
    ("Passes Totais Campo Rival", "Passes Certos Campo Rival", "% Passes C. Rival"),
    ("Dribles Totais", "Dribles Certos", "% Acerto Dribles"),
    ("Desarmes Totais", "Desarmes Certos", "% Acerto Desarmes")
]

# --- Helper Functions (BigQuery) ---
@st.cache_resource
def get_bq_credentials():
    try:
        if "gcp_service_account" in st.secrets:
            creds_dict = dict(st.secrets["gcp_service_account"])
            return service_account.Credentials.from_service_account_info(creds_dict)
    except Exception:
        pass
    return None

credentials = get_bq_credentials()

@st.cache_data(ttl=3600*24)
def load_base_data():
    q_tournaments = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.tournaments`"
    df_tournaments = pandas_gbq.read_gbq(q_tournaments, project_id=PROJECT_ID, credentials=credentials).drop_duplicates(subset=['unique_tournament_id', 'season_id'])
    
    q_matches = f"SELECT match_id, tournament_id, season_id, round_id, match_date, home_team_id, away_team_id, home_team_name, away_team_name, round_name, round_slug FROM `{PROJECT_ID}.{DATASET_ID}.matches`"
    df_matches = pandas_gbq.read_gbq(q_matches, project_id=PROJECT_ID, credentials=credentials).drop_duplicates(subset=['match_id'])
    _numeric = pd.to_numeric(df_matches['match_date'], errors='coerce')
    if _numeric.notna().mean() > 0.5:
        df_matches['match_date'] = pd.to_datetime(_numeric, unit='s', errors='coerce')
    else:
        _parsed = pd.to_datetime(df_matches['match_date'], errors='coerce', utc=True)
        df_matches['match_date'] = _parsed.dt.tz_convert(None)
    
    q_clubs = f"SELECT team_id, name FROM `{PROJECT_ID}.{DATASET_ID}.clubs`"
    df_clubs = pandas_gbq.read_gbq(q_clubs, project_id=PROJECT_ID, credentials=credentials).drop_duplicates(subset=['team_id'])
    
    q_players = f"SELECT player_id, team_id, name, position FROM `{PROJECT_ID}.{DATASET_ID}.players`"
    df_players = pandas_gbq.read_gbq(q_players, project_id=PROJECT_ID, credentials=credentials).drop_duplicates(subset=['player_id'])
    
    return df_tournaments, df_matches, df_clubs, df_players

@st.cache_data(ttl=3600*24)
def fetch_player_category_stats(category, match_ids, valid_team_ids=None, valid_player_ids=None, valid_positions=None, group_mode="Acumulado"):
    if not match_ids: return pd.DataFrame()
    match_ids_str = ",".join(map(str, match_ids))

    pt_metrics = CATEGORIES_UI_PT.get(category, [])
    metric_keys = []
    key_to_pt = {}
    
    for pt in pt_metrics:
        mkey, source = get_metric_info(pt)
        if source == 'player':
            metric_keys.append(mkey)
            key_to_pt[mkey] = pt
            
    if not metric_keys:
        return pd.DataFrame() # No player metrics in this category
         
    keys_str = ",".join([f"'{k}'" for k in metric_keys + ['minutesPlayed']])
    
    filters = []
    if valid_team_ids:
        team_ids_str = ",".join(map(str, valid_team_ids))
        filters.append(f"psl.team_id IN ({team_ids_str})")
        
    if valid_player_ids:
        p_ids_str = ",".join(map(str, valid_player_ids))
        filters.append(f"psl.player_id IN ({p_ids_str})")
        
    final_filters = []
    if valid_positions:
        pos_str = ",".join(f"'{p}'" for p in valid_positions)
        final_filters.append(f"p.position IN ({pos_str})")
        
    where_clause = (" AND " + " AND ".join(filters)) if filters else ""
    final_where_clause = (" WHERE " + " AND ".join(final_filters)) if final_filters else ""
    
    join_matches_cte = ""
    select_m_cte = ""
    group_m_cte = ""
    select_m_final = ""
    group_m_final = ""
    join_cte_matches = "pm.player_id = pmx.player_id"
    pivot_index = ['Jogador', 'Clube', 'Pos', 'Jogos', 'Minutos']

    if group_mode != "Acumulado":
        join_matches_cte = f"""
        JOIN `{PROJECT_ID}.{DATASET_ID}.matches` m ON psl.match_id = m.match_id
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.tournaments` t ON m.tournament_id = t.unique_tournament_id AND m.season_id = t.season_id
        """
        if group_mode == "Por temporada e competição":
            select_m_cte = "t.name as comp_name, t.season_year as sea_year,"
            group_m_cte = ", t.name, t.season_year"
            select_m_final = "pmx.comp_name as Competicao, pmx.sea_year as Temporada,"
            group_m_final = ", Competicao, Temporada"
            join_cte_matches += " AND pm.comp_name = pmx.comp_name AND pm.sea_year = pmx.sea_year"
            pivot_index = ['Jogador', 'Clube', 'Pos', 'Competicao', 'Temporada', 'Jogos', 'Minutos']
        elif group_mode == "Por temporada":
            select_m_cte = "t.season_year as sea_year,"
            group_m_cte = ", t.season_year"
            select_m_final = "pmx.sea_year as Temporada,"
            group_m_final = ", Temporada"
            join_cte_matches += " AND pm.sea_year = pmx.sea_year"
            pivot_index = ['Jogador', 'Clube', 'Pos', 'Temporada', 'Jogos', 'Minutos']
        elif group_mode == "Por competição":
            select_m_cte = "t.name as comp_name,"
            group_m_cte = ", t.name"
            select_m_final = "pmx.comp_name as Competicao,"
            group_m_final = ", Competicao"
            join_cte_matches += " AND pm.comp_name = pmx.comp_name"
            pivot_index = ['Jogador', 'PlayerID', 'Clube', 'Pos', 'Competicao', 'Jogos', 'Minutos']
    
    query = f"""
    WITH PlayerMatches AS (
        SELECT psl.player_id, 
               {select_m_cte}
               COUNT(DISTINCT psl.match_id) as Jogos,
               SUM(CAST(psl.value AS FLOAT64)) as Minutos
        FROM `{PROJECT_ID}.{DATASET_ID}.player_stats_log` psl
        {join_matches_cte}
        WHERE psl.match_id IN ({match_ids_str}) AND psl.metric_key = 'minutesPlayed' {where_clause}
        GROUP BY psl.player_id {group_m_cte}
    ),
    PlayerMetrics AS (
        SELECT psl.player_id, 
               {select_m_cte}
               psl.metric_key, SUM(CAST(psl.value AS FLOAT64)) as Valor_Total,
               MAX(psl.team_id) as team_id 
        FROM `{PROJECT_ID}.{DATASET_ID}.player_stats_log` psl
        {join_matches_cte}
        WHERE psl.match_id IN ({match_ids_str}) AND psl.metric_key IN ({keys_str}) {where_clause}
        GROUP BY psl.player_id, psl.metric_key {group_m_cte}
    ),
    PlayerClubs AS (
        SELECT psl.player_id,
               STRING_AGG(DISTINCT COALESCE(c.name, 'Desconhecido') ORDER BY c.name) as Clube
        FROM `{PROJECT_ID}.{DATASET_ID}.player_stats_log` psl
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.clubs` c ON c.team_id = psl.team_id
        WHERE psl.match_id IN ({match_ids_str}) AND psl.metric_key = 'minutesPlayed'
        {("AND psl.team_id IN (" + ",".join(map(str, valid_team_ids)) + ")") if valid_team_ids else ""}
        {("AND psl.player_id IN (" + ",".join(map(str, valid_player_ids)) + ")") if valid_player_ids else ""}
        GROUP BY psl.player_id
    )
    SELECT 
        p.name as Jogador,
        pmx.player_id as PlayerID,
        COALESCE(pc.Clube, 'Desconhecido') as Clube,
        MAX(p.position) as Pos,
        {select_m_final}
        pm.Jogos,
        pm.Minutos,
        pmx.metric_key,
        pmx.Valor_Total
    FROM PlayerMetrics pmx
    JOIN PlayerMatches pm ON {join_cte_matches}
    LEFT JOIN PlayerClubs pc ON pc.player_id = pmx.player_id
    JOIN `{PROJECT_ID}.{DATASET_ID}.players` p ON p.player_id = pmx.player_id
    {final_where_clause}
    GROUP BY p.name, pmx.player_id, Clube, pm.Jogos, pm.Minutos, pmx.metric_key, pmx.Valor_Total {group_m_final}
    """
    
    df_raw = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials)
    if df_raw.empty: return df_raw
    
    df_raw['metric_pt'] = df_raw['metric_key'].map(key_to_pt)
    df_raw = df_raw[df_raw['metric_pt'].notna()]
    
    df_pivot = df_raw.pivot_table(index=pivot_index, 
                                  columns='metric_pt', 
                                  values='Valor_Total', 
                                  aggfunc='sum').reset_index()
                                  
    df_pivot = df_pivot.fillna(0)
    
    for total_col, cert_col, pct_col in PAIRS_PERCENTAGE:
        if total_col in df_pivot.columns and cert_col in df_pivot.columns:
            df_pivot[pct_col] = np.where(df_pivot[total_col] > 0, (df_pivot[cert_col] / df_pivot[total_col]) * 100.0, 0)
             
    if "Duelos Ganhos" in df_pivot.columns and "Duelos Perdidos" in df_pivot.columns:
        tot = df_pivot["Duelos Ganhos"] + df_pivot["Duelos Perdidos"]
        df_pivot["% Duelos Ganhos"] = np.where(tot > 0, (df_pivot["Duelos Ganhos"] / tot) * 100.0, 0)
        
    if "Duelos Aéreos Ganhos" in df_pivot.columns and "Duelos Aéreos Perdidos" in df_pivot.columns:
        tot_ae = df_pivot["Duelos Aéreos Ganhos"] + df_pivot["Duelos Aéreos Perdidos"]
        df_pivot["% Aéreos Ganhos"] = np.where(tot_ae > 0, (df_pivot["Duelos Aéreos Ganhos"] / tot_ae) * 100.0, 0)
    
    return df_pivot

@st.cache_data(ttl=3600*24)
def fetch_club_category_stats(category, match_ids, valid_team_ids=None, group_mode="Acumulado", perspective="A favor"):
    if not match_ids: return pd.DataFrame()
    match_ids_str = ",".join(map(str, match_ids))

    pt_metrics = CATEGORIES_UI_PT.get(category, [])
    player_keys = []
    match_keys = []
    key_to_pt = {}
    
    for pt in pt_metrics:
        mkey, source = get_metric_info(pt)
        if mkey:
            if source == 'player':
                player_keys.append(mkey)
            elif source == 'match':
                match_keys.append(mkey)
            key_to_pt[mkey] = pt
            
    if not player_keys and not match_keys:
        return pd.DataFrame() # No metrics in this category
        
    p_keys_str = ",".join([f"'{k}'" for k in player_keys]) if player_keys else ""
    m_keys_str = ",".join([f"'{k}'" for k in match_keys]) if match_keys else ""
    team_ids_str = ",".join(map(str, valid_team_ids)) if valid_team_ids else ""
    
    select_m_cte = ""
    group_m_cte = ""
    select_m_final = ""
    group_m_final = ""
    join_cte_matches = "pm.team_id = cmx.team_id"
    pivot_index = ['Clube', 'Jogos', 'Minutos']
    join_tournaments_str = ""

    if group_mode != "Acumulado":
        join_tournaments_str = f"LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.tournaments` t ON m.tournament_id = t.unique_tournament_id AND m.season_id = t.season_id"
        if group_mode == "Por temporada e competição":
            select_m_cte = "t.name as comp_name, t.season_year as sea_year,"
            group_m_cte = ", t.name, t.season_year"
            select_m_final = "cmx.comp_name as Competicao, cmx.sea_year as Temporada,"
            group_m_final = ", Competicao, Temporada"
            join_cte_matches += " AND pm.comp_name = cmx.comp_name AND pm.sea_year = cmx.sea_year"
            pivot_index = ['Clube', 'Competicao', 'Temporada', 'Jogos', 'Minutos']
        elif group_mode == "Por temporada":
            select_m_cte = "t.season_year as sea_year,"
            group_m_cte = ", t.season_year"
            select_m_final = "cmx.sea_year as Temporada,"
            group_m_final = ", Temporada"
            join_cte_matches += " AND pm.sea_year = cmx.sea_year"
            pivot_index = ['Clube', 'Temporada', 'Jogos', 'Minutos']
        elif group_mode == "Por competição":
            select_m_cte = "t.name as comp_name,"
            group_m_cte = ", t.name"
            select_m_final = "cmx.comp_name as Competicao,"
            group_m_final = ", Competicao"
            join_cte_matches += " AND pm.comp_name = cmx.comp_name"
            pivot_index = ['Clube', 'Competicao', 'Jogos', 'Minutos']

    ctes = []
    
    ctes.append(f"""
    ClubMatches AS (
        SELECT psl.team_id,
               {select_m_cte}
               COUNT(DISTINCT psl.match_id) as Jogos,
               COUNT(DISTINCT psl.match_id) * 90 as Minutos
        FROM `{PROJECT_ID}.{DATASET_ID}.player_stats_log` psl
        JOIN `{PROJECT_ID}.{DATASET_ID}.matches` m ON psl.match_id = m.match_id
        {join_tournaments_str}
        WHERE psl.match_id IN ({match_ids_str}) AND psl.metric_key = 'minutesPlayed'
        {("AND psl.team_id IN (" + team_ids_str + ")") if valid_team_ids else ""}
        GROUP BY psl.team_id {group_m_cte}
    )
    """)
    
    if player_keys:
        if perspective == "Sofrido":
            ctes.append(f"""
            ClubPlayerStats AS (
                SELECT target_team_id as team_id,
                       {select_m_cte}
                       psl.metric_key, SUM(CAST(psl.value AS FLOAT64)) as Valor_Total
                FROM `{PROJECT_ID}.{DATASET_ID}.player_stats_log` psl
                JOIN `{PROJECT_ID}.{DATASET_ID}.matches` m ON psl.match_id = m.match_id
                {join_tournaments_str}
                CROSS JOIN UNNEST([m.home_team_id, m.away_team_id]) as target_team_id
                WHERE psl.match_id IN ({match_ids_str}) AND psl.metric_key IN ({p_keys_str})
                AND target_team_id != psl.team_id
                {("AND target_team_id IN (" + team_ids_str + ")") if valid_team_ids else ""}
                GROUP BY target_team_id, psl.metric_key {group_m_cte}
            )
            """)
        else:
            ctes.append(f"""
            ClubPlayerStats AS (
                SELECT psl.team_id,
                       {select_m_cte}
                       psl.metric_key, SUM(CAST(psl.value AS FLOAT64)) as Valor_Total
                FROM `{PROJECT_ID}.{DATASET_ID}.player_stats_log` psl
                JOIN `{PROJECT_ID}.{DATASET_ID}.matches` m ON psl.match_id = m.match_id
                {join_tournaments_str}
                WHERE psl.match_id IN ({match_ids_str}) AND psl.metric_key IN ({p_keys_str})
                {("AND psl.team_id IN (" + team_ids_str + ")") if valid_team_ids else ""}
                GROUP BY psl.team_id, psl.metric_key {group_m_cte}
            )
            """)

    if match_keys:
        val_expr = "IF(m.home_team_id = team_id, msl.away_value, msl.home_value)" if perspective == "Sofrido" else "IF(m.home_team_id = team_id, msl.home_value, msl.away_value)"
        ctes.append(f"""
        ClubMatchStats AS (
            SELECT team_id,
                   {select_m_cte}
                   msl.metric_key, 
                   SUM(CAST({val_expr} AS FLOAT64)) as Valor_Total
            FROM `{PROJECT_ID}.{DATASET_ID}.match_stats_log` msl
            JOIN `{PROJECT_ID}.{DATASET_ID}.matches` m ON msl.match_id = m.match_id
            {join_tournaments_str}
            CROSS JOIN UNNEST([m.home_team_id, m.away_team_id]) as team_id
            WHERE msl.match_id IN ({match_ids_str}) AND msl.metric_key IN ({m_keys_str}) AND msl.period = 'ALL'
            {("AND team_id IN (" + team_ids_str + ")") if valid_team_ids else ""}
            GROUP BY team_id, msl.metric_key {group_m_cte}
        )
        """)

    union_parts = []
    if player_keys: union_parts.append("SELECT * FROM ClubPlayerStats")
    if match_keys: union_parts.append("SELECT * FROM ClubMatchStats")
    union_query = " UNION ALL ".join(union_parts)

    ctes.append(f"ClubMetrics AS ({union_query})")

    query = "WITH " + ", ".join(ctes) + f"""
    SELECT 
        COALESCE(c.name, 'Desconhecido') as Clube,
        {select_m_final}
        pm.Jogos,
        pm.Minutos,
        cmx.metric_key,
        cmx.Valor_Total
    FROM ClubMetrics cmx
    JOIN ClubMatches pm ON {join_cte_matches}
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.clubs` c ON c.team_id = cmx.team_id
    GROUP BY Clube, pm.Jogos, pm.Minutos, cmx.metric_key, cmx.Valor_Total {group_m_final}
    """
    
    df_raw = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials)
    if df_raw.empty: return df_raw
    
    df_raw['metric_pt'] = df_raw['metric_key'].map(key_to_pt)
    df_raw = df_raw[df_raw['metric_pt'].notna()]
    
    df_pivot = df_raw.pivot_table(index=pivot_index, 
                                  columns='metric_pt', 
                                  values='Valor_Total', 
                                  aggfunc='sum').reset_index()
                                  
    df_pivot = df_pivot.fillna(0)
    
    for total_col, cert_col, pct_col in PAIRS_PERCENTAGE:
        if total_col in df_pivot.columns and cert_col in df_pivot.columns:
            df_pivot[pct_col] = np.where(df_pivot[total_col] > 0, (df_pivot[cert_col] / df_pivot[total_col]) * 100.0, 0)
             
    if "Duelos Ganhos" in df_pivot.columns and "Duelos Perdidos" in df_pivot.columns:
        tot = df_pivot["Duelos Ganhos"] + df_pivot["Duelos Perdidos"]
        df_pivot["% Duelos Ganhos"] = np.where(tot > 0, (df_pivot["Duelos Ganhos"] / tot) * 100.0, 0)
        
    if "Duelos Aéreos Ganhos" in df_pivot.columns and "Duelos Aéreos Perdidos" in df_pivot.columns:
        tot_ae = df_pivot["Duelos Aéreos Ganhos"] + df_pivot["Duelos Aéreos Perdidos"]
        df_pivot["% Aéreos Ganhos"] = np.where(tot_ae > 0, (df_pivot["Duelos Aéreos Ganhos"] / tot_ae) * 100.0, 0)
    
    return df_pivot

@st.cache_data(ttl=3600*24)
def fetch_single_game_records(target, category, metric_sel, match_ids, valid_team_ids=None, skip_limit=False):
    if not match_ids: return pd.DataFrame()
    match_ids_str = ",".join(map(str, match_ids))

    pt_metrics = CATEGORIES_UI_PT.get(category, [])
    player_keys = []
    match_keys = []
    key_to_pt = {}
    
    for pt in pt_metrics:
        mkey, source = get_metric_info(pt)
        if mkey:
            if source == 'player':
                player_keys.append(mkey)
            elif source == 'match':
                match_keys.append(mkey)
            key_to_pt[mkey] = pt
            
    if not player_keys and not match_keys:
        return pd.DataFrame()
        
    if target == "Jogador":
        if 'minutesPlayed' not in player_keys:
            player_keys.append('minutesPlayed')
            key_to_pt['minutesPlayed'] = 'Minutos Jogados'
            
    p_keys_str = ",".join([f"'{k}'" for k in player_keys]) if player_keys else ""
    m_keys_str = ",".join([f"'{k}'" for k in match_keys]) if match_keys else ""
    team_ids_str = ",".join(map(str, valid_team_ids)) if valid_team_ids else ""
    
    ctes = []

    if target == "Jogador":
        if not player_keys: return pd.DataFrame()
        
        query = f"""
        WITH PlayerMatches AS (
            SELECT psl.player_id, psl.match_id, psl.team_id,
                   psl.metric_key, SUM(CAST(psl.value AS FLOAT64)) as Valor_Total
            FROM `{PROJECT_ID}.{DATASET_ID}.player_stats_log` psl
            WHERE psl.match_id IN ({match_ids_str}) AND psl.metric_key IN ({p_keys_str})
            {("AND psl.team_id IN (" + team_ids_str + ")") if valid_team_ids else ""}
            GROUP BY psl.player_id, psl.match_id, psl.team_id, psl.metric_key
        )
        SELECT DISTINCT
            p.name as Jogador,
            pm.player_id as PlayerID,
            COALESCE(c.name, 'Desconhecido') as Clube,
            t.name as Competicao,
            t.season_year as Temporada,
            CONCAT(m.home_team_name, ' vs ', m.away_team_name) as Jogo,
            m.match_date as Data,
            pm.match_id as MatchID,
            pm.metric_key,
            pm.Valor_Total
        FROM PlayerMatches pm
        JOIN `{PROJECT_ID}.{DATASET_ID}.matches` m ON pm.match_id = m.match_id
        JOIN `{PROJECT_ID}.{DATASET_ID}.players` p ON p.player_id = pm.player_id AND p.team_id = pm.team_id
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.clubs` c ON c.team_id = pm.team_id
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.tournaments` t ON m.tournament_id = t.unique_tournament_id AND m.season_id = t.season_id
        """
        pivot_index = ['Jogador', 'PlayerID', 'Clube', 'Competicao', 'Temporada', 'Jogo', 'Data', 'MatchID']
        
    else: # Clube
        if player_keys:
            ctes.append(f"""
            ClubPlayerStats AS (
                SELECT psl.team_id, psl.match_id,
                       psl.metric_key, SUM(CAST(psl.value AS FLOAT64)) as Valor_Total
                FROM `{PROJECT_ID}.{DATASET_ID}.player_stats_log` psl
                WHERE psl.match_id IN ({match_ids_str}) AND psl.metric_key IN ({p_keys_str})
                {("AND psl.team_id IN (" + team_ids_str + ")") if valid_team_ids else ""}
                GROUP BY psl.team_id, psl.match_id, psl.metric_key
            )
            """)

        if match_keys:
            ctes.append(f"""
            ClubMatchStats AS (
                SELECT team_id, msl.match_id,
                       msl.metric_key, 
                       SUM(CAST(IF(m.home_team_id = team_id, msl.home_value, msl.away_value) AS FLOAT64)) as Valor_Total
                FROM `{PROJECT_ID}.{DATASET_ID}.match_stats_log` msl
                JOIN `{PROJECT_ID}.{DATASET_ID}.matches` m ON msl.match_id = m.match_id
                CROSS JOIN UNNEST([m.home_team_id, m.away_team_id]) as team_id
                WHERE msl.match_id IN ({match_ids_str}) AND msl.metric_key IN ({m_keys_str}) AND msl.period = 'ALL'
                {("AND team_id IN (" + team_ids_str + ")") if valid_team_ids else ""}
                GROUP BY team_id, msl.match_id, msl.metric_key
            )
            """)
            
        union_parts = []
        if player_keys: union_parts.append("SELECT * FROM ClubPlayerStats")
        if match_keys: union_parts.append("SELECT * FROM ClubMatchStats")
        
        if not union_parts: return pd.DataFrame()
        union_query = " UNION ALL ".join(union_parts)

        query = "WITH " + ", ".join(ctes) + f"""
        , ClubMetrics AS ({union_query})
        SELECT DISTINCT
            COALESCE(c.name, 'Desconhecido') as Clube,
            t.name as Competicao,
            t.season_year as Temporada,
            CONCAT(m.home_team_name, ' vs ', m.away_team_name) as Jogo,
            m.match_date as Data,
            cmx.metric_key,
            cmx.Valor_Total
        FROM ClubMetrics cmx
        JOIN `{PROJECT_ID}.{DATASET_ID}.matches` m ON m.match_id = cmx.match_id
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.clubs` c ON c.team_id = cmx.team_id
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.tournaments` t ON m.tournament_id = t.unique_tournament_id AND m.season_id = t.season_id
        """
        pivot_index = ['Clube', 'Competicao', 'Temporada', 'Jogo', 'Data', 'MatchID']
        
    df_raw = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials)
    if df_raw.empty: return df_raw
    
    _num_date = pd.to_numeric(df_raw['Data'], errors='coerce')
    if _num_date.notna().mean() > 0.5:
        df_raw['Data'] = pd.to_datetime(_num_date, unit='s', errors='coerce').dt.strftime('%d/%m/%Y')
    else:
        df_raw['Data'] = pd.to_datetime(df_raw['Data'], errors='coerce').dt.strftime('%d/%m/%Y')
    df_raw['metric_pt'] = df_raw['metric_key'].map(key_to_pt)
    df_raw = df_raw[df_raw['metric_pt'].notna()]
    
    df_pivot = df_raw.pivot_table(index=pivot_index, 
                                  columns='metric_pt', 
                                  values='Valor_Total', 
                                  aggfunc='sum').reset_index()
                                  
    df_pivot = df_pivot.fillna(0)
    
    for total_col, cert_col, pct_col in PAIRS_PERCENTAGE:
        if total_col in df_pivot.columns and cert_col in df_pivot.columns:
            df_pivot[pct_col] = np.where(df_pivot[total_col] > 0, (df_pivot[cert_col] / df_pivot[total_col]) * 100.0, 0)
             
    if "Duelos Ganhos" in df_pivot.columns and "Duelos Perdidos" in df_pivot.columns:
        tot = df_pivot["Duelos Ganhos"] + df_pivot["Duelos Perdidos"]
        df_pivot["% Duelos Ganhos"] = np.where(tot > 0, (df_pivot["Duelos Ganhos"] / tot) * 100.0, 0)
        
    if "Duelos Aéreos Ganhos" in df_pivot.columns and "Duelos Aéreos Perdidos" in df_pivot.columns:
        tot_ae = df_pivot["Duelos Aéreos Ganhos"] + df_pivot["Duelos Aéreos Perdidos"]
        df_pivot["% Aéreos Ganhos"] = np.where(tot_ae > 0, (df_pivot["Duelos Aéreos Ganhos"] / tot_ae) * 100.0, 0)
        
    if metric_sel in df_pivot.columns:
        if not skip_limit:
            df_pivot = df_pivot.sort_values(by=metric_sel, ascending=False).head(100)
    
    return df_pivot
    
def apply_calc_mode(df_pivot, pt_metrics, calc_mode):
    if df_pivot.empty: return df_pivot
    
    df_res = df_pivot.copy()
    
    # Colunas de % nunca devem ser afetadas pela mudança de modo
    pct_cols_protected = {pct_col for _, _, pct_col in PAIRS_PERCENTAGE}
    pct_cols_protected |= {"% Duelos Ganhos", "% Aéreos Ganhos"}
    
    cols_to_transform = [c for c in pt_metrics if c in df_res.columns and c not in pct_cols_protected]
    
    if calc_mode == "Total":
        return df_res
        
    for col in cols_to_transform:
        if calc_mode == "Média por Jogo":
            df_res[col] = np.where(df_res['Jogos'] > 0, df_res[col] / df_res['Jogos'], 0)
        elif calc_mode == "Por 90 min":
            df_res[col] = np.where(df_res['Minutos'] > 0, (df_res[col] * 90.0) / df_res['Minutos'], 0)
            
    return df_res

def process_streaks_and_forms(df_records, metric_sel, target_val, condition, group_col, df_matches_base):
    if df_records.empty or metric_sel not in df_records.columns: return pd.DataFrame()
    
    if condition == ">=":
        hits = df_records[metric_sel] >= target_val
    elif condition == ">":
        hits = df_records[metric_sel] > target_val
    elif condition == "<=":
        hits = df_records[metric_sel] <= target_val
    elif condition == "<":
        hits = df_records[metric_sel] < target_val
    else: 
        hits = df_records[metric_sel] == target_val
        
    df = df_records.copy()
    df['hit'] = hits
    
    df['Data_dt'] = pd.to_datetime(df['Data'], format='%d/%m/%Y')

    results = []
    
    grp_c = 'PlayerID' if group_col == 'Jogador' else 'Clube'
    
    for entity, group in df.groupby(grp_c):
        clube_str = group['Clube'].iloc[-1]
        jogador_str = group['Jogador'].iloc[-1] if group_col == 'Jogador' else None
        
        if group_col == 'Clube':
            c_matches = df_matches_base[
                ((df_matches_base['home_team_name'] == entity) | 
                (df_matches_base['away_team_name'] == entity)) &
                (df_matches_base['status_code'].isin([100, 110, 120]))
            ].copy()
            
            if c_matches.empty:
                continue
                
            c_matches = c_matches.sort_values(by='match_date')
            
            group_hits = group[['MatchID', 'hit']].set_index('MatchID')
            c_matches['hit'] = c_matches['match_id'].map(group_hits['hit'])
            c_matches['hit'] = c_matches['hit'].fillna(False)
            
            hits_array = c_matches['hit'].values
        else:
            group_sorted = group.sort_values(by='Data_dt')
            hits_array = group_sorted['hit'].values
            
        total_games = len(hits_array)
        if total_games == 0: continue
        
        success_count = hits_array.sum()
        pct_success = (success_count / total_games) * 100.0
        
        max_s = 0
        cur_s = 0
        for h in hits_array:
            if h:
                cur_s += 1
                max_s = max(max_s, cur_s)
            else:
                cur_s = 0
                
        active_s = 0
        for h in reversed(hits_array):
            if h: active_s += 1
            else: break
            
        rec = {
            group_col: jogador_str if group_col == 'Jogador' else entity,
            'Jogos Analisados': total_games,
            'Jogos c/ Sucesso': success_count,
            '% Sucesso': pct_success,
            'Seq. Atual': active_s,
            'Melhor Seq.': max_s
        }
        if group_col == 'Jogador':
            rec['Clube/Seleção Atual'] = clube_str
            
        results.append(rec)
        
    df_res = pd.DataFrame(results)
    if not df_res.empty:
        df_res = df_res.sort_values(by=['Seq. Atual', '% Sucesso'], ascending=[False, False]).head(100)
        
        if group_col == 'Jogador':
            df_res = df_res[['Jogador', 'Clube/Seleção Atual', 'Jogos Analisados', 'Jogos c/ Sucesso', '% Sucesso', 'Seq. Atual', 'Melhor Seq.']]
        else:
            df_res = df_res[['Clube', 'Jogos Analisados', 'Jogos c/ Sucesso', '% Sucesso', 'Seq. Atual', 'Melhor Seq.']]
            
    return df_res

# Load into memory
df_tournaments, df_matches, df_clubs, df_players = load_base_data()

# --- Navigation ---
st.sidebar.markdown(f'<h1 style="color:#00d4ff; text-align:center;">Sofascore Board</h1>', unsafe_allow_html=True)
nav_page = st.sidebar.radio("Navegação", ["Ranking", "Sequências", "Comparação"])
st.sidebar.divider()

# --- Sidebar: Filter Engine ---
st.sidebar.title("🔍 Configuração de Análise")

if df_tournaments.empty:
    st.error("Base de dados vazia. Execute o crawler primeiro.")
    st.stop()

with st.sidebar.expander("🏆 Competição e Temporada", expanded=True):
    all_tournaments = df_tournaments['name'].unique().tolist()
    sel_tournaments = st.multiselect("Competição", all_tournaments, default=all_tournaments[:1])

    if sel_tournaments:
        available_seasons = df_tournaments[df_tournaments['name'].isin(sel_tournaments)]['season_year'].unique().tolist()
        available_seasons.sort(reverse=True)
        
        default_seasons = []
        if 2026 in available_seasons:
            default_seasons = [2026]
        elif "2026" in available_seasons:
            default_seasons = ["2026"]
        elif "25/26" in available_seasons:
            default_seasons = ["25/26"]
        elif available_seasons:
            default_seasons = [available_seasons[0]]
            
        sel_seasons = st.multiselect("Temporada", available_seasons, default=default_seasons)
    else:
        sel_seasons = []

    if not sel_tournaments or not sel_seasons:
        st.warning("Selecione Competição e Temporada para começar.")
        st.stop()

selected_configs = df_tournaments[
    (df_tournaments['name'].isin(sel_tournaments)) & 
    (df_tournaments['season_year'].isin(sel_seasons))
]
valid_tournament_ids = selected_configs['unique_tournament_id'].unique().tolist()
valid_season_ids = selected_configs['season_id'].unique().tolist()

if not valid_tournament_ids:
    st.warning("Combinação inválida de Competição/Temporada.")
    st.stop()

context_matches = df_matches[
    df_matches['tournament_id'].isin(valid_tournament_ids) &
    df_matches['season_id'].isin(valid_season_ids)
]

with st.sidebar.expander("📅 Período e Rodadas", expanded=False):
    valid_dates = context_matches['match_date'].dropna()
    if not context_matches.empty and not valid_dates.empty:
        db_min_date = valid_dates.min().date()
        db_max_date = valid_dates.max().date()
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

    sel_rounds_slider = None
    sel_extra_knockouts = []
    
    if not context_matches.empty:
        unique_rounds = sorted(context_matches['round_id'].dropna().unique())
        continuous_rounds = []
        if unique_rounds and unique_rounds[0] == 1:
            for r in unique_rounds:
                if len(continuous_rounds) == 0 or r == continuous_rounds[-1] + 1:
                    continuous_rounds.append(int(r))
                else:
                    break
                    
        extra_rounds = [int(r) for r in unique_rounds if r not in continuous_rounds]
        
        if len(continuous_rounds) > 1:
            use_slider = st.checkbox("Incluir Fase de Grupos", value=True)
            if use_slider:
                sel_rounds_slider = st.slider(
                    "Fase Contínua (Rodadas)", 
                    min(continuous_rounds), max(continuous_rounds), 
                    (min(continuous_rounds), max(continuous_rounds))
                )
        elif len(continuous_rounds) == 1:
            extra_rounds.insert(0, continuous_rounds[0])
            continuous_rounds = []

        if extra_rounds:
            extra_mapping = {}
            for r_id in extra_rounds:
                r_name_row = context_matches[context_matches['round_id'] == r_id]['round_name'].dropna().unique()
                display_name = r_name_row[0] if len(r_name_row) > 0 else f"Fase {r_id}"
                extra_mapping[display_name] = r_id
                
            sel_extra_names = st.multiselect("Fases Extras / Mata-Mata", list(extra_mapping.keys()), default=list(extra_mapping.keys()))
            sel_extra_knockouts = [extra_mapping[n] for n in sel_extra_names]

with st.sidebar.expander("🛡️ Clubes e Jogadores", expanded=False):
    clubs_in_matches = pd.concat([context_matches['home_team_id'], context_matches['away_team_id']]).unique()
    context_clubs = df_clubs[df_clubs['team_id'].isin(clubs_in_matches)].sort_values('name')

    all_clubs = context_clubs['name'].tolist()
    sel_clubs = st.multiselect("Clubes (Alvo principal)", all_clubs, default=[])
    selected_team_ids = []
    if sel_clubs:
        selected_team_ids = context_clubs[context_clubs['name'].isin(sel_clubs)]['team_id'].tolist()

    sel_players = []
    if selected_team_ids:
        context_players = df_players[df_players['team_id'].isin(selected_team_ids)].sort_values('name')
        sel_players = st.multiselect("Jogadores (Opcional)", context_players['name'].tolist(), default=[])

    sel_pos = st.multiselect("Posição", ["G", "D", "M", "F"], default=["G", "D", "M", "F"])

st.sidebar.divider()

if len(sel_dates) == 2:
    start_date = pd.to_datetime(sel_dates[0])
    end_date = pd.to_datetime(sel_dates[1]).replace(hour=23, minute=59, second=59)
else:
    start_date = pd.to_datetime(sel_dates[0])
    end_date = pd.to_datetime(datetime.date.today()).replace(hour=23, minute=59, second=59)

mask_matches = context_matches['match_date'].between(start_date, end_date)

if sel_rounds_slider is not None and sel_extra_knockouts:
    mask_matches &= (context_matches['round_id'].between(sel_rounds_slider[0], sel_rounds_slider[1]) | context_matches['round_id'].isin(sel_extra_knockouts))
elif sel_rounds_slider is not None:
    mask_matches &= context_matches['round_id'].between(sel_rounds_slider[0], sel_rounds_slider[1])
elif sel_extra_knockouts:
    mask_matches &= context_matches['round_id'].isin(sel_extra_knockouts)
else:
    mask_matches &= (context_matches['round_id'] == -1)

if selected_team_ids:
    mask_matches &= (context_matches['home_team_id'].isin(selected_team_ids) | context_matches['away_team_id'].isin(selected_team_ids))

filtered_matches = context_matches[mask_matches]
match_ids_list = tuple(filtered_matches['match_id'].tolist())

p_id_list = selected_team_ids if selected_team_ids else None
if sel_players:
    p_name_filtered = df_players[df_players['name'].isin(sel_players)]['player_id'].tolist()
else:
    p_name_filtered = None

if nav_page == "Ranking":
    st.markdown('<h1 class="title-gradient">🏆 Data Explorer - Rankings</h1>', unsafe_allow_html=True)
    
    col_c1, col_c2, col_c3 = st.columns([1, 1, 1])
    with col_c1:
        calc_mode = st.selectbox("Visualização das Variáveis:", ["Total", "Média por Jogo", "Por 90 min"])
    with col_c2:
        group_mode = st.selectbox("Agrupamento:", [
            "Acumulado", 
            "Por temporada e competição", 
            "Por temporada", 
            "Por competição"
        ])
    with col_c3:
        sub_c1, sub_c2 = st.columns(2)
        with sub_c1:
            min_games = st.number_input("Mín. Jogos", min_value=0, value=1)
        with sub_c2:
            min_minutes = st.number_input("Mín. Minutos", min_value=0, value=0)
        
    st.divider()
    
    tab_j, tab_c, tab_r = st.tabs(["🏃 Jogadores", "🛡️ Clubes", "🔥 Recordes em Jogo Único"])
    
    with tab_j:
        cat_j = st.pills("Selecione a Categoria:", list(CATEGORIES_UI_PT.keys()), default="Finalização")
        
        if not filtered_matches.empty:
            df_pivot_j = fetch_player_category_stats(
                category=cat_j, 
                match_ids=match_ids_list, 
                valid_team_ids=p_id_list, 
                valid_player_ids=p_name_filtered, 
                valid_positions=sel_pos if sel_pos else None,
                group_mode=group_mode
            )
            
            if not df_pivot_j.empty:
                # Filter mins and games
                df_pivot_j = df_pivot_j[(df_pivot_j['Jogos'] >= min_games) & (df_pivot_j['Minutos'] >= min_minutes)]
                df_calc = apply_calc_mode(df_pivot_j, CATEGORIES_UI_PT[cat_j], calc_mode)
                
                # Sort by the first logical relevant column (if exists)
                sort_col = CATEGORIES_UI_PT[cat_j][0] if CATEGORIES_UI_PT[cat_j][0] in df_calc.columns else 'Minutos'
                if sort_col in df_calc.columns:
                     df_calc = df_calc.sort_values(by=sort_col, ascending=False)
                # Order columns intelligently
                dynamic_base_cols = [c for c in ['Jogador', 'Clube', 'Pos', 'Competicao', 'Temporada', 'Jogos', 'Minutos'] if c in df_calc.columns]
                cat_defined_order = CATEGORIES_UI_PT[cat_j]
                
                if 'Jogos' in df_calc.columns: df_calc['Jogos'] = df_calc['Jogos'].astype(int)
                if 'Minutos' in df_calc.columns: df_calc['Minutos'] = df_calc['Minutos'].astype(int)
                
                # Intersect with what we actually have to avoid KeyErrors
                ordered_cols = [c for c in cat_defined_order if c in df_calc.columns]
                # In case there's any stray column
                ordered_cols += [c for c in df_calc.columns if c not in ordered_cols and c not in dynamic_base_cols]
                
                df_calc = df_calc.set_index(dynamic_base_cols)[ordered_cols]
                
                # Render using Styler
                format_dict = {}
                for col in df_calc.columns:
                    if "%" in col:
                        format_dict[col] = "{:.2f}%"
                    elif col.startswith("xG") or col.startswith("xA") or col == "Gols Evitados":
                        format_dict[col] = "{:.2f}"
                    else:
                        if calc_mode == "Total":
                            format_dict[col] = "{:.0f}"
                        else:
                            format_dict[col] = "{:.2f}"
                            
                styled_df = df_calc.style.format(format_dict, na_rep="-")
                
                st.dataframe(styled_df, use_container_width=True, hide_index=False, height=600)
            else:
                st.info("Nenhum dado encontrado com os filtros e métricas atuais.")
        else:
            st.info("Nenhuma partida encontrada no período/rodada base.")
    with tab_c:
        col_persp, col_cat = st.columns([1, 4])
        with col_persp:
            perspective_c = st.pills("Desempenho:", ["A favor", "Sofrido"], default="A favor")
        with col_cat:
            cat_c = st.pills("Selecione a Categoria:", list(CATEGORIES_UI_PT.keys()), default="Finalização", key="pills_c")
        
        if not filtered_matches.empty:
            df_pivot_c = fetch_club_category_stats(
                category=cat_c, 
                match_ids=match_ids_list, 
                valid_team_ids=p_id_list, 
                group_mode=group_mode,
                perspective=perspective_c
            )
            
            if not df_pivot_c.empty:
                # Filter mins and games
                df_pivot_c = df_pivot_c[(df_pivot_c['Jogos'] >= min_games) & (df_pivot_c['Minutos'] >= min_minutes)]
                df_calc_c = apply_calc_mode(df_pivot_c, CATEGORIES_UI_PT[cat_c], calc_mode)
                
                # Sort by the first logical relevant column (if exists)
                sort_col = CATEGORIES_UI_PT[cat_c][0] if CATEGORIES_UI_PT[cat_c][0] in df_calc_c.columns else 'Minutos'
                if sort_col in df_calc_c.columns:
                     df_calc_c = df_calc_c.sort_values(by=sort_col, ascending=False)
                     
                # Order columns intelligently
                dynamic_base_cols_c = [c for c in ['Clube', 'Competicao', 'Temporada', 'Jogos', 'Minutos'] if c in df_calc_c.columns]
                cat_defined_order_c = CATEGORIES_UI_PT[cat_c]
                
                if 'Jogos' in df_calc_c.columns: df_calc_c['Jogos'] = df_calc_c['Jogos'].astype(int)
                if 'Minutos' in df_calc_c.columns: df_calc_c['Minutos'] = df_calc_c['Minutos'].astype(int)
                
                # Intersect with what we actually have to avoid KeyErrors
                ordered_cols_c = [c for c in cat_defined_order_c if c in df_calc_c.columns]
                # In case there's any stray column
                ordered_cols_c += [c for c in df_calc_c.columns if c not in ordered_cols_c and c not in dynamic_base_cols_c]
                
                df_calc_c = df_calc_c.set_index(dynamic_base_cols_c)[ordered_cols_c]
                
                # Render using Styler
                format_dict_c = {}
                for col in df_calc_c.columns:
                    if "%" in col:
                        format_dict_c[col] = "{:.2f}%"
                    elif col.startswith("xG") or col.startswith("xA") or col == "Gols Evitados":
                        format_dict_c[col] = "{:.2f}"
                    else:
                        if calc_mode == "Total":
                            format_dict_c[col] = "{:.0f}"
                        else:
                            format_dict_c[col] = "{:.2f}"
                            
                styled_df_c = df_calc_c.style.format(format_dict_c, na_rep="-")
                
                st.dataframe(styled_df_c, use_container_width=True, hide_index=False, height=600)
            else:
                st.info("Nenhum dado encontrado com os filtros e métricas atuais.")
        else:
            st.info("Nenhuma partida encontrada no período/rodada base.")        
    with tab_r:
        col_type, col_cat, col_metric = st.columns([1, 2.5, 1.5])
        with col_type:
            record_target = st.pills("Recorde de:", ["Clube", "Jogador"], default="Jogador", key="rec_type")
        with col_cat:
            rec_cat = st.pills("Selecione a Categoria:", list(CATEGORIES_UI_PT.keys()), default="Finalização", key="rec_cat")

        if rec_cat:
            metrics_available = [m for m in CATEGORIES_UI_PT[rec_cat]]
            with col_metric:
                metric_sel = st.selectbox("Métrica Estatística:", metrics_available)
            
            if not filtered_matches.empty:
                df_records = fetch_single_game_records(
                    target=record_target,
                    category=rec_cat,
                    metric_sel=metric_sel,
                    match_ids=match_ids_list,
                    valid_team_ids=p_id_list
                )
                
                if not df_records.empty and metric_sel in df_records.columns:
                    if record_target == "Jogador":
                        base_cols = ['Jogador', 'Clube', 'Competicao', 'Temporada', 'Jogo', 'Data', metric_sel]
                    else:
                        base_cols = ['Clube', 'Competicao', 'Temporada', 'Jogo', 'Data', metric_sel]
                        
                    final_cols = [c for c in base_cols if c in df_records.columns]
                    df_display = df_records[final_cols]
                    
                    format_r = {}
                    if "%" in metric_sel:
                        format_r[metric_sel] = "{:.2f}%"
                    elif metric_sel.startswith("xG") or metric_sel.startswith("xA") or metric_sel == "Gols Evitados":
                        format_r[metric_sel] = "{:.2f}"
                    elif metric_sel == "Nota SofaScore":
                         format_r[metric_sel] = "{:.1f}"
                    else:
                        format_r[metric_sel] = "{:.0f}"
                        
                    styled_rec = df_display.style.format(format_r, na_rep="-")
                    st.dataframe(styled_rec, use_container_width=True, hide_index=True, height=700)
                else:
                    st.info(f"Nenhum dado encontrado para a métrica {metric_sel}.")
            else:
                st.info("Nenhuma partida encontrada no período/rodada base.")

elif nav_page == "Sequências":
    st.markdown('<h1 class="title-gradient">🔥 Analysis - Forma & Sequências</h1>', unsafe_allow_html=True)
    
    col_type, col_cat, col_metric = st.columns([1, 2.5, 1.5])
    with col_type:
        seq_target = st.pills("Analisar:", ["Clube", "Jogador"], default="Jogador", key="seq_type")
    with col_cat:
        seq_cat = st.pills("Categoria:", list(CATEGORIES_UI_PT.keys()), default="Finalização", key="seq_cat")
        
    if seq_cat:
        metrics_available = [m for m in CATEGORIES_UI_PT[seq_cat]]
        with col_metric:
            metric_sel = st.selectbox("Estatística Alvo:", metrics_available, key="seq_met")
            
        st.markdown("<br>", unsafe_allow_html=True)
        col_cond, col_val, col_btn = st.columns([1, 1, 3])
        with col_cond:
            condition = st.selectbox("Condição Lógica", [">=", ">", "=", "<=", "<"], index=0)
        with col_val:
            target_val = st.number_input("Valor Alvo", value=1.0, step=0.5)
        with col_btn:
            st.markdown("<br>", unsafe_allow_html=True)
            run_analysis = st.button("Executar Análise de Consistência 🚀", use_container_width=True, type="primary")
             
        if run_analysis:
            if not filtered_matches.empty:
                with st.spinner("Mapeando histórico e vetores cronológicos..."):
                    df_records = fetch_single_game_records(
                        target=seq_target,
                        category=seq_cat,
                        metric_sel=metric_sel,
                        match_ids=match_ids_list,
                        valid_team_ids=p_id_list,
                        skip_limit=True
                    )
                    
                    df_streaks = process_streaks_and_forms(
                        df_records=df_records, 
                        metric_sel=metric_sel, 
                        target_val=target_val, 
                        condition=condition, 
                        group_col=seq_target,
                        df_matches_base=filtered_matches
                    )
                    
                    if not df_streaks.empty:
                        st.markdown(f"**Resultados de Consistência** (Critério: `{metric_sel} {condition} {target_val}`)")
                        styled_streaks = df_streaks.style.format({
                            "% Sucesso": "{:.2f}%"
                        }, na_rep="-")
                        st.dataframe(styled_streaks, use_container_width=True, hide_index=True, height=600)
                    else:
                        st.warning("Nenhum histórico alcançou os critérios estabelecidos ou não há informações de base.")
            else:
                 st.info("Nenhuma partida correspondente aos filtros globais de Liga/Rodada encontrada.")

elif nav_page == "Comparação":
    st.markdown('<h1 class="title-gradient">⚖️ Player & Club Comparison</h1>', unsafe_allow_html=True)
    st.info("🚧 Em breve. Comparativo direto entre Atletas e Clubes com Radars e Evolução Numérica.")

