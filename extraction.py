import os
import time
import json
import random
import re
import pandas as pd
import pandas_gbq
from fp.fp import FreeProxy
from curl_cffi import requests as cffi_requests

# Configuration
PROJECT_ID = 'betterbet-467621'
DATASET_ID = 'sofascore'

FORCE_UPDATE_ALL = False # Set to True to re-process all matches (e.g. to fix missing data), False for incremental only

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Referer': 'https://www.sofascore.com/',
    'Origin': 'https://www.sofascore.com',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
}

# --- Shared Constants (Column Keys) ---
TRANSLATION_MAP = {
    'goals': 'Gols', 'goalAssist': 'Assistências', 'totalShots': 'Finalizações', 'shotsOnTarget': 'Chutes no Gol',
    'shotsOffTarget': 'Chutes pra Fora', 'blockedShots': 'Chutes Bloqueados', 'totalPass': 'Passes Totais',
    'accuratePass': 'Passes Certos', 'keyPass': 'Passes Decisivos', 'totalLongBalls': 'Bolas Longas',
    'accurateLongBalls': 'Bolas Longas Certas', 'totalCross': 'Cruzamentos', 'accurateCross': 'Cruzamentos Certos',
    'totalContest': 'Dribles Totais', 'wonContest': 'Dribles Certos', 'bigChanceCreated': 'Grandes Chances Criadas',
    'bigChanceMissed': 'Grandes Chances Perdidas', 'duelWon': 'Duelos Ganhos', 'duelLost': 'Duelos Perdidos',
    'aerialWon': 'Duelos Aéreos Ganhos', 'aerialLost': 'Duelos Aéreos Perdidos', 'interceptionWon': 'Interceptações',
    'totalTackle': 'Desarmes', 'wonTackle': 'Desarmes Certos', 'totalClearance': 'Cortes',
    'errorLeadToGoal': 'Erro Capital (Gol)', 'errorLeadToShot': 'Erro Capital (Chute)', 'wasFouled': 'Sofreu Falta',
    'fouls': 'Cometeu Falta', 'minutesPlayed': 'Minutos Jogados', 'touches': 'Ações com a Bola',
    'rating': 'Nota SofaScore', 'possessionLostCtrl': 'Perda de Posse', 'expectedGoals': 'xG (Gols Esperados)',
    'expectedAssists': 'xA (Assistências Esperadas)', 'challengeLost': 'Dribles Sofridos', 'dispossessed': 'Desarmado',
    'onTargetScoringAttempt': 'Chutes no Alvo', 'hitWoodwork': 'Na Trave', 'penaltyWon': 'Pênalti Sofrido',
    'penaltyConceded': 'Pênalti cometido', 'saves': 'Defesas de Goleiro', 'punches': 'Socos (Goleiro)',
    'claim': 'Saídas do Gol', 'cleanSheet': 'Sem Sofrer Gols', 'runsOut': 'Saídas do Gol (Líbero)',
    'successfulRunsOut': 'Saídas Certas', 'highClaims': 'Defesas Aéreas', 'crossesNotClaimed': 'Cruzamentos Não Cortados',
    'ballPossession': 'Posse de Bola', 'cornerKicks': 'Escanteios', 'freeKicks': 'Faltas', 'goalKicks': 'Tiros de Meta',
    'offsides': 'Impedimentos', 'ownGoals': 'Gols Contra', 'passes': 'Passes', 'redCards': 'Cartões Vermelhos',
    'throwIns': 'Laterais', 'yellowCards': 'Cartões Amarelos', 'goalsPrevented': 'Gols Evitados',
    'accuratePasses': 'Passes Certos', 'shotOffTarget': 'Chutes pra Fora', 'shotsOnGoal': 'Chutes no Gol',
    'totalShotsOnGoal': 'Finalizações no Gol', 'totalShotsInsideBox': 'Finalizações (Dentro da Área)',
    'totalShotsOutsideBox': 'Finalizações (Fora da Área)', 'diveSaves': 'Defesas Difíceis', 
    'goalkeeperSaves': 'Defesas de Goleiro', 'goodHighClaim': 'Defesas Aéreas', 'penaltySave': 'Pênaltis Defendidos',
    'penaltyFaced': 'Pênaltis Enfrentados', 'savedShotsFromInsideTheBox': 'Defesas (Dentro da Área)',
    'ballRecovery': 'Bolas Recuperadas', 'bigChanceScored': 'Grandes Chances Convertidas',
    'clearanceOffLine': 'Cortes em Cima da Linha', 'lastManTackle': 'Desarme (Último Homem)', 'penaltyMiss': 'Pênaltis Perdidos',
    'totalOffside': 'Impedimentos', 'touchesInOppBox': 'Toques na Área Rival', 'aerialDuelsPercentage': 'Duelos Aéreos Ganhos (%)',
    'groundDuelsPercentage': 'Duelos no Chão Ganhos (%)', 'dribblesPercentage': 'Dribles Certos (%)',
    'wonTacklePercent': 'Desarmes Certos (%)', 'expectedGoalsOnTarget': 'xGoT (Gols Esperados no Alvo)',
    'finalThirdEntries': 'Entradas no Terço Final', 'ballCarriesCount': 'Conduções', 'progressiveBallCarriesCount': 'Conduções Progressivas',
    'accurateCrossTotal': 'Cruzamentos Certos (Total)', 'accurateKeeperSweeper': 'Saídas Certas (Líbero)',
    'accurateLongBallsTotal': 'Bolas Longas Certas (Total)', 'accurateOppositionHalfPasses': 'Passes Certos Campo Rival',
    'accurateOwnHalfPasses': 'Passes Certos Campo Defesa', 'accurateThroughBall': 'Passes em Profundidade Certos',
    'aerialDuelsPercentageTotal': 'Duelos Aéreos Total (%)', 'bestBallCarryProgression': 'Melhor Condução (Distância)',
    'blockedScoringAttempt': 'Chutes Bloqueados (2)', 'crossNotClaimed': 'Cruzamentos Não Cortados (2)',
    'defensiveValueNormalized': 'Valor Defensivo (Norm)', 'dribbleValueNormalized': 'Valor Drible (Norm)',
    'dribblesPercentageTotal': 'Dribles Certos Total (%)', 'duelWonPercent': 'Duelos Ganhos (%)',
    'errorLeadToAGoal': 'Erro Capital (Gol) (2)', 'errorLeadToAShot': 'Erro Capital (Chute) (2)',
    'errorsLeadToGoal': 'Erros Capital (Gol)', 'errorsLeadToShot': 'Erros Capital (Chute)',
    'finalThirdPhaseStatistic': 'Ações Terço Final', 'finalThirdPhaseStatisticTotal': 'Ações Terço Final (Total)',
    'fouledFinalThird': 'Sofreu Falta (Terço Final)', 'goalkeeperValueNormalized': 'Valor Goleiro (Norm)',
    'groundDuelsPercentageTotal': 'Duelos Chão Total (%)', 'keeperSaveValue': 'Valor Defesas',
    'outfielderBlock': 'Bloqueios (Linha)', 'passValueNormalized': 'Valor Passe (Norm)',
    'penaltySaves': 'Pênaltis Defendidos (2)', 'shotValueNormalized': 'Valor Chute (Norm)',
    'shotsOffGoal': 'Chutes pra Fora (2)', 'totalBallCarriesDistance': 'Distância Conduzida Total',
    'totalKeeperSweeper': 'Saídas Líbero (Total)', 'totalOppositionHalfPasses': 'Passes Campo Rival',
    'totalOwnHalfPasses': 'Passes Campo Defesa', 'totalProgression': 'Progressão Total',
    'totalProgressiveBallCarriesDistance': 'Distância Condução Progressiva', 'unsuccessfulTouch': 'Domínios Errados',
    'wonTacklePercentTotal': 'Desarmes Certos Total (%)',
}

# --- Memory Storage for Updates ---
new_tournaments = []
new_clubs = []
new_matches = []
new_players = []
new_match_stats_log = []
new_player_stats_log = []

local_seen_tournaments = set()
local_seen_clubs = set()
local_seen_matches = set()
local_seen_players = set()

def fetch_json(url):
    """Fetch JSON using curl_cffi for browser impersonation + proxy rotation"""
    sleep_time = random.uniform(0, 1)
    print(f"Fetching: {url} (Waiting {sleep_time:.2f}s)...")
    time.sleep(sleep_time)

    max_retries = 3
    for attempt in range(max_retries):
        try:
            if attempt == 0:
                 response = cffi_requests.get(
                     url, 
                     impersonate="chrome131", 
                     headers={"Referer": "https://www.sofascore.com/", "Origin": "https://www.sofascore.com"},
                     timeout=15
                 )
            else:
                 print(f"  [Attempt {attempt+1}] Fetching fresh proxy...")
                 try:
                     proxy_url = FreeProxy(rand=True, https=True, timeout=3).get()
                     proxies = {"http": proxy_url, "https": proxy_url}
                     response = cffi_requests.get(
                         url, 
                         impersonate="chrome131", 
                         proxies=proxies,
                         headers={"Referer": "https://www.sofascore.com/", "Origin": "https://www.sofascore.com"},
                         timeout=20
                     )
                 except Exception as px_e:
                     continue

            if response.status_code == 200:
                try:
                    data = response.json()
                    if 'error' in data: continue
                    return data
                except:
                    pass
            elif response.status_code == 404:
                return None
            time.sleep(2)
        except Exception as e:
            time.sleep(2)
    return None

def insert_tournaments(event):
    tournament = event.get('tournament', {}).get('uniqueTournament', {})
    t_id = tournament.get('id')
    season = event.get('season', {})
    s_id = season.get('id')
    
    seen_key = f"{t_id}_{s_id}"
    if not t_id or seen_key in local_seen_tournaments: return
    local_seen_tournaments.add(seen_key)
    
    category = event.get('tournament', {}).get('category', {})
    
    new_tournaments.append({
        'unique_tournament_id': t_id,
        'name': tournament.get('name'),
        'slug': tournament.get('slug'),
        'category_name': category.get('name'),
        'season_id': season.get('id'),
        'season_name': season.get('name'),
        'season_year': season.get('year')
    })

def insert_clubs(team_data):
    t_id = team_data.get('id')
    if not t_id or t_id in local_seen_clubs: return
    local_seen_clubs.add(t_id)
    
    colors = team_data.get('teamColors', {})
    new_clubs.append({
        'team_id': t_id,
        'name': team_data.get('name'),
        'short_name': team_data.get('shortName'),
        'slug': team_data.get('slug'),
        'gender': team_data.get('gender'),
        'name_code': team_data.get('nameCode'),
        'disabled': str(team_data.get('disabled')), # boolean to string just in case
        'national': str(team_data.get('national')),
        'type': str(team_data.get('type')),
        'primary_color': colors.get('primary'),
        'secondary_color': colors.get('secondary')
    })

def insert_match(event, match_id):
    if match_id in local_seen_matches: return
    local_seen_matches.add(match_id)
    
    tournament = event.get('tournament', {}).get('uniqueTournament', {})
    season = event.get('season', {})
    round_info = event.get('roundInfo', {})
    home_team = event.get('homeTeam', {})
    away_team = event.get('awayTeam', {})
    status = event.get('status', {})
    
    new_matches.append({
        'match_id': match_id,
        'tournament_id': tournament.get('id'),
        'season_id': season.get('id'),
        'round_id': round_info.get('round'),
        'home_team_id': home_team.get('id'),
        'away_team_id': away_team.get('id'),
        'home_team_name': home_team.get('name'),
        'away_team_name': away_team.get('name'),
        'match_date': event.get('startTimestamp'), 
        'status_code': status.get('code'),
        'home_score': event.get('homeScore', {}).get('current'),
        'away_score': event.get('awayScore', {}).get('current'),
        'round_name': round_info.get('name'),
        'round_slug': round_info.get('slug')
    })

def insert_player_details(player_data, team_id, match_id):
    player = player_data.get('player', {})
    pid = player.get('id')
    
    if not pid or pid in local_seen_players: return
    local_seen_players.add(pid)
    
    market_value_struct = player.get('proposedMarketValueRaw')
    
    new_players.append({
        'player_id': pid,
        'name': player.get('name'),
        'slug': player.get('slug'),
        'short_name': player.get('shortName'),
        'team_id': team_id,
        'position': player.get('position'),
        'jersey_number': str(player.get('jerseyNumber')),
        'height': str(player.get('height')),
        'country_alpha3': player.get('country', {}).get('alpha3'),
        'date_of_birth_timestamp': player.get('dateOfBirthTimestamp'),
        'market_value': float(market_value_struct.get('value')) if market_value_struct else None,
        'last_updated_match_id': match_id
    })

def process_stats(match_id):
    url = f"https://www.sofascore.com/api/v1/event/{match_id}/statistics"
    data = fetch_json(url)
    if not data: return
    
    statistics = data.get('statistics', [])
    for period in statistics:
        period_name = period.get('period')
        for group in period.get('groups', []):
            for item in group.get('statisticsItems', []):
                key = item.get('key')
                if key:
                    def parse_val(val):
                        if isinstance(val, str):
                            return float(val.replace('%', '')) if '%' in val else 0 
                        return val
                    
                    val_home = parse_val(item.get('homeValue', 0))
                    val_away = parse_val(item.get('awayValue', 0))
                    
                    if key in TRANSLATION_MAP:
                        metric_name = TRANSLATION_MAP[key]
                    else:
                        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1 \2', key)
                        metric_name = re.sub('([a-z0-9])([A-Z])', r'\1 \2', s1).title()

                    new_match_stats_log.append({
                        'match_id': match_id, 'period': period_name, 'metric_key': key, 
                        'metric_name_pt': metric_name, 'home_value': val_home, 'away_value': val_away
                    })

                    if 'homeTotal' in item or 'awayTotal' in item:
                        key_total = key + "Total"
                        metric_name_total = metric_name + " (Total)"
                        new_match_stats_log.append({
                            'match_id': match_id, 'period': period_name, 'metric_key': key_total, 
                            'metric_name_pt': metric_name_total, 
                            'home_value': parse_val(item.get('homeTotal', 0)), 
                            'away_value': parse_val(item.get('awayTotal', 0))
                        })

def process_lineups(match_id, db_home_id, db_away_id):
    url = f"https://www.sofascore.com/api/v1/event/{match_id}/lineups"
    data = fetch_json(url)
    if not data: return
    
    team_map = {'home': db_home_id, 'away': db_away_id}
    teams = [('home', data.get('home', {})), ('away', data.get('away', {}))]
    
    for side, team_data in teams:
        team_players = team_data.get('players', [])
        tid = team_map.get(side)
        
        for p in team_players:
            player = p.get('player', {})
            stats = p.get('statistics', {})
            
            insert_player_details(p, tid, match_id)
            
            def format_camels(name):
                s1 = re.sub('(.)([A-Z][a-z]+)', r'\1 \2', name)
                return re.sub('([a-z0-9])([A-Z])', r'\1 \2', s1).title()

            for key, val in stats.items():
                if val is None: val = 0 
                if isinstance(val, (dict, list)): continue
                
                metric_name = TRANSLATION_MAP.get(key, format_camels(key))
                
                new_player_stats_log.append({
                    'match_id': match_id,
                    'player_id': player.get('id'),
                    'team_id': tid,
                    'position': p.get('position'),
                    'metric_key': key,
                    'metric_name_pt': str(metric_name),
                    'value': float(val) if isinstance(val, (int, float)) else val
                })

def get_bq_processed_matches():
    """Retrieve set of match_ids that have stats already populated from BigQuery"""
    try:
        print("Buscando partidas já processadas no BigQuery...")
        query = f"SELECT DISTINCT match_id FROM `{PROJECT_ID}.{DATASET_ID}.match_stats_log`"
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
        return set(df['match_id'].tolist())
    except Exception as e:
        print(f"Aviso: Não foi possível obter as partidas do BigQuery (Tabela pode estar vazia). Erro: {e}")
        return set()

def upsert_to_bq(new_data, table_name, primary_key):
    if not new_data:
        print(f"-- Nenhuma alteração para a tabela {table_name}")
        return
        
    df_new = pd.DataFrame(new_data)
    print(f"-- Sincronizando tabela {table_name} ({len(df_new)} novos registros)...")
    
    if isinstance(primary_key, str):
        primary_key = [primary_key]
    
    try:
        query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`"
        df_old = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
        
        combined = pd.concat([df_old, df_new], ignore_index=True)
        combined.drop_duplicates(subset=primary_key, keep='last', inplace=True)
        
        pandas_gbq.to_gbq(combined, destination_table=f"{DATASET_ID}.{table_name}", project_id=PROJECT_ID, if_exists='replace')
        print(f"   -> Tabela {table_name} atualizada no BigQuery. Total: {len(combined)} registros.")
    except Exception as e:
        print(f"   -> Tabela inexistente no BQ ou erro, criando nova: {e}")
        df_new.drop_duplicates(subset=primary_key, keep='last', inplace=True)
        pandas_gbq.to_gbq(df_new, destination_table=f"{DATASET_ID}.{table_name}", project_id=PROJECT_ID, if_exists='replace')

def sync_stats_to_bq(new_data, table_name, fetched_match_ids):
    if not new_data:
        print(f"-- Nenhuma alteração adicionada à {table_name}.")
        return
    df_new = pd.DataFrame(new_data)
    print(f"-- Adicionando/Atualizando {len(df_new)} registros na tabela {table_name}...")
    
    if FORCE_UPDATE_ALL:
        try:
            query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`"
            df_old = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
            
            # Remove existing rows for the freshly fetched matches
            df_old = df_old[~df_old['match_id'].isin(fetched_match_ids)]
            
            combined = pd.concat([df_old, df_new], ignore_index=True)
            pandas_gbq.to_gbq(combined, destination_table=f"{DATASET_ID}.{table_name}", project_id=PROJECT_ID, if_exists='replace')
            print(f"   -> Tabela atualizada no BigQuery (Matches antigos limpos antes de adicionar novos). Total: {len(combined)} registros.")
        except Exception as e:
            print(f"   -> Tabela {table_name} inexistente no BQ ou erro, criando nova: {e}")
            pandas_gbq.to_gbq(df_new, destination_table=f"{DATASET_ID}.{table_name}", project_id=PROJECT_ID, if_exists='replace')
    else:
        try:
            pandas_gbq.to_gbq(df_new, destination_table=f"{DATASET_ID}.{table_name}", project_id=PROJECT_ID, if_exists='append')
            print(f"   -> Novos registros adicionados (append) à tabela {table_name}.")
        except Exception as e:
            print(f"   -> Tabela inexistente no BQ ou erro, criando nova: {e}")
            pandas_gbq.to_gbq(df_new, destination_table=f"{DATASET_ID}.{table_name}", project_id=PROJECT_ID, if_exists='replace')

def run_crawler():
    TOURNAMENTS = [
        {'id': 379, 'season_id': 87236, 'name': 'Mineiro 2026'},
        {'id': 325, 'season_id': 87678, 'name': 'Brasileirão 2026'},
        {'id': 384, 'season_id': 87760, 'name': 'Libertadores 2026'},
    ]

    print(f"Starting API Data Extraction to BigQuery...")
    
    existing_matches = get_bq_processed_matches()
    print(f"Encontradas {len(existing_matches)} partidas processadas no BigQuery.")
    
    total_new = 0
    fetched_match_ids = set()

    for tourn in TOURNAMENTS:
        t_id = tourn['id']
        s_id = tourn['season_id']
        t_name = tourn.get('name', str(t_id))
        
        print(f"\n========================================")
        print(f"Processando Torneio: {t_name}")
        print(f"========================================")

        rounds_url = f"https://www.sofascore.com/api/v1/unique-tournament/{t_id}/season/{s_id}/rounds"
        rounds_data = fetch_json(rounds_url)
        
        rounds_list = rounds_data.get('rounds', []) if rounds_data else [{'round': r} for r in range(1, 39)]
        
        for r_info in rounds_list:
            r_num = r_info.get('round')
            r_slug = r_info.get('slug')
            r_name = r_info.get('name', f"Round {r_num}")
            
            print(f"\n--- Round {r_num} ({r_name}) ---")
            
            if r_slug:
                 url = f"https://www.sofascore.com/api/v1/unique-tournament/{t_id}/season/{s_id}/events/round/{r_num}/slug/{r_slug}"
            else:
                 url = f"https://www.sofascore.com/api/v1/unique-tournament/{t_id}/season/{s_id}/events/round/{r_num}"
                 
            data = fetch_json(url)
            if not data or 'events' not in data or not data['events']: continue
                
            events = data['events']
            if len(events) > 0:
                insert_tournaments(events[0])

            for event in events:
                m_id = event.get('id')
                status_code = event.get('status', {}).get('code')
                
                insert_match(event, m_id)
                insert_clubs(event.get('homeTeam', {}))
                insert_clubs(event.get('awayTeam', {}))
                
                if status_code in (100, 110, 120):
                    if m_id in existing_matches and not FORCE_UPDATE_ALL:
                        continue

                    print(f"    Extraindo Partida: {event.get('homeTeam').get('name')} vs {event.get('awayTeam').get('name')}")
                    
                    fetched_match_ids.add(m_id)
                    process_stats(m_id)
                    process_lineups(m_id, event.get('homeTeam').get('id'), event.get('awayTeam').get('id'))
                    
                    total_new += 1
                    time.sleep(2)
        
        print(f"Finalizado {t_name}.")
    
    print("\n========================================")
    print("Atualizando dados no Google BigQuery...")
    print("========================================")
    
    upsert_to_bq(new_tournaments, 'tournaments', ['unique_tournament_id', 'season_id'])
    upsert_to_bq(new_clubs, 'clubs', 'team_id')
    upsert_to_bq(new_matches, 'matches', 'match_id')
    upsert_to_bq(new_players, 'players', ['player_id', 'team_id'])
    
    sync_stats_to_bq(new_match_stats_log, 'match_stats_log', fetched_match_ids)
    sync_stats_to_bq(new_player_stats_log, 'player_stats_log', fetched_match_ids)
    
    print(f"\nExtração e atualização finalizadas com sucesso! Partidas novas adicionadas: {total_new}")

if __name__ == '__main__':
    run_crawler()
