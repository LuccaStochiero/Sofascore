# Configuration of Metrics for Sofascore Dashboard
# Structure:
# "Nome PT": {
#    "key": "key_in_db",
#    "source": "player" OR "match",
#    "category": "Category Name"
# }

METRICS_CONFIG = {
    # --- 1. Ataque ---
    "Gols": {"key": "goals", "source": "player", "category": "Ataque"},
    "xG (Gols Esperados)": {"key": "expectedGoals", "match_key": "expectedGoals", "source": "player", "category": "Ataque"},
    "Finalizações Totais": {"key": "totalShots", "source": "player", "category": "Ataque"},
    "Finalizações no Alvo": {"key": "onTargetScoringAttempt", "match_key": "shotsOnGoal", "source": "player", "category": "Ataque"},
    "Chutes pra Fora": {"key": "shotOffTarget", "match_key": "shotsOffGoal", "source": "player", "category": "Ataque"},
    "Chutes Bloqueados": {"key": "blockedScoringAttempt", "match_key": "blockedScoringAttempt", "source": "player", "category": "Ataque"},
    "Na Trave": {"key": "hitWoodwork", "match_key": "hitWoodwork", "source": "player", "category": "Ataque"},
    "Grandes Chances Perdidas": {"key": "bigChanceMissed", "match_key": "bigChanceMissed", "source": "player", "category": "Ataque"},
    "Grandes Chances Convertidas": {"key": "bigChanceScored", "match_key": "bigChanceScored", "source": "player", "category": "Ataque"},
    "xGoT (No Alvo)": {"key": "expectedGoalsOnTarget", "source": "player", "category": "Ataque"},
    "Pênalti Sofrido": {"key": "penaltyWon", "source": "player", "category": "Ataque"},
    "Finalizações (Dentro da Área)": {"key": "totalShotsInsideBox", "source": "match", "category": "Ataque"},
    "Finalizações (Fora da Área)": {"key": "totalShotsOutsideBox", "source": "match", "category": "Ataque"},

    # --- 2. Criação ---
    "Assistências": {"key": "goalAssist", "source": "player", "category": "Criação"},
    "xA (Assistências Esperadas)": {"key": "expectedAssists", "source": "player", "category": "Criação"},
    "Grandes Chances Criadas": {"key": "bigChanceCreated", "match_key": "bigChanceCreated", "source": "player", "category": "Criação"},
    "Passes Decisivos": {"key": "keyPass", "source": "player", "category": "Criação"},
    "Entradas no Terço Final": {"key": "finalThirdEntries", "source": "match", "category": "Criação"},
    "Ações no Terço Final (Certas)": {"key": "finalThirdPhaseStatistic", "source": "match", "category": "Criação"},
    "Ações no Terço Final (Total)": {"key": "finalThirdPhaseStatisticTotal", "source": "match", "category": "Criação"},

    # --- 3. Distribuição ---
    "Passes Certos": {"key": "accuratePass", "match_key": "accuratePasses", "source": "player", "category": "Distribuição"},
    "Passes Totais": {"key": "totalPass", "match_key": "passes", "source": "player", "category": "Distribuição"},
    "Bolas Longas Certas": {"key": "accurateLongBalls", "match_key": "accurateLongBalls", "source": "player", "category": "Distribuição"},
    "Bolas Longas Totais": {"key": "totalLongBalls", "match_key": "accurateLongBallsTotal", "source": "player", "category": "Distribuição"},
    "Cruzamentos Certos": {"key": "accurateCross", "match_key": "accurateCross", "source": "player", "category": "Distribuição"},
    "Cruzamentos Totais": {"key": "totalCross", "match_key": "accurateCrossTotal", "source": "player", "category": "Distribuição"},
    "Passes em Profundidade Certos": {"key": "accurateThroughBall", "match_key": "accurateThroughBall", "source": "player", "category": "Distribuição"},
    "Passes Certos Campo Rival": {"key": "accurateOppositionHalfPasses", "source": "player", "category": "Distribuição"},
    "Passes Totais Campo Rival": {"key": "totalOppositionHalfPasses", "source": "player", "category": "Distribuição"},
    "Passes Certos Campo Defesa": {"key": "accurateOwnHalfPasses", "source": "player", "category": "Distribuição"},
    "Laterais": {"key": "throwIns", "source": "match", "category": "Distribuição"},

    # --- 4. Defesa ---
    "Desarmes Totais": {"key": "totalTackle", "match_key": "totalTackle", "source": "player", "category": "Defesa"},
    "Desarmes Certos": {"key": "wonTackle", "source": "player", "category": "Defesa"},
    "Interceptações": {"key": "interceptionWon", "match_key": "interceptionWon", "source": "player", "category": "Defesa"},
    "Cortes": {"key": "totalClearance", "match_key": "totalClearance", "source": "player", "category": "Defesa"},
    "Cortes na Linha": {"key": "clearanceOffLine", "source": "player", "category": "Defesa"},
    "Bloqueios": {"key": "outfielderBlock", "source": "player", "category": "Defesa"},
    "Bolas Recuperadas": {"key": "ballRecovery", "match_key": "ballRecovery", "source": "player", "category": "Defesa"},
    "Dribles Sofridos": {"key": "challengeLost", "source": "player", "category": "Defesa"},
    "Gols Evitados": {"key": "goalsPrevented", "match_key": "goalsPrevented", "source": "player", "category": "Goleiro"},
    "Sem Sofrer Gols": {"key": "cleanSheet", "source": "match", "category": "Defesa"},
    "Gols Contra": {"key": "ownGoals", "source": "player", "category": "Defesa"},
    "Erro Capital (Gol)": {"key": "errorLeadToAGoal", "match_key": "errorsLeadToGoal", "source": "player", "category": "Defesa"},
    "Erro Capital (Chute)": {"key": "errorLeadToAShot", "match_key": "errorsLeadToShot", "source": "player", "category": "Defesa"},
    "Pênalti Cometido": {"key": "penaltyConceded", "source": "player", "category": "Defesa"},

    # --- 5. Duelos & Dribles ---
    "Dribles Certos": {"key": "wonContest", "source": "player", "category": "Duelos"},
    "Dribles Totais": {"key": "totalContest", "source": "player", "category": "Duelos"},
    "Duelos Ganhos": {"key": "duelWon", "source": "player", "category": "Duelos"},
    "Duelos Perdidos": {"key": "duelLost", "source": "player", "category": "Duelos"},
    "Duelos no Chão Totais": {"key": "groundDuelsPercentageTotal", "source": "match", "category": "Duelos"},
    "Duelos Aéreos Ganhos": {"key": "aerialWon", "source": "player", "category": "Duelos"},
    "Duelos Aéreos Perdidos": {"key": "aerialLost", "source": "player", "category": "Duelos"},
    "Duelos Aéreos Totais": {"key": "aerialDuelsPercentageTotal", "source": "match", "category": "Duelos"},
    "Desarmado": {"key": "dispossessed", "match_key": "dispossessed", "source": "player", "category": "Duelos"},

    # --- 6. Goleiro ---
    "Defesas": {"key": "saves", "match_key": "goalkeeperSaves", "source": "player", "category": "Goleiro"},
    "Socos": {"key": "punches", "match_key": "punches", "source": "player", "category": "Goleiro"},
    "Defesas Aéreas": {"key": "highClaims", "source": "match", "category": "Goleiro"},
    "Saídas Líbero Certas": {"key": "accurateKeeperSweeper", "source": "player", "category": "Goleiro"},
    "Pênaltis Defendidos": {"key": "penaltySave", "match_key": "penaltySaves", "source": "player", "category": "Goleiro"},
    "Defesas Difíceis": {"key": "diveSaves", "match_key": "diveSaves", "source": "player", "category": "Goleiro"},
    "Defesas Dentro da Área": {"key": "savedShotsFromInsideTheBox", "source": "player", "category": "Goleiro"},

    # --- 7. Posse & Disciplina ---
    "Posse de Bola (%)": {"key": "ballPossession", "source": "match", "category": "Geral"},
    "Ações com a Bola": {"key": "touches", "source": "player", "category": "Geral"},
    "Toques na Área Rival": {"key": "touchesInOppBox", "source": "player", "category": "Geral"},
    "Perda de Posse": {"key": "possessionLostCtrl", "source": "player", "category": "Geral"},
    "Domínios Errados": {"key": "unsuccessfulTouch", "source": "player", "category": "Geral"},
    "Conduções": {"key": "ballCarriesCount", "source": "player", "category": "Geral"},
    "Conduções Progressivas": {"key": "progressiveBallCarriesCount", "source": "player", "category": "Geral"},
    "Faltas Cometidas": {"key": "fouls", "match_key": "fouls", "source": "player", "category": "Disciplina"},
    "Faltas Sofridas": {"key": "wasFouled", "source": "player", "category": "Disciplina"},
    "Cartões Amarelos": {"key": "yellowCards", "match_key": "yellowCards", "source": "player", "category": "Disciplina"},
    "Cartões Vermelhos": {"key": "redCards", "match_key": "redCards", "source": "player", "category": "Disciplina"},
    "Impedimentos": {"key": "totalOffside", "match_key": "offsides", "source": "player", "category": "Disciplina"},
    "Escanteios": {"key": "cornerKicks", "source": "match", "category": "Disciplina"},
    "Tiros de Meta": {"key": "goalKicks", "source": "match", "category": "Disciplina"},

    # --- 8. Índices ---
    "Nota SofaScore": {"key": "rating", "source": "player", "category": "Índices"},
}

def get_metric_info(metric_name):
    """Returns the DB key and the source ('player' or 'match') for the given metric."""
    cfg = METRICS_CONFIG.get(metric_name)
    if not cfg: return None, None
    return cfg.get("key"), cfg.get("source")

def get_club_metric_info(metric_name):
    """Returns the most appropriate key for club aggregations.
    If match_key exists, uses it with source 'match' for better period support."""
    cfg = METRICS_CONFIG.get(metric_name)
    if not cfg: return None, None
    if cfg.get("match_key"):
        return cfg.get("match_key"), "match"
    return cfg.get("key"), cfg.get("source")
