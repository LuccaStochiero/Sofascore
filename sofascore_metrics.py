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
    "xG (Gols Esperados)": {"key": "expectedGoals", "source": "player", "category": "Ataque"},
    "Finalizações Totais": {"key": "totalShots", "source": "player", "category": "Ataque"},
    "Finalizações no Alvo": {"key": "onTargetScoringAttempt", "source": "player", "category": "Ataque"},
    "Chutes pra Fora": {"key": "shotOffTarget", "source": "player", "category": "Ataque"},
    "Chutes Bloqueados": {"key": "blockedScoringAttempt", "source": "player", "category": "Ataque"},
    "Na Trave": {"key": "hitWoodwork", "source": "player", "category": "Ataque"},
    "Grandes Chances Perdidas": {"key": "bigChanceMissed", "source": "player", "category": "Ataque"},
    "Grandes Chances Convertidas": {"key": "bigChanceScored", "source": "player", "category": "Ataque"},
    "xGoT (No Alvo)": {"key": "expectedGoalsOnTarget", "source": "player", "category": "Ataque"},
    "Pênalti Sofrido": {"key": "penaltyWon", "source": "player", "category": "Ataque"},
    "Finalizações (Dentro da Área)": {"key": "totalShotsInsideBox", "source": "match", "category": "Ataque"},
    "Finalizações (Fora da Área)": {"key": "totalShotsOutsideBox", "source": "match", "category": "Ataque"},

    # --- 2. Criação ---
    "Assistências": {"key": "goalAssist", "source": "player", "category": "Criação"},
    "xA (Assistências Esperadas)": {"key": "expectedAssists", "source": "player", "category": "Criação"},
    "Grandes Chances Criadas": {"key": "bigChanceCreated", "source": "player", "category": "Criação"},
    "Passes Decisivos": {"key": "keyPass", "source": "player", "category": "Criação"},
    "Entradas no Terço Final": {"key": "finalThirdEntries", "source": "match", "category": "Criação"},
    "Ações no Terço Final (Certas)": {"key": "finalThirdPhaseStatistic", "source": "match", "category": "Criação"},
    "Ações no Terço Final (Total)": {"key": "finalThirdPhaseStatisticTotal", "source": "match", "category": "Criação"},

    # --- 3. Distribuição ---
    "Passes Certos": {"key": "accuratePass", "source": "player", "category": "Distribuição"},
    "Passes Totais": {"key": "totalPass", "source": "player", "category": "Distribuição"},
    "Bolas Longas Certas": {"key": "accurateLongBalls", "source": "player", "category": "Distribuição"},
    "Bolas Longas Totais": {"key": "totalLongBalls", "source": "player", "category": "Distribuição"},
    "Cruzamentos Certos": {"key": "accurateCross", "source": "player", "category": "Distribuição"},
    "Cruzamentos Totais": {"key": "totalCross", "source": "player", "category": "Distribuição"},
    "Passes em Profundidade Certos": {"key": "accurateThroughBall", "source": "player", "category": "Distribuição"},
    "Passes Certos Campo Rival": {"key": "accurateOppositionHalfPasses", "source": "player", "category": "Distribuição"},
    "Passes Totais Campo Rival": {"key": "totalOppositionHalfPasses", "source": "player", "category": "Distribuição"},
    "Passes Certos Campo Defesa": {"key": "accurateOwnHalfPasses", "source": "player", "category": "Distribuição"},
    "Laterais": {"key": "throwIns", "source": "match", "category": "Distribuição"},

    # --- 4. Defesa ---
    "Desarmes Totais": {"key": "totalTackle", "source": "player", "category": "Defesa"},
    "Desarmes Certos": {"key": "wonTackle", "source": "player", "category": "Defesa"},
    "Interceptações": {"key": "interceptionWon", "source": "player", "category": "Defesa"},
    "Cortes": {"key": "totalClearance", "source": "player", "category": "Defesa"},
    "Cortes na Linha": {"key": "clearanceOffLine", "source": "player", "category": "Defesa"},
    "Bloqueios": {"key": "outfielderBlock", "source": "player", "category": "Defesa"},
    "Bolas Recuperadas": {"key": "ballRecovery", "source": "player", "category": "Defesa"},
    "Dribles Sofridos": {"key": "challengeLost", "source": "player", "category": "Defesa"},
    "Gols Evitados": {"key": "goalsPrevented", "source": "player", "category": "Defesa"},
    "Sem Sofrer Gols": {"key": "cleanSheet", "source": "match", "category": "Defesa"},
    "Gols Contra": {"key": "ownGoals", "source": "player", "category": "Defesa"},
    "Erro Capital (Gol)": {"key": "errorLeadToAGoal", "source": "player", "category": "Defesa"},
    "Erro Capital (Chute)": {"key": "errorLeadToAShot", "source": "player", "category": "Defesa"},
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
    "Desarmado": {"key": "dispossessed", "source": "player", "category": "Duelos"},

    # --- 6. Goleiro ---
    "Defesas": {"key": "saves", "source": "player", "category": "Goleiro"},
    "Socos": {"key": "punches", "source": "player", "category": "Goleiro"},
    "Defesas Aéreas": {"key": "highClaims", "source": "match", "category": "Goleiro"},
    "Saídas Líbero Certas": {"key": "accurateKeeperSweeper", "source": "player", "category": "Goleiro"},
    "Pênaltis Defendidos": {"key": "penaltySave", "source": "player", "category": "Goleiro"},
    "Defesas Difíceis": {"key": "diveSaves", "source": "player", "category": "Goleiro"},
    "Defesas Dentro da Área": {"key": "savedShotsFromInsideTheBox", "source": "player", "category": "Goleiro"},

    # --- 7. Posse & Disciplina ---
    "Posse de Bola (%)": {"key": "ballPossession", "source": "match", "category": "Geral"},
    "Ações com a Bola": {"key": "touches", "source": "player", "category": "Geral"},
    "Toques na Área Rival": {"key": "touchesInOppBox", "source": "player", "category": "Geral"},
    "Perda de Posse": {"key": "possessionLostCtrl", "source": "player", "category": "Geral"},
    "Domínios Errados": {"key": "unsuccessfulTouch", "source": "player", "category": "Geral"},
    "Conduções": {"key": "ballCarriesCount", "source": "player", "category": "Geral"},
    "Conduções Progressivas": {"key": "progressiveBallCarriesCount", "source": "player", "category": "Geral"},
    "Faltas Cometidas": {"key": "fouls", "source": "player", "category": "Disciplina"},
    "Faltas Sofridas": {"key": "wasFouled", "source": "player", "category": "Disciplina"},
    "Cartões Amarelos": {"key": "yellowCards", "source": "player", "category": "Disciplina"},
    "Cartões Vermelhos": {"key": "redCards", "source": "player", "category": "Disciplina"},
    "Impedimentos": {"key": "totalOffside", "source": "player", "category": "Disciplina"},
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
