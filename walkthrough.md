# SofaScore Analytics - User Walkthrough

This guide explains how to use the new features of the SofaScore Analytics Dashboard.

## 1. Getting Started

Ensure your database is up to date by running the ETL process:
```bash
python etl_process.py
```
然后 launch the dashboard:
```bash
streamlit run top_n_dashboard.py
```

## 2. Using the Dashboard

### 🔍 The Filter Engine (Sidebar)
The sidebar is the control center. All charts update based on these selections.
1.  **Competição & Temporada**: Select the leagues you want to analyze. You can select multiple seasons to compare historical data.
2.  **Período**: Fine-tune the date range or select specific Rounds (Gameweeks) using the slider.
3.  **Filtragem de Entidade**:
    *   **Clubes**: Focus on specific teams.
    *   **Jogadores**: When teams are selected, this lists only their players.
    *   **Posição**: Filter metrics for specific roles (e.g., only Defenders).

### 🏆 Tab 1: Records & Rankings
Use this tab to find out "Who is the best?".
1.  **Select a Metric**: Choose from the dropdown (e.g., "Gols", "Desarmes", "Nota SofaScore").
2.  **Configure Mode**:
    *   **Total**: Aggregate volume (Sum).
    *   **Média**: Average per match.
    *   **Por 90 min**: Normalized stats for players with different playing times.
3.  **View Results**:
    *   **Clubes**: Shows top teams for the metric (Total and Single Match record).
    *   **Jogadores**: Shows top players. The "Em uma Partida" table reveals individual masterclasses.

### 🔥 Tab 2: Form & Streaks
Use this tab to find "Who is consistent?".
1.  **Define a Target**: Set a numerical goal (e.g., "Alvo: 0.5" for Goals, or "Alvo: 5" for Tackles).
2.  **Set Condition**: Choose operator (e.g., `>=`).
    *   *Example*: "Gols >= 0.5" finds players scoring in consecutive games.
3.  **Analyze**:
    *   **Sequência Atual**: How many games in a row is the streak active *right now*?
    *   **% Sucesso**: In what percentage of selected games did they hit the target?
    *   **Maior Seq.**: What was their longest run in the selected period?

## 3. Data Maintenance
If you notice data inconsistencies (e.g., players assigned to the "wrong" club for historical matches), you can force a reprocessing of match data to fix team attributes:

```bash
python reprocess_matches.py
```
This script will:
1.  Iterate through all finished matches.
2.  Clear existing player statistics for those matches.
3.  Re-fetch and re-insert player stats, forcing the correct historical Team ID (Home/Away) instead of the player's current club.

## 4. Database Schema
For developers, the `sofascore.db` schema is documented in `database_schema.md`.
