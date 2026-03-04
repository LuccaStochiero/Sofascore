# SofaScore Analytics Dashboard

This project provides a comprehensive analytics suite for football data extracted from SofaScore. It includes an automated ETL pipeline to build a robust SQLite database and a Streamlit dashboard for advanced record tracking and form analysis.

## 🚀 Features

### 1. Automated Data Pipeline (`etl_process.py`)
-   **Multi-Tournament Crawler**: Fetches data for configured tournaments (e.g., Brasileirão, State Leagues).
-   **Incremental Updates**: smart detection of existing matches to minimize API calls.
-   **Rich Data**: Captures Match Scores, Lineups, Player Stats, Shot Maps, and Heatmap data.

### 2. Analytics Dashboard (`top_n_dashboard.py`)
A powerful interface to explore the database.

*   **🔍 Advanced Filtering**:
    *   **Context**: Filter by specific Competitions and Seasons.
    *   **Time**: Date ranges and Gameweek (Round) sliders.
    *   **Entities**: Filter by specific Clubs, Players, and Positions.
*   **🏆 Records Tab**:
    *   **Leaderboards**: See top performers (Players and Clubs) for any metric.
    *   **Modes**: Toggle between "Total", "Average per Game", and "Per 90 min".
    *   **Single Match Records**: Find the highest value achieved in a single game (e.g., "Most Saves in a Match").
*   **🔥 Form & Streaks Tab**:
    *   **Consistency Analysis**: Set a target (e.g., ">= 1 Goal" or ">= 30 Passes").
    *   **Streak Tracking**: See current active streaks and historic maximum streaks for players/clubs meeting the condition.
    *   **Success Rate**: Calculate the % of games where the target was met.

## 📂 Project Structure

-   **`sofascore.db`**: The core SQLite database.
-   **`etl_process.py`**: Script to fetch data and populate the DB.
-   **`top_n_dashboard.py`**: The Streamlit application.
-   **`sofascore_schema.json`**: JSON documentation of the database structure.

## 🛠️ Usage

### 1. Update the Database
Run the crawler to fetch the latest match results and stats.
```bash
python etl_process.py
```

### 2. Launch the Dashboard
Start the local analytics server.
```bash
streamlit run top_n_dashboard.py
```
