import dlt
from shared.app_config import get_app_config
from sources.collegefootballdata.cfb_games import cfb_games
from sources.collegefootballdata.cfb_rankings import cfb_rankings
from sources.collegefootballdata.cfb_drives import cfb_drives
from sources.collegefootballdata.cfb_plays import cfb_plays

if __name__ == "__main__":
    # Load configuration
    app_config = get_app_config()

    api_key = app_config["CFB_API_KEY"]
    year = 2025

    # Define the pipeline with DuckDB destination
    pipeline = dlt.pipeline(
        pipeline_name="cfb_analytics",
        destination="duckdb",  # local free database
        dataset_name="cfb"
    )

    print(f"🏗️ Running pipeline for {year} season...")

    # Create sources list
    sources = []

    # --- Load each source safely ---
    try:
        print("Fetching games data...")
        sources.append(cfb_games(api_key, year))
    except Exception as e:
        print(f"⚠️ Skipping games source due to error: {e}")

    try:
        print("Fetching rankings data...")
        sources.append(cfb_rankings(api_key, year))
    except Exception as e:
        print(f"⚠️ Skipping rankings source due to error: {e}")

    try:
        print("Fetching drives data...")
        sources.append(cfb_drives(api_key, year))
    except Exception as e:
        print(f"⚠️ Skipping drives source due to error: {e}")

    try:
        print("Fetching plays data...")
        sources.append(cfb_plays(api_key, year))
    except Exception as e:
        print(f"⚠️ Skipping plays source due to error: {e}")

    # --- Run pipeline ---
    if not sources:
        print("❌ No valid sources available. Aborting pipeline.")
    else:
        load_info = pipeline.run(sources)
        print("✅ Pipeline completed successfully!")
        print(load_info)
