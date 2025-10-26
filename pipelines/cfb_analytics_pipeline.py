# cfb_analytics_pipeline.py
import dlt
from shared.app_config import get_app_config
from data.pipelines.sources.cfb_games import cfb_games
from data.pipelines.sources.cfb_rankings import cfb_rankings
from data.pipelines.sources.cfb_drives import cfb_drives
from data.pipelines.sources.cfb_plays import cfb_plays
from data.pipelines.sources.cfb_lines import cfb_lines
import traceback

def run_pipeline(years: list[int]):
    app_config = get_app_config()
    api_key = app_config["CFB_API_KEY"]

    pipeline = dlt.pipeline(
        pipeline_name="cfb_analytics",
        destination="duckdb",
        dataset_name="cfb"
    )

    if not years:
        print("❌ No years provided. Aborting pipeline.")
        return

    for year in years:
        print(f"\n🏗️ Running pipeline for {year} season...")

        sources = []
        try:
            sources.append(cfb_games(api_key, year))
        except Exception as e:
            print(f"⚠️ Skipping cfb_games for {year}: {e}")
            traceback.print_exc()

        try:
            sources.append(cfb_rankings(api_key, year))
        except Exception as e:
            print(f"⚠️ Skipping cfb_rankings for {year}: {e}")
            traceback.print_exc()

        try:
            sources.append(cfb_drives(api_key, year))
        except Exception as e:
            print(f"⚠️ Skipping cfb_drives for {year}: {e}")
            traceback.print_exc()

        try:
            sources.append(cfb_plays(api_key, year))
        except Exception as e:
            print(f"⚠️ Skipping cfb_plays for {year}: {e}")
            traceback.print_exc()

        try:
            sources.append(cfb_lines(api_key, year))
        except Exception as e:
            print(f"⚠️ Skipping cfb_lines for {year}: {e}")
            traceback.print_exc()

        if not sources:
            print(f"❌ No valid sources for {year}. Skipping.")
            continue

        load_info = pipeline.run(sources)
        print(f"✅ Pipeline completed for {year}!")
        print(load_info)

if __name__ == "__main__":
    run_pipeline(years=[2025])
