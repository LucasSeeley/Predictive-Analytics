# Predictive Analytics — README

Quick setup:

1. Get API keys
- Go to https://collegefootballdata.com/key 
- Enter your email to receive your free API Key

2. Configure key in your .env file at the top of this project (Only CFB_API_KEY is required)
- export CFB_API_KEY = "..."

3. Common commands
- Install dependencies
    - pip install -r requirements.txt
- Run the pipeline to get CFB Data
    - python -m pipelines.cfb_analytics_pipeline
- Run the Predictive Insights
    - python -m ai.cfb_ai
- Run the Dashboard
    - streamlit run dashboards/cfb_dashboard.py