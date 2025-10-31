# mis300-sparksql_3110
Spark SQL Experiment: E-commerce User Behavior Analysis

Overview
- Implements all tasks from `req.txt` using PySpark SQL, including basic and advanced analyses, plus popular video stats from access logs.
- Produces CSV outputs under `spark-sql-project/output/` for easy consumption and review.
- Single runner: `run_all_tasks.py` executes Tasks 1.1–2.5 end-to-end.

Environment Preparation
- Requirements: Python 3.8+, Java 8+, Spark 3.x.
- Install PySpark: `pip install pyspark==3.5.0` (or match your Spark version).
- Windows: local mode (`master=local[*]`) requires no Hadoop setup since we use the local filesystem.

Data Inputs
- `user_behavior_10m.csv` (in `spark-sql-project/`): expected columns order without header: `user_id,item_id,action,timestamp,category`.
  - If your dataset uses a different order (e.g., `category` before `action`), adjust the schema in `run_all_tasks.py` accordingly.
- `access.log/access.20161111.log`: plain text log lines with fields: date, time, URL, traffic, IP. URL examples: `http://www.imooc.com/video/4500`.

How to Run
- From the `spark-sql-project` directory:
- `python run_all_tasks.py`
- Outputs are written to `output/` subfolders. Each is a Spark CSV directory with header.

Task Coverage and Outputs
- Task 1.1: Behavior totals + purchase conversion rate
  - View: `action_count_view`
  - Output: `output/sql_basic_action_count/`
  - Terminal prints `purchase_conversion_rate`.
- Task 1.2: Category popularity (by clicks)
  - View: `category_click_view`
  - Output: `output/sql_basic_category_click/`
  - Terminal prints Top 3 categories.
- Task 1.3: Active hours (total behaviors by hour)
  - View: `hourly_behavior_view`
  - Output: `output/sql_basic_hourly_behavior/`
  - Terminal prints peak hours (example threshold ≥ 8000).
- Task 2.1: Top 3 purchasing users
  - View: `top3_buy_user_view`
  - Output: `output/sql_advanced_top3_user/`
  - Terminal prints Top 3 users.
- Task 2.2: Funnel analysis (click → collect → cart → buy)
  - Views: `user_item_behavior_path`, `valid_funnel_behavior`, `funnel_user_count_view`, `funnel_conversion_view`
  - Output: `output/sql_advanced_funnel/`
  - Terminal prints link conversion rates.
- Task 2.3: Item repurchase rate
  - Views: `item_user_buy_count_view`, `item_repurchase_view`
  - Output: `output/sql_advanced_repurchase/`
  - Terminal prints Top 3 by repurchase rate.
- Task 2.4: Day-2 retention
  - Views: `user_daily_active_view`, `user_prev_active_view`, `user_retention_view`
  - Output: `output/sql_advanced_retention/`
  - Terminal prints last 3 days retention.
