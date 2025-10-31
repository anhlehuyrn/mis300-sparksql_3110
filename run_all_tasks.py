from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
import os
import traceback


def main():
    # 初始化 SparkSession
    spark = (
        SparkSession.builder
        .appName("EcommerceSQLAnalysis_AllTasks")
        .master("local[*]")
        .getOrCreate()
    )

    # 定义数据 Schema（与当前项目数据文件一致）
    user_behavior_schema = StructType([
        StructField("user_id", StringType(), nullable=True),
        StructField("item_id", StringType(), nullable=True),
        StructField("action", StringType(), nullable=True),
        StructField("timestamp", LongType(), nullable=True),
        StructField("category", StringType(), nullable=True),
    ])

    # 加载 CSV 数据
    df = (
        spark.read
        .csv(
            "user_behavior_10m.csv",  # 按项目根目录下的路径
            header=False,
            schema=user_behavior_schema,
        )
    )

    # 创建临时视图
    df.createOrReplaceTempView("user_behavior_view")

    # 汇总结果文本
    summary_lines = []

    # 任务 1.1：统计四类行为 + 购买转化率
    spark.sql(
        """
        CREATE TEMPORARY VIEW action_count_view AS
        SELECT 
          action,
          COUNT(*) AS total_count
        FROM user_behavior_view
        GROUP BY action
        ORDER BY total_count DESC;
        """
    )
    # 避免标量子查询的代码生成问题，采用 CROSS JOIN 计算转化率
    conversion_result = spark.sql(
        """
        SELECT 
          ROUND(buy.total_count / click.total_count * 100, 2) AS purchase_conversion_rate
        FROM 
          (SELECT total_count FROM action_count_view WHERE action = 'buy') AS buy
        CROSS JOIN 
          (SELECT total_count FROM action_count_view WHERE action = 'click') AS click;
        """
    )
    print("=== 购买转化率结果 ===")
    conversion_result.show()
    try:
        rate = conversion_result.collect()[0][0]
        summary_lines.append(f"Task 1.1 purchase_conversion_rate: {rate}%")
    except Exception as e:
        summary_lines.append(f"Task 1.1 purchase_conversion_rate: ERROR {e}")
    # 确保输出目录存在
    os.makedirs("output", exist_ok=True)

    def safe_write_csv(df, out_dir, header=True):
        """
        优先使用 Spark CSV 写出；若失败（如 Windows/Hadoop 环境问题），
        回退为本地单文件 CSV（pandas）。
        """
        try:
            (df.coalesce(1)
               .write
               .mode("overwrite")
               .option("header", "true" if header else "false")
               .csv(out_dir))
        except Exception as e:
            try:
                import pandas as pd
                out_file = out_dir.rstrip("/\\") + ".csv"
                pd_df = df.toPandas()
                pd_df.to_csv(out_file, index=False)
                print(f"[WARN] Spark CSV 写入失败 {out_dir}，已回退写入 {out_file}。")
                with open("output/results_summary.txt", "a", encoding="utf-8") as fh:
                    fh.write(f"\n[WARN] Spark 写入失败 {out_dir}，回退文件: {out_file}.\n")
                    fh.write(f"Reason: {e}\n")
            except Exception as e2:
                print(f"[ERROR] Spark 与 pandas 写入均失败 {out_dir}: {e2}")
                print(traceback.format_exc())
                with open("output/results_summary.txt", "a", encoding="utf-8") as fh:
                    fh.write(f"\n[ERROR] Spark 与 pandas 写入均失败 {out_dir}: {e2}\n")
                    fh.write(traceback.format_exc() + "\n")

    safe_write_csv(spark.sql("SELECT * FROM action_count_view"), "output/sql_basic_action_count")

    # 任务 1.2：品类点击热度
    spark.sql(
        """
        CREATE TEMPORARY VIEW category_click_view AS
        SELECT 
          category,
          COUNT(*) AS click_count
        FROM user_behavior_view
        WHERE action = 'click'
        GROUP BY category
        ORDER BY click_count DESC;
        """
    )
    safe_write_csv(spark.sql("SELECT * FROM category_click_view"), "output/sql_basic_category_click")
    print("\n=== 商品类别热度 Top3 ===")
    spark.sql("SELECT * FROM category_click_view LIMIT 3").show()
    try:
        cats = spark.sql("SELECT category, click_count FROM category_click_view").collect()
        summary_lines.append("Task 1.2 category_clicks:")
        for r in cats:
            summary_lines.append(f"  {r['category']}: {r['click_count']}")
    except Exception as e:
        summary_lines.append(f"Task 1.2 category_clicks: ERROR {e}")

    # 任务 1.3：活跃时段（按小时）
    spark.sql(
        """
        CREATE TEMPORARY VIEW hourly_behavior_view AS
        SELECT 
          FROM_UNIXTIME(timestamp, 'HH') AS hour,
          COUNT(*) AS total_behavior_count
        FROM user_behavior_view
        GROUP BY hour
        ORDER BY hour;
        """
    )
    safe_write_csv(spark.sql("SELECT * FROM hourly_behavior_view"), "output/sql_basic_hourly_behavior")
    print("\n=== 用户活跃高峰时段（示例阈值：>=8000） ===")
    spark.sql(
        """
        SELECT hour, total_behavior_count 
        FROM hourly_behavior_view 
        WHERE total_behavior_count >= 8000 
        ORDER BY total_behavior_count DESC;
        """
    ).show()
    try:
        hours = spark.sql("SELECT hour, total_behavior_count FROM hourly_behavior_view ORDER BY hour").collect()
        summary_lines.append("Task 1.3 hourly_behavior:")
        for r in hours:
            summary_lines.append(f"  {r['hour']}: {r['total_behavior_count']}")
    except Exception as e:
        summary_lines.append(f"Task 1.3 hourly_behavior: ERROR {e}")

    # 任务 2.1：Top3 购买用户
    spark.sql(
        """
        CREATE TEMPORARY VIEW top3_buy_user_view AS
        SELECT 
          user_id,
          COUNT(*) AS buy_count
        FROM user_behavior_view
        WHERE action = 'buy'
        GROUP BY user_id
        ORDER BY buy_count DESC
        LIMIT 3;
        """
    )
    safe_write_csv(spark.sql("SELECT * FROM top3_buy_user_view"), "output/sql_advanced_top3_user")
    print("\n=== Top3 高价值用户（购买次数最多） ===")
    spark.sql("SELECT * FROM top3_buy_user_view").show()
    try:
        top3 = spark.sql("SELECT user_id, buy_count FROM top3_buy_user_view").collect()
        summary_lines.append("Task 2.1 top3_buy_users:")
        for r in top3:
            summary_lines.append(f"  {r['user_id']}: {r['buy_count']}")
    except Exception as e:
        summary_lines.append(f"Task 2.1 top3_buy_users: ERROR {e}")

    # 任务 2.2：转化漏斗（click→collect→cart→buy）
    spark.sql(
        """
        CREATE TEMPORARY VIEW user_item_behavior_path AS
        SELECT 
          user_id,
          item_id,
          action,
          timestamp,
          ROW_NUMBER() OVER (PARTITION BY user_id, item_id ORDER BY timestamp) AS action_order
        FROM user_behavior_view;
        """
    )
    spark.sql(
        """
        CREATE TEMPORARY VIEW valid_funnel_behavior AS
        SELECT 
          ubv.user_id,
          ubv.item_id,
          ubv.action,
          CASE 
            WHEN ubv.action = 'click' AND ubv.action_order = 1 THEN 1
            WHEN ubv.action = 'collect' AND ubv.action_order = 2 THEN 1
            WHEN ubv.action = 'cart' AND ubv.action_order = 3 THEN 1
            WHEN ubv.action = 'buy' AND ubv.action_order = 4 THEN 1
            ELSE 0
          END AS is_valid_funnel
        FROM user_item_behavior_path ubv
        WHERE ubv.action IN ('click', 'collect', 'cart', 'buy');
        """
    )
    spark.sql(
        """
        CREATE TEMPORARY VIEW funnel_user_count_view AS
        SELECT 
          COUNT(DISTINCT CASE WHEN action = 'click' AND is_valid_funnel = 1 THEN user_id END) AS click_user,
          COUNT(DISTINCT CASE WHEN action = 'collect' AND is_valid_funnel = 1 THEN user_id END) AS collect_user,
          COUNT(DISTINCT CASE WHEN action = 'cart' AND is_valid_funnel = 1 THEN user_id END) AS cart_user,
          COUNT(DISTINCT CASE WHEN action = 'buy' AND is_valid_funnel = 1 THEN user_id END) AS buy_user
        FROM valid_funnel_behavior;
        """
    )
    spark.sql(
        """
        CREATE TEMPORARY VIEW funnel_conversion_view AS
        SELECT 
          ROUND(collect_user / click_user * 100, 2) AS click_to_collect,
          ROUND(cart_user / collect_user * 100, 2) AS collect_to_cart,
          ROUND(buy_user / cart_user * 100, 2) AS cart_to_buy,
          ROUND(buy_user / click_user * 100, 2) AS overall_conversion
        FROM funnel_user_count_view;
        """
    )
    safe_write_csv(spark.sql("SELECT * FROM funnel_conversion_view"), "output/sql_advanced_funnel")
    print("\n=== 转化漏斗各环节转化率 ===")
    spark.sql("SELECT * FROM funnel_conversion_view").show()
    try:
        conv = spark.sql("SELECT * FROM funnel_conversion_view").collect()[0]
        summary_lines.append("Task 2.2 funnel_conversion:")
        summary_lines.append(f"  click_to_collect: {conv['click_to_collect']}%")
        summary_lines.append(f"  collect_to_cart: {conv['collect_to_cart']}%")
        summary_lines.append(f"  cart_to_buy: {conv['cart_to_buy']}%")
        summary_lines.append(f"  overall_conversion: {conv['overall_conversion']}%")
    except Exception as e:
        summary_lines.append(f"Task 2.2 funnel_conversion: ERROR {e}")

    # 任务 2.3：商品复购率
    spark.sql(
        """
        CREATE TEMPORARY VIEW item_user_buy_count_view AS
        SELECT 
          item_id,
          user_id,
          COUNT(*) AS buy_times
        FROM user_behavior_view
        WHERE action = 'buy'
        GROUP BY item_id, user_id;
        """
    )
    spark.sql(
        """
        CREATE TEMPORARY VIEW item_repurchase_view AS
        SELECT 
          item_id,
          COUNT(DISTINCT user_id) AS total_buy_user,
          COUNT(DISTINCT CASE WHEN buy_times >= 2 THEN user_id END) AS repurchase_user,
          ROUND(
            (COUNT(DISTINCT CASE WHEN buy_times >= 2 THEN user_id END) 
            / COUNT(DISTINCT user_id)) * 100, 
            2
          ) AS repurchase_rate
        FROM item_user_buy_count_view
        GROUP BY item_id
        ORDER BY repurchase_rate DESC;
        """
    )
    safe_write_csv(spark.sql("SELECT * FROM item_repurchase_view"), "output/sql_advanced_repurchase")
    print("\n=== 商品复购率 Top3 ===")
    spark.sql("SELECT * FROM item_repurchase_view LIMIT 3").show()
    try:
        tops = spark.sql("SELECT item_id, repurchase_rate, repurchase_user, total_buy_user FROM item_repurchase_view ORDER BY repurchase_rate DESC LIMIT 10").collect()
        summary_lines.append("Task 2.3 item_repurchase_top10:")
        for r in tops:
            summary_lines.append(f"  item {r['item_id']}: rate {r['repurchase_rate']}% (repurchase {r['repurchase_user']}/total {r['total_buy_user']})")
    except Exception as e:
        summary_lines.append(f"Task 2.3 item_repurchase_top10: ERROR {e}")

    # 任务 2.4：用户次日留存率
    spark.sql(
        """
        CREATE TEMPORARY VIEW user_daily_active_view AS
        SELECT 
          DISTINCT user_id,
          DATE(FROM_UNIXTIME(timestamp)) AS active_date
        FROM user_behavior_view;
        """
    )
    spark.sql(
        """
        CREATE TEMPORARY VIEW user_prev_active_view AS
        SELECT 
          user_id,
          active_date,
          LAG(active_date, 1) OVER (PARTITION BY user_id ORDER BY active_date) AS prev_active_date
        FROM user_daily_active_view;
        """
    )
    spark.sql(
        """
        CREATE TEMPORARY VIEW user_retention_view AS
        SELECT 
          active_date AS current_date,
          COUNT(DISTINCT user_id) AS current_active_user,
          COUNT(DISTINCT CASE WHEN DATEDIFF(active_date, prev_active_date) = 1 THEN user_id END) AS retained_user,
          ROUND(
            (COUNT(DISTINCT CASE WHEN DATEDIFF(active_date, prev_active_date) = 1 THEN user_id END) 
            / COUNT(DISTINCT user_id)) * 100, 
            2
          ) AS day2_retention_rate
        FROM user_prev_active_view
        GROUP BY active_date
        ORDER BY active_date;
        """
    )
    safe_write_csv(spark.sql("SELECT * FROM user_retention_view"), "output/sql_advanced_retention")
    print("\n=== 近3天用户次日留存率 ===")
    spark.sql(
        """
        SELECT current_date, current_active_user, retained_user, day2_retention_rate 
        FROM user_retention_view 
        ORDER BY current_date DESC 
        LIMIT 3;
        """
    ).show()
    try:
        ret = spark.sql("SELECT current_date, current_active_user, retained_user, day2_retention_rate FROM user_retention_view ORDER BY current_date DESC LIMIT 10").collect()
        summary_lines.append("Task 2.4 day2_retention_last10:")
        for r in ret:
            summary_lines.append(f"  {r['current_date']}: active {r['current_active_user']}, retained {r['retained_user']}, rate {r['day2_retention_rate']}%")
    except Exception as e:
        summary_lines.append(f"Task 2.4 day2_retention_last10: ERROR {e}")


if __name__ == "__main__":
    main()