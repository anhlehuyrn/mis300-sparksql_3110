# 1. 初始化SparkSession（SQL核心入口）
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

spark = SparkSession.builder.appName("EcommerceSQLAnalysis") .master("local[*]").getOrCreate()  # 本地模式，集群模式可改为.master("yarn")
    

# 2. 定义数据Schema（与10万条用户行为数据字段匹配）
user_behavior_schema = StructType([
    StructField("user_id", StringType(), nullable=True),
    StructField("item_id", StringType(), nullable=True),
    StructField("action", StringType(), nullable=True),
    StructField("timestamp", LongType(), nullable=True),
    StructField("category", StringType(), nullable=True)
])

# 3. 加载CSV数据（避免自动推断Schema的误差）
df = spark.read \
    .csv("user_behavior_10m.csv",  # 数据文件路径（与generate_large_behavior.py同目录）
         header=False,
         schema=user_behavior_schema)

# 4. 创建临时视图（后续所有SQL均基于此视图查询）
df.createOrReplaceTempView("user_behavior_view")

### 进阶分析任务
## 任务 1.1：统计 4 种行为总次数 + 计算购买转化率
# 1. 统计行为总次数并创建视图
spark.sql("""
CREATE TEMPORARY VIEW action_count_view AS
SELECT 
  action,
  COUNT(*) AS total_count  -- 行为总次数
FROM user_behavior_view
GROUP BY action
ORDER BY total_count DESC;
""")
# 2. 计算购买转化率（终端输出）
conversion_result = spark.sql("""
SELECT 
  ROUND(
    (SELECT total_count FROM action_count_view WHERE action = 'buy') 
    / (SELECT total_count FROM action_count_view WHERE action = 'click') * 100, 
    2
  ) AS purchase_conversion_rate
FROM action_count_view
LIMIT 1;
""")
print("=== 购买转化率结果 ===")
conversion_result.show()
# 3. 保存行为统计结果（CSV格式，带表头）
spark.sql("SELECT * FROM action_count_view") \
     .write \
     .csv("output/sql_basic_action_count",
          header=True,
          mode="overwrite")  # 覆盖已有文件


## 任务 1.2：分析商品类别热度（按点击量降序）
# 1. 统计品类点击量并创建视图
spark.sql("""
CREATE TEMPORARY VIEW category_click_view AS
SELECT 
  category,
  COUNT(*) AS click_count  -- 品类点击量
FROM user_behavior_view
WHERE action = 'click'  -- 仅关注点击行为
GROUP BY category
ORDER BY click_count DESC;
""")
# 2. 保存品类热度结果
spark.sql("SELECT * FROM category_click_view") \
     .write \
     .csv("output/sql_basic_category_click",
          header=True,
          mode="overwrite")
# 3. 终端输出品类热度Top3
print("\n=== 商品类别热度Top3 ===")
spark.sql("SELECT * FROM category_click_view LIMIT 3").show()

## 任务 1.3：分析用户活跃时段（按小时统计行为次数）
# 1. 统计时段行为次数并创建视图
spark.sql("""
CREATE TEMPORARY VIEW hourly_behavior_view AS
SELECT 
  FROM_UNIXTIME(timestamp, 'HH') AS hour,  -- 提取小时（如08、20）
  COUNT(*) AS total_behavior_count  -- 该小时总行为次数
FROM user_behavior_view
GROUP BY hour
ORDER BY hour;
""")
# 2. 保存活跃时段结果
spark.sql("SELECT * FROM hourly_behavior_view") \
     .write \
     .csv("output/sql_basic_hourly_behavior",
          header=True,
          mode="overwrite")
# 3. 终端输出高峰时段（行为次数≥8000的小时）
print("\n=== 用户活跃高峰时段 ===")
spark.sql("""
SELECT hour, total_behavior_count 
FROM hourly_behavior_view 
WHERE total_behavior_count >= 8000 
ORDER BY total_behavior_count DESC;
""").show()

### 进阶分析任务
## 任务 2.1：高价值用户识别（Top3 购买用户）
# 1. 筛选Top3购买用户并创建视图
spark.sql("""
CREATE TEMPORARY VIEW top3_buy_user_view AS
SELECT 
  user_id,
  COUNT(*) AS buy_count  -- 用户购买次数
FROM user_behavior_view
WHERE action = 'buy'
GROUP BY user_id
ORDER BY buy_count DESC
LIMIT 3;
""")
# 2. 保存Top3用户结果
spark.sql("SELECT * FROM top3_buy_user_view") \
     .write \
     .csv("output/sql_advanced_top3_user",
          header=True,
          mode="overwrite")

# 3. 终端输出Top3高价值用户
print("\n=== Top3高价值用户（购买次数最多） ===")
spark.sql("SELECT * FROM top3_buy_user_view").show()

## 任务 2.2：转化漏斗分析（点击→收藏→加购→购买，基于同一商品的递进行为）
# 1. 统计用户对每个商品的行为路径（按时间排序）
spark.sql("""
CREATE TEMPORARY VIEW user_item_behavior_path AS
SELECT 
  user_id,
  item_id,
  action,
  timestamp,
  -- 按时间排序，标记用户对该商品的行为顺序
  ROW_NUMBER() OVER (PARTITION BY user_id, item_id ORDER BY timestamp) AS action_order
FROM user_behavior_view;
""")
# 2. 筛选符合“click→collect→cart→buy”递进顺序的用户-商品行为
spark.sql("""
CREATE TEMPORARY VIEW valid_funnel_behavior AS
SELECT 
  ubv.user_id,
  ubv.item_id,
  ubv.action,
  -- 确保行为按 click→collect→cart→buy 顺序发生
  CASE 
    WHEN ubv.action = 'click' AND ubv.action_order = 1 THEN 1
    WHEN ubv.action = 'collect' AND ubv.action_order = 2 THEN 1
    WHEN ubv.action = 'cart' AND ubv.action_order = 3 THEN 1
    WHEN ubv.action = 'buy' AND ubv.action_order = 4 THEN 1
    ELSE 0
  END AS is_valid_funnel
FROM user_item_behavior_path ubv
WHERE ubv.action IN ('click', 'collect', 'cart', 'buy');
""")
# 3. 统计各环节的独立用户数（仅包含符合递进顺序的行为）
spark.sql("""
CREATE TEMPORARY VIEW funnel_user_count_view AS
SELECT 
  COUNT(DISTINCT CASE WHEN action = 'click' AND is_valid_funnel = 1 THEN user_id END) AS click_user,
  COUNT(DISTINCT CASE WHEN action = 'collect' AND is_valid_funnel = 1 THEN user_id END) AS collect_user,
  COUNT(DISTINCT CASE WHEN action = 'cart' AND is_valid_funnel = 1 THEN user_id END) AS cart_user,
  COUNT(DISTINCT CASE WHEN action = 'buy' AND is_valid_funnel = 1 THEN user_id END) AS buy_user
FROM valid_funnel_behavior;
""")
# 4. 计算各环节转化率（逻辑与之前一致，基于修正后的用户数）
spark.sql("""
CREATE TEMPORARY VIEW funnel_conversion_view AS
SELECT 
  ROUND(collect_user / click_user * 100, 2) AS click_to_collect,
  ROUND(cart_user / collect_user * 100, 2) AS collect_to_cart,
  ROUND(buy_user / cart_user * 100, 2) AS cart_to_buy,
  ROUND(buy_user / click_user * 100, 2) AS overall_conversion
FROM funnel_user_count_view;
""")
# 5. 保存并输出结果
spark.sql("SELECT * FROM funnel_conversion_view") \
     .write \
     .csv("output/sql_advanced_funnel",
          header=True,
          mode="overwrite")

print("\n=== 转化漏斗各环节转化率 ===")
spark.sql("SELECT * FROM funnel_conversion_view").show()

## 任务 2.3：商品复购率分析
# 1. 统计用户-商品购买次数
spark.sql("""
CREATE TEMPORARY VIEW item_user_buy_count_view AS
SELECT 
  item_id,
  user_id,
  COUNT(*) AS buy_times  -- 用户对该商品的购买次数
FROM user_behavior_view
WHERE action = 'buy'
GROUP BY item_id, user_id;
""")
# 2. 计算商品复购率
spark.sql("""
CREATE TEMPORARY VIEW item_repurchase_view AS
SELECT 
  item_id,
  COUNT(DISTINCT user_id) AS total_buy_user,  -- 商品总购买用户数
  -- 复购用户数（保留别名，用于结果展示）
  COUNT(DISTINCT CASE WHEN buy_times >= 2 THEN user_id END) AS repurchase_user,
  -- 用原始表达式替换别名，避免引用未解析的字段
  ROUND(
    (COUNT(DISTINCT CASE WHEN buy_times >= 2 THEN user_id END) / COUNT(DISTINCT user_id)) * 100, 
    2
  ) AS repurchase_rate  -- 复购率
FROM item_user_buy_count_view
GROUP BY item_id
ORDER BY repurchase_rate DESC;
""")
# 3. 保存复购率结果
spark.sql("SELECT * FROM item_repurchase_view") \
     .write \
     .csv("output/sql_advanced_repurchase",
          header=True,
          mode="overwrite")
# 4. 终端输出复购率Top3商品
print("\n=== 商品复购率Top3 ===")
spark.sql("SELECT * FROM item_repurchase_view LIMIT 3").show()

## 任务 2.4：用户次日留存率
# 1. 提取用户每日活跃记录（去重）
spark.sql("""
CREATE TEMPORARY VIEW user_daily_active_view AS
SELECT 
  DISTINCT user_id,
  DATE(FROM_UNIXTIME(timestamp)) AS active_date  -- 转为日期格式（如2023-11-01）
FROM user_behavior_view;
""")
# 2. 用窗口函数获取前一天活跃日期
spark.sql("""
CREATE TEMPORARY VIEW user_prev_active_view AS
SELECT 
  user_id,
  active_date,
  LAG(active_date, 1) OVER (PARTITION BY user_id ORDER BY active_date) AS prev_active_date
FROM user_daily_active_view;
""")
# 3. 计算次日留存率（修正版）
spark.sql("""
CREATE TEMPORARY VIEW user_retention_view AS
SELECT 
  active_date AS current_date,
  COUNT(DISTINCT user_id) AS current_active_user,  -- 当天活跃用户数
  -- 留存用户数（保留别名，用于结果展示）
  COUNT(DISTINCT CASE WHEN DATEDIFF(active_date, prev_active_date) = 1 THEN user_id END) AS retained_user,
  -- 用原始表达式替换别名，避免引用未解析的字段
  ROUND(
    (COUNT(DISTINCT CASE WHEN DATEDIFF(active_date, prev_active_date) = 1 THEN user_id END) 
    / COUNT(DISTINCT user_id)) * 100, 
    2
  ) AS day2_retention_rate
FROM user_prev_active_view
GROUP BY active_date
ORDER BY active_date;
""")
# 4. 保存留存率结果
spark.sql("SELECT * FROM user_retention_view") \
     .write \
     .csv("output/sql_advanced_retention",
          header=True,
          mode="overwrite")
# 5. 终端输出近3天留存率
print("\n=== 近3天用户次日留存率 ===")
spark.sql("""
SELECT current_date, current_active_user, retained_user, day2_retention_rate 
FROM user_retention_view 
ORDER BY current_date DESC 
LIMIT 3;
""").show()