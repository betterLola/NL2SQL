# -*- coding: utf-8 -*-
import os
import re
from datetime import datetime, timedelta
import streamlit as st
from dotenv import load_dotenv

# LangChain 相关
from langchain_community.utilities import SQLDatabase
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_community.agent_toolkits import create_sql_agent
from langchain_community.llms import Tongyi
from langchain_core.tools import tool
import pymysql

# 加载 .env 文件中的环境变量
load_dotenv()

# ====================== 1. 业务日期预处理 ======================
def parse_business_date(text):
    """
    处理模糊的时间词汇，将其转换为规范年份或具体日期区间
    """
    current_date = datetime.now()
    current_year = current_date.year
    yesterday = current_date - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y年%m月%d日')
    today_str = current_date.strftime('%Y年%m月%d日')

    # 替换中文年份词汇
    text = text.replace('今年', f'{current_year}年')
    text = text.replace('去年', f'{current_year - 1}年')
    text = text.replace('前年', f'{current_year - 2}年')
    text = text.replace('大前年', f'{current_year - 3}年')

    # 替换中文数字年份 (如: 二五年 -> 25年)
    text = text.replace('二四年', '24年').replace('二五年', '25年').replace('二六年', '26年')

    # 补全年份：25年 → 2025年
    text = re.sub(r'(?<!\d)(\d{2})年', r'20\1年', text)

    # 补全当前年份：如果只有"X月"，没有"年"，则加上当前年份
    text = re.sub(r'(?<!年)(\d{1,2}月)', f'{current_year}年\g<1>', text)

    # 处理"至今" / "截止今日/今天" → 替换为今天日期
    text = text.replace('至今', f'至{today_str}')
    text = text.replace('截止今日', f'截止{today_str}')
    text = text.replace('截止今天', f'截止{today_str}')

    # 处理"截止昨日/昨天" → 替换为昨天日期
    text = text.replace('截止昨日', f'截止{yesterday_str}')
    text = text.replace('截止昨天', f'截止{yesterday_str}')

    # 处理"近X天" → 替换为具体日期区间（开始日到昨天）
    def replace_recent_days(m):
        n = int(m.group(1))
        start = (current_date - timedelta(days=n)).strftime('%Y年%m月%d日')
        return f'{start}至{yesterday_str}'
    text = re.sub(r'近(\d+)天', replace_recent_days, text)

    # 处理"近X周" → 替换为具体日期区间
    def replace_recent_weeks(m):
        n = int(m.group(1))
        start = (current_date - timedelta(weeks=n)).strftime('%Y年%m月%d日')
        return f'{start}至{yesterday_str}'
    text = re.sub(r'近(\d+)周', replace_recent_weeks, text)

    # 处理"近X个月" → 替换为具体日期区间（按30天近似）
    def replace_recent_months(m):
        n = int(m.group(1))
        start = (current_date - timedelta(days=n * 30)).strftime('%Y年%m月%d日')
        return f'{start}至{yesterday_str}'
    text = re.sub(r'近(\d+)个月', replace_recent_months, text)

    # 处理"本月" → 当前年月
    text = text.replace('本月', f'{current_year}年{current_date.month}月')

    # 处理"上月" → 上个月（含年份）
    if current_date.month == 1:
        last_month_year, last_month = current_year - 1, 12
    else:
        last_month_year, last_month = current_year, current_date.month - 1
    text = text.replace('上月', f'{last_month_year}年{last_month}月')

    return text

# ====================== 2. 数据库配置 ======================
# 从 .env 文件或环境变量中读取（请参考 .env.example 进行配置）
DB_USER = os.getenv('DB_USER', 'root')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '3306')
DB_NAME = os.getenv('DB_NAME', 'your_database_name')

# 构建 SQLAlchemy URI
# 需要确保安装: pip install pymysql sqlalchemy
MYSQL_URI = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"

# 定义友好的错误/解析失败处理函数
def friendly_error_handler(error):
    error_str = str(error)
    # 模型把 Final Answer 写成了 Action Input，提取真实内容直接返回
    match = re.search(r'Action Input:\s*Final Answer:\s*(.*)', error_str, re.DOTALL)
    if match:
        return match.group(1).strip()
    return (
        "💡 为了帮你精准查到数据，请补充3个关键信息就好：\n\n"
        "1. **要查的指标**（比如：月活、点击量、服务次数）\n"
        "2. **时间范围**（比如：2026年3月、近3天）\n"
        "3. **可选：特定维度**（比如：安卓、苹果、具体服务名称）\n\n"
        "**举个例子**：「查询2026年3月1日的月活」"
    )

# ====================== 3. 自定义工具 ======================
@tool
def check_date_available(stat_date: str) -> str:
    """校验某日期是否在数据库中，参数：stat_date（必须是单个日期字符串，例如 '2025-02-01' 或 '2025-02'，千万不要传入用逗号分隔的多个日期）。在执行任何具体的SQL查询之前，应该优先使用此工具检查日期范围内是否有数据。"""
    try:
        conn = pymysql.connect(host=DB_HOST, port=int(DB_PORT), user=DB_USER, password=DB_PASSWORD, database=DB_NAME, charset='utf8mb4')
        cursor = conn.cursor()
        
        # 清理可能被大模型误传入的冗余字符，如空格或引号
        stat_date = stat_date.strip(" '\"")
        
        if "-" in stat_date and len(stat_date) == 7:  # 月份格式 YYYY-MM
            cursor.execute("SELECT COUNT(*) FROM platform_daily_metrics WHERE stat_date LIKE %s", (f"{stat_date}%",))
            count1 = cursor.fetchone()[0]
            # 也顺便查一下月活表
            cursor.execute("SELECT COUNT(*) FROM platform_mau WHERE date_month = %s", (f"{stat_date}-01",))
            count2 = cursor.fetchone()[0]
            # 校验新增用户留存表
            cursor.execute("SELECT COUNT(*) FROM app_retention WHERE stat_date LIKE %s", (f"{stat_date}%",))
            count3 = cursor.fetchone()[0]
            count = count1 + count2 + count3
        else: # 具体日期格式 YYYY-MM-DD
            cursor.execute("SELECT COUNT(*) FROM platform_daily_metrics WHERE stat_date = %s", (stat_date,))
            count1 = cursor.fetchone()[0]
            # 也顺便查一下月活表
            cursor.execute("SELECT COUNT(*) FROM platform_mau WHERE date_month = %s", (stat_date,))
            count2 = cursor.fetchone()[0]
            # 校验新增用户留存表
            cursor.execute("SELECT COUNT(*) FROM app_retention WHERE stat_date = %s", (stat_date,))
            count3 = cursor.fetchone()[0]
            count = count1 + count2 + count3
            
        conn.close()
        
        if count > 0:
            return f"✅ {stat_date} 有数据，可以进一步编写SQL查询具体指标。"
        else:
            return f"❌ {stat_date} 暂无数据，请告知用户换个时间范围试试。"
    except Exception as e:
        return f"❌ 校验失败: {e}"

# ====================== 4. 初始化 LangChain 核心组件 ======================
@st.cache_resource
def get_db_and_agent():
    # 1. 连接数据库
    db = SQLDatabase.from_uri(
        MYSQL_URI,
        include_tables=['resource_total', 'platform_daily_metrics', '5100_detail', 'app_retention', 'platform_mau', 'resource_detail', 'core_detail', 'search_detail'],
        sample_rows_in_table_info=3
    )    
    # 2. 初始化大模型
    llm = Tongyi(
        model="qwen-turbo",
        api_key=os.getenv("TONGYI_API_KEY"),  # 配置于 .env 文件的 TONGYI_API_KEY
        temperature=0.7,
    )

    # 3. 创建 SQL Agent
    agent_executor = create_sql_agent(
        llm=llm,
        db=db,
        agent_type="zero-shot-react-description",
        verbose=True,
        handle_parsing_errors=friendly_error_handler,
        max_iterations=15,  # 增大迭代次数，避免模型在思考、尝试多工具时提前终止
        extra_tools=[check_date_available],
        prefix=”””你是一个强大的数据分析助手。你可以访问一个 MySQL 数据库，其中包含平台各项运营数据。请根据用户的自然语言请求，编写并执行 SQL 查询，然后用通俗易懂的自然语言回答用户。

重要表结构及业务说明：
1. `resource_total` (资源位点击汇总数据)：包含 resource_amount(点击量), resource_name(资源位名称), stat_date(统计日期), port(端，如安卓、苹果、鸿蒙)。
   **资源位名称(resource_name)中英文映射关系：**
   - `mid_banner`：代表 腰封、首页腰封、腰封banner
   - `news_click`：代表 头条、成都头条、头条新闻
   - `person_banner_click`：代表 个人中心腰封
   - `top_banner_click`：代表 顶部、顶部banner
   - `Hometopic_click`：代表 卡片位、首页专题、专题卡片
   - `king_kong`：代表 金刚位
2. `platform_daily_metrics` (平台每日综合指标)：包含 stat_date(日期), alipay_dau(支付宝小程序日活), app_dau(APP总日活), total_register_users(累计注册用户数), total_service_times(累计服务次数) 等。
3. `5100_detail` (所有服务使用明细数据，包含服务超市及部分其他资源位)：包含 service_amount(使用次数), service_name(服务名称), stat_date(日期), port, resource_name。
4. `resource_detail` (资源位明细数据，包含腰封、卡片位、头条等)：包含 resource_amount(点击量), resource_name(资源位类型), item_name(具体资源位名称), stat_date(日期), port。
5. `core_detail` (核心功能/金刚位明细数据)：包含 resource_amount(点击量), resource_name(功能类型), item_name(具体功能名称), stat_date(日期), port。
6. `search_detail` (搜索词明细数据)：包含 search_amount(搜索次数), search_name(搜索关键词), stat_date(日期), port, resource_name。
7. `app_retention` (APP新增用户留存数据)：包含 platform（APP中是安卓/苹果/鸿蒙）, stat_date（日期）, day_1_retention（次日新增用户留存率）。
8. `platform_mau` (APP月活及留存表)：包含 date_month（月份,格式YYYY-MM-01）, mau(月活), mau_percent(月活率), total_register_users(月末注册用户数), dau(月度日均日活), retention_percent(平均留存率)。

【新增业务场景处理规则 - 极其重要】：
- **时间信息判断规则**：用户输入在传入前已经过预处理，”近X天/周/月”、”至今”、”截止昨日”等均已转换为具体日期区间（如”2026年3月1日至2026年3月29日”）。因此：
  - 如果你看到具体日期区间（含年月日），说明时间已明确，可以直接查询。
  - 如果你看到”近期”、”最近”、”最新”等**没有数字的模糊词**，或用户完全没提时间，则必须直接回复：”Final Answer: 为了帮您精准查询数据，请补充具体的时间范围，例如：2026年3月、近7天、3月1日至3月15日等。”，然后停止，不要尝试查询数据库。
- **”资源位”概念澄清（极其重要）**：
  - 在 `resource_detail` 和 `core_detail` 表中，`resource_name` 字段代表**资源位类型**（如 mid_banner=腰封、Hometopic_click=卡片位、news_click=成都头条），`item_name` 字段代表**具体活动/服务名称**（如”装修补贴”、”医保服务”）。
  - 当用户问”哪些资源位”、”在哪些资源位上线”、”各资源位流量”时，指的是 `resource_name` 字段，应该按 `resource_name` 分组汇总，并用中文名展示（如”腰封(mid_banner)”）。
  - 当用户问”有哪些服务/活动”、”具体名称”时，才是指 `item_name` 字段。
- **业务流量/表现查询**：当用户输入”查xx业务流量”、”xx业务表现情况”或类似询问具体业务的请求时，你必须：
  1. **第一步：检查时间**。按照上述”时间信息判断规则”处理，如果时间不明确则追问，不要查库。
  2. **第二步：主动追问资源位**。如果时间已明确但没有具体资源位名称，先回答：”Final Answer: 为了帮您更精准地分析【xx业务】在【时间段】的表现，请问该业务上线了哪些具体的资源位？（如果您不清楚具体资源位名称，可以告诉我关键词，我会为您进行模糊查询）”
  3. **第三步：执行模糊查询并用SQL汇总**。当用户提供关键词或确认进行模糊查询后，你必须：
     - 在以下**四个表**中匹配名称包含该关键词的数据（**缺一不可**）：
       * `5100_detail`（所有服务）：匹配 `service_name LIKE '%关键词%'`，字段为 `service_amount`
       * `core_detail`（金刚位）：匹配 `item_name LIKE '%关键词%'`，字段为 `resource_amount`
       * `resource_detail`（腰封/卡片位/头条等）：匹配 `item_name LIKE '%关键词%'`，字段为 `resource_amount`
       * `search_detail`（搜索词）：匹配 `search_name LIKE '%关键词%'`，字段为 `search_amount`
     - **必须使用 SQL 的 SUM() 和 GROUP BY 进行汇总**，严禁在 Final Answer 中手动列出加法公式（如 24+86+91...）！
     - 推荐 SQL 结构：使用 UNION ALL 合并四表数据，外层再 GROUP BY 汇总，例如：
       SELECT category, item_name, SUM(amount) as total FROM (
         SELECT '所有服务' as category, service_name as item_name, service_amount as amount FROM 5100_detail WHERE service_name LIKE '%关键词%' AND stat_date BETWEEN 'start' AND 'end'
         UNION ALL
         SELECT '金刚位', item_name, resource_amount FROM core_detail WHERE item_name LIKE '%关键词%' AND stat_date BETWEEN 'start' AND 'end'
         UNION ALL
         SELECT '资源位', item_name, resource_amount FROM resource_detail WHERE item_name LIKE '%关键词%' AND stat_date BETWEEN 'start' AND 'end'
         UNION ALL
         SELECT '搜索词', search_name, search_amount FROM search_detail WHERE search_name LIKE '%关键词%' AND stat_date BETWEEN 'start' AND 'end'
       ) t GROUP BY category, item_name ORDER BY total DESC LIMIT 50
  4. **如果用户追问”XX业务在哪些资源位”**：此时应该按 `resource_name` 分组，查询 SQL 改为：
       SELECT resource_name, SUM(resource_amount) as total
       FROM resource_detail
       WHERE item_name LIKE '%关键词%' AND stat_date BETWEEN 'start' AND 'end'
       GROUP BY resource_name
       ORDER BY total DESC
     并在回答中用中文展示资源位名称，如”腰封(mid_banner)”。
  5. **汇总回答**：将查询结果按类别分类展示，并在回答开头**明确标注实际查询的日期区间**，格式为：「查询时间段：XXXX年XX月XX日 - XXXX年XX月XX日」，方便用户确认。使用 Markdown 表格展示数据更清晰。

查询时间处理规则与计算说明（非常重要）：
- 工具使用提示：在真正写SQL前，如果用户指明了特定日期或月份，可以调用 `check_date_available` 工具检查该日期是否有数据。但注意：如果用户给的是日期区间（如”3月17日至3月30日”），只需检查起始日期即可，不要因为结束日期暂无数据就停止查询——数据库可能还在更新中，区间内有数据就应该继续查询并返回已有数据。
- **整年多条数据确认（极其重要）**：如果用户的请求中**只有年份而没有具体月份**（例如”查询24年和25年的月活”），**绝对不允许你私自假设为1月份的数据进行查询！** 面对整年宽泛范围，请**不要去调用任何工具，也不要写 Action: Final Answer**，你只需要**直接在下一行输出**：`Final Answer: 您查询的是整年数据，包含多个月份。请问您是希望展示这几年所有月份的完整明细，还是想查看年度平均值/总量？或者您想指定某个具体的月份（如2月）进行对比？`，然后停止。
- 范围查询：如果用户明确表示要看”全年所有数据”或”每个月的数据”，你应该使用 `BETWEEN '2024-01-01' AND '2024-12-31'` 查出多条结果，并用 Markdown 表格完整展示。
- **留存数据查询逻辑**：
  - 如果用户查询”新增用户留存”（如新增留存均值），必须查询 `app_retention` 表。
    - 如果是查某个月（如2026年2月），应使用 `stat_date LIKE '2026-02%'` 并计算平均值 `AVG(day_1_retention)`。
  - 如果用户查询”活跃用户留存”（如月活留存），必须查询 `platform_mau` 表。
    - 此时必须将月份转换为该月1号的日期，例如：”2025年2月”对应 `date_month = '2025-02-01'`，字段为 `retention_percent`。
- 如果用户查询特定月份，但在 `platform_mau` 表中查询，你必须将该月转换为该月1号的日期，例如：”2025年2月”在 `platform_mau` 表中对应 `date_month = '2025-02-01'`。
- 当用户询问跨年对比（如：2025年和2026年的2月数据），请确保 SQL 中包含这两个特定月份，例如：对于 `platform_mau` 表，查询条件应为 `date_month IN ('2025-02-01', '2026-02-01')`。
- **计算与对比要求**：如果用户提出计算【增幅、变化率、同比、环比】（例如查询25和26年2月月活及其增幅），你必须：
  1. 写**一条**SQL同时查出两个时期的数据（强烈建议使用 `IN` 语法，如 `WHERE date_month IN ('2025-02-01', '2026-02-01')`），**绝对不要**分成两次分别查询。
  2. **红线规则**：你必须真实调用 `sql_db_query` 工具执行上一步写好的 SQL，并**亲眼看到**工具返回的具体数字后，才能进行计算和回答。**严禁**在没有任何数据的情况下，直接输出包含 “XXX”、”YYY”、”ZZZ” 等占位符的废话！如果遇到阻碍，请继续调用工具直到拿到数据！
  3. 利用内部计算能力计算增幅，公式为：(本期数据 - 同期数据) / 同期数据，并在最终回答中展示真实的具体数值和计算出的百分比结果。

回答要求：
- **资源位名称展示规则（极其重要）**：在最终回答中展示 resource_name 字段值时，必须使用”中文名（英文名）”格式，对照关系为：卡片位（Hometopic_click）、腰封（mid_banner）、成都头条（news_click）、个人中心腰封（person_banner_click）、顶部banner（top_banner_click）、金刚位（king_kong）。如果英文名不在以上列表中，直接显示英文名即可。
- 如果用户的请求模糊（例如”查一下数据”），请不要尝试执行 SQL，直接引导用户补充：指标、时间、维度。
- 请先思考需要查询哪个表，用什么SQL语句。
- **强制执行顺序**：`sql_db_query_checker` 只是用来检查语法，它**不会**返回数据！你必须紧接着调用 `sql_db_query` 工具并传入 SQL 才能真正从数据库中取出数据！千万不要查完语法就直接回答！
- 对于排名、统计类问题，优先使用 GROUP BY 和 ORDER BY、SUM()。
- **大数据量优化**：面对可能返回大量数据的查询（如明细数据、多天全量数据查询），必须在 SQL 语句末尾主动添加 `LIMIT 50`（或合适的限制条数），以优化性能并避免结果集过大。
- **空结果友好提示**：如果执行 SQL 后发现结果为空（即查不到数据），请务必向用户回复这段提示：”您查询该时间段该字段暂无数据，可换一种提问方式或者找数据分析师确认是否数据入库。”
- 给出直接的数据结论，不要暴露具体的SQL语句给最终用户。
- 如果返回的是列表数据，请使用 Markdown 的表格（Table）形式进行美观展示。
- **最终答案格式（极其重要）**：当你完成所有步骤并准备回答用户时，你输出的最后一行内容**必须**以 `Final Answer: ` 开头！否则系统将无法识别你的答案并报错！例如：`Final Answer: 平台累计注册用户数为XXX人，累计服务次数为XXX次。`
“””
    )
    return agent_executor

# ====================== 5. Streamlit 前端界面 ======================
def main():
    st.set_page_config(page_title="运营数据智能问答系统", page_icon="📊", layout="wide")
    st.title("📊 运营数据智能问答系统 (NL2SQL)")
    st.markdown("""
    **功能说明**：通过自然语言输入查询需求，AI会自动连接本地数据库，转换为对应的 SQL 并实时返回分析结果。

    **💡 常见提问示例**：
    - 🔍 *查询2026年3月排名前5的服务及使用次数*
    - 📈 *告诉我平台最新的月活、累计注册用户数和累计服务次数是多少？*
    - 🎯 *近三天 Hometopic_click（卡片位） 资源位在各端的总点击量情况如何？*
    """)
    
    # ... 省略中间代码 ...
    # 检查 API Key
    if not os.getenv("TONGYI_API_KEY"):
        st.warning("⚠️ 未找到 TONGYI_API_KEY 环境变量。请确保在同级目录下的 `.env` 文件中配置了该值。")
        st.stop()
        
    try:
        agent = get_db_and_agent()
    except Exception as e:
        st.error(f"❌ 系统初始化失败，请检查 MySQL 数据库连接或所需依赖包 (sqlalchemy, pymysql): \n\n{e}")
        st.stop()

    # 初始化聊天记录
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # 展示历史聊天
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # 处理用户输入
    if prompt := st.chat_input("请输入您的数据查询需求... (例如: 查询最新的注册用户数)"):
        # 在此处调用业务日期预处理函数
        processed_prompt = parse_business_date(prompt)
        
        # 构造带有上下文的输入：将最近的6轮对话传给 AI，帮它记住年份及业务主体等信息
        context_history = ""
        if len(st.session_state.messages) >= 6:
            last_few_msgs = st.session_state.messages[-6:]
            context_history = "--- 以下是之前的对话背景 ---\n"
            for m in last_few_msgs:
                context_history += f"{m['role']}: {m['content']}\n"
            context_history += "--- 【极其重要】以上是本轮对话的完整背景。如果背景中用户曾询问过某个具体业务（如装修、医保等关键词），则当前请求【即使没有再次提及该业务名称】，也必须继续沿用该业务关键词，在所有 *_detail 表中用 LIKE '%关键词%' 进行过滤查询，严禁直接查询 resource_total 全量汇总数据来回答。 ---\n"
        elif len(st.session_state.messages) > 0:
            context_history = "--- 以下是之前的对话背景 ---\n"
            for m in st.session_state.messages:
                context_history += f"{m['role']}: {m['content']}\n"
            context_history += "--- 【极其重要】以上是本轮对话的完整背景。如果背景中用户曾询问过某个具体业务（如装修、医保等关键词），则当前请求【即使没有再次提及该业务名称】，也必须继续沿用该业务关键词，在所有 *_detail 表中用 LIKE '%关键词%' 进行过滤查询，严禁直接查询 resource_total 全量汇总数据来回答。 ---\n"
        
        full_input = f"{context_history}用户新请求: {processed_prompt}"

        # 将用户问题添加到界面
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # AI 回答
        with st.chat_message("assistant"):
            with st.spinner("🧠 正在理解需求并查询数据库，请稍候..."):
                try:
                    # 传入带有上下文的输入
                    response = agent.invoke({"input": full_input})
                    answer = response["output"]
                    
                    st.markdown(answer)
                    # 保存到历史记录
                    st.session_state.messages.append({"role": "assistant", "content": answer})
                except Exception as e:
                    error_str = str(e)
                    # 解析错误：尝试从中提取 Final Answer 内容
                    match = re.search(r'Action Input:\s*Final Answer:\s*(.*)', error_str, re.DOTALL)
                    if match:
                        answer = match.group(1).strip()
                    else:
                        answer = f"❌ 查询分析过程中发生错误: \n\n```python\n{e}\n```"
                    st.markdown(answer)
                    st.session_state.messages.append({"role": "assistant", "content": answer})

if __name__ == "__main__":
    main()