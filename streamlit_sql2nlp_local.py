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
from langchain_ollama import OllamaLLM
from langchain_core.tools import tool
import pymysql

# 加载环境变量
load_dotenv()

# ====================== 1. 业务日期预处理 ======================
def parse_business_date(text):
    """
    处理模糊的时间词汇，将其转换为规范年份或具体日期区间
    """
    current_date = datetime.now()
    current_year = current_date.year
    yesterday_str = (current_date - timedelta(days=1)).strftime('%Y年%m月%d日')
    today_str = current_date.strftime('%Y年%m月%d日')
    before_yesterday_str = (current_date - timedelta(days=2)).strftime('%Y年%m月%d日')

    # 1. 处理相对词汇 (正则替换)
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
    text = re.sub(r'昨[天日]', yesterday_str, text)
    text = re.sub(r'今[天日]', today_str, text)
    text = re.sub(r'前天', before_yesterday_str, text)

    # 2. 处理"至今" / "截止"
    text = text.replace('至今', f'至{today_str}')
    text = re.sub(r'截止(昨天|昨日|今天|今日)', f'至{today_str}', text)

    # 3. 处理"近X天"
    def replace_recent_days(m):
        n = int(m.group(1))
        start = (current_date - timedelta(days=n)).strftime('%Y年%m月%d日')
        return f'{start}至{yesterday_str}'
    text = re.sub(r'近(\d+)天', replace_recent_days, text)

    # 4. 处理"本月"/"上月"
    text = text.replace('本月', f'{current_date.year}年{current_date.month}月')
    if current_date.month == 1:
        last_month_year, last_month = current_date.year - 1, 12
    else:
        last_month_year, last_month = current_date.year, current_date.month - 1
    text = text.replace('上月', f'{last_month_year}年{last_month}月')

    return text

# ====================== 2. 数据库配置 ======================
DB_USER = 'root'
DB_PASSWORD = '填写自己的密码'
DB_HOST = 'localhost'
DB_PORT = '填写自己的端口'
DB_NAME = '填写自己的表名'
MYSQL_URI = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"

def friendly_error_handler(error):
    error_str = str(error)
    if "produced both a final answer and a parse-able action" in error_str:
        return "⚠️ 格式错误：你在同一次输出中同时包含了 Final Answer 和 Action，这是不允许的。请重新决策：如果还需要查询数据库，只输出 Thought / Action / Action Input；如果已有数据准备回答，只输出 Final Answer。"
    if "Could not parse LLM output" in error_str:
        match = re.search(r'(「查询时间段：.*)', error_str, re.DOTALL)
        if match:
            return f"格式错误。请重新输出，必须以 'Final Answer: ' 开头并紧跟：{match.group(1).strip()}"
        return "⚠️ 格式错误。请严格按照 Thought: / Action: / Action Input: 顺序操作，最后一步必须以 Final Answer: 开头。"
    return "💡 请补充更具体的时间范围或业务关键词。"

# ====================== 3. 自定义工具 ======================
@tool
def check_date_available(stat_date: str) -> str:
    """校验日期是否有数据。参数必须是单个日期 (YYYY-MM-DD)。"""
    try:
        conn = pymysql.connect(host=DB_HOST, port=int(DB_PORT), user=DB_USER, password=DB_PASSWORD, database=DB_NAME, charset='utf8mb4')
        cursor = conn.cursor()
        stat_date = stat_date.strip(" '\"")
        if len(stat_date) == 7:
            cursor.execute("SELECT COUNT(*) FROM platform_daily_metrics WHERE stat_date LIKE %s", (f"{stat_date}%",))
        else:
            cursor.execute("SELECT COUNT(*) FROM platform_daily_metrics WHERE stat_date = %s", (stat_date,))
        count = cursor.fetchone()[0]
        conn.close()
        return f"✅ {stat_date} {'有数据' if count > 0 else '无数据'}。"
    except Exception as e:
        return f"❌ 校验失败: {e}"

# ====================== 4. 初始化 SQL Agent ======================
@st.cache_resource
def get_db_and_agent():
    db = SQLDatabase.from_uri(MYSQL_URI, include_tables=['resource_total', 'platform_daily_metrics', '5100_detail', 'app_retention', 'platform_mau', 'resource_detail', 'core_detail', 'search_detail'])
    llm = OllamaLLM(model="qwen2.5:7b", temperature=0.1)

    today_str = datetime.now().strftime('%Y-%m-%d')
    yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    prefix = f"""你是一个专业的数据分析师。你【拥有执行 SQL 的权限】。

【重要日期】：今天是 {today_str}，昨天是 {yesterday_str}。

【极其重要：输出格式 (REDLINE)】：
你必须严格按照以下步骤思考和行动。**绝对禁止一次性输出所有步骤**。
1. Thought: 思考下一步要做什么。
2. Action: 工具名 (必须从 [sql_db_query, sql_db_schema, sql_db_list_tables, sql_db_query_checker, check_date_available] 中选一个)。
3. Action Input: 纯文本 SQL 或参数。**【红线】Action Input 必须是裸 SQL，绝对禁止用 ```sql ``` 代码块包裹，否则系统会直接报语法错误！**
(此处停止输出，等待系统 Observation 返回结果)
... (重复直到拿到真实数据库数据)
最后一步：
Final Answer: 「查询时间段：YYYY-MM-DD 至 YYYY-MM-DD」\n具体的结论表格。

【重要表结构及业务说明】：
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


【业务流量查询逻辑参考】：
查询某业务（如装修）总流量：使用 UNION ALL 汇总 5100_detail(service_name), core_detail(item_name), resource_detail(item_name), search_detail(search_name) 四个表，查询item_name或service_name或search_name like %某业务%的数据。

**【重要】UNION ALL 查询必须使用以下模板，严禁在外层SELECT中引用子查询内部列名（如resource_amount），严禁生成DATE_ADD/FLOOR等复杂表达式：**
```sql
SELECT
    resource_name,
    port,
    SUM(amount) AS total_amount
FROM (
    SELECT service_amount AS amount, service_name AS item_name, stat_date, port, resource_name FROM `5100_detail` WHERE service_name LIKE '%关键词%' AND stat_date BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'
    UNION ALL
    SELECT resource_amount AS amount, item_name, stat_date, port, resource_name FROM `core_detail` WHERE item_name LIKE '%关键词%' AND stat_date BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'
    UNION ALL
    SELECT resource_amount AS amount, item_name, stat_date, port, resource_name FROM `resource_detail` WHERE item_name LIKE '%关键词%' AND stat_date BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'
    UNION ALL
    SELECT search_amount AS amount, search_name AS item_name, stat_date, port, resource_name FROM `search_detail` WHERE search_name LIKE '%关键词%' AND stat_date BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'
) combined
GROUP BY resource_name, port
ORDER BY total_amount DESC
LIMIT 50;
```
注意：`5100_detail` 的数量字段是 `service_amount`，必须在子查询中 alias 为 `amount`，外层只能用 `amount`。
- **时间信息判断规则**：用户输入在传入前已经过预处理，”近X天/周/月”、”至今”、”截止昨日”等均已转换为具体日期区间（如”2026年3月1日至2026年3月29日”）。因此：
  - 如果你看到具体日期区间（含年月日），说明时间已明确，可以直接查询。
  - 如果你看到”近期”、”最近”、”最新”等**没有数字的模糊词**，或用户完全没提时间，则必须直接回复：”Final Answer: 为了帮您精准查询数据，请补充具体的时间范围，例如：2026年3月、近7天、3月1日至3月15日等。”，然后停止，不要尝试查询数据库。
- **业务流量/表现查询**：当用户输入”查xx业务流量”、”xx业务表现情况”或类似询问具体业务（如”装修业务”）的请求时，你必须：
  1. **第一步：检查时间**。按照上述”时间信息判断规则”处理，如果时间不明确则追问，不要查库。
  2. **第二步：主动追问资源位**。如果时间已明确但没有具体资源位名称，先回答：”Final Answer: 为了帮您更精准地分析【xx业务】在【时间段】的表现，请问该业务上线了哪些具体的资源位？（如果您不清楚具体资源位名称，可以告诉我关键词，我会为您进行模糊查询）”
  3. **第三步：执行模糊查询并用SQL汇总**。当用户提供关键词（如”装修”）或确认进行模糊查询后，你必须：
     - 在以下**四个表**中匹配名称包含该关键词的数据（**缺一不可**）：
       * `5100_detail`（服务超市）：匹配 `service_name LIKE '%关键词%'`，字段为 `service_amount`
       * `core_detail`（金刚位）：匹配 `item_name LIKE '%关键词%'`，字段为 `resource_amount`
       * `resource_detail`（腰封/卡片位/头条等）：匹配 `item_name LIKE '%关键词%'`，字段为 `resource_amount`
       * `search_detail`（搜索词）：匹配 `search_name LIKE '%关键词%'`，字段为 `search_amount`
     - **必须使用 SQL 的 SUM() 和 GROUP BY 进行汇总**，严禁在 Final Answer 中手动列出加法公式（如 24+86+91...）！
  4. **汇总回答**：将查询结果按类别（服务超市、金刚位、资源位、搜索词）分类展示，并在回答开头**明确标注实际查询的日期区间**，格式为：「查询时间段：XXXX年XX月XX日 - XXXX年XX月XX日」，方便用户确认。使用 Markdown 表格展示数据更清晰。
  
  【查询时间处理规则与计算说明（非常重要）】：
- 工具使用提示：在真正写SQL前，如果用户指明了特定日期或月份，可以调用 `check_date_available` 工具检查该日期是否有数据。但注意：如果用户给的是日期区间（如"3月17日至3月30日"），只需检查起始日期即可，不要因为结束日期暂无数据就停止查询——数据库可能还在更新中，区间内有数据就应该继续查询并返回已有数据。
- **整年多条数据确认（极其重要）**：如果用户的请求中**只有年份而没有具体月份**（例如“查询24年和25年的月活”），**绝对不允许你私自假设为1月份的数据进行查询！** 面对整年宽泛范围，请**不要去调用任何工具，也不要写 Action: Final Answer**，你只需要**直接在下一行输出**：`Final Answer: 您查询的是整年数据，包含多个月份。请问您是希望展示这几年所有月份的完整明细，还是想查看年度平均值/总量？或者您想指定某个具体的月份（如2月）进行对比？`，然后停止。
- 范围查询：如果用户明确表示要看“全年所有数据”或“每个月的数据”，你应该使用 `BETWEEN '2024-01-01' AND '2024-12-31'` 查出多条结果，并用 Markdown 表格完整展示。
- **留存数据查询逻辑**：
  - 如果用户查询“新增用户留存”（如新增留存均值），必须查询 `app_retention` 表。
    - 如果是查某个月（如2026年2月），应使用 `stat_date LIKE '2026-02%'` 并计算平均值 `AVG(day_1_retention)`。
  - 如果用户查询“活跃用户留存”（如月活留存），必须查询 `platform_mau` 表。
    - 此时必须将月份转换为该月1号的日期，例如：“2025年2月”对应 `date_month = '2025-02-01'`，字段为 `retention_percent`。
- 如果用户查询特定月份，但在 `platform_mau` 表中查询，你必须将该月转换为该月1号的日期，例如：“2025年2月”在 `platform_mau` 表中对应 `date_month = '2025-02-01'`。
- 当用户询问跨年对比（如：2025年和2026年的2月数据），请确保 SQL 中包含这两个特定月份，例如：对于 `platform_mau` 表，查询条件应为 `date_month IN ('2025-02-01', '2026-02-01')`。
- **计算与对比要求**：如果用户提出计算【增幅、变化率、同比、环比】（例如查询25和26年2月月活及其增幅），你必须：
  1. 写**一条**SQL同时查出两个时期的数据（强烈建议使用 `IN` 语法，如 `WHERE date_month IN ('2025-02-01', '2026-02-01')`），**绝对不要**分成两次分别查询。
  2. **红线规则**：你必须真实调用 `sql_db_query` 工具执行上一步写好的 SQL，并**亲眼看到**工具返回的具体数字后，才能进行计算和回答。**严禁**在没有任何数据的情况下，直接输出包含 "XXX"、"YYY"、"ZZZ" 等占位符的废话！如果遇到阻碍，请继续调用工具直到拿到数据！
  3. 利用内部计算能力计算增幅，公式为：(本期数据 - 同期数据) / 同期数据，并在最终回答中展示真实的具体数值和计算出的百分比结果。

【回答要求】：
- **资源位名称展示规则（极其重要）**：在最终回答中展示 resource_name 字段值时，必须使用”中文名（英文名）”格式，对照关系为：卡片位（Hometopic_click）、腰封（mid_banner）、成都头条（news_click）、个人中心腰封（person_banner_click）、顶部banner（top_banner_click）、金刚位（king_kong）。如果英文名不在以上列表中，直接显示英文名即可。
- 如果用户的请求模糊（例如”查一下数据”），请不要尝试执行 SQL，直接引导用户补充：指标、时间、维度。
- 请先思考需要查询哪个表，用什么SQL语句。
- **强制执行顺序**：`sql_db_query_checker` 只是用来检查语法，它**不会**返回数据！你必须紧接着调用 `sql_db_query` 工具并传入 SQL 才能真正从数据库中取出数据！千万不要查完语法就直接回答！
- 对于排名、统计类问题，优先使用 GROUP BY 和 ORDER BY、SUM()。
- **大数据量优化**：面对可能返回大量数据的查询（如明细数据、多天全量数据查询），必须在 SQL 语句末尾主动添加 `LIMIT 50`（或合适的限制条数），以优化性能并避免结果集过大。
- **空结果友好提示**：如果执行 SQL 后发现结果为空（即查不到数据），请务必向用户回复这段提示：“您查询该时间段该字段暂无数据，可换一种提问方式或者找数据分析师确认是否数据入库。”
- 如果上一回答追问用户具体资源位名称、时间范围、活动名称等，用户新回答的内容需沿用上一对话中的业务名作为前提假设
- 给出直接的数据结论，不要暴露具体的SQL语句给最终用户。
- 如果返回的是列表数据，请使用 Markdown 的表格（Table）形式进行美观展示。
- **最终答案格式（极其重要）**：当你完成所有步骤并准备回答用户时，你输出的最后一行内容**必须**以 `Final Answer: ` 开头！否则系统将无法识别你的答案并报错！例如：`Final Answer: 平台累计注册用户数为XXX人，累计服务次数为XXX次。`

    # 【REDLINE 核心禁令】：
    # - **严禁捏造**：禁止提供任何模拟或示例数据。必须调用工具获取真实 Observation 结果。
    # - **强制查库**：`sql_db_query_checker` 仅用于检查语法，之后**必须**紧跟 `sql_db_query` 查库。
    # - **严禁输出 SQL**：在 Final Answer 中**绝对禁止**包含任何 SQL 语句或代码块。
    # - **业务过滤**：具体业务（如装修）查询必须在名称字段应用 `LIKE '%关键词%'`。
"""

    return create_sql_agent(
        llm=llm,
        db=db,
        agent_type="zero-shot-react-description",
        verbose=True,
        handle_parsing_errors=friendly_error_handler,
        max_iterations=12,
        extra_tools=[check_date_available],
        prefix=prefix
    )

# ====================== 5. Streamlit 前端 ======================
def main():
    st.set_page_config(page_title="天府市民云数据分析", page_icon="📊", layout="wide")
    st.title("📊 运营数据智能问答系统")
    st.markdown("""
       **功能说明**：通过自然语言输入查询需求，AI会自动连接本地数据库，转换为对应的 SQL 并实时返回分析结果。

       **💡 常见提问示例**：
       - 🔍 *查询2026年3月排名前5的服务及使用次数*
       - 📈 *告诉我平台最新的月活、累计注册用户数和累计服务次数是多少？*
       - 🎯 *近三天 Hometopic_click（卡片位） 资源位在各端的总点击量情况如何？*
       - 🔍 *查询2026年3月某某业务流量情况，包含所有资源位，分资源位给出汇总数据*
       """)
    
    if "messages" not in st.session_state:
        st.session_state.messages = []

    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    if prompt := st.chat_input("请输入查询需求..."):
        processed_prompt = parse_business_date(prompt)
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("🧠 正在调取数据库，请稍候..."):
                try:
                    agent = get_db_and_agent()
                    # 动态注入当前背景，防止模型迷路
                    response = agent.invoke({"input": f"当前背景：今天是{datetime.now().strftime('%Y-%m-%d')}。用户请求：{processed_prompt}"})
                    answer = response["output"]
                    st.markdown(answer)
                    st.session_state.messages.append({"role": "assistant", "content": answer})
                except Exception as e:
                    # 最后一道防御：尝试从异常字符串中提取模型生成的答案
                    error_msg = str(e)
                    match = re.search(r'「查询时间段：.*', error_msg, re.DOTALL)
                    if match:
                        answer = match.group(0).strip()
                        st.markdown(answer)
                        st.session_state.messages.append({"role": "assistant", "content": answer})
                    else:
                        st.error(f"❌ 系统异常: {e}")

if __name__ == "__main__":
    main()

