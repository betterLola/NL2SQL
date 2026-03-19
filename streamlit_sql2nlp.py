# -*- coding: utf-8 -*-
import os
import re
from datetime import datetime
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
    处理模糊的时间词汇，将其转换为规范年份
    """
    current_year = datetime.now().year
    
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
    # 匹配 "X月" 但前面不是 "年" 的情况
    text = re.sub(r'(?<!年)(\d{1,2}月)', f'{current_year}年\g<1>', text)
    
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
        include_tables=['resource_total', 'platform_daily_metrics', '5100_detail', 'app_retention', 'platform_mau'],
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
        prefix="""你是一个强大的数据分析助手。你可以访问一个 MySQL 数据库，其中包含平台各项运营数据。请根据用户的自然语言请求，编写并执行 SQL 查询，然后用通俗易懂的自然语言回答用户。

重要表结构及业务说明：
1. `resource_total` (资源位点击数据)：包含 resource_amount(点击量), resource_name(资源位名称), stat_date(统计日期), port(端，如安卓、苹果、鸿蒙)。
   **资源位名称(resource_name)中英文映射关系：**
   - `mid_banner`：代表 腰封、首页腰封、腰封banner
   - `news_click`：代表 头条、成都头条、头条新闻
   - `person_banner_click`：代表 个人中心腰封
   - `top_banner_click`：代表 顶部、顶部banner
   - `Hometopic_click`：代表 卡片位、首页专题、专题卡片
2. `platform_daily_metrics` (平台每日综合指标)：包含 stat_date(日期), alipay_dau(支付宝小程序日活), app_dau(APP总日活), total_register_users(累计注册用户数), total_service_times(累计服务次数) 等。
3. `5100_detail` (具体服务使用详情)：包含 service_amount(使用次数), service_name(服务名称), stat_date(日期), port。
4. `app_retention` (APP新增用户留存数据)：包含 platform（APP中是安卓/苹果/鸿蒙）, stat_date（日期）, day_1_retention（次日新增用户留存率）。
5. `platform_mau` (APP月活及留存表)：包含 date_month（月份,格式YYYY-MM-01）, mau(月活), mau_percent(月活率), total_register_users(月末注册用户数), dau(月度日均日活), retention_percent(平均留存率)。

查询时间处理规则与计算说明（非常重要）：
- 工具使用提示：在真正写SQL前，如果用户指明了特定日期或月份，请先调用 `check_date_available` 工具检查该日期是否有数据。如果没有，直接告诉用户暂无数据。
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

回答要求：
- 如果用户的请求模糊（例如“查一下数据”），请不要尝试执行 SQL，直接引导用户补充：指标、时间、维度。
- 请先思考需要查询哪个表，用什么SQL语句。
- **强制执行顺序**：`sql_db_query_checker` 只是用来检查语法，它**不会**返回数据！你必须紧接着调用 `sql_db_query` 工具并传入 SQL 才能真正从数据库中取出数据！千万不要查完语法就直接回答！
- 对于排名、统计类问题，优先使用 GROUP BY 和 ORDER BY、SUM()。
- **大数据量优化**：面对可能返回大量数据的查询（如明细数据、多天全量数据查询），必须在 SQL 语句末尾主动添加 `LIMIT 50`（或合适的限制条数），以优化性能并避免结果集过大。
- **空结果友好提示**：如果执行 SQL 后发现结果为空（即查不到数据），请务必向用户回复这段提示：“您查询该时间段该字段暂无数据，可换一种提问方式或者找数据分析师确认是否数据入库。”
- 给出直接的数据结论，不要暴露具体的SQL语句给最终用户。
- 如果返回的是列表数据，请使用 Markdown 的表格（Table）形式进行美观展示。
- **最终答案格式（极其重要）**：当你完成所有步骤并准备回答用户时，你输出的最后一行内容**必须**以 `Final Answer: ` 开头！否则系统将无法识别你的答案并报错！例如：`Final Answer: 平台累计注册用户数为XXX人，累计服务次数为XXX次。`
"""
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
        
        # 构造带有上下文的输入：将最近的两轮对话传给 AI，帮它记住年份等信息
        context_history = ""
        if len(st.session_state.messages) >= 2:
            last_few_msgs = st.session_state.messages[-2:]
            context_history = "--- 以下是之前的对话背景 ---\n"
            for m in last_few_msgs:
                context_history += f"{m['role']}: {m['content']}\n"
            context_history += "--- 请结合背景处理当前新请求 ---\n"
        
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
                    error_msg = f"❌ 查询分析过程中发生错误: \n\n```python\n{e}\n```"
                    st.error(error_msg)
                    st.session_state.messages.append({"role": "assistant", "content": error_msg})

if __name__ == "__main__":
    main()