# NL2SQL — 运营数据智能问答系统

> 用自然语言提问，AI 自动生成 SQL 并返回分析结论。
> 基于 LangChain SQL Agent + 通义千问 + Streamlit，支持多表联查、同比环比计算、模糊时间识别。

---

## 功能特性

- **自然语言转 SQL**：无需懂 SQL，直接用中文提问，Agent 自动生成并执行查询
- **多轮对话记忆**：携带最近 6 轮上下文，AI 能记住前文提及的业务关键词、年份、维度等信息，跨轮查询自动保持过滤条件
- **模糊时间识别**：支持"今年/去年"、"25年"、"2月"等词汇自动补全，以及"近7天"、"近2周"、"近3个月"自动展开为具体日期区间，"至今"、"截止昨日"、"本月"、"上月"等均可识别
- **业务流量查询引导**：询问"xx业务流量"时，主动追问时间范围和资源位，支持关键词模糊匹配，在四张明细表中联合查询并汇总
- **数据可用性预查**：查询前先校验日期是否有数据；日期区间只检查起始日期，避免因数据尚未更新而误报无数据
- **同比 / 环比计算**：强制合并查询 + 内部计算，给出增幅百分比及具体数值
- **防编造红线**：未执行 SQL 获取真实数据前，严禁 Agent 输出占位符
- **整年查询拦截**：用户仅输入年份时，主动反问是要明细、汇总还是指定月份
- **解析错误自动恢复**：模型输出格式异常时自动提取 Final Answer 内容，以正常对话呈现而非红框报错
- **结果美化展示**：列表数据自动渲染为 Markdown 表格；资源位名称统一用"中文名（英文名）"格式展示

---

## 技术栈

| 层级    | 技术                                     |
| ----- | -------------------------------------- |
| 前端界面  | Streamlit                              |
| AI 框架 | LangChain (SQL Agent, zero-shot-react) |
| 大语言模型 | 通义千问 `qwen-turbo` (阿里云百炼)              |
| 数据库驱动 | PyMySQL + SQLAlchemy                   |
| 环境管理  | python-dotenv                          |

---

## 数据库结构

系统连接 8 张业务表，Agent 内置对应的业务知识与查询规则：

| 表名                       | 说明                      | 关键字段                                     |
| ------------------------ | ----------------------- | ---------------------------------------- |
| `platform_daily_metrics` | 平台每日综合指标                | `stat_date`, `app_dau`, `alipay_dau`, `total_register_users`, `total_service_times` |
| `platform_mau`           | APP 月活及留存               | `date_month`(格式 YYYY-MM-01), `mau`, `mau_percent`, `dau`, `retention_percent` |
| `resource_total`         | 资源位点击量汇总                | `resource_name`, `resource_amount`, `stat_date`, `port` |
| `5100_detail`            | 所有服务使用明细（含服务超市及部分其他资源位） | `service_name`, `service_amount`, `stat_date`, `port`, `resource_name` |
| `resource_detail`        | 资源位明细（腰封/卡片位/头条等具体活动数据） | `resource_name`(资源位类型), `item_name`(活动名), `resource_amount`, `stat_date`, `port` |
| `core_detail`            | 核心功能/金刚位明细              | `resource_name`(功能类型), `item_name`(功能名), `resource_amount`, `stat_date`, `port` |
| `search_detail`          | 搜索词明细                   | `search_name`(搜索词), `search_amount`, `stat_date`, `port`, `resource_name` |
| `app_retention`          | APP 新增用户次日留存            | `platform`, `stat_date`, `day_1_retention` |

> **注意**：`resource_detail` / `core_detail` 中，`resource_name` 是资源位**类型**（如 `mid_banner`），`item_name` 是具体**活动/服务名称**（如"装修补贴"）。查询"哪些资源位"时按 `resource_name` 分组，查询"有哪些活动"时按 `item_name` 分组。

**资源位中英文映射（内置到 Prompt）：**

| 英文字段值                 | 中文含义        |
| --------------------- | ----------- |
| `mid_banner`          | 腰封 / 首页腰封   |
| `news_click`          | 成都头条 / 头条新闻 |
| `person_banner_click` | 个人中心腰封      |
| `top_banner_click`    | 顶部 banner   |
| `Hometopic_click`     | 卡片位 / 首页专题  |
| `king_kong`           | 金刚位         |

---

## 快速开始

### 1. 克隆项目

```bash
git clone https://github.com/your-username/nl2sql.git
cd nl2sql
```

### 2. 安装依赖

```bash
pip install -r requirements.txt
```

### 3. 配置环境变量

```bash
cp .env.example .env
```

编辑 `.env` 文件，填入你的配置：

```ini
# 数据库
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=your_database_name

# 通义千问 API Key（阿里云百炼控制台获取）
TONGYI_API_KEY=your_tongyi_api_key
```

### 4. 初始化数据库

按照以下 DDL 在你的 MySQL 中建表并导入业务数据：

```sql
-- 平台每日综合指标
CREATE TABLE platform_daily_metrics (
    stat_date          DATE NOT NULL UNIQUE,
    android_dau        INT UNSIGNED,
    ios_dau            INT UNSIGNED,
    harmonyos_dau      INT UNSIGNED,
    app_dau            INT UNSIGNED,
    alipay_dau         INT UNSIGNED,
    mini_program_dau   INT UNSIGNED,
    smart_frontend_dau INT UNSIGNED,
    new_register_users INT UNSIGNED,
    new_realname_users INT UNSIGNED,
    total_register_users BIGINT UNSIGNED,
    total_service_times  BIGINT UNSIGNED,
    platform_dau       INT UNSIGNED
);

-- APP 月活及留存
CREATE TABLE platform_mau (
    date_month           DATE NOT NULL UNIQUE,  -- 格式 YYYY-MM-01
    mau                  INT UNSIGNED,
    mau_percent          DECIMAL(5,2),
    total_register_users BIGINT UNSIGNED,
    dau                  INT UNSIGNED,
    retention_percent    DECIMAL(5,2)
);

-- 资源位点击量汇总
CREATE TABLE resource_total (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    resource_name   VARCHAR(100),
    resource_amount INT UNSIGNED,
    stat_date       DATE,
    port            VARCHAR(50),
    UNIQUE KEY uq_resource (resource_name, stat_date, port)
);

-- 所有服务使用明细
CREATE TABLE `5100_detail` (
    id             INT AUTO_INCREMENT PRIMARY KEY,
    service_name   VARCHAR(200),
    service_amount INT UNSIGNED,
    stat_date      DATE,
    port           VARCHAR(50),
    resource_name  VARCHAR(100),
    UNIQUE KEY uq_service (service_name, stat_date, port)
);

-- 资源位明细（腰封/卡片位/头条等具体活动数据）
CREATE TABLE resource_detail (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    resource_amount BIGINT,
    resource_name   VARCHAR(255),
    item_name       VARCHAR(255),
    stat_date       DATE,
    port            VARCHAR(50)
);

-- 核心功能/金刚位明细
CREATE TABLE core_detail (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    resource_amount BIGINT,
    resource_name   VARCHAR(255),
    item_name       VARCHAR(500),
    stat_date       DATE,
    port            VARCHAR(50)
);

-- 搜索词明细
CREATE TABLE search_detail (
    id             INT AUTO_INCREMENT PRIMARY KEY,
    search_amount  BIGINT,
    search_name    VARCHAR(255),
    stat_date      DATE,
    port           VARCHAR(50),
    resource_name  VARCHAR(255)
);

-- APP 新增用户次日留存
CREATE TABLE app_retention (
    platform        VARCHAR(20),
    stat_date       DATE,
    day_1_retention DECIMAL(5,2),
    PRIMARY KEY (platform, stat_date)
);
```

### 5. 启动应用

```bash
streamlit run streamlit_sql2nlp.py
```

浏览器访问 `http://localhost:8501` 即可使用。

---

## 使用示例

启动后在聊天框输入自然语言问题：

```
查询2026年3月排名前5的服务及使用次数
```
```
告诉我平台最新的月活、累计注册用户数和累计服务次数是多少？
```
```
近三天卡片位资源位在各端的总点击量情况如何？
```
```
对比25年和26年2月的月活，增幅是多少？
```
```
查询2026年2月安卓新增用户的平均次日留存率
```

**预期输出示例：**

```
2026年3月排名前5的服务及使用次数如下：

| 排名 | 服务名称 | 使用次数 |
|------|---------|---------|
| 1    | 社保查询  | 12,450  |
| 2    | 公积金   | 9,831   |
| ...  | ...     | ...     |
```

---

## 核心设计说明

### Agent 工作流程

```
用户自然语言输入
  ↓
parse_business_date()   — 模糊时间预处理（今年/去年/25年/2月 → 标准日期）
  ↓
LangChain SQL Agent (zero-shot-react-description)
  ↓
  ├─ check_date_available (自定义工具)   — 预查日期是否有数据
  ├─ sql_db_schema                      — 获取表结构
  ├─ sql_db_query_checker               — SQL 语法校验
  └─ sql_db_query                       — 真正执行 SQL 取数
  ↓
Final Answer: <结论 + Markdown 表格>
```

### 关键 Prompt 规则（部分）

| 规则       | 描述                                       |
| -------- | ---------------------------------------- |
| 数据预查     | 有明确日期时，先调用 `check_date_available`；日期区间只查起始日期，不因结束日期无数据而停止 |
| 业务场景追问   | 询问"xx业务流量"时，先追问时间范围，再追问资源位；用户确认后在四张明细表中联合查询 |
| 资源位概念区分  | `resource_name` = 资源位类型（腰封/卡片位），`item_name` = 具体活动名；"哪些资源位"按前者分组，"有哪些活动"按后者分组 |
| 四表联查     | 业务关键词查询必须覆盖：`5100_detail`（service_name）、`core_detail`（item_name）、`resource_detail`（item_name）、`search_detail`（search_name） |
| 上下文保持    | 携带最近 6 轮对话，业务关键词在后续请求中自动保持，禁止直接查 `resource_total` 全量表 |
| 合并查询     | 同比/环比必须用 `IN` 一次查出两个时期                   |
| SQL 汇总强制 | 多行数据汇总必须用 SQL `SUM() + GROUP BY`，严禁在回答中手写加法公式 |
| 红线规则     | 未拿到真实数据，严禁使用 XXX/YYY 等占位符                |
| 整年拦截     | 仅有年份无月份时，主动向用户澄清粒度                       |
| 解析错误恢复   | `handle_parsing_errors` 自动提取 Final Answer 内容，异常改为 `st.markdown` 展示而非红框 |
| 最终格式     | 输出必须以 `Final Answer: ` 开头，否则解析器报错        |
| 大结果限制    | 明细查询末尾自动加 `LIMIT 50`                     |
| 资源位展示    | `resource_name` 字段一律以"中文名（英文名）"格式展示      |

---

## 常见问题

| 错误信息                     | 原因               | 解决方案                                   |
| ------------------------ | ---------------- | -------------------------------------- |
| `未找到 TONGYI_API_KEY`     | .env 未配置 API Key | 参考 `.env.example` 补全配置                 |
| `系统初始化失败`                | 数据库连接异常          | 检查 MySQL 服务状态及 `.env` 中的 DB 配置         |
| `OUTPUT_PARSING_FAILURE` | Agent 未按格式回答     | 已由 `handle_parsing_errors` 捕获，提示用户重新提问 |
| 查询结果为空                   | 所查日期暂无数据         | 系统会提示"暂无数据，可换一种提问方式"                   |
| Agent 提前终止               | 推理链过长            | 已将 `max_iterations` 设为 15，可按需调大        |

---

## 项目结构

```
nl2sql/
├── streamlit_sql2nlp.py   # 主程序（前端界面 + LangChain Agent）
├── requirements.txt       # Python 依赖
├── .env.example           # 环境变量配置示例
├── .gitignore             # 忽略 .env 等敏感文件
└── README.md              # 项目文档
```

---

## 依赖说明

```
streamlit>=1.32.0          # Web 界面
langchain>=0.1.0           # AI Agent 框架
langchain-community>=0.0.20 # SQL Agent、Tongyi 集成
python-dotenv>=1.0.0       # 环境变量加载
pymysql>=1.1.0             # MySQL 驱动
sqlalchemy>=2.0.0          # ORM / URI 连接
```

---

## License

MIT

---

## 更新日志

### 2026-03-30

**变更文件：** `streamlit_sql2nlp.py`

#### 1. 时间解析增强（`parse_business_date`）

新增对以下模糊时间词汇的自动展开，展开后直接以具体日期传入 Agent：

| 输入示例            | 展开结果                      |
| --------------- | ------------------------- |
| `近7天`           | `2026年03月24日至2026年03月29日` |
| `近2周`           | `2026年03月16日至2026年03月29日` |
| `近3个月`          | `2025年12月31日至2026年03月29日` |
| `至今` / `截止今日`   | 替换为今天具体日期                 |
| `截止昨日` / `截止昨天` | 替换为昨天具体日期                 |
| `本月`            | `2026年3月`                 |
| `上月`            | `2026年2月`                 |

#### 2. 业务场景查询规则（Prompt 新增）

- **时间模糊词拦截**：输入包含"近期"、"最近"等无数字修饰的模糊词时，直接回复引导语，不查库
- **资源位概念澄清**：明确 `resource_name`（资源位类型）与 `item_name`（活动名）的区别，按用户意图选择正确分组维度
- **业务流量查询三步走**：① 检查时间是否明确 → ② 追问资源位名称（可模糊查询）→ ③ 在四张明细表中 UNION ALL 联合查询并以 SUM/GROUP BY 汇总
- **四表联查字段**：`5100_detail.service_name`、`core_detail.item_name`、`resource_detail.item_name`、`search_detail.search_name`
- **追问"在哪些资源位"**：此时改为按 `resource_name` 分组查询 `resource_detail`，返回腰封/卡片位等类型汇总
- **结果标注日期区间**：回答开头附「查询时间段：XXXX年XX月XX日 - XXXX年XX月XX日」供用户确认

#### 3. 上下文记忆强化

- 历史对话从最近 2 轮扩展到最近 **6 轮**
- 补充 `elif` 分支，确保少于 6 条历史时也携带全部上下文
- 上下文注入指令明确：若背景中出现业务关键词，当前请求无论是否提及，均须继续在 `*_detail` 表中过滤，禁止查 `resource_total` 全量表

#### 4. 解析错误自动恢复

- `friendly_error_handler` 新增正则提取：当模型将 Final Answer 误写为 `Action Input: Final Answer: ...` 格式时，自动提取内容直接返回
- 异常处理改为 `st.markdown` 展示，前端不再显示红框错误

#### 5. 日期检查逻辑优化

- 日期区间查询只检查**起始日期**，不因结束日期暂无数据就停止查询（数据库可能仍在更新中）

#### 6. 表结构与 Prompt 扩展

- `include_tables` 新增 `resource_detail`、`core_detail`、`search_detail` 三张明细表
- `resource_total` 补充 `king_kong`（金刚位）映射
- `5100_detail` 描述更正为"所有服务使用明细数据，包含服务超市及部分其他资源位"，补充 `resource_name` 字段说明
- 回答要求新增资源位中英文展示规则：`resource_name` 字段一律用"中文名（英文名）"格式



------

### 2026-04-02 - 智能问答系统切换本地大模型 (`streamlit_sql2nlp_local.py`)

文件： `streamlit_sql2nlp_local.py`

#### 变更内容

- **替换 LLM 后端**：将在线千问 API（`Tongyi / qwen-plus`）替换为本地 Ollama 部署的 `qwen2.5:7b`，不再依赖 `DASHSCOPE_API_KEY` 环境变量。
- **依赖更新**：
  - 移除：`langchain_community.llms.Tongyi`
  - 新增：`langchain_ollama.OllamaLLM`（需安装 `langchain-ollama` 包）
- **模型选型说明**：初始尝试 `qwen2.5:3b`，但因参数量不足（30亿），模型无法稳定遵循 ReAct Agent 格式（反复使用自编工具名、SQL 被 markdown 代码块包裹导致解析失败），改用 `qwen2.5:7b`。

#### 使用前提

1. 已安装并启动 Ollama 服务：`ollama serve`

2. 已拉取模型：`ollama pull qwen2.5:7b`

3. 已安装新依赖：

   ```bash
   C:\路径已做脱敏\venv311\Scripts\pip.exe install langchain-ollama
   ```

### 