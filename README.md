# NL2SQL — 运营数据智能问答系统

> 用自然语言提问，AI 自动生成 SQL 并返回分析结论。
> 基于 LangChain SQL Agent + 通义千问 + Streamlit，支持多表联查、同比环比计算、模糊时间识别。

---

## 功能特性

- **自然语言转 SQL**：无需懂 SQL，直接用中文提问，Agent 自动生成并执行查询
- **多轮对话记忆**：携带最近两轮上下文，AI 能记住前文提及的年份、维度等信息
- **模糊时间识别**：支持"今年"、"去年"、"25年"、"2月"等口语化时间自动补全
- **数据可用性预查**：查询前先调用自定义工具校验日期是否有数据，避免无效查询
- **同比 / 环比计算**：强制合并查询 + 内部计算，给出增幅百分比及具体数值
- **防编造红线**：未执行 SQL 获取真实数据前，严禁 Agent 输出占位符
- **整年查询拦截**：用户仅输入年份时，主动反问是要明细、汇总还是指定月份
- **结果美化展示**：列表数据自动渲染为 Markdown 表格

---

## 技术栈

| 层级 | 技术 |
|------|------|
| 前端界面 | Streamlit |
| AI 框架 | LangChain (SQL Agent, zero-shot-react) |
| 大语言模型 | 通义千问 `qwen-turbo` (阿里云百炼) |
| 数据库驱动 | PyMySQL + SQLAlchemy |
| 环境管理 | python-dotenv |

---

## 数据库结构

系统连接 5 张业务表，Agent 内置对应的业务知识与查询规则：

| 表名 | 说明 | 关键字段 |
|------|------|---------|
| `platform_daily_metrics` | 平台每日综合指标 | `stat_date`, `app_dau`, `alipay_dau`, `total_register_users`, `total_service_times` |
| `platform_mau` | APP 月活及留存 | `date_month`(格式 YYYY-MM-01), `mau`, `mau_percent`, `dau`, `retention_percent` |
| `resource_total` | 关键资源位点击量 | `resource_name`, `resource_amount`, `stat_date`, `port` |
| `5100_detail` | 子服务使用明细 | `service_name`, `service_amount`, `stat_date`, `port` |
| `app_retention` | APP 新增用户次日留存 | `platform`, `stat_date`, `day_1_retention` |

**资源位中英文映射（内置到 Prompt）：**

| 英文字段值 | 中文含义 |
|-----------|---------|
| `mid_banner` | 腰封 / 首页腰封 |
| `news_click` | 头条 / 头条新闻 |
| `person_banner_click` | 个人中心腰封 |
| `top_banner_click` | 顶部 banner |
| `Hometopic_click` | 卡片位 / 首页专题 |

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

-- 关键资源位点击量
CREATE TABLE resource_total (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    resource_name   VARCHAR(100),
    resource_amount INT UNSIGNED,
    stat_date       DATE,
    port            VARCHAR(50),
    UNIQUE KEY uq_resource (resource_name, stat_date, port)
);

-- 子服务使用明细
CREATE TABLE `5100_detail` (
    id             INT AUTO_INCREMENT PRIMARY KEY,
    service_name   VARCHAR(200),
    service_amount INT UNSIGNED,
    stat_date      DATE,
    port           VARCHAR(50),
    UNIQUE KEY uq_service (service_name, stat_date, port)
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

| 规则 | 描述 |
|------|------|
| 数据预查 | 有明确日期时，必须先调用 `check_date_available` |
| 合并查询 | 同比/环比必须用 `IN` 一次查出两个时期 |
| 红线规则 | 未拿到真实数据，严禁使用 XXX/YYY 等占位符 |
| 整年拦截 | 仅有年份无月份时，主动向用户澄清粒度 |
| 最终格式 | 输出必须以 `Final Answer: ` 开头，否则解析器报错 |
| 大结果限制 | 明细查询末尾自动加 `LIMIT 50` |

---

## 常见问题

| 错误信息 | 原因 | 解决方案 |
|---------|------|---------|
| `未找到 TONGYI_API_KEY` | .env 未配置 API Key | 参考 `.env.example` 补全配置 |
| `系统初始化失败` | 数据库连接异常 | 检查 MySQL 服务状态及 `.env` 中的 DB 配置 |
| `OUTPUT_PARSING_FAILURE` | Agent 未按格式回答 | 已由 `handle_parsing_errors` 捕获，提示用户重新提问 |
| 查询结果为空 | 所查日期暂无数据 | 系统会提示"暂无数据，可换一种提问方式" |
| Agent 提前终止 | 推理链过长 | 已将 `max_iterations` 设为 15，可按需调大 |

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
