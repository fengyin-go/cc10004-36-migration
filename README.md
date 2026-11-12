# 数据洞察分析工作台

面向多源数据接入、交互式查询分析、可视化看板搭建、分析报告生成与数据服务编排的数据洞察分析工作台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 交互查询 | `query` | 查询语句 | 语句编号、查询表达式、目标数据集 |
| 可视化图表 | `chart` | 图表配置 | 图表编号、图表名称、图表类型 |
| 数据源接入 | `source` | 接入配置 | 配置编号、连接类型、接入地址 |
| 聚合计算 | `aggregate` | 聚合方案 | 方案编号、聚合函数、目标字段 |
| 数据筛选 | `filter` | 筛选规则 | 规则编号、目标数据集、筛选字段 |
| 报告生成 | `reportgen` | 报告模板 | 模板编号、模板名称、包含图表 |
| 异常检测 | `anomaly` | 检测规则 | 规则编号、监控指标、基线均值 |
| 多维下钻 | `drill` | 下钻路径 | 路径编号、起点维度、下钻层数 |
| 对比分析 | `compare` | 对比方案 | 方案编号、基准数据集、对照数据集 |
| 分析流水线 | `pipeline` | 流水线节点 | 节点编号、节点类型、输入源 |
| 数据集管理 | `datastore` | 数据集定义 | 数据集编号、数据集名称、来源表名 |
| 趋势预测 | `forecast` | 预测模型 | 模型编号、模型类型、训练数据集 |
| 数据分片 | `segment` | 分片规则 | 规则编号、目标数据集、分片字段 |
| 调度管理 | `scheduler` | 调度配置 | 配置编号、关联任务、调度表达式 |
| 通知分发 | `notify` | 通知规则 | 规则编号、触发条件、通知渠道 |
| 查询缓存 | `cache` | 缓存策略 | 策略编号、目标查询、缓存键规则 |
| 数据标签 | `tag` | 标签定义 | 标签编号、标签名称、计算表达式 |
| 数据分享 | `share` | 分享链接 | 链接编号、目标数据集、访问密码 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 查询缓存的 SQLite 共享后端

查询缓存不再放在各进程独有的内存里，而是落到所有 FastAPI 进程共享的一个 SQLite 文件
（`backend/app/query_cache.py`），多进程对同一目标查询使用同一份缓存事实。

- 位置可配置：环境变量 `QUERY_CACHE_DB` 指定 SQLite 文件路径，缺省为
  `backend/data/query_cache.sqlite3`；`docker-compose.yml` 把它指到挂载卷 `/data`，
  本地与容器重启后都能继续读取。
- 每条缓存读写都携带完整查询身份（语句编号、表达式、目标数据集、入参、来源进程的指纹）
  与绝对过期时点 `expires_at_epoch_ms`；过期时点在取得填充租约时固化，进程重启不会
  重新计算，因此不会延长生存期。
- 同键并发填充只有一个有效写入者：通过唯一键 + 租约 + 单调递增的 `fence_token` 实现。
  租用者退出后租约到期，其它进程可接管并推进 fence，晚到的旧写入（旧 fence）会被拒绝，
  无法覆盖新结果。
- 损坏条目（结果负载无法解析等）会被隔离为 `quarantined` 状态并允许同键重新计算，
  不影响其它条目；`cache_events` 表记录租约、接管、隔离、拒绝等事件。
- 落库失败（数据库不可写等）不影响查询：接口返回 `cache.status=bypassed` 明确说明已
  绕过缓存，查询结果照常返回。
- 缓存策略迁移保留原策略编号；无法解析的过期秒数（如旧样例里的占位文本）标记为
  「待确认」（`ttl_unknown=1`），这类策略不会产生真实缓存命中，旧样例内容也不会被
  当成缓存事实。

相关环境变量（见 `.env.example`）：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `QUERY_CACHE_DB` | `backend/data/query_cache.sqlite3` | 共享缓存 SQLite 文件路径 |
| `QUERY_CACHE_LEASE_MS` | `30000` | 填充租约时长（毫秒），到期后其它进程可接管 |
| `QUERY_CACHE_WAIT_MS` | `1500` | 同键已有租约时等待共享结果的最长时间 |

新增/保留的接口：

- `POST /api/query/{id}/execute`：执行交互查询并接入共享缓存，返回体含 `result` 与
  `cache`（`hit` / `miss` / `bypassed`，及身份、fence、绝对过期时点）。
- `GET /api/cache/entries`、`GET /api/cache/stats`：观察共享缓存条目与统计。
- `DELETE /api/cache/entries/{cache_key}`：清理单个缓存键，下次查询重新计算。
- 原有策略接口（列表、明细、登记、动作、导出）路径与出入参保持不变。

测试（含真实多进程的单写入者、接管、fence 拒绝、损坏隔离验证）：

```bash
make test
# 或 cd backend && .venv/bin/python -m unittest app.tests.test_query_cache -v
```

领域：数据工程
子方向：数据分析与数据应用
