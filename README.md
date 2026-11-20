# 线上展馆观众参观路径 API

从浏览器上报的 `open` / `close` 事件还原匿名观众的实际参观路径，避免把未关闭页面
计作完整参观；支持乱序补报、重传去重与数据同意撤回。

技术栈：Python 3.11 + FastAPI + SQLite（WAL 模式），Docker Compose 一键启动。

## 快速启动

```bash
docker compose up --build
# 打开交互文档：
#   Swagger UI  http://localhost:8000/docs
#   ReDoc       http://localhost:8000/redoc
# 健康检查     http://localhost:8000/health
```

SQLite 数据文件存放在命名卷 `exhibition-data`（容器内 `/data/exhibition.db`），
容器重建不丢数据。

本地（无 Docker）运行：

```bash
pip install -r requirements.txt
EXHIBITION_DB=./exhibition.db uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/events` | 上报单个事件 |
| POST | `/events/batch` | 批量上报，单事务原子提交 |
| GET | `/visitors/{visitor_id}/path` | 查询观众参观路径 |
| POST | `/visitors/{visitor_id}/withdraw` | 撤回数据使用同意（body 可省略，或传 `{"visitor_id": "..."}`） |
| GET | `/health` | 健康检查 |

事件字段：`event_id`（全局唯一，重传复用同一号）、`visitor_id`（匿名观众号）、
`page_view_id`（页面浏览号，同一次浏览的开关共享）、`exhibit_id`（展品号）、
`event_type`（`open` / `close`）、`event_time`（**带时区的 UTC ISO-8601**，
如 `2026-11-18T08:00:00Z`；无时区时间戳返回 400）。

### 上报响应语义

- `stored`：首次入库
- `duplicate`：`event_id` 已存在，幂等忽略（即使重传内容被篡改，也以首次内容为准）
- `rejected_withdrawn`：该观众已撤回同意，事件不入库，路径不会因此恢复

HTTP 状态码：成功受理固定 `201`，具体结果看响应体 `status`；参数/时间非法为
`422` / `400`。

### 路径响应

按 `page_view_id` 配对开关，同一观众按 **open 时间** 排序（`sequence` 从 1 连续编号）：

- 有关闭：`dwell_known=true`，`dwell_seconds` 为停留秒数
- 缺关闭：仍计为一次参观，但 `closed_at=null`、`dwell_known=false`、`dwell_seconds=null`（停留未知）
- 只有 close 没有 open 的孤儿事件不产生参观项
- close 早于 open 的异常数据：保留关闭时间，但停留时长标记未知

## 设计要点（如何满足需求）

1. **重传不重复计数**：`events.event_id` 为主键；插入前先查存在性，存在即返回
   `duplicate`，整段逻辑在同一事务内。
2. **乱序补报照常处理**：路径不做物化存储，每次查询从已提交事件实时配对、排序。
   后补的较早 open 会在下次查询时自然改变路径顺序；后补的 close 会把“停留未知”
   变成已知时长。
3. **撤回后不可恢复**：`withdrawals` 表持久化撤回标记；撤回在一个 `BEGIN IMMEDIATE`
   事务内完成「删除该观众全部事件 + 写撤回标记」。此后任何上传（包括旧事件重放）
   在同一事务内先查撤回标记，一律 `rejected_withdrawn`。
4. **查询内存随参观项数增长，不随上报条数增长（本次修复）**：
   - 旧实现把该观众的全部事件行 `fetchall()` 进 Python 再配对；有的观众只看过
     一件展品却因重传/重复上报积攒了大量重复关页或找不到开页的孤儿关页，查询时
     内存暴涨直至 OOM，表现为 `500`。
   - 现在「每个 `page_view_id` 取最早 open、最早 close」的聚合直接下推到 SQLite
     （`GROUP BY page_view_id` + `MIN(event_time)`），只把每个浏览号一行带回应用；
     配对语义与旧实现完全一致（最早 open、最早 close、孤儿 close 不成参观项、
     close 早于 open 标记停留未知）。
   - 新增覆盖索引 `(visitor_id, event_type, page_view_id, event_time)`，聚合可按
     索引流式完成；close 聚合先用 `IN (仅该观众有 open 的浏览号)` 收窄，孤儿关页
     再多也不会被逐行物化。
   - `init_db()` 使用 `IF NOT EXISTS` 语句逐条执行，**旧 SQLite 库无需迁移脚本、
     直接可查**，首次启动自动补上缺失索引，已有事件行保持不变；
     旧库中 `...Z` / `...+0000` 形式的时间戳也能正常解析。
5. **交错操作基于同一已提交状态、不露半份路径**：
   - SQLite 开 WAL，写请求统一 `BEGIN IMMEDIATE` 立即拿写锁，配合 `busy_timeout`
     串行化所有写事务；
   - 读请求在单个 `BEGIN DEFERRED` 快照事务里完成「撤回检查 + open 聚合 +
     close 聚合」，三条语句看到同一份已提交状态：要么是撤回前的完整路径，要么是
     撤回后的空路径，不会出现“撤回检查通过、聚合却读到已删除数据”的半份结果；
   - 路径是查询时推导的，不存在“路径表更新一半”的中间态；
   - 批量上报为单事务，任一事件非法则整批回滚；迟到补报（更早的 open、缺失的
     close）提交后，下次查询按 open 时间重新排序并重算停留时长。
6. **线程/进程并发**：每请求一个独立 SQLite 连接（`check_same_thread=False`
   以适配 uvicorn 线程池，连接不在请求间共享）；多个 uvicorn worker 共享同一
   数据卷时由 SQLite 文件锁保证安全。

## 目录结构

```
app/
  config.py    环境变量配置（DB 路径、busy timeout）
  db.py        连接、schema、WAL/pragma、事务管理
  models.py    Pydantic 请求/响应模型
  crud.py      领域逻辑：入库去重、开关配对、路径推导、撤回
  main.py      FastAPI 路由与生命周期
tests/
  conftest.py
  test_api.py      21 项功能/并发/规模/旧库测试
  stress_http.py   针对运行中服务的真实 HTTP 并发压测（手动运行）
Dockerfile
docker-compose.yml
```

## 自测

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

覆盖：正常配对与停留时长、乱序（close 先到）与更早事件补报导致的重排、
缺关闭标记未知及补关后转已知、孤儿 close、`event_id` 重传（含篡改内容）、
批量去重与整批原子回滚、撤回后路径消失/重复撤回幂等/旧事件重放与新事件均被拒、
多线程上传-查询-撤回交错（进程内 TestClient 与真实 HTTP 两种）、
无时区时间拒绝、时区偏移归一化、负停留异常、WAL 与主键的存储层校验，
以及本次修复的回归测试：

- `test_path_query_scales_with_visits_not_event_rows`：200 个浏览号各 500 条
  重复 close，再加 5000 条孤儿 close，查询仍返回 200 项且停留时长正确；
- `test_path_query_does_not_fetch_raw_event_rows`：代理连接记录每次 `fetchall`
  的行数，断言任何一次查询抓取的行数都不超过浏览号数量（而不是事件条数）；
- `test_legacy_database_is_queryable_and_upgraded`：构造旧版 schema/旧时间戳格式
  的库，`init_db` 后直接查询成功、补上索引且事件行不变；
- `test_late_open_backfills_orphan_close_into_a_visit`：只到 close 的孤儿先不
  产生参观项，迟到补上更早的 open 后成为完整参观并正确排序；
- `test_late_backfill_and_withdrawal_are_serialized_snapshots` 与
  `test_reader_snapshot_is_stable_while_writer_commits`：迟到补报按原规则重排并
  重算停留；读快照期间另一连接提交撤回，本次查询仍返回完整旧路径，之后的查询
  返回空路径。

## 已知限制 / 未完成项

- 本开发沙箱没有 Docker 守护进程（`docker` / `podman` 均不可用），
  `Dockerfile` 与 `docker-compose.yml` 已编写并做 YAML/结构校验，但**未在真实
  Docker 中构建运行**；等价验证是在容器外以 `uvicorn app.main:app` 单进程、
  `EXHIBITION_DB` 指向与容器相同的 SQLite 文件完成的（含 `/docs` 与全部 API）。
- 旧库首次启动时若缺少覆盖索引，`init_db()` 会建索引：实测 80 万事件行约 5 秒，
  属于一次性成本；数据量更大时建议在维护窗口预建或手工执行同名
  `CREATE INDEX IF NOT EXISTS`。
- 同一 `page_view_id` 若出现多个 open，取最早 open；多个 close 取最早 close，
  异常上报不报错但只保留一组配对。
- 没有鉴权/限流与分页；观众号、事件号均按字符串处理，未做格式白名单。
- 撤回是硬删除事件数据（仅保留撤回标记本身），不做审计留档。
