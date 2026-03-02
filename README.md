# DuckDB MySQL Sharding Extension

DuckDB 扩展，支持连接多个 MySQL 实例，自动发现分库分表并建立逻辑映射，以统一视图查询分片数据。

## 功能

- **多 Host 连接**：直接传入连接串或通过配置文件指定多个 MySQL 实例
- **自动 Schema 发现**：ATTACH 时扫描所有 host 的 `information_schema.tables`，按分片命名规则（`name_00000001`）自动建立逻辑表到物理表的映射
- **透明查询路由**：查询逻辑表时，自动生成 UNION ALL 分发到各 host 的物理表，合并结果返回
- **Filter & Projection Pushdown**：WHERE 条件和列投影下推到每个物理表查询，CTE 外层 WHERE 也能穿透下推
- **懒加载列信息**：列定义按 schema 粒度懒加载，首次查询某 schema 时才从 MySQL 获取
- **Schema 缓存**：使用 DuckDB 数据库文件作为 schema 元数据的唯一存储，增量读写，可直接用 DuckDB 打开查看
- **缓存 TTL**：支持设置缓存过期时间，过期后自动从 MySQL 重新发现
- **手动刷新**：通过 `CALL` 命令全量刷新或按 schema 刷新
- **只读**：仅支持 SELECT，不支持写操作
- **SQL 日志**：查询时输出实际发送到 MySQL 的 SQL

## 分片命名规则

物理数据库和表名遵循 `{base_name}_{8位数字}` 格式：

```
物理: my_db_00000001.my_table_00000001  →  逻辑: my_db.my_table
物理: my_db_00000001.my_table_00000002  →  逻辑: my_db.my_table
物理: my_db_00000002.my_table_00000001  →  逻辑: my_db.my_table
```

不带数字后缀的库/表名保持原样作为逻辑名。

## 使用方式

### 直接传入连接串（推荐，避免密码落盘）

```sql
LOAD mysql_sharding;

-- 单个 host
ATTACH 'host=10.0.0.1 user=root password=secret port=3306' AS rt (TYPE mysql_sharding);

-- 多个 host，分号分隔
ATTACH 'host=10.0.0.1 user=root password=secret port=3306;host=10.0.0.2 user=root password=secret port=3306' AS rt (TYPE mysql_sharding);
```

### 通过配置文件

创建配置文件（每行一个连接串，`#` 开头为注释）：

```
# hosts.conf
host=10.0.0.1 user=root password=secret port=3306
host=10.0.0.2 user=root password=secret port=3306
```

```sql
ATTACH '/path/to/hosts.conf' AS rt (TYPE mysql_sharding);
```

连接字符串格式与 [duckdb-mysql](https://github.com/duckdb/duckdb-mysql) 一致。

### ATTACH 选项

| 选项 | 说明 | 默认值 |
|------|------|--------|
| `CACHE_PATH` | 缓存文件路径（DuckDB 数据库文件） | 文件模式：`{file}.cache.duckdb`；字符串模式：内存（不持久化） |
| `CACHE_TTL` | 缓存过期时间（秒），0 表示永不过期 | `0` |

```sql
-- 字符串模式 + 显式缓存
ATTACH 'host=10.0.0.1 ...' AS rt (TYPE mysql_sharding, CACHE_PATH '/tmp/rt.cache.duckdb', CACHE_TTL 3600);
```

### 查询

```sql
-- 查看逻辑 schema
SELECT schema_name FROM information_schema.schemata WHERE catalog_name = 'rt';

-- 查询逻辑表（自动路由到所有物理分片）
SELECT * FROM rt.my_db.my_table WHERE user_id = 12345;

-- 复杂 CTE + JOIN（filter 和 projection 自动下推到 MySQL）
WITH history AS (
  SELECT user_id, item_id, holding_unit
  FROM rt.accounting_db.fund_account_history_tab
)
SELECT * FROM history WHERE item_id = 1;
-- ↑ item_id=1 会被下推到 MySQL 执行，不会全表扫描
```

### 缓存管理

```sql
-- 全量刷新：清除缓存，重新从 MySQL 发现所有 schema
CALL mysql_sharding_clear_cache('rt');

-- 按 schema 刷新：重新加载指定 schema 的列信息
CALL mysql_sharding_refresh_schema('rt', 'my_db');
```

## 构建

前置条件：
- DuckDB 源码（作为上级目录 `../`）
- duckdb-mysql 源码（作为兄弟目录 `../duckdb-mysql`），本扩展复用其 MySQL 连接代码

```bash
# 在 duckdb 根目录，添加到 extension/extension_config_local.cmake:
# duckdb_extension_load(mysql_sharding DONT_LINK SOURCE_DIR ${CMAKE_SOURCE_DIR}/duckdb-mysql-sharding)

USE_MERGED_VCPKG_MANIFEST=1 make release
```

构建产物为 `build/release/extension/mysql_sharding/mysql_sharding.duckdb_extension`。

## 架构

```
ATTACH 'host=... ; host=...' AS cluster (TYPE mysql_sharding, CACHE_PATH '/tmp/cache.duckdb')
  │
  ├─ ShardingConfig::Parse()                 解析连接串（直接字符串或文件）
  ├─ InitCacheDB()                           打开/创建 cache DuckDB（文件或 :memory:）
  │    └─ CREATE TABLE IF NOT EXISTS physical_tables, table_columns, cache_meta
  ├─ 检查 physical_tables 是否有数据 + TTL
  │    ├─ 有数据且未过期 → 直接使用，秒级就绪
  │    └─ 无数据或已过期 → DiscoverSchemas()
  └─ DiscoverSchemas()                       连接各 host，写入 cache DB
       ├─ DELETE FROM physical_tables
       ├─ Host1: SELECT TABLE_SCHEMA, TABLE_NAME → INSERT INTO physical_tables
       └─ Host2: ...

SELECT * FROM cluster.my_db.my_table WHERE id = 1
  │
  ├─ LookupSchema("my_db")                  查询 cache DB: SELECT DISTINCT logic_db
  ├─ EnsureTablesLoaded()
  │    ├─ HasColumnsForSchema("my_db")?      查询 cache DB: SELECT COUNT(*) FROM table_columns
  │    ├─ 无 → LoadColumnsForSchema()        从 MySQL 加载 → INSERT INTO table_columns
  │    └─ 有 → GetLogicTable()               直接从 cache DB 读取列定义和物理表列表
  ├─ GetScanFunction() → 内部 scan function
  └─ InitGlobalState()
       ├─ Projection pushdown: 只 SELECT 需要的列
       ├─ Filter pushdown: WHERE id=1 下推（支持穿透 CTE）
       ├─ 按 host 分组物理表，生成 UNION ALL 查询
       └─ Scan(): 依次连接各 host 执行，流式返回结果

缓存文件可直接用 DuckDB 打开查看:
  duckdb /tmp/cache.duckdb -c "SELECT * FROM physical_tables LIMIT 10"
```
