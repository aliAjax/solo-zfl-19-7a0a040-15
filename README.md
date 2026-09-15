# 手摇风琴纸带打孔API

纯后端零依赖 Node 服务，使用 `data/db.json` 持久化曲目、纸带区间、试奏问题，以及纸材库存、领料、退料、核销与盘点。

## 启动

```bash
PORT=3019 node server.js
# 也可用 DB_FILE 指定别的数据文件（测试用）
```

## 曲目 / 区间 / 试奏问题（旧接口，保持兼容）

- `GET /health`
- `GET /tunes`
- `POST /tunes`
- `GET /tunes/:id/progress`
- `GET /tunes/:id/sections`
- `POST /tunes/:id/sections`
- `GET /tunes/:id/unchecked-sections`
- `PATCH /sections/:id/check`
- `GET /issues?tuneId=&status=`
- `POST /issues`
- `PATCH /issues/:id/status`

旧版 `db.json`（只有 `tunes/sections/issues`）启动时自动原地补齐库存六张表，旧数据与旧接口响应不变。

## 纸材库存与领料核销

### 数据模型

- **批次 `paperBatches`**：按纸型 + 批次号登记入库长度（米）。批次号唯一。
- **入库流水 `stockIns`**：每登记一个批次同步落一条入库记录。
- **领料单 `requisitions`**：必须挂真实曲目（`tuneId` 不存在直接 404），含按批次的 `allocations` 分摊明细。
- **退料流水 `materialReturns`**：退料必须指定（或自动匹配）原领料单上的批次，只能回原批次。
- **核销流水 `writeOffs`**：按批次记录真实消耗。
- **盘点调整 `stockAdjustments`**：必须写明 `reason`，盘盈/盘亏落到批次初始长度并保留原因。

批次账实恒等式：

```
初始长度(含历次盘点调整) = 占用量 heldLength + 可用量 availableLength
已摊 issuedLength        = 已退 returnedLength + 占用 heldLength
```

活动单占用 = 分摊 − 已退（已核销但未退仍是真实消耗，继续占用）；作废单占用 = 已核销。

### 接口

| 方法/路径 | 说明 |
| --- | --- |
| `POST /paper/batches` | 入库登记：`{paperType, batchNo, length, supplier?, note?, receivedAt?}` |
| `GET /paper/batches?paperType=&includeEmpty=` | 批次列表及结余（默认隐藏已空批次，按入库时间 FIFO 排序） |
| `GET /paper/batches/:id` | 单批次台账 |
| `GET /paper/stock` | 按纸型汇总：初始/已摊/已退/已核/占用/可用 |
| `POST /requisitions` | 领料。不传 `allocations` 时同纸型 FIFO 自动跨批分摊；可传 `[{batchId,length}]` 指定分摊 |
| `GET /requisitions?tuneId=&status=` | 领料单列表（含总量/已退/已核/未结及每批明细） |
| `GET /requisitions/:id` | 单张领料单 |
| `POST /requisitions/:id/returns` | 退料：`{length, batchId?(跨批必填), note?}`，只能回原批次，不可超量 |
| `POST /requisitions/:id/write-offs` | 核销：`{items:[{batchId,length}], note?}`；不传 items 则整单未结部分全核 |
| `POST /requisitions/:id/void` | 作废：**只回补未退未核部分**，已退/已核不重复回补；作废后禁止再退/再核/再作废 |
| `POST /stock-adjustments` | 盘点：`{batchId, actualLength, reason}`，原因必填，调整后可用量不能为负 |
| `GET /stock-adjustments?batchId=` | 盘点记录 |

错误约定：校验失败 `400`，找不到资源 `404`，库存不足/超量/状态冲突 `409`，短缺量等细节放在 `details` 字段。

### 事务与并发

- 所有写操作串行经过同一把内存写锁，读-改-写全程持锁：并发领料时后一个一定看到前一个的扣减，**不会超卖**；库存不足返回 409 且带缺口信息。
- 领料单、批次扣减、结余在同一事务内一次落盘（写临时文件 + `rename`），任一步校验失败则整笔不写盘，**不留半笔账**。
- 启动时迁移旧表、清理上次崩溃残留的 `*.tmp`；SIGKILL 后重启账目完全一致。

## 联调

```bash
node tests/integration.js
```

测试会 spawn 真实 HTTP 服务（临时 db.json、独立端口），覆盖：旧数据迁移与旧接口兼容、跨批次领料（FIFO 与指定分摊）、并发领料不超卖、退料超量、作废重叠（已退已核不重复回补）、盘点原因与负库存拦截、失败不留半笔账、SIGKILL 重启恢复与残留 tmp 清理，结尾全量核对账实恒等式。

## 闭环示例

```bash
curl http://127.0.0.1:3019/tunes/tune_demo/progress

# 入库两个批次 → FIFO 跨批领料 12 米 → 退 2 米回原批次 → 核销剩余
curl -X POST http://127.0.0.1:3019/paper/batches -H 'Content-Type: application/json' \
  -d '{"paperType":"半透明纸带","batchNo":"P001","length":10}'
curl -X POST http://127.0.0.1:3019/paper/batches -H 'Content-Type: application/json' \
  -d '{"paperType":"半透明纸带","batchNo":"P002","length":5}'
curl -X POST http://127.0.0.1:3019/requisitions -H 'Content-Type: application/json' \
  -d '{"tuneId":"tune_demo","paperType":"半透明纸带","length":12,"reason":"全段试奏"}'
```
