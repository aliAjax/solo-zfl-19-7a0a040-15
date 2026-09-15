const http = require("http");
const { readFile, writeFile, mkdir, rename, rm, readdir } = require("fs/promises");
const path = require("path");

const PORT = Number(process.env.PORT || 3019);
const DB_FILE = process.env.DB_FILE || path.join(__dirname, "data", "db.json");
const LENGTH_EPS = 1e-6; // 长度按米计，比较时容忍浮点误差

const initialData = {
  tunes: [
    {
      id: "tune_demo",
      title: "雨后圆舞曲",
      composer: "匿名",
      stripSpec: {
        widthMm: 70,
        scale: "20音",
        tempoBpm: 82,
        paperType: "半透明纸带"
      },
      createdAt: new Date().toISOString()
    }
  ],
  sections: [
    {
      id: "section_demo_1",
      tuneId: "tune_demo",
      startBeat: 1,
      endBeat: 32,
      laneRange: "1-10",
      checked: true,
      note: "开头主题已试奏"
    },
    {
      id: "section_demo_2",
      tuneId: "tune_demo",
      startBeat: 33,
      endBeat: 64,
      laneRange: "4-18",
      checked: false,
      note: "副歌段等待校对"
    }
  ],
  issues: [
    {
      id: "issue_demo",
      tuneId: "tune_demo",
      sectionId: "section_demo_2",
      type: "漏孔",
      beat: 41,
      lane: 12,
      description: "第41拍高音孔漏打",
      status: "open",
      createdAt: new Date().toISOString(),
      resolvedAt: null
    }
  ],
  // ---- 纸材库存与领料核销（旧数据文件没有这些表，启动时自动补齐）----
  paperBatches: [],
  stockIns: [],
  requisitions: [],
  materialReturns: [],
  writeOffs: [],
  stockAdjustments: []
};

const MATERIAL_COLLECTIONS = [
  "paperBatches",
  "stockIns",
  "requisitions",
  "materialReturns",
  "writeOffs",
  "stockAdjustments"
];

const routes = [
  "GET /health",
  "GET /tunes",
  "POST /tunes",
  "GET /tunes/:id/progress",
  "GET /tunes/:id/sections",
  "POST /tunes/:id/sections",
  "GET /tunes/:id/unchecked-sections",
  "PATCH /sections/:id/check",
  "GET /issues",
  "POST /issues",
  "PATCH /issues/:id/status",
  "POST /paper/batches",
  "GET /paper/batches",
  "GET /paper/batches/:id",
  "GET /paper/stock",
  "POST /requisitions",
  "GET /requisitions",
  "GET /requisitions/:id",
  "POST /requisitions/:id/returns",
  "POST /requisitions/:id/write-offs",
  "POST /requisitions/:id/void",
  "POST /stock-adjustments",
  "GET /stock-adjustments"
];

// ---------------------------------------------------------------------------
// 持久化：整库读改写。所有写操作串行经过 writeChain，写入走临时文件 + rename，
// 保证“领料单 + 批次扣减 + 结余”要么整体可见、要么完全不可见，不出现半笔账。
// ---------------------------------------------------------------------------

let readyPromise = null;
function ensureReady() {
  if (!readyPromise) readyPromise = prepareDb();
  return readyPromise;
}

// 所有写事务串行排队：读-改-写全程持锁，并发领料也只能一个个提交，
// 后一个一定看到前一个扣减后的结余，因此不可能超卖。
let writeChain = Promise.resolve();

function fail(message, status = 400, extra = undefined) {
  const error = new Error(message);
  error.status = status;
  if (extra) error.extra = extra;
  return error;
}

async function prepareDb() {
  await mkdir(path.dirname(DB_FILE), { recursive: true });
  let data = null;
  try {
    data = JSON.parse(await readFile(DB_FILE, "utf8"));
  } catch {
    data = JSON.parse(JSON.stringify(initialData));
    await writeFile(DB_FILE, JSON.stringify(data, null, 2));
  }
  // 旧数据兼容：老版本 db.json 没有库存相关表，原地补齐而不是覆盖旧数据。
  let migrated = false;
  for (const key of ["tunes", "sections", "issues", ...MATERIAL_COLLECTIONS]) {
    if (!Array.isArray(data[key])) {
      data[key] = key in initialData ? JSON.parse(JSON.stringify(initialData[key])) : [];
      migrated = true;
    }
  }
  if (migrated) await writeFile(DB_FILE, JSON.stringify(data, null, 2));
  // 崩溃恢复：原子 rename 完成后临时文件不会留下；若有残留说明上次写入中断，
  // 主文件仍是上一版完整数据，直接清掉残留 tmp。
  try {
    for (const name of await readdir(path.dirname(DB_FILE))) {
      if (name.startsWith(path.basename(DB_FILE)) && name.endsWith(".tmp")) {
        await rm(path.join(path.dirname(DB_FILE), name), { force: true });
      }
    }
  } catch {
    // 目录读不出来时让后续正常读写报错即可
  }
  return data;
}

async function readDb() {
  await ensureReady();
  return JSON.parse(await readFile(DB_FILE, "utf8"));
}

async function atomicWriteDb(data) {
  const tmpFile = `${DB_FILE}.${process.pid}.${writeChainNextSeq()}.tmp`;
  await writeFile(tmpFile, JSON.stringify(data, null, 2));
  await rename(tmpFile, DB_FILE);
}

let tmpSeq = 0;
function writeChainNextSeq() {
  tmpSeq += 1;
  return tmpSeq;
}

/**
 * 串行化一个读-改-写事务。mutator 在内存里完成全部校验与修改：
 * 抛错则一行都不会写盘；返回值作为响应体。
 */
function withTransaction(mutator) {
  const run = writeChain.then(async () => {
    const db = await readDb();
    const result = await mutator(db);
    await atomicWriteDb(db);
    return result;
  });
  // 防止单个失败把整条链打成 rejected 链
  writeChain = run.then(
    () => undefined,
    () => undefined
  );
  return run;
}

function send(res, status, body) {
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(body, null, 2));
}

function parseUrl(req) {
  const url = new URL(req.url, `http://${req.headers.host}`);
  return { pathname: url.pathname, searchParams: url.searchParams };
}

async function parseBody(req) {
  let raw = "";
  for await (const chunk of req) raw += chunk;
  if (!raw) return {};
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw fail("请求体必须是合法JSON", 400);
  }
  // null、数组、字符串、数字、布尔都不是合法请求体，必须明确报参数错误，
  // 否则后面 required(body, ...) 会直接抛 TypeError 变成 500。
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw fail("请求体必须是 JSON 对象", 400);
  }
  return parsed;
}

function makeId(prefix) {
  return `${prefix}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

function required(body, fields) {
  const missing = fields.filter((field) => body[field] === undefined || body[field] === "");
  if (missing.length) throw fail(`缺少字段：${missing.join(", ")}`, 400);
}

function parseLength(value, label, { allowZero = false } = {}) {
  // 严格数字类型：Boolean 会被 Number(true)=1 误收，数字字符串也不属于约定的数值字段，
  // 这类入参一律按参数错误拒绝。
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw fail(`${label}必须是数字`, 400);
  }
  if (value < 0 || (!allowZero && value === 0)) {
    throw fail(allowZero ? `${label}必须是非负数` : `${label}必须是正数`, 400);
  }
  return value;
}

function requireObject(value, label) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw fail(`${label}必须是对象`, 400);
  }
  return value;
}

function requireString(value, field) {
  if (typeof value !== "string") throw fail(`字段 ${field} 必须是字符串`, 400);
  return value;
}

function roundLength(num) {
  return Math.round(num * 1e6) / 1e6;
}

// ---------------------------------------------------------------------------
// 库存领域逻辑
// ---------------------------------------------------------------------------

function findTune(db, tuneId) {
  const tune = db.tunes.find((item) => item.id === tuneId);
  if (!tune) throw fail("曲目不存在", 404);
  return tune;
}

function findBatch(db, batchId) {
  const batch = db.paperBatches.find((item) => item.id === batchId);
  if (!batch) throw fail("纸材批次不存在", 404);
  return batch;
}

function findRequisition(db, requisitionId) {
  const requisition = db.requisitions.find((item) => item.id === requisitionId);
  if (!requisition) throw fail("领料单不存在", 404);
  return requisition;
}

/**
 * 批次口径（恒等式：初始长度(含盘点调整) = 占用量 + 可用量）：
 *  - 活动单占用 = 分摊长度 - 已退长度（已核销但未退的仍是真实消耗，继续占用）
 *  - 作废单占用 = 已核销长度（作废只回补“未退未核”部分，已退/已核不重复回补）
 */
function computeBatchStats(db) {
  const stats = new Map();
  for (const req of db.requisitions) {
    for (const alloc of req.allocations) {
      const entry = stats.get(alloc.batchId) || {
        issuedLength: 0,
        returnedLength: 0,
        writtenOffLength: 0,
        heldLength: 0
      };
      entry.issuedLength = roundLength(entry.issuedLength + alloc.length);
      entry.writtenOffLength = roundLength(entry.writtenOffLength + alloc.writtenOffLength);
      if (req.status === "voided") {
        // 作废时未退未核部分已回补原批次：回补 = 长度 - 已退 - 已核
        entry.returnedLength = roundLength(entry.returnedLength + alloc.length - alloc.writtenOffLength);
        entry.heldLength = roundLength(entry.heldLength + alloc.writtenOffLength);
      } else {
        entry.returnedLength = roundLength(entry.returnedLength + alloc.returnedLength);
        entry.heldLength = roundLength(entry.heldLength + alloc.length - alloc.returnedLength);
      }
      stats.set(alloc.batchId, entry);
    }
  }
  return stats;
}

function batchSummary(batch, stats) {
  const s = stats.get(batch.id) || { issuedLength: 0, returnedLength: 0, writtenOffLength: 0, heldLength: 0 };
  return {
    ...batch,
    issuedLength: roundLength(s.issuedLength),
    returnedLength: roundLength(s.returnedLength),
    writtenOffLength: roundLength(s.writtenOffLength),
    heldLength: roundLength(s.heldLength),
    consumedLength: roundLength(s.writtenOffLength),
    availableLength: roundLength(batch.initialLength - s.heldLength)
  };
}

function summarizeBatch(db, batchId) {
  const batch = findBatch(db, batchId);
  return batchSummary(batch, computeBatchStats(db));
}

/** 自动分摊：同纸型按先进先出（入库时间、批次序号）扣减，单批不足跨批继续。 */
function allocateFifo(db, paperType, length, stats) {
  const candidates = db.paperBatches
    .filter((item) => item.paperType === paperType)
    .map((item) => batchSummary(item, stats))
    .filter((item) => item.availableLength > LENGTH_EPS)
    .sort((a, b) => a.receivedAt.localeCompare(b.receivedAt) || a.id.localeCompare(b.id));

  const allocations = [];
  let remaining = length;
  for (const batch of candidates) {
    if (remaining <= LENGTH_EPS) break;
    const take = roundLength(Math.min(remaining, batch.availableLength));
    if (take > 0) {
      allocations.push({
        batchId: batch.id,
        length: take,
        returnedLength: 0,
        writtenOffLength: 0
      });
      remaining = roundLength(remaining - take);
    }
  }
  if (remaining > LENGTH_EPS) {
    throw fail(
      `纸型 ${paperType} 可用长度不足，还差 ${remaining} 米`,
      409,
      {
        requestedLength: roundLength(length),
        shortageLength: remaining,
        availableByBatch: candidates.map((item) => ({ batchId: item.id, availableLength: item.availableLength }))
      }
    );
  }
  return allocations;
}

/** 指定批次分摊：必须同纸型、不重复、合计等于申请长度、任一批次不超扣。 */
function allocateExplicit(db, paperType, length, requested, stats) {
  if (!Array.isArray(requested) || !requested.length) throw fail("allocations 必须是非空数组", 400);
  const seen = new Set();
  const allocations = [];
  for (const item of requested) {
    requireObject(item, "分摊项");
    required(item, ["batchId", "length"]);
    requireString(item.batchId, "batchId");
    if (seen.has(item.batchId)) throw fail(`批次 ${item.batchId} 在分摊中重复`, 400);
    seen.add(item.batchId);
    const take = parseLength(item.length, "分摊长度");
    const batch = findBatch(db, item.batchId);
    if (batch.paperType !== paperType) {
      throw fail(`批次 ${item.batchId} 的纸型是 ${batch.paperType}，与领料纸型 ${paperType} 不一致`, 400);
    }
    const summary = batchSummary(batch, stats);
    if (take > summary.availableLength + LENGTH_EPS) {
      throw fail(
        `批次 ${batch.batchNo} 可用长度不足：申请 ${take} 米，可用 ${summary.availableLength} 米`,
        409,
        { batchId: batch.id, availableLength: summary.availableLength }
      );
    }
    allocations.push({ batchId: batch.id, length: roundLength(take), returnedLength: 0, writtenOffLength: 0 });
  }
  const total = roundLength(allocations.reduce((sum, item) => sum + item.length, 0));
  if (Math.abs(total - roundLength(length)) > LENGTH_EPS) {
    throw fail(`分摊合计 ${total} 米与领料长度 ${roundLength(length)} 米不一致`, 400);
  }
  return allocations;
}

function serializeRequisition(db, requisition) {
  const totalLength = roundLength(requisition.allocations.reduce((sum, item) => sum + item.length, 0));
  const writtenOffLength = roundLength(requisition.allocations.reduce((sum, item) => sum + item.writtenOffLength, 0));
  let returnedLength;
  let voidRestoredLength = 0;
  if (requisition.status === "voided") {
    // 作废单的“退回”= 实际退料 + 作废时回补的未退未核部分
    voidRestoredLength = roundLength(
      requisition.allocations.reduce((sum, item) => sum + (item.length - item.returnedLength - item.writtenOffLength), 0)
    );
    returnedLength = roundLength(
      requisition.allocations.reduce((sum, item) => sum + (item.length - item.writtenOffLength), 0)
    );
  } else {
    returnedLength = roundLength(requisition.allocations.reduce((sum, item) => sum + item.returnedLength, 0));
  }
  const openLength = roundLength(totalLength - returnedLength - writtenOffLength);
  return {
    ...requisition,
    totalLength,
    returnedLength,
    voidRestoredLength,
    writtenOffLength,
    openLength,
    fullySettled: openLength <= LENGTH_EPS,
    allocations: requisition.allocations.map((alloc) => {
      const batch = db.paperBatches.find((item) => item.id === alloc.batchId) || null;
      return {
        ...alloc,
        openLength:
          requisition.status === "voided"
            ? 0
            : roundLength(alloc.length - alloc.returnedLength - alloc.writtenOffLength),
        batch: batch ? { id: batch.id, batchNo: batch.batchNo, paperType: batch.paperType } : null
      };
    })
  };
}

// ---------------------------------------------------------------------------
// HTTP 处理
// ---------------------------------------------------------------------------

function buildProgress(db, tuneId) {
  findTune(db, tuneId);
  const sections = db.sections.filter((item) => item.tuneId === tuneId);
  const issues = db.issues.filter((item) => item.tuneId === tuneId);
  const checkedCount = sections.filter((item) => item.checked).length;
  const openIssues = issues.filter((item) => item.status !== "resolved").length;
  return {
    tuneId,
    totalSections: sections.length,
    checkedSections: checkedCount,
    uncheckedSections: sections.length - checkedCount,
    openIssues,
    resolvedIssues: issues.length - openIssues,
    percent: sections.length ? Math.round((checkedCount / sections.length) * 100) : 0
  };
}

async function handle(req, res) {
  const { pathname, searchParams } = parseUrl(req);

  if (req.method === "GET" && pathname === "/health") {
    return send(res, 200, { ok: true, service: "organ-strip-punch-api", routes });
  }

  // ---------------- 曲目（旧接口，行为保持不变，写操作纳入同一事务锁） ----------------

  if (req.method === "GET" && pathname === "/tunes") {
    const db = await readDb();
    const tunes = db.tunes.map((tune) => ({ ...tune, progress: buildProgress(db, tune.id) }));
    return send(res, 200, { data: tunes });
  }

  if (req.method === "POST" && pathname === "/tunes") {
    const body = await parseBody(req);
    required(body, ["title", "stripSpec"]);
    const data = await withTransaction((db) => {
      const tune = {
        id: makeId("tune"),
        title: body.title,
        composer: body.composer || "",
        stripSpec: body.stripSpec,
        createdAt: new Date().toISOString()
      };
      db.tunes.push(tune);
      return tune;
    });
    return send(res, 201, { data });
  }

  const tuneSectionsMatch = pathname.match(/^\/tunes\/([^/]+)\/sections$/);
  if (tuneSectionsMatch && req.method === "GET") {
    const tuneId = tuneSectionsMatch[1];
    const db = await readDb();
    findTune(db, tuneId);
    return send(res, 200, { data: db.sections.filter((item) => item.tuneId === tuneId) });
  }

  if (tuneSectionsMatch && req.method === "POST") {
    const tuneId = tuneSectionsMatch[1];
    const body = await parseBody(req);
    required(body, ["startBeat", "endBeat", "laneRange"]);
    const data = await withTransaction((db) => {
      findTune(db, tuneId);
      const section = {
        id: makeId("section"),
        tuneId,
        startBeat: Number(body.startBeat),
        endBeat: Number(body.endBeat),
        laneRange: body.laneRange,
        checked: Boolean(body.checked),
        note: body.note || ""
      };
      db.sections.push(section);
      return section;
    });
    return send(res, 201, { data });
  }

  const uncheckedMatch = pathname.match(/^\/tunes\/([^/]+)\/unchecked-sections$/);
  if (uncheckedMatch && req.method === "GET") {
    const tuneId = uncheckedMatch[1];
    const db = await readDb();
    findTune(db, tuneId);
    return send(res, 200, { data: db.sections.filter((item) => item.tuneId === tuneId && !item.checked) });
  }

  const progressMatch = pathname.match(/^\/tunes\/([^/]+)\/progress$/);
  if (progressMatch && req.method === "GET") {
    const db = await readDb();
    return send(res, 200, { data: buildProgress(db, progressMatch[1]) });
  }

  const checkMatch = pathname.match(/^\/sections\/([^/]+)\/check$/);
  if (checkMatch && req.method === "PATCH") {
    const sectionId = checkMatch[1];
    const body = await parseBody(req);
    const data = await withTransaction((db) => {
      const section = db.sections.find((item) => item.id === sectionId);
      if (!section) throw fail("区间不存在", 404);
      section.checked = body.checked !== undefined ? Boolean(body.checked) : true;
      section.note = body.note ?? section.note;
      return section;
    });
    return send(res, 200, { data });
  }

  if (req.method === "GET" && pathname === "/issues") {
    const tuneId = searchParams.get("tuneId");
    const status = searchParams.get("status");
    const db = await readDb();
    const issues = db.issues.filter((item) => (!tuneId || item.tuneId === tuneId) && (!status || item.status === status));
    return send(res, 200, { data: issues });
  }

  if (req.method === "POST" && pathname === "/issues") {
    const body = await parseBody(req);
    required(body, ["tuneId", "sectionId", "type", "description"]);
    const data = await withTransaction((db) => {
      findTune(db, body.tuneId);
      const section = db.sections.find((item) => item.id === body.sectionId && item.tuneId === body.tuneId);
      if (!section) throw fail("区间不存在或不属于该曲目", 400);
      const issue = {
        id: makeId("issue"),
        tuneId: body.tuneId,
        sectionId: body.sectionId,
        type: body.type,
        beat: body.beat === undefined ? null : Number(body.beat),
        lane: body.lane === undefined ? null : Number(body.lane),
        description: body.description,
        status: "open",
        createdAt: new Date().toISOString(),
        resolvedAt: null
      };
      db.issues.push(issue);
      return issue;
    });
    return send(res, 201, { data });
  }

  const issueStatusMatch = pathname.match(/^\/issues\/([^/]+)\/status$/);
  if (issueStatusMatch && req.method === "PATCH") {
    const issueId = issueStatusMatch[1];
    const body = await parseBody(req);
    required(body, ["status"]);
    const data = await withTransaction((db) => {
      const issue = db.issues.find((item) => item.id === issueId);
      if (!issue) throw fail("问题不存在", 404);
      issue.status = body.status;
      issue.resolvedAt = body.status === "resolved" ? new Date().toISOString() : null;
      issue.note = body.note ?? issue.note;
      return issue;
    });
    return send(res, 200, { data });
  }

  // ---------------- 纸材：批次 / 入库 / 库存查询 ----------------

  if (req.method === "POST" && pathname === "/paper/batches") {
    const body = await parseBody(req);
    required(body, ["paperType", "batchNo", "length"]);
    requireString(body.paperType, "paperType");
    if (typeof body.batchNo !== "string" && typeof body.batchNo !== "number") {
      throw fail("字段 batchNo 必须是字符串或数字", 400);
    }
    const batchNo = String(body.batchNo);
    const length = parseLength(body.length, "入库长度");
    const data = await withTransaction((db) => {
      // 判重一律按归一化后的字符串批次号，数字 9001 与 "9001" 不能重复入库
      if (db.paperBatches.some((item) => item.batchNo === batchNo)) {
        throw fail(`批次号 ${batchNo} 已存在`, 409);
      }
      const now = new Date().toISOString();
      const batch = {
        id: makeId("batch"),
        paperType: body.paperType,
        batchNo,
        initialLength: roundLength(length),
        supplier: body.supplier || "",
        note: body.note || "",
        receivedAt: body.receivedAt ? String(body.receivedAt) : now,
        createdAt: now
      };
      db.paperBatches.push(batch);
      const stockIn = {
        id: makeId("stockin"),
        batchId: batch.id,
        paperType: batch.paperType,
        batchNo: batch.batchNo,
        length: batch.initialLength,
        supplier: batch.supplier,
        note: batch.note,
        createdAt: now
      };
      db.stockIns.push(stockIn);
      return { batch: batchSummary(batch, new Map()), stockIn };
    });
    return send(res, 201, { data });
  }

  if (req.method === "GET" && pathname === "/paper/batches") {
    const db = await readDb();
    const paperType = searchParams.get("paperType");
    const includeEmpty = searchParams.get("includeEmpty") === "true";
    const stats = computeBatchStats(db);
    let batches = db.paperBatches
      .filter((item) => !paperType || item.paperType === paperType)
      .map((item) => batchSummary(item, stats))
      .sort((a, b) => a.receivedAt.localeCompare(b.receivedAt) || a.id.localeCompare(b.id));
    if (!includeEmpty) batches = batches.filter((item) => item.availableLength > LENGTH_EPS);
    return send(res, 200, { data: batches });
  }

  const batchMatch = pathname.match(/^\/paper\/batches\/([^/]+)$/);
  if (batchMatch && req.method === "GET") {
    const db = await readDb();
    return send(res, 200, { data: summarizeBatch(db, batchMatch[1]) });
  }

  if (req.method === "GET" && pathname === "/paper/stock") {
    const db = await readDb();
    const stats = computeBatchStats(db);
    const byType = new Map();
    for (const batch of db.paperBatches) {
      const summary = batchSummary(batch, stats);
      const entry = byType.get(batch.paperType) || {
        paperType: batch.paperType,
        initialLength: 0,
        issuedLength: 0,
        returnedLength: 0,
        writtenOffLength: 0,
        heldLength: 0,
        consumedLength: 0,
        availableLength: 0,
        batchCount: 0
      };
      entry.initialLength = roundLength(entry.initialLength + summary.initialLength);
      entry.issuedLength = roundLength(entry.issuedLength + summary.issuedLength);
      entry.returnedLength = roundLength(entry.returnedLength + summary.returnedLength);
      entry.writtenOffLength = roundLength(entry.writtenOffLength + summary.writtenOffLength);
      entry.heldLength = roundLength(entry.heldLength + summary.heldLength);
      entry.consumedLength = roundLength(entry.consumedLength + summary.consumedLength);
      entry.availableLength = roundLength(entry.availableLength + summary.availableLength);
      entry.batchCount += 1;
      byType.set(batch.paperType, entry);
    }
    return send(res, 200, { data: [...byType.values()].sort((a, b) => a.paperType.localeCompare(b.paperType)) });
  }

  // ---------------- 领料：跨批次分摊 / 退料 / 核销 / 作废 ----------------

  if (req.method === "POST" && pathname === "/requisitions") {
    const body = await parseBody(req);
    required(body, ["tuneId", "paperType", "length"]);
    requireString(body.tuneId, "tuneId");
    requireString(body.paperType, "paperType");
    const length = parseLength(body.length, "领料长度");
    if (body.allocations !== undefined && !Array.isArray(body.allocations)) {
      throw fail("字段 allocations 必须是数组", 400);
    }
    const data = await withTransaction((db) => {
      findTune(db, body.tuneId); // 领料必须挂在真实曲目上
      const stats = computeBatchStats(db);
      const allocations = Array.isArray(body.allocations)
        ? allocateExplicit(db, body.paperType, length, body.allocations, stats)
        : allocateFifo(db, body.paperType, length, stats);
      const requisition = {
        id: makeId("req"),
        tuneId: body.tuneId,
        paperType: body.paperType,
        reason: body.reason || "",
        operator: body.operator || "",
        status: "active",
        allocations,
        restorations: [],
        voidedAt: null,
        voidReason: null,
        createdAt: new Date().toISOString()
      };
      db.requisitions.push(requisition);
      // 同一事务内用新数据重算一遍：任何批次结余不得为负，
      // 配合写锁保证并发领料不会超卖。
      const afterStats = computeBatchStats(db);
      for (const alloc of allocations) {
        const batch = findBatch(db, alloc.batchId);
        const summary = batchSummary(batch, afterStats);
        if (summary.availableLength < -LENGTH_EPS) {
          throw fail(`批次 ${summary.batchNo} 可用量不足`, 409, { availableLength: summary.availableLength });
        }
      }
      return serializeRequisition(db, requisition);
    });
    return send(res, 201, { data });
  }

  if (req.method === "GET" && pathname === "/requisitions") {
    const db = await readDb();
    const tuneId = searchParams.get("tuneId");
    const status = searchParams.get("status");
    const data = db.requisitions
      .filter((item) => (!tuneId || item.tuneId === tuneId) && (!status || item.status === status))
      .map((item) => serializeRequisition(db, item))
      .sort((a, b) => b.createdAt.localeCompare(a.createdAt));
    return send(res, 200, { data });
  }

  const requisitionMatch = pathname.match(/^\/requisitions\/([^/]+)$/);
  if (requisitionMatch && req.method === "GET") {
    const db = await readDb();
    return send(res, 200, { data: serializeRequisition(db, findRequisition(db, requisitionMatch[1])) });
  }

  const returnsMatch = pathname.match(/^\/requisitions\/([^/]+)\/returns$/);
  if (returnsMatch && req.method === "POST") {
    const requisitionId = returnsMatch[1];
    const body = await parseBody(req);
    required(body, ["length"]);
    const length = parseLength(body.length, "退料长度");
    if (body.batchId !== undefined) requireString(body.batchId, "batchId");
    const data = await withTransaction((db) => {
      const requisition = findRequisition(db, requisitionId);
      if (requisition.status === "voided") throw fail("领料单已作废，不能再退料", 409);

      let batchId = body.batchId;
      if (!batchId) {
        if (requisition.allocations.length === 1) {
          batchId = requisition.allocations[0].batchId;
        } else {
          throw fail("该领料单跨多个批次，退料必须指定 batchId", 400);
        }
      }
      const alloc = requisition.allocations.find((item) => item.batchId === batchId);
      if (!alloc) throw fail("该退料批次不属于这张领料单，退料只能回到原来的批次", 400);
      const batch = findBatch(db, alloc.batchId);

      const already = roundLength(alloc.returnedLength + alloc.writtenOffLength);
      const openForBatch = roundLength(alloc.length - already);
      if (length > openForBatch + LENGTH_EPS) {
        throw fail(
          `退料超量：批次 ${batch.batchNo} 该单可退 ${openForBatch} 米，申请退 ${roundLength(length)} 米`,
          409,
          { batchId: batch.id, returnableLength: openForBatch }
        );
      }

      alloc.returnedLength = roundLength(alloc.returnedLength + length);
      // 退料回到原来的批次：可用量随之回升
      const summary = summarizeBatch(db, batch.id);
      if (summary.availableLength < -LENGTH_EPS) throw fail("退料后批次结余异常", 500);

      const record = {
        id: makeId("return"),
        requisitionId: requisition.id,
        tuneId: requisition.tuneId,
        batchId: batch.id,
        batchNo: batch.batchNo,
        paperType: requisition.paperType,
        length: roundLength(length),
        note: body.note || "",
        createdAt: new Date().toISOString()
      };
      db.materialReturns.push(record);
      return { return: record, requisition: serializeRequisition(db, requisition), batch: summary };
    });
    return send(res, 201, { data });
  }

  const writeOffMatch = pathname.match(/^\/requisitions\/([^/]+)\/write-offs$/);
  if (writeOffMatch && req.method === "POST") {
    const requisitionId = writeOffMatch[1];
    const body = await parseBody(req);
    const data = await withTransaction((db) => {
      const requisition = findRequisition(db, requisitionId);
      if (requisition.status === "voided") throw fail("领料单已作废，不能再核销", 409);

      // items 缺省表示把该单尚未退、尚未核的部分全部核销
      let requested = body.items;
      if (requested === undefined) {
        requested = requisition.allocations
          .map((alloc) => ({
            batchId: alloc.batchId,
            length: roundLength(alloc.length - alloc.returnedLength - alloc.writtenOffLength)
          }))
          .filter((item) => item.length > LENGTH_EPS);
      } else if (!Array.isArray(requested)) {
        throw fail("字段 items 必须是数组", 400);
      }
      if (!requested.length) throw fail("没有可核销的长度", 400);

      const seen = new Set();
      const items = [];
      for (const item of requested) {
        requireObject(item, "核销项");
        required(item, ["batchId", "length"]);
        requireString(item.batchId, "batchId");
        if (seen.has(item.batchId)) throw fail(`批次 ${item.batchId} 的核销重复提交`, 400);
        seen.add(item.batchId);
        const take = parseLength(item.length, "核销长度");
        const alloc = requisition.allocations.find((a) => a.batchId === item.batchId);
        if (!alloc) throw fail("核销批次不属于这张领料单", 400);
        const batch = findBatch(db, alloc.batchId);
        const openForBatch = roundLength(alloc.length - alloc.returnedLength - alloc.writtenOffLength);
        if (take > openForBatch + LENGTH_EPS) {
          throw fail(
            `核销超量：批次 ${batch.batchNo} 该单可核销 ${openForBatch} 米，申请核销 ${roundLength(take)} 米`,
            409,
            { batchId: batch.id, writableOffLength: openForBatch }
          );
        }
        alloc.writtenOffLength = roundLength(alloc.writtenOffLength + take);
        items.push({ batchId: batch.id, batchNo: batch.batchNo, length: roundLength(take) });
      }

      const totalLength = roundLength(items.reduce((sum, item) => sum + item.length, 0));
      const record = {
        id: makeId("writeoff"),
        requisitionId: requisition.id,
        tuneId: requisition.tuneId,
        paperType: requisition.paperType,
        items,
        length: totalLength,
        note: body.note || "",
        createdAt: new Date().toISOString()
      };
      db.writeOffs.push(record);
      return { writeOff: record, requisition: serializeRequisition(db, requisition) };
    });
    return send(res, 201, { data });
  }

  const voidMatch = pathname.match(/^\/requisitions\/([^/]+)\/void$/);
  if (voidMatch && req.method === "POST") {
    const requisitionId = voidMatch[1];
    const body = await parseBody(req).catch(() => ({}));
    const data = await withTransaction((db) => {
      const requisition = findRequisition(db, requisitionId);
      if (requisition.status === "voided") throw fail("领料单已经作废，不能重复作废", 409);

      // 只回补“还没退回、也没核销”的部分；已退料已在退料时回补原批次，
      // 已核销是真实消耗，两者都不能重复回补。
      const restorations = [];
      for (const alloc of requisition.allocations) {
        const restoreLength = roundLength(alloc.length - alloc.returnedLength - alloc.writtenOffLength);
        if (restoreLength > LENGTH_EPS) {
          const batch = findBatch(db, alloc.batchId);
          restorations.push({
            batchId: batch.id,
            batchNo: batch.batchNo,
            paperType: batch.paperType,
            length: restoreLength
          });
        }
      }

      const now = new Date().toISOString();
      if (!restorations.length) {
        throw fail("领料单已全部退料或核销，没有可回补的部分，无需作废", 409);
      }
      requisition.status = "voided";
      requisition.voidedAt = now;
      requisition.voidReason = body.reason || "";
      // 未退未核部分回补原批次——结余由 computeBatchStats 对作废单的
      // 特殊口径直接体现（占用只留已核销），这里只落审计记录，
      // 不再改写 alloc.returnedLength，避免和真实退料重复计算。
      requisition.restorations = restorations;
      // 作废后不允许再退/再核，回补后批次可用量必须回正
      const stats = computeBatchStats(db);
      for (const alloc of requisition.allocations) {
        const summary = batchSummary(findBatch(db, alloc.batchId), stats);
        if (summary.availableLength < -LENGTH_EPS) throw fail("作废回补后批次结余异常", 500);
      }
      return { requisition: serializeRequisition(db, requisition), restorations };
    });
    return send(res, 200, { data });
  }

  // ---------------- 盘点调整 ----------------

  if (req.method === "POST" && pathname === "/stock-adjustments") {
    const body = await parseBody(req);
    required(body, ["batchId", "actualLength", "reason"]);
    requireString(body.batchId, "batchId");
    requireString(body.reason, "reason");
    if (!body.reason.trim()) throw fail("盘点调整必须写明原因", 400);
    const actualLength = parseLength(body.actualLength, "盘点实际长度", { allowZero: true });
    const data = await withTransaction((db) => {
      const batch = findBatch(db, body.batchId);
      const summary = batchSummary(batch, computeBatchStats(db));
      const deltaLength = roundLength(actualLength - summary.availableLength);
      if (Math.abs(deltaLength) <= LENGTH_EPS) throw fail("盘点实际长度与可用量一致，无需调整", 400);
      const record = {
        id: makeId("adjust"),
        batchId: batch.id,
        batchNo: batch.batchNo,
        paperType: batch.paperType,
        expectedAvailableLength: summary.availableLength,
        actualLength: roundLength(actualLength),
        deltaLength,
        reason: String(body.reason).trim(),
        createdAt: new Date().toISOString()
      };
      db.stockAdjustments.push(record);
      // 盘点差异直接落到批次初始长度上（盘亏为负），之后仍恒有
      // 初始长度(含历次调整) = 已摊出未退 + 可用量。
      batch.initialLength = roundLength(batch.initialLength + deltaLength);
      const after = summarizeBatch(db, batch.id);
      if (after.availableLength < -LENGTH_EPS) throw fail("调整后批次可用量不能为负", 409);
      return { adjustment: record, batch: after };
    });
    return send(res, 201, { data });
  }

  if (req.method === "GET" && pathname === "/stock-adjustments") {
    const db = await readDb();
    const batchId = searchParams.get("batchId");
    const data = db.stockAdjustments
      .filter((item) => !batchId || item.batchId === batchId)
      .sort((a, b) => b.createdAt.localeCompare(a.createdAt));
    return send(res, 200, { data });
  }

  return send(res, 404, { error: "接口不存在", routes });
}

const server = http.createServer((req, res) => {
  handle(req, res).catch((error) =>
    send(res, error.status || 500, error.extra ? { error: error.message, details: error.extra } : { error: error.message || "服务器错误" })
  );
});

// 启动先完成迁移与残留 tmp 清理，再开始接客，避免重启后第一笔请求读到半成品状态。
ensureReady()
  .then(() => {
    server.listen(PORT, () => {
      console.log(`Organ strip punch API running at http://127.0.0.1:${PORT}`);
    });
  })
  .catch((error) => {
    console.error("启动初始化失败：", error);
    process.exit(1);
  });

module.exports = { server, ensureReady, withTransaction };
