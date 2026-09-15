#!/usr/bin/env node
/**
 * 纸材库存与领料核销 —— 真实 HTTP 接口联调脚本（零依赖）。
 *
 * 直接 spawn server.js 子进程，用临时 db.json 跑，覆盖：
 *   S0 旧数据迁移 / 旧接口兼容
 *   S1 跨批次领料（FIFO + 指定分摊 + 各类失败）
 *   S2 并发领料不超卖
 *   S3 退料超量拦截 + 退料/核销回原批次
 *   S4 作废重叠（已退已核不重复回补，作废后再退/再核/再作废被拒）
 *   S5 盘点调整（原因必填、负库存拦截）
 *   S6 失败不留半笔账
 *   S7 SIGKILL 重启恢复 + 残留 tmp 清理
 * 结尾对全局账实恒等式做一次全量核对。
 *
 * 用法：node tests/integration.js
 */
const http = require("http");
const { spawn } = require("child_process");
const { mkdtemp, rm, readFile, readdir, writeFile, copyFile } = require("fs/promises");
const path = require("path");
const os = require("os");
const assert = require("assert");

const SERVER = path.join(__dirname, "..", "server.js");
const PAPER = "半透明纸带";
const PAPER_OTHER = "牛皮纸带";
const EPS = 1e-5;

let passed = 0;
function check(name, cond, extra) {
  assert.ok(cond, extra ? `${name} :: ${JSON.stringify(extra)}` : name);
  passed += 1;
  console.log(`  ✓ ${name}`);
}
async function step(title, fn) {
  console.log(`\n${title}`);
  await fn();
}
function approx(a, b) {
  return Math.abs(Number(a) - Number(b)) <= EPS;
}

// ---------------------------------------------------------------------------

async function startServer(dir, port) {
  const dbFile = path.join(dir, "db.json");
  const child = spawn(process.execPath, [SERVER], {
    env: { ...process.env, PORT: String(port), DB_FILE: dbFile },
    stdio: ["ignore", "pipe", "pipe"]
  });
  let logs = "";
  child.stdout.on("data", (d) => (logs += d));
  child.stderr.on("data", (d) => (logs += d));

  const base = `http://127.0.0.1:${port}`;
  const deadline = Date.now() + 10000;
  for (;;) {
    try {
      const res = await call(base, "GET", "/health");
      if (res.status === 200) break;
    } catch {
      // 还没起来
    }
    if (Date.now() > deadline) {
      child.kill("SIGKILL");
      throw new Error(`服务启动超时\n${logs}`);
    }
    await new Promise((r) => setTimeout(r, 50));
  }
  return { child, base, dbFile, logs };
}

async function stopServer(child) {
  if (!child.killed) child.kill("SIGTERM");
  await new Promise((r) => child.once("exit", r));
}

function call(base, method, urlPath, body) {
  return new Promise((resolve, reject) => {
    const payload = body === undefined ? null : JSON.stringify(body);
    const req = http.request(
      `${base}${urlPath}`,
      {
        method,
        headers: payload
          ? { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) }
          : {}
      },
      (res) => {
        let raw = "";
        res.on("data", (d) => (raw += d));
        res.on("end", () => {
          let parsed;
          try {
            parsed = raw ? JSON.parse(raw) : null;
          } catch {
            parsed = raw;
          }
          resolve({ status: res.statusCode, body: parsed });
        });
      }
    );
    req.on("error", reject);
    if (payload) req.write(payload);
    req.end();
  });
}

/** 成功请求：返回 { status, data }（data 已解包）。 */
async function ok(base, method, urlPath, body, expected) {
  const res = await call(base, method, urlPath, body);
  const want = expected || (method === "POST" ? 201 : 200);
  assert.strictEqual(
    res.status,
    want,
    `${method} ${urlPath} 期望 ${want}，实际 ${res.status}：${JSON.stringify(res.body)}`
  );
  return { status: res.status, data: res.body.data };
}
/** 预期失败：返回完整响应体，便于核对 details。 */
async function fail(base, method, urlPath, body, expectedStatus) {
  const res = await call(base, method, urlPath, body);
  assert.strictEqual(
    res.status,
    expectedStatus,
    `${method} ${urlPath} 期望失败 ${expectedStatus}，实际 ${res.status}：${JSON.stringify(res.body)}`
  );
  return res.body;
}

const byId = (list) => Object.fromEntries(list.map((item) => [item.id, item]));
async function batches(base) {
  return byId((await ok(base, "GET", "/paper/batches?includeEmpty=true")).data);
}
async function batchByNo(base, batchNo) {
  return (await ok(base, "GET", "/paper/batches?includeEmpty=true")).data.find((b) => b.batchNo === batchNo);
}
function allocOf(req, batchNo) {
  const alloc = req.allocations.find((item) => item.batch.batchNo === batchNo);
  assert.ok(alloc, `领料单没有分摊到 ${batchNo}`);
  return alloc;
}

// ---------------------------------------------------------------------------

(async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "paper-integ-"));
  let server;
  let base;
  try {
    // ---------------- S0 旧数据迁移 + 旧接口兼容 ----------------
    await step("S0 旧数据迁移与旧接口兼容", async () => {
      // 用仓库里的老格式 db.json（只有 tunes/sections/issues）启动
      await copyFile(path.join(__dirname, "..", "data", "db.json"), path.join(dir, "db.json"));
      server = await startServer(dir, 3919);
      base = server.base;

      const health = await call(base, "GET", "/health");
      assert.strictEqual(health.status, 200);
      check("health 正常", health.body.ok === true);

      const progress = (await ok(base, "GET", "/tunes/tune_demo/progress")).data;
      check(
        "旧 progress 数字不变",
        progress.totalSections === 2 && progress.checkedSections === 1 && progress.openIssues === 1,
        progress
      );

      const issue = (
        await ok(base, "POST", "/issues", {
          tuneId: "tune_demo",
          sectionId: "section_demo_2",
          type: "错孔",
          beat: 45,
          lane: 9,
          description: "第45拍第9轨多打孔"
        })
      ).data;
      check("旧 POST /issues 可用", issue.id.startsWith("issue_"));
      const openIssues = (await ok(base, "GET", "/issues?status=open")).data;
      check("旧 GET /issues 过滤可用", openIssues.length === 2 && openIssues.every((i) => i.status === "open"));

      const checked = (await ok(base, "PATCH", "/sections/section_demo_2/check", { checked: true })).data;
      check("旧 PATCH /sections/:id/check 可用", checked.checked === true);

      const stock = (await ok(base, "GET", "/paper/stock")).data;
      check("迁移后库存为空表而不是报错", Array.isArray(stock) && stock.length === 0);

      const onDisk = JSON.parse(await readFile(server.dbFile, "utf8"));
      check(
        "老 db.json 原地补齐六张库存表且旧数据还在",
        ["paperBatches", "stockIns", "requisitions", "materialReturns", "writeOffs", "stockAdjustments"].every(
          (key) => Array.isArray(onDisk[key])
        ) &&
          onDisk.tunes.length === 1 &&
          onDisk.sections.length === 2 &&
          onDisk.issues.length === 2
      );
    });

    // ---------------- S1 跨批次领料 ----------------
    await step("S1 入库与跨批次领料（FIFO / 指定分摊）", async () => {
      const inbound = async (batchNo, length, receivedAt) =>
        (
          await ok(base, "POST", "/paper/batches", {
            paperType: PAPER,
            batchNo,
            length,
            supplier: "纸厂甲",
            receivedAt
          })
        ).data.batch;

      const b1 = await inbound("B2026-01", 10, "2026-09-01T00:00:00.000Z");
      const b2 = await inbound("B2026-02", 5, "2026-09-02T00:00:00.000Z");
      const b3 = await inbound("B2026-03", 5, "2026-09-03T00:00:00.000Z");
      check("入库登记纸型/批次/长度", approx(b1.availableLength, 10) && b1.paperType === PAPER);

      const reqsBefore = (await ok(base, "GET", "/requisitions")).data.length;

      // FIFO 跨批：B1 整批 10 + B2 出 2
      const rCross = (
        await ok(base, "POST", "/requisitions", {
          tuneId: "tune_demo",
          paperType: PAPER,
          length: 12,
          reason: "雨后圆舞曲全段试奏"
        })
      ).data;
      check("FIFO 领料单落库", rCross.status === "active" && rCross.allocations.length === 2, rCross);
      check("FIFO 先扣 B2026-01 整批 10", approx(allocOf(rCross, "B2026-01").length, 10));
      check("FIFO 不足部分跨到 B2026-02 扣 2", approx(allocOf(rCross, "B2026-02").length, 2));
      check(
        "领料单结余字段",
        approx(rCross.totalLength, 12) && approx(rCross.openLength, 12) && rCross.fullySettled === false
      );

      let bal = await batches(base);
      check("扣减后 B1 结余 0", approx(bal[b1.id].availableLength, 0), bal[b1.id]);
      check("扣减后 B2 结余 3", approx(bal[b2.id].availableLength, 3), bal[b2.id]);
      check("B3 未动用 结余 5", approx(bal[b3.id].availableLength, 5), bal[b3.id]);

      // 总量不足：剩余 8，申请 9
      const shortage = await fail(
        base,
        "POST",
        "/requisitions",
        { tuneId: "tune_demo", paperType: PAPER, length: 9 },
        409
      );
      check("库存不足 409 且带缺口信息", approx(shortage.details.shortageLength, 1), shortage);

      await fail(
        base,
        "POST",
        "/requisitions",
        { tuneId: "tune_not_exist", paperType: PAPER, length: 1 },
        404
      );
      check("领料必须挂真实曲目（404）", true);

      await fail(
        base,
        "POST",
        "/requisitions",
        { tuneId: "tune_demo", paperType: PAPER_OTHER, length: 3, allocations: [{ batchId: b2.id, length: 3 }] },
        400
      );
      check("指定批次纸型与领料纸型不一致 → 400", true);

      await fail(
        base,
        "POST",
        "/requisitions",
        { tuneId: "tune_demo", paperType: PAPER, length: 3, allocations: [{ batchId: b2.id, length: 2.5 }] },
        400
      );
      check("分摊合计与申请长度不符 → 400", true);

      await fail(
        base,
        "POST",
        "/requisitions",
        { tuneId: "tune_demo", paperType: PAPER, length: 4, allocations: [{ batchId: b2.id, length: 4 }] },
        409
      );
      check("指定批次超扣 → 409", true);

      // 指定分摊：B3 整批 5 + B2 剩余 3，正好 8
      const rExplicit = (
        await ok(base, "POST", "/requisitions", {
          tuneId: "tune_demo",
          paperType: PAPER,
          length: 8,
          allocations: [
            { batchId: b3.id, length: 5 },
            { batchId: b2.id, length: 3 }
          ]
        })
      ).data;
      check("跨批指定分摊成功", rExplicit.allocations.length === 2);
      bal = await batches(base);
      check("指定分摊后 B2/B3 全部为 0", approx(bal[b2.id].availableLength, 0) && approx(bal[b3.id].availableLength, 0));

      const reqsAfter = (await ok(base, "GET", "/requisitions")).data.length;
      check("失败的申请没有产生领料单", reqsAfter - reqsBefore === 2);
    });

    // ---------------- S2 并发不超卖 ----------------
    await step("S2 并发领料不超卖", async () => {
      const b4 = (
        await ok(base, "POST", "/paper/batches", {
          paperType: PAPER,
          batchNo: "B2026-04",
          length: 10,
          receivedAt: "2026-09-04T00:00:00.000Z"
        })
      ).data.batch;

      const fire = (n, length) =>
        Promise.all(
          Array.from({ length: n }, (_, i) =>
            call(base, "POST", "/requisitions", {
              tuneId: "tune_demo",
              paperType: PAPER,
              length,
              reason: `并发-${i}`
            })
          )
        );

      let results = await fire(10, 1);
      check("10 并发抢 10 米全部成功", results.every((r) => r.status === 201), results.map((r) => r.status));
      let bal = await batches(base);
      check("抢完 B4 结余恰为 0，没有超卖", approx(bal[b4.id].availableLength, 0), bal[b4.id]);

      results = await fire(10, 1);
      check("库存为 0 后 10 并发全部 409", results.every((r) => r.status === 409));

      const b5 = (
        await ok(base, "POST", "/paper/batches", {
          paperType: PAPER,
          batchNo: "B2026-05",
          length: 5,
          receivedAt: "2026-09-05T00:00:00.000Z"
        })
      ).data.batch;
      results = await fire(8, 1);
      const okCount = results.filter((r) => r.status === 201).length;
      const failCount = results.filter((r) => r.status === 409).length;
      check("8 并发抢 5 米：恰好 5 成 3 败", okCount === 5 && failCount === 3, { okCount, failCount });
      bal = await batches(base);
      check("B5 结余 0", approx(bal[b5.id].availableLength, 0));
    });

    // ---------------- S3 退料超量 ----------------
    await step("S3 退料必须回原批次且不能超量", async () => {
      const b6 = (
        await ok(base, "POST", "/paper/batches", {
          paperType: PAPER,
          batchNo: "B2026-06",
          length: 20,
          receivedAt: "2026-09-06T00:00:00.000Z"
        })
      ).data.batch;
      const b5 = await batchByNo(base, "B2026-05");

      const r3 = (
        await ok(base, "POST", "/requisitions", {
          tuneId: "tune_demo",
          paperType: PAPER,
          length: 8,
          reason: "S3 退料测试"
        })
      ).data;
      let bal = await batches(base);
      check("领料后 B6 结余 12", approx(bal[b6.id].availableLength, 12));

      const over = await fail(base, "POST", `/requisitions/${r3.id}/returns`, { length: 9 }, 409);
      check("退料超量 → 409 且返回可退量", approx(over.details.returnableLength, 8), over);

      await fail(base, "POST", `/requisitions/${r3.id}/returns`, { batchId: b5.id, length: 1 }, 400);
      check("退到不属于该单的批次 → 400（只能回原批次）", true);

      const ret1 = (
        await ok(base, "POST", `/requisitions/${r3.id}/returns`, { length: 3, note: "试奏余料" })
      ).data;
      check("退料记录回原批次 B6", ret1.return.batchNo === "B2026-06" && approx(ret1.return.length, 3));
      check("退料后批次结余回升到 15", approx(ret1.batch.availableLength, 15), ret1.batch);
      check(
        "领料单已退 3、未结 5",
        approx(ret1.requisition.returnedLength, 3) && approx(ret1.requisition.openLength, 5)
      );

      (await ok(base, "POST", `/requisitions/${r3.id}/returns`, { length: 3 })).data;
      bal = await batches(base);
      check("二次退料累计 6，结余 18", approx(bal[b6.id].availableLength, 18) && approx(bal[b6.id].returnedLength, 6));

      const woOver = await fail(
        base,
        "POST",
        `/requisitions/${r3.id}/write-offs`,
        { items: [{ batchId: b6.id, length: 3 }] },
        409
      );
      check("核销超量 → 409（只剩 2 可核）", approx(woOver.details.writableOffLength, 2), woOver);

      const wo = (
        await ok(base, "POST", `/requisitions/${r3.id}/write-offs`, {
          items: [{ batchId: b6.id, length: 2 }],
          note: "打孔正常消耗"
        })
      ).data;
      check("核销 2 后整张单结清", wo.requisition.fullySettled === true && approx(wo.requisition.openLength, 0));
      bal = await batches(base);
      check(
        "核销不回升结余：B6 结余仍 18，已核 2",
        approx(bal[b6.id].availableLength, 18) && approx(bal[b6.id].writtenOffLength, 2),
        bal[b6.id]
      );

      await fail(base, "POST", `/requisitions/${r3.id}/returns`, { length: 0.01 }, 409);
      check("结清后再退料 → 409", true);
      await fail(base, "POST", `/requisitions/${r3.id}/void`, { reason: "x" }, 409);
      check("全部退完/核完后作废 → 409（无半笔可回补）", true);
    });

    // ---------------- S4 作废重叠 ----------------
    await step("S4 作废只回补未退未核部分，重叠操作被拒", async () => {
      const b6 = await batchByNo(base, "B2026-06");

      const r4 = (
        await ok(base, "POST", "/requisitions", {
          tuneId: "tune_demo",
          paperType: PAPER,
          length: 8,
          reason: "S4 作废测试"
        })
      ).data;
      let bal = await batches(base);
      check("新领 8 后 B6 结余 10", approx(bal[b6.id].availableLength, 10), bal[b6.id]);

      (await ok(base, "POST", `/requisitions/${r4.id}/returns`, { length: 3, note: "先退 3" })).data;
      (
        await ok(base, "POST", `/requisitions/${r4.id}/write-offs`, {
          items: [{ batchId: b6.id, length: 2 }],
          note: "再核 2"
        })
      ).data;
      bal = await batches(base);
      check("退 3 后结余回升到 13，核 2 不影响结余", approx(bal[b6.id].availableLength, 13), bal[b6.id]);

      const voided = (
        await ok(base, "POST", `/requisitions/${r4.id}/void`, { reason: "曲目取消，余料退回" }, 200)
      ).data;
      check("作废单状态为 voided", voided.requisition.status === "voided");
      check(
        "作废只回补未退未核的 3 米",
        voided.restorations.length === 1 && approx(voided.restorations[0].length, 3),
        voided.restorations
      );
      check(
        "回补审计：已退6(实退3+回补3)，已核2保留，未结0",
        approx(voided.requisition.voidRestoredLength, 3) &&
          approx(voided.requisition.returnedLength, 6) &&
          approx(voided.requisition.writtenOffLength, 2) &&
          approx(voided.requisition.openLength, 0),
        voided.requisition
      );
      bal = await batches(base);
      check(
        "B6：初始20 = 已核4占用 + 可用16；累计退回12",
        approx(bal[b6.id].availableLength, 16) &&
          approx(bal[b6.id].writtenOffLength, 4) &&
          approx(bal[b6.id].heldLength, 4) &&
          approx(bal[b6.id].returnedLength, 12),
        bal[b6.id]
      );

      await fail(base, "POST", `/requisitions/${r4.id}/void`, { reason: "再作废" }, 409);
      await fail(base, "POST", `/requisitions/${r4.id}/returns`, { length: 1 }, 409);
      await fail(base, "POST", `/requisitions/${r4.id}/write-offs`, {}, 409);
      check("重复作废 / 作废后再退 / 作废后再核 全部 409（不重复回补）", true);
    });

    // ---------------- S5 盘点调整 ----------------
    await step("S5 盘点调整必须写原因，不能调出负库存", async () => {
      const b6 = await batchByNo(base, "B2026-06");

      await fail(base, "POST", "/stock-adjustments", { batchId: b6.id, actualLength: 1, reason: "" }, 400);
      await fail(base, "POST", "/stock-adjustments", { batchId: b6.id, actualLength: 1 }, 400);
      check("原因缺失/为空 → 400", true);
      await fail(base, "POST", "/stock-adjustments", { batchId: "batch_nope", actualLength: 1, reason: "盘" }, 404);
      check("调整不存在批次 → 404", true);
      await fail(base, "POST", "/stock-adjustments", { batchId: b6.id, actualLength: -1, reason: "盘亏" }, 400);
      check("盘点实际长度为负 → 400", true);
      await fail(base, "POST", "/stock-adjustments", { batchId: b6.id, actualLength: 16, reason: "无差异" }, 400);
      check("账实相符无需调整 → 400", true);

      const loss = (
        await ok(base, "POST", "/stock-adjustments", {
          batchId: b6.id,
          actualLength: 14.5,
          reason: "盘点发现边沿破损 1.5 米"
        })
      ).data;
      check("盘亏 -1.5 落账", approx(loss.adjustment.deltaLength, -1.5) && approx(loss.batch.availableLength, 14.5), loss);

      const gain = (
        await ok(base, "POST", "/stock-adjustments", {
          batchId: b6.id,
          actualLength: 15,
          reason: "上次受潮测量偏短，复测盘盈 0.5 米"
        })
      ).data;
      check("盘盈 +0.5 落账", approx(gain.adjustment.deltaLength, 0.5) && approx(gain.batch.availableLength, 15), gain);

      const adjustments = (await ok(base, "GET", `/stock-adjustments?batchId=${b6.id}`)).data;
      check("调整记录可查且带原因", adjustments.length === 2 && adjustments.every((a) => a.reason.length > 0));
    });

    // ---------------- S6 失败不留半笔账 ----------------
    await step("S6 任一步失败不留半笔账", async () => {
      const onDiskBefore = JSON.parse(await readFile(server.dbFile, "utf8"));
      const before = {
        requisitions: onDiskBefore.requisitions.length,
        returns: onDiskBefore.materialReturns.length,
        writeOffs: onDiskBefore.writeOffs.length,
        batches: onDiskBefore.paperBatches.length,
        adjustments: onDiskBefore.stockAdjustments.length
      };
      const balBefore = await batches(base);

      await fail(base, "POST", "/requisitions", { tuneId: "tune_nope", paperType: PAPER, length: 1 }, 404);
      // 分摊合计(2) ≠ 申请长度(1)：挑有余量的批次，确保先命中“合计不符”而非“超扣”
      const stockedBatch = Object.values(balBefore).find((b) => b.availableLength >= 2);
      assert.ok(stockedBatch, "需要一个余量≥2的批次来验证分摊合计校验");
      await fail(
        base,
        "POST",
        "/requisitions",
        { tuneId: "tune_demo", paperType: PAPER, length: 1, allocations: [{ batchId: stockedBatch.id, length: 2 }] },
        400
      );
      await fail(base, "POST", "/requisitions", { tuneId: "tune_demo", paperType: PAPER, length: 999 }, 409);
      const anyReq = (await ok(base, "GET", "/requisitions?status=active")).data[0];
      assert.ok(anyReq, "至少要有一张活动单用于验证退/核失败");
      await fail(base, "POST", `/requisitions/${anyReq.id}/returns`, { length: 9999 }, 409);
      await fail(
        base,
        "POST",
        `/requisitions/${anyReq.id}/write-offs`,
        { items: [{ batchId: anyReq.allocations[0].batchId, length: 9999 }] },
        409
      );
      await fail(
        base,
        "POST",
        "/stock-adjustments",
        { batchId: stockedBatch.id, actualLength: 0, reason: "" },
        400
      );

      const onDiskAfter = JSON.parse(await readFile(server.dbFile, "utf8"));
      check(
        "失败后各类记录数全部不变",
        onDiskAfter.requisitions.length === before.requisitions &&
          onDiskAfter.materialReturns.length === before.returns &&
          onDiskAfter.writeOffs.length === before.writeOffs &&
          onDiskAfter.paperBatches.length === before.batches &&
          onDiskAfter.stockAdjustments.length === before.adjustments,
        {
          before,
          after: [
            onDiskAfter.requisitions.length,
            onDiskAfter.materialReturns.length,
            onDiskAfter.writeOffs.length,
            onDiskAfter.paperBatches.length,
            onDiskAfter.stockAdjustments.length
          ]
        }
      );
      const balAfter = await batches(base);
      check(
        "失败后每个批次结余纹丝不动",
        Object.keys(balBefore).every((id) => approx(balBefore[id].availableLength, balAfter[id].availableLength))
      );
    });

    // ---------------- S8 入参反例（批次号跨类型判重 / 空请求体 / 非数字长度） ----------------
    await step("S8 入参反例：明确 400 且不落任何记录", async () => {
      const counts = async () => {
        const d = JSON.parse(await readFile(server.dbFile, "utf8"));
        return {
          batches: d.paperBatches.length,
          stockIns: d.stockIns.length,
          requisitions: d.requisitions.length,
          returns: d.materialReturns.length,
          writeOffs: d.writeOffs.length,
          adjustments: d.stockAdjustments.length
        };
      };
      const before = await counts();
      const balBefore = await batches(base);

      // 8a 批次号跨类型判重：先字符串入库，再用数字、再用字符串都必须 409
      const created = (
        await ok(base, "POST", "/paper/batches", {
          paperType: PAPER,
          batchNo: "9001",
          length: 4,
          receivedAt: "2026-09-08T00:00:00.000Z"
        })
      ).data.batch;
      check("字符串批次号 9001 入库成功", created.batchNo === "9001");
      await fail(
        base,
        "POST",
        "/paper/batches",
        { paperType: PAPER, batchNo: 9001, length: 4 },
        409
      );
      check("数字型批次号 9001 重复入库 → 409", true);
      await fail(
        base,
        "POST",
        "/paper/batches",
        { paperType: PAPER, batchNo: "9001", length: 4 },
        409
      );
      check("字符串批次号 9001 再次重复入库 → 409", true);
      const dupList = (await ok(base, "GET", "/paper/batches?includeEmpty=true")).data.filter(
        (b) => b.batchNo === "9001"
      );
      check("库存中同号批次只有一个，长度 4 不翻倍", dupList.length === 1 && approx(dupList[0].initialLength, 4), dupList);

      // 8b 空值/非对象请求体必须 400，不能 500
      for (const bad of [undefined, null, [], "oops", 42, true]) {
        const res = await call(base, "POST", "/requisitions", bad);
        assert.strictEqual(res.status, 400, `领料 body=${JSON.stringify(bad)} 期望400，实际${res.status}：${JSON.stringify(res.body)}`);
      }
      check("领料：无体/null/数组/字符串/数字/布尔 全部 400", true);
      const resNullInbound = await call(base, "POST", "/paper/batches", null);
      assert.strictEqual(resNullInbound.status, 400);
      check("入库空体 400 而不是 500", true);

      // 8c 分摊数组混入空值/非法值
      await fail(
        base,
        "POST",
        "/requisitions",
        { tuneId: "tune_demo", paperType: PAPER, length: 1, allocations: "x" },
        400
      );
      check("allocations 不是数组 → 400", true);
      for (const badItems of [
        [null],
        [undefined],
        ["x"],
        [{}],
        [{ batchId: created.id }],
        [{ batchId: created.id, length: true }],
        [{ batchId: 123, length: 1 }]
      ]) {
        const res = await call(base, "POST", "/requisitions", {
          tuneId: "tune_demo",
          paperType: PAPER,
          length: 1,
          allocations: badItems
        });
        assert.strictEqual(
          res.status,
          400,
          `分摊 ${JSON.stringify(badItems)} 期望400，实际${res.status}：${JSON.stringify(res.body)}`
        );
      }
      check("分摊项为 null/缺字段/布尔长度/非字符串batchId 全部 400", true);

      // 8d 布尔等非数字长度一律拒绝（入库/领料/退料/核销/盘点）
      for (const badLen of [true, false, "3", null, []]) {
        const res = await call(base, "POST", "/paper/batches", {
          paperType: PAPER_OTHER,
          batchNo: `X-${String(badLen)}`,
          length: badLen
        });
        assert.strictEqual(res.status, 400, `入库 length=${JSON.stringify(badLen)} 期望400，实际${res.status}`);
      }
      check("入库长度为布尔/字符串/null/数组 → 400", true);
      await fail(base, "POST", "/requisitions", { tuneId: "tune_demo", paperType: PAPER, length: true }, 400);
      check("领料长度为布尔真值 → 400（不会被当成 1 米）", true);
      await fail(base, "POST", "/requisitions", { tuneId: "tune_demo", paperType: PAPER, length: "2" }, 400);
      check("领料长度为数字字符串 → 400", true);

      // 先正常领一张单（显式分摊到 9001 批次），再拿它打退料/核销反例——坏请求不能动这张单
      const guardReq = (
        await ok(base, "POST", "/requisitions", {
          tuneId: "tune_demo",
          paperType: PAPER,
          length: 2,
          reason: "S8 护栏单",
          allocations: [{ batchId: created.id, length: 2 }]
        })
      ).data;
      const guardBatchId = guardReq.allocations[0].batchId;
      await fail(base, "POST", `/requisitions/${guardReq.id}/returns`, { length: true }, 400);
      await fail(base, "POST", `/requisitions/${guardReq.id}/returns`, { length: "1" }, 400);
      await fail(base, "POST", `/requisitions/${guardReq.id}/returns`, null, 400);
      check("退料长度为布尔/字符串/空体 → 400", true);
      await fail(
        base,
        "POST",
        `/requisitions/${guardReq.id}/write-offs`,
        { items: [{ batchId: guardBatchId, length: true }] },
        400
      );
      await fail(base, "POST", `/requisitions/${guardReq.id}/write-offs`, { items: [null] }, 400);
      await fail(base, "POST", `/requisitions/${guardReq.id}/write-offs`, { items: "x" }, 400);
      check("核销长度布尔/空项/非数组 items → 400", true);
      await fail(
        base,
        "POST",
        "/stock-adjustments",
        { batchId: guardBatchId, actualLength: true, reason: "布尔实测" },
        400
      );
      check("盘点实际长度为布尔 → 400", true);

      // 反例打完后：记录数只多了 1 个批次 + 1 张护栏领料单，其余不变；护栏单原封不动
      const after = await counts();
      check(
        "反例没有写入多余批次/入库/退料/核销/盘点",
        after.batches === before.batches + 1 &&
          after.stockIns === before.stockIns + 1 &&
          after.returns === before.returns &&
          after.writeOffs === before.writeOffs &&
          after.adjustments === before.adjustments,
        { before, after }
      );
      check("反例没有写入多余领料单", after.requisitions === before.requisitions + 1, { before, after });

      const guardAfter = (await ok(base, "GET", `/requisitions/${guardReq.id}`)).data;
      check(
        "护栏单没被坏退料/坏核销动过：仍 active、2 米未结",
        guardAfter.status === "active" &&
          approx(guardAfter.returnedLength, 0) &&
          approx(guardAfter.writtenOffLength, 0) &&
          approx(guardAfter.openLength, 2),
        guardAfter
      );
      const balAfter = await batches(base);
      check(
        "坏请求后全部批次结余不变（9001 批次除外，它被护栏单显式扣过）",
        Object.keys(balBefore).every((id) => approx(balBefore[id].availableLength, balAfter[id].availableLength))
      );
      const b9001 = balAfter[created.id];
      check(
        "9001 批次只被 2 米护栏单扣减：可用 2",
        approx(b9001.initialLength, 4) && approx(b9001.availableLength, 2) && approx(b9001.issuedLength, 2),
        b9001
      );
    });

    // ---------------- S7 SIGKILL 重启恢复 + tmp 清理 ----------------
    await step("S7 杀进程重启后账目一致，残留 tmp 被清理", async () => {
      const stockBefore = (await ok(base, "GET", "/paper/stock")).data;
      const reqsBefore = (await ok(base, "GET", "/requisitions")).data;
      const balBefore = await batches(base);

      // 模拟“写入中途崩溃”留下的残留临时文件
      await writeFile(`${server.dbFile}.99999.777.tmp`, "{ this looks broken");

      server.child.kill("SIGKILL");
      await new Promise((r) => server.child.once("exit", r));
      server = await startServer(dir, 3919);
      base = server.base;

      const stockAfter = (await ok(base, "GET", "/paper/stock")).data;
      const reqsAfter = (await ok(base, "GET", "/requisitions")).data;
      check("重启后纸型汇总完全一致", JSON.stringify(stockAfter) === JSON.stringify(stockBefore));
      check("重启后领料单（含退/核/作废状态）完全一致", JSON.stringify(reqsAfter) === JSON.stringify(reqsBefore));
      const balAfter = await batches(base);
      check(
        "重启后每批结余完全一致",
        Object.keys(balBefore).every((id) => JSON.stringify(balBefore[id]) === JSON.stringify(balAfter[id]))
      );

      const files = await readdir(dir);
      check("残留 tmp 已在启动时清理", files.every((name) => !name.endsWith(".tmp")), files);
      check("主数据文件仍是合法 JSON", !!JSON.parse(await readFile(server.dbFile, "utf8")));
    });

    // ---------------- 全局账实恒等式核对 ----------------
    await step("全局账实恒等式核对", async () => {
      const all = (await ok(base, "GET", "/paper/batches?includeEmpty=true")).data;
      for (const b of all) {
        check(`[${b.batchNo}] 初始 = 占用 + 可用`, approx(b.initialLength, b.heldLength + b.availableLength), {
          initialLength: b.initialLength,
          heldLength: b.heldLength,
          availableLength: b.availableLength
        });
        check(`[${b.batchNo}] 可用量不为负`, b.availableLength >= -EPS);
        check(`[${b.batchNo}] 已摊 = 已退 + 占用`, approx(b.issuedLength, b.returnedLength + b.heldLength), {
          issuedLength: b.issuedLength,
          returnedLength: b.returnedLength,
          heldLength: b.heldLength
        });
      }

      const onDisk = JSON.parse(await readFile(server.dbFile, "utf8"));
      const reqs = (await ok(base, "GET", "/requisitions")).data;
      for (const req of reqs) {
        const sumAlloc = req.allocations.reduce((s, a) => s + a.length, 0);
        check(`[${req.id}] 分摊合计 = 单据总量`, approx(sumAlloc, req.totalLength));
        const actualReturns = onDisk.materialReturns
          .filter((r) => r.requisitionId === req.id)
          .reduce((s, r) => s + r.length, 0);
        const actualWriteOffs = onDisk.writeOffs
          .filter((w) => w.requisitionId === req.id)
          .reduce((s, w) => s + w.length, 0);
        check(
          `[${req.id}] 核销流水合计与单据已核一致`,
          approx(actualWriteOffs, req.writtenOffLength),
          { actualWriteOffs, writtenOffLength: req.writtenOffLength }
        );
        if (req.status === "voided") {
          const restored = (req.restorations || []).reduce((s, r) => s + r.length, 0);
          check(`[${req.id}] 作废单无未结量`, approx(req.openLength, 0));
          check(
            `[${req.id}] 已退(含作废回补) = 真实退料流水 + 作废回补`,
            approx(req.returnedLength, actualReturns + restored) && approx(restored, req.voidRestoredLength),
            { returnedLength: req.returnedLength, actualReturns, restored }
          );
          check(`[${req.id}] 总量 = 已退(含回补) + 已核`, approx(req.totalLength, req.returnedLength + req.writtenOffLength));
        } else {
          check(
            `[${req.id}] 活动单：总量 = 已退 + 已核 + 未结`,
            approx(req.totalLength, req.returnedLength + req.writtenOffLength + req.openLength),
            {
              totalLength: req.totalLength,
              returnedLength: req.returnedLength,
              writtenOffLength: req.writtenOffLength,
              openLength: req.openLength
            }
          );
          check(`[${req.id}] 退料流水合计与单据已退一致`, approx(actualReturns, req.returnedLength), {
            actualReturns,
            returnedLength: req.returnedLength
          });
        }
      }

      check(
        "入库流水与批次一一对应（纸型/批次号/长度）",
        onDisk.stockIns.length === onDisk.paperBatches.length &&
          onDisk.stockIns.every((si) => {
            const b = onDisk.paperBatches.find((x) => x.id === si.batchId);
            return b && b.batchNo === si.batchNo && b.paperType === si.paperType && approx(si.length, b.initialLength -
              onDisk.stockAdjustments.filter((a) => a.batchId === b.id).reduce((s, a) => s + a.deltaLength, 0));
          })
      );

      const stockRows = (await ok(base, "GET", "/paper/stock")).data;
      for (const row of stockRows) {
        const members = all.filter((b) => b.paperType === row.paperType);
        check(
          `[纸型 ${row.paperType}] 汇总可用 = 各批可用之和`,
          approx(row.availableLength, members.reduce((s, b) => s + b.availableLength, 0)),
          { row, sum: members.reduce((s, b) => s + b.availableLength, 0) }
        );
      }

      console.log(`\n全部通过：${passed} 项断言`);
    });
  } catch (error) {
    console.error("\n联调失败：", error);
    if (server) console.error("服务日志：\n" + server.logs);
    process.exitCode = 1;
  } finally {
    if (server && server.child && !server.child.killed) await stopServer(server.child);
    await rm(dir, { recursive: true, force: true });
  }
})();
