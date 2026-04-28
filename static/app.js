const metricKeys = ["limit", "obligation", "cash", "agreement", "contract", "payment", "buau"];
const metricLabels = {
  limit: "Лимиты",
  obligation: "БО",
  cash: "Касса",
  agreement: "Соглашения",
  contract: "Контракты",
  payment: "Платежи",
  buau: "БУ/АУ",
};

let currentRows = [];
let currentDetails = [];
let currentCompareRows = [];
let currentMode = "slice";
let templatesByCode = new Map();

const els = {
  q: document.querySelector("#q"),
  code: document.querySelector("#code"),
  template: document.querySelector("#template"),
  budget: document.querySelector("#budget"),
  source: document.querySelector("#source"),
  start: document.querySelector("#start"),
  end: document.querySelector("#end"),
  baseDate: document.querySelector("#baseDate"),
  targetDate: document.querySelector("#targetDate"),
  reset: document.querySelector("#reset"),
  exportCsv: document.querySelector("#exportCsv"),
  metricFilter: document.querySelector("#metricFilter"),
  kpis: document.querySelector("#kpis"),
  summaryHead: document.querySelector("#summaryHead"),
  summaryBody: document.querySelector("#summaryBody"),
  compareHead: document.querySelector("#compareHead"),
  compareBody: document.querySelector("#compareBody"),
  detailBody: document.querySelector("#detailBody"),
  rowCount: document.querySelector("#rowCount"),
  compareCount: document.querySelector("#compareCount"),
  detailCount: document.querySelector("#detailCount"),
  recordCount: document.querySelector("#recordCount"),
  resultTitle: document.querySelector("#resultTitle"),
  loadStatus: document.querySelector("#loadStatus"),
  qualityStatus: document.querySelector("#qualityStatus"),
  chart: document.querySelector("#chart"),
  sliceMode: document.querySelector("#sliceMode"),
  compareMode: document.querySelector("#compareMode"),
  traceDialog: document.querySelector("#traceDialog"),
  traceBody: document.querySelector("#traceBody"),
  closeTrace: document.querySelector("#closeTrace"),
};

const money = new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 0 });

function formatMoney(value) {
  return money.format(Math.round(Number(value || 0)));
}

function fillSelect(select, values, label) {
  select.innerHTML = "";
  const all = document.createElement("option");
  all.value = "";
  all.textContent = label;
  select.appendChild(all);
  values.forEach((value) => {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = value;
    select.appendChild(option);
  });
}

function selectedMetricKeys() {
  const checked = [...els.metricFilter.querySelectorAll("input:checked")].map((input) => input.value);
  return checked.length ? checked : metricKeys;
}

function filterParams(includeDates = true) {
  const params = new URLSearchParams();
  ["q", "code", "budget", "source", "template"].forEach((key) => {
    const value = els[key].value.trim();
    if (value) params.set(key, value);
  });
  if (includeDates) {
    if (els.start.value) params.set("start", els.start.value);
    if (els.end.value) params.set("end", els.end.value);
  }
  params.set("metrics", selectedMetricKeys().join(","));
  return params;
}

async function loadMeta() {
  const [meta, templates, metrics, quality] = await Promise.all([
    fetch("/api/meta").then((r) => r.json()),
    fetch("/api/catalog/templates").then((r) => r.json()),
    fetch("/api/catalog/metrics").then((r) => r.json()),
    fetch("/api/quality").then((r) => r.json()),
  ]);
  fillSelect(els.budget, meta.budgets, "Все бюджеты");
  fillSelect(els.source, meta.sources, "Все источники");
  fillSelect(els.baseDate, meta.snapshots, "Дата базы");
  fillSelect(els.targetDate, meta.snapshots, "Дата цели");
  els.template.innerHTML = "";
  templatesByCode = new Map(templates.map((item) => [item.code, item]));
  templates.forEach((item) => {
    const option = document.createElement("option");
    option.value = item.code;
    option.textContent = item.label;
    els.template.appendChild(option);
  });
  renderMetricFilter(metrics);
  if (meta.snapshots.length) {
    els.start.value = meta.snapshots[0];
    els.end.value = meta.snapshots[meta.snapshots.length - 1];
    els.baseDate.value = meta.snapshots[0];
    els.targetDate.value = meta.snapshots[meta.snapshots.length - 1];
  }
  els.loadStatus.textContent = `Загружено ${meta.records} записей, источников: ${meta.sources.length}`;
  els.qualityStatus.textContent = `Предупреждения: ${quality.summary.warnings}, ошибки: ${quality.summary.errors}`;
}

function renderMetricFilter(metrics) {
  els.metricFilter.innerHTML = "";
  metrics.forEach((metric) => {
    const label = document.createElement("label");
    label.className = "check";
    label.innerHTML = `<input type="checkbox" value="${escapeHtml(metric.code)}" checked /> <span>${escapeHtml(metric.label)}</span>`;
    els.metricFilter.appendChild(label);
  });
}

async function loadData() {
  if (currentMode === "compare") {
    await loadCompare();
    return;
  }
  const data = await fetch(`/api/query?${filterParams(true).toString()}`).then((r) => r.json());
  currentRows = data.rows;
  currentDetails = data.details;
  currentCompareRows = [];
  const metrics = selectedMetricKeys();
  renderKpis(data.totals, metrics);
  renderSummary(data.rows, metrics);
  renderDetails(data.details, metrics);
  drawChart(data.timeline, metrics);
  const template = templatesByCode.get(els.template.value || "all");
  els.resultTitle.textContent = `Динамика: ${template ? template.label : "Все данные"}`;
  els.recordCount.textContent = `${data.count} записей в выборке`;
}

async function loadCompare() {
  const params = filterParams(false);
  if (els.baseDate.value) params.set("base", els.baseDate.value);
  if (els.targetDate.value) params.set("target", els.targetDate.value);
  const data = await fetch(`/api/compare?${params.toString()}`).then((r) => r.json());
  currentCompareRows = data.rows;
  currentRows = [];
  renderCompare(data.rows, data.metrics);
  renderDetails([], data.metrics);
  renderKpis({}, data.metrics);
  els.compareCount.textContent = `${data.rows.length} строк`;
}

function renderKpis(totals, metrics) {
  els.kpis.innerHTML = "";
  metrics.forEach((metric) => {
    const article = document.createElement("article");
    article.innerHTML = `<span>${escapeHtml(metricLabels[metric] || metric)}</span><strong>${formatMoney(totals[metric])}</strong>`;
    els.kpis.appendChild(article);
  });
}

function renderSummary(rows, metrics) {
  els.summaryHead.innerHTML = `<tr>
    <th>Код</th><th>Объект</th><th>Бюджет</th><th>Источники</th>
    ${metrics.map((metric) => `<th class="number">${escapeHtml(metricLabels[metric])}</th>`).join("")}
  </tr>`;
  els.summaryBody.innerHTML = "";
  els.rowCount.textContent = `${rows.length} строк`;
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHtml(row.object_code)}</td>
      <td class="truncate" title="${escapeHtml(row.object_name)}">${escapeHtml(row.object_name)}</td>
      <td class="truncate" title="${escapeHtml(row.budget)}">${escapeHtml(row.budget)}</td>
      <td><span class="source-pill">${escapeHtml(row.sources)}</span></td>
      ${metrics.map((metric) => `<td class="number">${formatMoney(row[metric])}</td>`).join("")}
    `;
    els.summaryBody.appendChild(tr);
  });
}

function renderCompare(rows, metrics) {
  els.compareHead.innerHTML = `<tr>
    <th>Код</th><th>Объект</th><th>Бюджет</th>
    ${metrics.map((metric) => `<th class="number">${escapeHtml(metricLabels[metric])}: база</th><th class="number">${escapeHtml(metricLabels[metric])}: цель</th><th class="number">Δ</th>`).join("")}
  </tr>`;
  els.compareBody.innerHTML = "";
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHtml(row.object_code)}</td>
      <td class="truncate" title="${escapeHtml(row.object_name)}">${escapeHtml(row.object_name)}</td>
      <td class="truncate" title="${escapeHtml(row.budget)}">${escapeHtml(row.budget)}</td>
      ${metrics.map((metric) => {
        const values = row.metrics[metric] || {};
        return `<td class="number">${formatMoney(values.base)}</td><td class="number">${formatMoney(values.target)}</td><td class="number delta">${formatMoney(values.delta)}</td>`;
      }).join("")}
    `;
    els.compareBody.appendChild(tr);
  });
}

function renderDetails(rows, metrics) {
  els.detailBody.innerHTML = "";
  els.detailCount.textContent = `${rows.length} записей`;
  rows.forEach((row) => {
    const amount = metrics.reduce((sum, key) => sum + Number(row[key] || 0), 0);
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHtml(row.event_date || row.snapshot)}</td>
      <td>${escapeHtml(row.source)}</td>
      <td>${escapeHtml(row.object_code)}</td>
      <td class="truncate" title="${escapeHtml(row.object_name)}">${escapeHtml(row.object_name)}</td>
      <td>${escapeHtml(row.document_number)}</td>
      <td class="truncate" title="${escapeHtml(row.counterparty)}">${escapeHtml(row.counterparty)}</td>
      <td class="number">${formatMoney(amount)}</td>
      <td><button class="trace-btn" type="button" data-id="${escapeHtml(row.id)}">↗</button></td>
    `;
    els.detailBody.appendChild(tr);
  });
}

function drawChart(timeline, metrics) {
  const canvas = els.chart;
  const ratio = window.devicePixelRatio || 1;
  const width = canvas.clientWidth;
  const height = canvas.clientHeight || 260;
  canvas.width = width * ratio;
  canvas.height = height * ratio;
  const ctx = canvas.getContext("2d");
  ctx.scale(ratio, ratio);
  ctx.clearRect(0, 0, width, height);

  const pad = { left: 68, right: 20, top: 18, bottom: 36 };
  const points = timeline.map((row) => ({
    date: row.date,
    value: metrics.reduce((sum, metric) => sum + Number(row[metric] || 0), 0),
  }));
  const max = Math.max(1, ...points.map((p) => p.value));

  ctx.strokeStyle = "#d9e0e4";
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(pad.left, pad.top);
  ctx.lineTo(pad.left, height - pad.bottom);
  ctx.lineTo(width - pad.right, height - pad.bottom);
  ctx.stroke();

  [0, 0.5, 1].forEach((tick) => {
    const y = height - pad.bottom - tick * (height - pad.top - pad.bottom);
    ctx.fillStyle = "#63717b";
    ctx.font = "12px Arial";
    ctx.fillText(formatMoney(max * tick), 8, y + 4);
    ctx.strokeStyle = "#edf2f5";
    ctx.beginPath();
    ctx.moveTo(pad.left, y);
    ctx.lineTo(width - pad.right, y);
    ctx.stroke();
  });

  drawLine(ctx, points, "#0f766e", pad, width, height, max);
  ctx.fillStyle = "#41505a";
  ctx.font = "12px Arial";
  ctx.fillText("Выбранные метрики", Math.max(80, width - 190), 16);
  if (points.length) {
    ctx.fillStyle = "#63717b";
    ctx.fillText(points[0].date, pad.left, height - 12);
    ctx.fillText(points[points.length - 1].date, width - pad.right - 74, height - 12);
  }
}

function drawLine(ctx, points, color, pad, width, height, max) {
  if (!points.length) return;
  const chartWidth = width - pad.left - pad.right;
  const chartHeight = height - pad.top - pad.bottom;
  ctx.strokeStyle = color;
  ctx.lineWidth = 2;
  ctx.beginPath();
  points.forEach((point, index) => {
    const x = pad.left + (points.length === 1 ? 0 : (index / (points.length - 1)) * chartWidth);
    const y = height - pad.bottom - (point.value / max) * chartHeight;
    if (index === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();
}

async function showTrace(id) {
  const trace = await fetch(`/api/trace?id=${encodeURIComponent(id)}`).then((r) => r.json());
  els.traceBody.textContent = JSON.stringify(trace, null, 2);
  els.traceDialog.showModal();
}

function exportCsv() {
  const metrics = selectedMetricKeys();
  const header = ["Дата формирования", new Date().toISOString(), "Шаблон", els.template.options[els.template.selectedIndex]?.textContent || ""];
  const tableHeader = ["Код", "Объект", "Бюджет", "Источники", ...metrics.map((metric) => metricLabels[metric] || metric)];
  const lines = [header, ["Метрики", metrics.join(",")], [], tableHeader];
  currentRows.forEach((row) => {
    lines.push([row.object_code, row.object_name, row.budget, row.sources, ...metrics.map((metric) => row[metric])]);
  });
  const csv = lines.map((line) => line.map(csvCell).join(";")).join("\n");
  const blob = new Blob(["\ufeff", csv], { type: "text/csv;charset=utf-8" });
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = "analytics_selection.csv";
  link.click();
  URL.revokeObjectURL(link.href);
}

function setMode(mode) {
  currentMode = mode;
  els.sliceMode.classList.toggle("active", mode === "slice");
  els.compareMode.classList.toggle("active", mode === "compare");
  document.querySelectorAll(".slice-only,.slice-panel").forEach((node) => node.classList.toggle("hidden", mode !== "slice"));
  document.querySelectorAll(".compare-only,.compare-panel").forEach((node) => node.classList.toggle("hidden", mode !== "compare"));
  loadData();
}

function csvCell(value) {
  return `"${String(value ?? "").replaceAll('"', '""')}"`;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function debounce(fn, delay = 250) {
  let timer;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), delay);
  };
}

const debouncedLoad = debounce(loadData);
["q", "code", "template", "budget", "source", "start", "end", "baseDate", "targetDate"].forEach((key) => {
  els[key].addEventListener(key === "q" || key === "code" ? "input" : "change", debouncedLoad);
});
els.metricFilter.addEventListener("change", debouncedLoad);
els.reset.addEventListener("click", async () => {
  ["q", "code", "budget", "source"].forEach((key) => {
    els[key].value = "";
  });
  els.template.value = "all";
  await loadData();
});
els.exportCsv.addEventListener("click", exportCsv);
els.sliceMode.addEventListener("click", () => setMode("slice"));
els.compareMode.addEventListener("click", () => setMode("compare"));
els.closeTrace.addEventListener("click", () => els.traceDialog.close());
els.detailBody.addEventListener("click", (event) => {
  const button = event.target.closest(".trace-btn");
  if (button) showTrace(button.dataset.id);
});
window.addEventListener("resize", debounce(loadData, 400));

loadMeta().then(loadData);
