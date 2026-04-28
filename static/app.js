const metricKeys = ["limit", "obligation", "cash", "agreement", "contract", "payment", "buau"];
let currentRows = [];

const els = {
  q: document.querySelector("#q"),
  code: document.querySelector("#code"),
  budget: document.querySelector("#budget"),
  source: document.querySelector("#source"),
  start: document.querySelector("#start"),
  end: document.querySelector("#end"),
  reset: document.querySelector("#reset"),
  exportCsv: document.querySelector("#exportCsv"),
  summaryBody: document.querySelector("#summaryBody"),
  detailBody: document.querySelector("#detailBody"),
  rowCount: document.querySelector("#rowCount"),
  detailCount: document.querySelector("#detailCount"),
  recordCount: document.querySelector("#recordCount"),
  chart: document.querySelector("#chart"),
};

const money = new Intl.NumberFormat("ru-RU", {
  maximumFractionDigits: 0,
});

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

function queryString() {
  const params = new URLSearchParams();
  ["q", "code", "budget", "source", "start", "end"].forEach((key) => {
    const value = els[key].value.trim();
    if (value) params.set(key, value);
  });
  return params.toString();
}

async function loadMeta() {
  const meta = await fetch("/api/meta").then((r) => r.json());
  fillSelect(els.budget, meta.budgets, "Все бюджеты");
  fillSelect(els.source, meta.sources, "Все источники");
  if (meta.snapshots.length) {
    els.start.value = meta.snapshots[0];
    els.end.value = meta.snapshots[meta.snapshots.length - 1];
  }
}

async function loadData() {
  const data = await fetch(`/api/query?${queryString()}`).then((r) => r.json());
  currentRows = data.rows;
  renderKpis(data.totals);
  renderSummary(data.rows);
  renderDetails(data.details);
  drawChart(data.timeline);
  els.recordCount.textContent = `${data.count} записей в выборке`;
}

function renderKpis(totals) {
  document.querySelector("#limit").textContent = formatMoney(totals.limit);
  document.querySelector("#obligation").textContent = formatMoney(totals.obligation);
  document.querySelector("#cash").textContent = formatMoney(totals.cash);
  document.querySelector("#agreement").textContent = formatMoney(totals.agreement);
  document.querySelector("#procurement").textContent = formatMoney((totals.contract || 0) + (totals.payment || 0));
  document.querySelector("#buau").textContent = formatMoney(totals.buau);
}

function renderSummary(rows) {
  els.summaryBody.innerHTML = "";
  els.rowCount.textContent = `${rows.length} строк`;
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHtml(row.object_code)}</td>
      <td class="truncate" title="${escapeHtml(row.object_name)}">${escapeHtml(row.object_name)}</td>
      <td class="truncate" title="${escapeHtml(row.budget)}">${escapeHtml(row.budget)}</td>
      <td><span class="source-pill">${escapeHtml(row.sources)}</span></td>
      <td class="number">${formatMoney(row.limit)}</td>
      <td class="number">${formatMoney(row.obligation)}</td>
      <td class="number">${formatMoney(row.cash)}</td>
      <td class="number">${formatMoney(row.agreement)}</td>
      <td class="number">${formatMoney(row.contract)}</td>
      <td class="number">${formatMoney(row.payment)}</td>
      <td class="number">${formatMoney(row.buau)}</td>
    `;
    els.summaryBody.appendChild(tr);
  });
}

function renderDetails(rows) {
  els.detailBody.innerHTML = "";
  els.detailCount.textContent = `${rows.length} записей`;
  rows.forEach((row) => {
    const amount = metricKeys.reduce((sum, key) => sum + Number(row[key] || 0), 0);
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHtml(row.event_date || row.snapshot)}</td>
      <td>${escapeHtml(row.source)}</td>
      <td>${escapeHtml(row.object_code)}</td>
      <td class="truncate" title="${escapeHtml(row.object_name)}">${escapeHtml(row.object_name)}</td>
      <td>${escapeHtml(row.document_number)}</td>
      <td class="truncate" title="${escapeHtml(row.counterparty)}">${escapeHtml(row.counterparty)}</td>
      <td class="number">${formatMoney(amount)}</td>
    `;
    els.detailBody.appendChild(tr);
  });
}

function drawChart(timeline) {
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
    plan: Number(row.limit || 0) + Number(row.obligation || 0),
    fact: Number(row.cash || 0) + Number(row.payment || 0) + Number(row.buau || 0),
    agreements: Number(row.agreement || 0) + Number(row.contract || 0),
  }));
  const max = Math.max(1, ...points.flatMap((p) => [p.plan, p.fact, p.agreements]));

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

  drawLine(ctx, points, "plan", "#1d4ed8", pad, width, height, max);
  drawLine(ctx, points, "fact", "#0f766e", pad, width, height, max);
  drawLine(ctx, points, "agreements", "#b45309", pad, width, height, max);

  drawLegend(ctx, width);
  if (points.length) {
    ctx.fillStyle = "#63717b";
    ctx.font = "12px Arial";
    ctx.fillText(points[0].date, pad.left, height - 12);
    ctx.fillText(points[points.length - 1].date, width - pad.right - 74, height - 12);
  }
}

function drawLine(ctx, points, key, color, pad, width, height, max) {
  if (!points.length) return;
  const chartWidth = width - pad.left - pad.right;
  const chartHeight = height - pad.top - pad.bottom;
  ctx.strokeStyle = color;
  ctx.lineWidth = 2;
  ctx.beginPath();
  points.forEach((point, index) => {
    const x = pad.left + (points.length === 1 ? 0 : (index / (points.length - 1)) * chartWidth);
    const y = height - pad.bottom - (point[key] / max) * chartHeight;
    if (index === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();
}

function drawLegend(ctx, width) {
  const items = [
    ["План и БО", "#1d4ed8"],
    ["Касса и оплаты", "#0f766e"],
    ["Соглашения и контракты", "#b45309"],
  ];
  let x = Math.max(80, width - 500);
  items.forEach(([label, color]) => {
    ctx.fillStyle = color;
    ctx.fillRect(x, 12, 14, 3);
    ctx.fillStyle = "#41505a";
    ctx.font = "12px Arial";
    ctx.fillText(label, x + 20, 16);
    x += label.length * 7 + 42;
  });
}

function exportCsv() {
  const header = ["Код", "Объект", "Бюджет", "Источники", "Лимиты", "БО", "Касса", "Соглашения", "Контракты", "Платежи", "БУАУ"];
  const lines = [header, ...currentRows.map((row) => [
    row.object_code,
    row.object_name,
    row.budget,
    row.sources,
    row.limit,
    row.obligation,
    row.cash,
    row.agreement,
    row.contract,
    row.payment,
    row.buau,
  ])];
  const csv = lines.map((line) => line.map(csvCell).join(";")).join("\n");
  const blob = new Blob(["\ufeff", csv], { type: "text/csv;charset=utf-8" });
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = "analytics_selection.csv";
  link.click();
  URL.revokeObjectURL(link.href);
}

function csvCell(value) {
  const text = String(value ?? "");
  return `"${text.replaceAll('"', '""')}"`;
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
["q", "code", "budget", "source", "start", "end"].forEach((key) => {
  els[key].addEventListener(key === "q" || key === "code" ? "input" : "change", debouncedLoad);
});
els.reset.addEventListener("click", async () => {
  ["q", "code", "budget", "source", "start", "end"].forEach((key) => {
    els[key].value = "";
  });
  await loadData();
});
els.exportCsv.addEventListener("click", exportCsv);
window.addEventListener("resize", debounce(loadData, 400));

loadMeta().then(loadData);
