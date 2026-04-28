const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

let capturedOptions = null;
let emittedDownloads = [];
let emittedDialogs = 0;
let lastFetch = null;

const context = {
  console,
  setTimeout,
  clearTimeout,
  Intl,
  Math,
  Number,
  String,
  Blob,
  URLSearchParams,
  encodeURIComponent,
  Vue: {
    createApp(options) {
      capturedOptions = options;
      return {
        mount(selector) {
          context.mountedSelector = selector;
          return {};
        },
      };
    },
    nextTick: async () => {},
  },
  localStorage: {
    value: {},
    getItem(key) {
      return this.value[key] || null;
    },
    setItem(key, value) {
      this.value[key] = String(value);
    },
  },
  document: {
    documentElement: { dataset: {} },
    createElement(tag) {
      assert.equal(tag, "a");
      const link = {
        href: "",
        download: "",
        click() {
          emittedDownloads.push({ href: this.href, download: this.download, blob: context.lastBlob });
        },
      };
      return link;
    },
  },
  window: {
    devicePixelRatio: 1,
    onresize: null,
  },
  getComputedStyle() {
    return {
      getPropertyValue(name) {
        const values = {
          "--chart-grid": "#ddd",
          "--chart-axis": "#999",
          "--chart-text": "#666",
          "--chart-line": "#0f766e",
        };
        return values[name] || "";
      },
    };
  },
  URL: {
    createObjectURL(blob) {
      context.lastBlob = blob;
      return "blob:test-url";
    },
    revokeObjectURL() {},
  },
};

context.fetch = async (url, options = {}) => {
  lastFetch = { url: String(url), options };
  if (String(url).startsWith("/api/assistant")) {
    return {
      ok: true,
      status: 200,
      json: async () => ({
        mode: "rule_based",
        intent: "run_query",
        message: "Я понял запрос как выборку: СКК.",
        action: { mode: "slice", template: "skk", q: "Благовещенск", metrics: ["limit", "cash"] },
        alternatives: [{ label: "Искать во всех данных", action: { mode: "slice", template: "all", q: "Благовещенск", code: "", budget: "", source: "", post_filter: "", reset_scope: true } }],
      }),
    };
  }
  if (String(url).startsWith("/api/trace")) {
    return {
      ok: true,
      status: 200,
      json: async () => ({
        id: "r1",
        source: "РЧБ",
        source_file: "case/1_RCB/test.csv",
        source_row: 10,
        human_summary: {
          title: "Сумма из исходной строки",
          date: "2026-04-01",
          object: "Объект",
          document: "Документ",
          amount_fields: [{ label: "Касса", field: "cash", value: 10 }],
        },
        raw: {},
        normalized: {},
      }),
    };
  }
  return { ok: true, status: 200, json: async () => ({}) };
};

vm.createContext(context);
const appSource = fs.readFileSync(path.join(__dirname, "..", "static", "app.js"), "utf8");
vm.runInContext(appSource, context, { filename: "static/app.js" });

assert.equal(context.mountedSelector, "#app");
assert.ok(capturedOptions, "Vue options were not captured");

function makeInstance() {
  const instance = capturedOptions.data();
  instance.$refs = {
    traceDialog: {
      showModal() {
        emittedDialogs += 1;
      },
      close() {},
    },
  };
  instance.meta = {
    records: 10,
    budgets: ["Областной бюджет", "Бюджет Благовещенска"],
    sources: ["РЧБ", "Соглашения", "ГЗ: контракты", "БУАУ"],
    snapshots: ["2025-02-01", "2026-04-01"],
    objects: [],
  };
  instance.templates = [
    { code: "all", label: "Все данные" },
    { code: "skk", label: "СКК" },
    { code: "kik", label: "КИК" },
    { code: "two_thirds", label: "2/3" },
    { code: "okv", label: "ОКВ" },
  ];
  instance.metrics = [
    { code: "limit", label: "Лимиты" },
    { code: "obligation", label: "БО" },
    { code: "cash", label: "Касса" },
    { code: "agreement", label: "Соглашения" },
    { code: "contract", label: "Договоры" },
    { code: "payment", label: "Оплаты" },
    { code: "buau", label: "БУ/АУ" },
  ];
  instance.selectedMetrics = instance.metrics.map((metric) => metric.code);
  instance.filters.start = "2025-02-01";
  instance.filters.end = "2026-04-01";
  instance.filters.base = "2025-02-01";
  instance.filters.target = "2026-04-01";
  instance.query = {
    totals: { limit: 100, obligation: 20, cash: 10, agreement: 30, contract: 40, payment: 5, buau: 0 },
    rows: [
      { object_code: "101016105", object_name: "СКК объект", budget: "Областной бюджет", sources: "РЧБ, Соглашения", limit: 100, obligation: 20, cash: 10, agreement: 30, contract: 40, payment: 5, buau: 0 },
    ],
    details: [{ id: "r1", event_date: "2026-04-01", source: "РЧБ", object_code: "101016105", object_name: "СКК объект", cash: 10 }],
    timeline: [{ date: "2026-04-01", limit: 100, cash: 10 }],
    count: 1,
  };
  instance.compare = {
    rows: [
      { object_code: "101016105", object_name: "СКК объект", budget: "", metrics: { limit: { base: 1, target: 3, delta: 2, delta_percent: 200 }, cash: { base: 2, target: 1, delta: -1, delta_percent: -50 } }, total_delta: 3 },
    ],
  };
  for (const [name, getter] of Object.entries(capturedOptions.computed)) {
    Object.defineProperty(instance, name, { get: getter.bind(instance), configurable: true });
  }
  for (const [name, method] of Object.entries(capturedOptions.methods)) {
    instance[name] = method.bind(instance);
  }
  instance.loadCalls = 0;
  instance.loadData = async function loadDataMock() {
    this.loadCalls += 1;
  };
  instance.drawChart = function drawChartMock() {};
  return instance;
}

(async () => {
  const vmApp = makeInstance();

  assert.equal(vmApp.activeTemplateLabel, "Все данные");
  assert.deepEqual(vmApp.activeMetricCodes, ["limit", "obligation", "cash", "agreement", "contract", "payment", "buau"]);

  const quick = { mode: "slice", template: "skk", metrics: ["limit", "cash"], post_filter: "low_execution" };
  vmApp.applyQuickAction(quick);
  assert.equal(vmApp.mode, "slice");
  assert.equal(vmApp.filters.template, "skk");
  assert.equal(vmApp.filters.post_filter, "low_execution");
  assert.deepEqual(vmApp.selectedMetrics, ["limit", "cash"]);
  assert.equal(vmApp.currentView, "overview");
  assert.equal(vmApp.loadCalls, 1);

  vmApp.applyQuickAction({ mode: "compare", template: "skk", metrics: ["limit"] });
  assert.equal(vmApp.mode, "compare");
  assert.equal(vmApp.currentView, "changes");
  assert.equal(vmApp.filters.base, "2025-02-01");
  assert.equal(vmApp.filters.target, "2026-04-01");

  const params = vmApp.buildQueryParams(true);
  assert.equal(params.get("template"), "skk");
  assert.equal(params.get("metrics"), "limit");
  assert.equal(params.get("start"), "2025-02-01");

  assert.equal(vmApp.buildSmartSuggestions("6105")[0].action.template, "skk");
  assert.equal(vmApp.buildSmartSuggestions("978")[0].action.template, "kik");
  assert.equal(vmApp.buildSmartSuggestions("970")[0].action.template, "two_thirds");
  assert.equal(vmApp.buildSmartSuggestions("окв капитал")[0].action.template, "okv");
  assert.equal(vmApp.buildSmartSuggestions("сравни СКК")[0].action.mode, "compare");
  assert.equal(vmApp.buildSmartSuggestions("Благовещенск").at(-1).action.q, "Благовещенск");

  vmApp.mode = "slice";
  vmApp.smartInput = "6105";
  vmApp.onSmartInput();
  assert.ok(vmApp.smartSuggestions.length > 0);
  vmApp.applyFirstSuggestion();
  assert.equal(vmApp.filters.template, "skk");

  assert.equal(vmApp.rowStatus({ limit: 100, obligation: 0, cash: 0, payment: 0, buau: 0, agreement: 0, contract: 0 }), "no_execution");
  assert.equal(vmApp.rowStatus({ limit: 100, obligation: 0, cash: 10, payment: 0, buau: 0, agreement: 0, contract: 0 }), "low_execution");
  assert.equal(vmApp.rowStatus({ limit: 100, obligation: 0, cash: 100, payment: 0, buau: 0, agreement: 0, contract: 0 }), "executed");
  assert.equal(vmApp.rowStatus({ limit: 0, obligation: 0, cash: 0, payment: 0, buau: 0, agreement: 10, contract: 0 }), "has_documents_no_payment");
  assert.equal(vmApp.rowStatusClass("low_execution"), "warning");
  assert.equal(vmApp.simpleRows[0].statusLabel, "Низкое исполнение");

  vmApp.mode = "slice";
  assert.equal(vmApp.resultNarrative.severity, "warning");
  assert.match(vmApp.resultNarrative.title, /Найдено/);
  vmApp.filters.template = "skk";
  vmApp.filters.code = "6105";
  vmApp.filters.budget = "Областной бюджет";
  vmApp.filters.source = "РЧБ";
  vmApp.filters.post_filter = "low_execution";
  vmApp.query.rows = [];
  assert.equal(vmApp.resultNarrative.severity, "empty");
  const allDataSuggestion = vmApp.emptyStateSuggestions.find((item) => item.label === "Искать во всех данных");
  assert.ok(allDataSuggestion);
  vmApp.applyEmptySuggestion(allDataSuggestion);
  assert.equal(vmApp.filters.template, "all");
  assert.equal(vmApp.filters.code, "");
  assert.equal(vmApp.filters.budget, "");
  assert.equal(vmApp.filters.source, "");
  assert.equal(vmApp.filters.post_filter, "");

  vmApp.filters.budget = "Областной бюджет";
  vmApp.applyEmptySuggestion({ label: "Убрать бюджет", patch: { budget: "" } });
  assert.equal(vmApp.filters.budget, "");

  vmApp.mode = "compare";
  assert.equal(vmApp.resultNarrative.severity, "normal");
  assert.match(vmApp.resultNarrative.title, /Изменения/);

  vmApp.assistant.message = "покажи СКК";
  await vmApp.askAssistant();
  assert.equal(lastFetch.url, "/api/assistant");
  assert.equal(vmApp.assistant.response.action.template, "skk");
  vmApp.filters.code = "6105";
  vmApp.filters.budget = "Областной бюджет";
  vmApp.filters.source = "РЧБ";
  vmApp.filters.post_filter = "low_execution";
  vmApp.applyAssistantAction(vmApp.assistant.response.alternatives[0].action);
  assert.equal(vmApp.filters.template, "all");
  assert.equal(vmApp.filters.code, "");
  assert.equal(vmApp.filters.budget, "");
  assert.equal(vmApp.filters.source, "");
  assert.equal(vmApp.filters.post_filter, "");
  vmApp.applyAssistantAction(vmApp.assistant.response.action);
  assert.equal(vmApp.filters.template, "skk");
  assert.equal(vmApp.filters.q, "Благовещенск");

  await vmApp.openTrace("r1");
  assert.equal(emittedDialogs, 1);
  assert.equal(vmApp.trace.human_summary.amount_fields[0].field, "cash");

  vmApp.mode = "slice";
  vmApp.query.rows = [
    { object_code: "1", object_name: "Объект", budget: "Бюджет", sources: "РЧБ", limit: 1, cash: 2 },
  ];
  vmApp.exportCsv();
  assert.equal(emittedDownloads.at(-1).download, "analytics_skk_2026-04-01.csv");
  assert.match(await emittedDownloads.at(-1).blob.text(), /Отчёт/);

  vmApp.mode = "compare";
  vmApp.exportCsv();
  assert.match(emittedDownloads.at(-1).download, /analytics_compare_skk_2025-02-01_2026-04-01/);

  vmApp.setMode("slice");
  assert.equal(vmApp.currentView, "overview");
  vmApp.setView("simple");
  assert.equal(vmApp.currentView, "simple");
  vmApp.filters.q = "x";
  vmApp.filters.code = "6105";
  vmApp.filters.template = "skk";
  vmApp.filters.post_filter = "low_execution";
  vmApp.resetFilters();
  assert.equal(vmApp.filters.q, "");
  assert.equal(vmApp.filters.template, "all");
  assert.equal(vmApp.filters.post_filter, "");

  assert.equal(vmApp.deltaClass(1), "positive");
  assert.equal(vmApp.deltaClass(-1), "negative");
  assert.equal(vmApp.deltaClass(0), "zero");
  assert.equal(vmApp.metricHint("cash"), "сколько фактически выбыло");
  vmApp.selectedMetrics = ["limit", "obligation", "cash", "agreement", "contract", "payment", "buau"];
  assert.equal(vmApp.detailAmount({ limit: 1, obligation: 2, cash: 3, agreement: 4, contract: 5, payment: 6, buau: 7 }), 28);

  console.log("frontend scenarios ok");
})();
