const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

let capturedOptions = null;
let emittedDownloads = [];
let emittedDialogs = 0;
let lastFetch = null;
let scrollCalls = 0;

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
    location: { href: "" },
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
        followups: [{ label: "Скачать Excel", action: { download: "excel" } }],
        alternatives: [{ label: "Искать во всех данных", action: { mode: "slice", template: "all", q: "Благовещенск", code: "", budget: "", source: "", post_filter: "", reset_scope: true } }],
      }),
    };
  }
  if (String(url).startsWith("/api/explain")) {
    return {
      ok: true,
      status: 200,
      json: async () => ({ mode: "rule_based", title: "Короткое объяснение", bullets: ["Сначала проверьте кассу."], next_actions: [] }),
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
  if (String(url).startsWith("/api/catalog/reporting-dates")) {
    return {
      ok: true,
      status: 200,
      json: async () => [
        { date: "2025-02-01", label: "01.02.2025" },
        { date: "2026-04-01", label: "01.04.2026" },
      ],
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
    resultTabs: {
      scrollIntoView(options) {
        scrollCalls += 1;
        assert.equal(options.behavior, "smooth");
        assert.equal(options.block, "start");
      },
    },
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
    reporting_dates: [
      { date: "2025-02-01", label: "01.02.2025" },
      { date: "2026-04-01", label: "01.04.2026" },
    ],
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
  instance.filters.date = "2026-04-01";
  instance.filters.base = "2025-02-01";
  instance.filters.target = "2026-04-01";
  instance.query = {
    totals: { limit: 100, obligation: 20, cash: 10, agreement: 30, contract: 40, payment: 5, buau: 0 },
    rows: [
      { object_code: "101016105", object_name: "СКК объект", budget: "Областной бюджет", sources: "РЧБ, Соглашения", limit: 100, obligation: 20, cash: 10, agreement: 30, contract: 40, payment: 5, buau: 0, pipeline: { plan: 120, documents: 70, paid: 5, cash: 10 }, problem_reasons: ["low_cash"] },
    ],
    details: [{ id: "r1", event_date: "2026-04-01", source: "РЧБ", object_code: "101016105", object_name: "СКК объект", cash: 10 }],
    timeline: [{ date: "2026-04-01", limit: 100, cash: 10 }],
    count: 1,
    attention_summary: {
      title: "Найдено: что требует внимания",
      severity: "warning",
      bullets: ["Есть объекты с низкой кассой."],
      next_actions: [{ label: "Скачать Excel", action: { download: "excel" } }],
      top_risks: [],
    },
  };
  instance.compare = {
    rows: [
      { object_code: "101016105", object_name: "СКК объект", budget: "", metrics: { limit: { base: 1, target: 3, delta: 2, delta_percent: 200 }, cash: { base: 2, target: 1, delta: -1, delta_percent: -50 } }, total_delta: 3 },
    ],
    compare_insights: {
      title: "Изменения за период",
      severity: "normal",
      bullets: ["Нет существенных изменений."],
      next_actions: [{ label: "Показать новые проблемы", action: { open_view: "changes" } }],
    },
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
    await this.scrollToResultsIfNeeded();
  };
  instance.drawChart = function drawChartMock() {};
  instance.drawFunnelChart = function drawFunnelChartMock() {};
  instance.drawRiskChart = function drawRiskChartMock() {};
  return instance;
}

(async () => {
  const vmApp = makeInstance();

  assert.equal(vmApp.activeTemplateLabel, "Все данные");
  assert.deepEqual(vmApp.activeMetricCodes, ["limit", "obligation", "cash", "agreement", "contract", "payment", "buau"]);

  const quick = { mode: "slice", template: "skk", metrics: ["limit", "cash"], post_filter: "no_cash" };
  vmApp.filters.q = "Благовещенск";
  vmApp.filters.code = "6105";
  vmApp.filters.budget = "Областной бюджет";
  vmApp.filters.source = "РЧБ";
  scrollCalls = 0;
  await vmApp.applyQuickAction(quick);
  assert.equal(vmApp.mode, "slice");
  assert.equal(vmApp.filters.template, "skk");
  assert.equal(vmApp.filters.post_filter, "no_cash");
  assert.equal(vmApp.filters.q, "");
  assert.equal(vmApp.filters.code, "");
  assert.equal(vmApp.filters.budget, "");
  assert.equal(vmApp.filters.source, "");
  assert.deepEqual(vmApp.selectedMetrics, ["limit", "cash"]);
  assert.equal(vmApp.currentView, "problems");
  assert.equal(vmApp.loadCalls, 1);
  assert.equal(scrollCalls, 1);
  assert.equal(vmApp.pendingResultScroll, false);
  assert.equal(vmApp.resultNextActions[0].label, "Скачать Excel");
  assert.equal(vmApp.funnelValues.plan, 120);
  assert.equal(vmApp.riskDistribution.low, 1);
  assert.equal(vmApp.riskHelpOpen, false);
  assert.equal(vmApp.riskHelpSummary.total, 1);
  assert.equal(vmApp.riskHelpSummary.low, 1);

  scrollCalls = 0;
  await vmApp.applyQuickAction({ mode: "compare", template: "skk", metrics: ["limit"] });
  assert.equal(vmApp.mode, "compare");
  assert.equal(vmApp.currentView, "changes");
  assert.equal(vmApp.filters.base, "2025-02-01");
  assert.equal(vmApp.filters.target, "2026-04-01");
  assert.equal(scrollCalls, 1);
  assert.equal(vmApp.resultNextActions[0].label, "Показать новые проблемы");

  await vmApp.applyAction({ mode: "slice", template: "skk", demo_mode: "skk_risks" }, { scrollToResults: false });
  assert.equal(vmApp.demoMode, "skk_risks");

  const params = vmApp.buildQueryParams(true);
  assert.equal(params.get("template"), "skk");
  assert.equal(params.get("metrics"), "limit");
  assert.equal(params.get("view"), "as_of");
  assert.equal(params.get("date"), "2026-04-01");

  assert.equal(vmApp.buildSmartSuggestions("6105")[0].action.template, "skk");
  assert.equal(vmApp.buildSmartSuggestions("978")[0].action.template, "kik");
  assert.equal(vmApp.buildSmartSuggestions("970")[0].action.template, "two_thirds");
  assert.equal(vmApp.buildSmartSuggestions("окв капитал")[0].action.template, "okv");
  assert.equal(vmApp.buildSmartSuggestions("сравни СКК")[0].action.mode, "compare");
  assert.equal(vmApp.buildSmartSuggestions("где нет кассы")[0].action.post_filter, "no_cash");
  assert.equal(vmApp.buildSmartSuggestions("Благовещенск").at(-1).action.q, "Благовещенск");

  vmApp.mode = "slice";
  vmApp.command.text = "6105";
  vmApp.onCommandInput();
  assert.ok(vmApp.command.suggestions.length > 0);
  scrollCalls = 0;
  await vmApp.applyCommandSuggestion(vmApp.command.suggestions[0]);
  assert.equal(vmApp.filters.template, "skk");
  assert.equal(scrollCalls, 1);

  assert.equal(vmApp.rowStatus({ limit: 100, obligation: 0, cash: 0, payment: 0, buau: 0, agreement: 0, contract: 0 }), "no_documents");
  assert.equal(vmApp.rowStatus({ limit: 100, obligation: 0, cash: 10, payment: 0, buau: 0, agreement: 10, contract: 0 }), "no_payments");
  assert.equal(vmApp.rowStatus({ limit: 100, obligation: 0, cash: 100, payment: 100, buau: 0, agreement: 100, contract: 0 }), "executed");
  assert.equal(vmApp.rowStatus({ limit: 100, obligation: 0, cash: 10, payment: 1, buau: 0, agreement: 10, contract: 0 }), "low_cash");
  assert.equal(vmApp.rowStatusClass("low_cash"), "warning");
  assert.equal(vmApp.simpleRows[0].statusLabel, "Низкая касса");

  vmApp.mode = "slice";
  assert.equal(vmApp.resultNarrative.severity, "warning");
  assert.match(vmApp.resultNarrative.title, /найдено/i);
  vmApp.filters.template = "skk";
  vmApp.filters.code = "6105";
  vmApp.filters.budget = "Областной бюджет";
  vmApp.filters.source = "РЧБ";
  vmApp.filters.post_filter = "low_cash";
  vmApp.query.rows = [];
  assert.equal(vmApp.resultNarrative.severity, "empty");
  const allDataSuggestion = vmApp.emptyStateSuggestions.find((item) => item.label === "Искать во всех данных");
  assert.ok(allDataSuggestion);
  scrollCalls = 0;
  await vmApp.applyEmptySuggestion(allDataSuggestion);
  assert.equal(vmApp.filters.template, "all");
  assert.equal(vmApp.filters.code, "");
  assert.equal(vmApp.filters.budget, "");
  assert.equal(vmApp.filters.source, "");
  assert.equal(vmApp.filters.post_filter, "");
  assert.equal(scrollCalls, 1);

  vmApp.filters.budget = "Областной бюджет";
  await vmApp.applyEmptySuggestion({ label: "Убрать бюджет", patch: { budget: "" } });
  assert.equal(vmApp.filters.budget, "");

  vmApp.mode = "compare";
  assert.equal(vmApp.resultNarrative.severity, "normal");
  assert.match(vmApp.resultNarrative.title, /Изменения/);

  vmApp.command.text = "покажи СКК";
  await vmApp.runCommand();
  assert.equal(vmApp.command.response.action.template, "skk");
  vmApp.filters.code = "6105";
  vmApp.filters.budget = "Областной бюджет";
  vmApp.filters.source = "РЧБ";
  vmApp.filters.post_filter = "low_cash";
  scrollCalls = 0;
  await vmApp.applyAssistantAction(vmApp.command.response.alternatives[0].action);
  assert.equal(vmApp.filters.template, "all");
  assert.equal(vmApp.filters.code, "");
  assert.equal(vmApp.filters.budget, "");
  assert.equal(vmApp.filters.source, "");
  assert.equal(vmApp.filters.post_filter, "");
  assert.equal(scrollCalls, 1);
  scrollCalls = 0;
  await vmApp.applyAssistantAction(vmApp.command.response.action);
  assert.equal(vmApp.filters.template, "skk");
  assert.equal(vmApp.filters.q, "Благовещенск");
  assert.equal(scrollCalls, 1);

  await vmApp.explainResult();
  assert.equal(lastFetch.url, "/api/explain");
  assert.equal(vmApp.explanation.title, "Короткое объяснение");

  vmApp.requestResultScroll();
  assert.equal(vmApp.pendingResultScroll, true);
  scrollCalls = 0;
  await vmApp.scrollToResultsIfNeeded();
  assert.equal(vmApp.pendingResultScroll, false);
  assert.equal(scrollCalls, 1);

  await vmApp.openTrace("r1");
  assert.equal(emittedDialogs, 1);
  assert.equal(vmApp.trace.human_summary.amount_fields[0].field, "cash");

  vmApp.mode = "slice";
  vmApp.query.rows = [
    { object_code: "1", object_name: "Объект", budget: "Бюджет", sources: "РЧБ", limit: 1, cash: 2, pipeline: { plan: 1, documents: 0, paid: 0, cash: 2 }, problem_reasons: [] },
  ];
  vmApp.exportCsv();
  assert.equal(emittedDownloads.at(-1).download, "analytics_skk_2026-04-01.csv");
  const csvText = await emittedDownloads.at(-1).blob.text();
  assert.match(csvText, /Отчёт/);
  assert.match(csvText, /"План";"Документы";"Оплачено";"Касса";"Статус"/);

  vmApp.mode = "compare";
  vmApp.exportCsv();
  assert.match(emittedDownloads.at(-1).download, /analytics_compare_skk_2025-02-01_2026-04-01/);

  vmApp.mode = "slice";
  vmApp.filters.template = "skk";
  vmApp.filters.date = "2026-04-01";
  vmApp.exportPdf();
  assert.match(context.window.location.href, /^\/api\/export\.pdf\?/);
  assert.match(context.window.location.href, /template=skk/);
  context.window.location.href = "";
  await vmApp.runFollowup({ download: "pdf" });
  assert.match(context.window.location.href, /^\/api\/export\.pdf\?/);

  vmApp.setMode("slice");
  assert.equal(vmApp.currentView, "overview");
  vmApp.setView("objects");
  assert.equal(vmApp.currentView, "objects");
  vmApp.filters.q = "x";
  vmApp.filters.code = "6105";
  vmApp.filters.template = "skk";
  vmApp.filters.post_filter = "low_cash";
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
