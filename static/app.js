const { createApp, nextTick } = Vue;

const money = new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 0 });
const percent = new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 1 });

const METRIC_LABELS = {
  limit: "Лимиты",
  obligation: "БО",
  cash: "Касса",
  agreement: "Соглашения",
  contract: "Договоры",
  payment: "Оплаты",
  buau: "БУ/АУ",
};

// CSV экспорт делается на клиенте, поэтому значения экранируются до сборки строк.
function csvCell(value) {
  return `"${String(value ?? "").replaceAll('"', '""')}"`;
}

// Общий debounce для поиска и перерисовки графика, чтобы не спамить API.
function debounce(fn, delay = 250) {
  let timer;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), delay);
  };
}

async function fetchJson(url, options) {
  const response = await fetch(url, options);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}

createApp({
  data() {
    return {
      // Режимы соответствуют двум backend-сценариям: as-of срез и сравнение дат.
      mode: "slice",
      theme: "dark",
      currentView: "overview",
      command: {
        text: "",
        loading: false,
        response: null,
        suggestions: [],
        source: "",
        error: "",
      },
      quickActions: [],
      readinessOpen: false,
      advancedOpen: false,

      assistant: {
        message: "",
        loading: false,
        response: null,
        history: [],
      },

      meta: {
        records: 0,
        budgets: [],
        sources: [],
        snapshots: [],
        reporting_dates: [],
        objects: [],
      },

      templates: [],
      metrics: [],

      quality: {
        issues: [],
        summary: {
          warnings: 0,
          errors: 0,
        },
      },

      filters: {
        // Все фильтры хранятся плоско, чтобы их можно было напрямую превратить
        // в URLSearchParams и переиспользовать в быстрых действиях.
        q: "",
        code: "",
        template: "all",
        budget: "",
        source: "",
        date: "",
        start: "",
        end: "",
        base: "",
        target: "",
        post_filter: "",
      },

      selectedMetrics: [],

      query: {
        totals: {},
        rows: [],
        details: [],
        timeline: [],
        count: 0,
      },

      readiness: {
        date: "",
        template: "",
        summary: { ok: 0, warn: 0, bad: 0 },
        checks: [],
      },

      compare: {
        base: "",
        target: "",
        rows: [],
        metrics: [],
      },

      trace: {
        id: "",
        source: "",
        source_file: "",
        source_row: "",
        human_summary: { amount_fields: [] },
        raw: {},
        normalized: {},
      },

      loading: {
        meta: false,
        query: false,
        compare: false,
        trace: false,
        object: false,
        explain: false,
      },

      error: "",
      explanation: null,
      demoMode: "",
      debounceTimer: null,
      chartRedraw: null,
      initialized: false,
      pendingResultScroll: false,
      suppressAutoLoad: false,
      expandedPipelines: {},
      objectCard: null,
      objectRowsOpen: false,
      problemRiskFilter: "all",
    };
  },

  computed: {
    // Computed-свойства приводят API-ответы к форме, удобной для шаблона.
    activeTemplateLabel() {
      const readable = {
        all: "Все данные",
        kik: "КИК",
        skk: "СКК",
        two_thirds: "2/3",
        okv: "ОКВ",
      };
      return readable[this.filters.template] || "Все данные";
    },

    activeMetricObjects() {
      const source = this.metrics.length
        ? this.metrics.map((metric) => ({ ...metric, label: METRIC_LABELS[metric.code] || metric.label }))
        : Object.entries(METRIC_LABELS).map(([code, label]) => ({ code, label }));
      if (!this.selectedMetrics.length) {
        return source;
      }
      const selected = new Set(this.selectedMetrics);
      return source.filter((metric) => selected.has(metric.code));
    },

    activeMetricCodes() {
      const codes = this.activeMetricObjects.map((metric) => metric.code);
      return codes.length ? codes : Object.keys(METRIC_LABELS);
    },

    prettyTrace() {
      return JSON.stringify(this.trace, null, 2);
    },

    isCompareMode() {
      return this.mode === "compare";
    },

    viewTabs() {
      if (this.mode === "compare") {
        return [{ code: "changes", label: "Что изменилось", count: this.compare.rows.length }];
      }
      return [
        { code: "overview", label: "Главное", count: this.query.count || 0 },
        { code: "objects", label: "Объекты", count: this.simpleRows.length },
        { code: "problems", label: "Проблемы", count: this.problemRows.length },
        { code: "records", label: "Исходные строки", count: this.query.details.length },
      ];
    },

    simpleRows() {
      // Backend уже возвращает pipeline и риск, но UI пересчитывает fallback
      // для старых ответов и тестовых данных.
      return (this.query.rows || []).map((row) => {
        const pipeline = row.pipeline || this.buildPipeline(row);
        const status = this.rowStatus(row);
        const key = row.object_key || `${row.object_code || row.object_name}-${row.budget || ""}`;
        return {
          ...row,
          rowKey: key,
          plan: pipeline.plan,
          paid: pipeline.paid,
          cash: pipeline.cash,
          documents: pipeline.documents,
          pipeline,
          status,
          statusLabel: this.rowStatusLabel(status),
          statusClass: this.rowStatusClass(status),
          riskLabel: row.risk_label || this.riskLabel(row.risk_level),
          riskScore: Number(row.risk_score || 0),
          riskClass: this.riskClass(row.risk_level),
        };
      });
    },

    problemRows() {
      return this.simpleRows
        .filter((row) => (row.problem_reasons || []).length)
        .filter((row) => {
          if (this.problemRiskFilter === "critical") return row.risk_level === "critical";
          if (this.problemRiskFilter === "high") return ["critical", "high"].includes(row.risk_level);
          return true;
        })
        .sort((left, right) => (Number(right.risk_score || 0) - Number(left.risk_score || 0)) || (Number(right.plan || 0) - Number(left.plan || 0)));
    },

    problemGroups() {
      const groups = {};
      this.problemRows.forEach((row) => {
        (row.problem_reasons || ["data_gap"]).forEach((reason) => {
          if (!groups[reason]) groups[reason] = { reason, label: this.problemReasonLabel(reason), rows: [] };
          groups[reason].rows.push(row);
        });
      });
      return Object.values(groups).map((group) => ({
        ...group,
        rows: group.rows.sort((left, right) => (Number(right.risk_score || 0) - Number(left.risk_score || 0)) || (Number(right.plan || 0) - Number(left.plan || 0))),
      }));
    },

    topRisks() {
      return this.query.attention_summary?.top_risks || [];
    },

    reportingDates() {
      const dates = this.meta.reporting_dates?.length ? this.meta.reporting_dates : this.meta.snapshots.map((date) => ({ date, label: date }));
      return dates;
    },

    currentDateLabel() {
      return this.reportingDates.find((item) => item.date === this.filters.date)?.label || this.filters.date;
    },

    activeDateValues() {
      return this.reportingDates.map((item) => item.date);
    },

    simpleTotals() {
      const totals = this.query.totals || {};
      return this.buildPipeline(totals);
    },

    resultNextActions() {
      if (this.mode === "compare") return this.compare.compare_insights?.next_actions || [];
      return this.query.attention_summary?.next_actions || [];
    },

    funnelValues() {
      return this.simpleTotals;
    },

    riskDistribution() {
      return this.simpleRows.reduce((acc, row) => {
        const level = row.risk_level || "low";
        acc[level] = (acc[level] || 0) + 1;
        return acc;
      }, { critical: 0, high: 0, medium: 0, low: 0 });
    },

    hasProblems() {
      return this.problemRows.length > 0;
    },

    resultNarrative() {
      // Короткий вывод сначала берет готовый attention_summary с backend,
      // а при его отсутствии строит простой текст из текущей таблицы.
      if (this.mode === "compare") {
        return this.compareNarrative;
      }
      if (!this.query.rows.length) {
        return {
          title: "Ничего не найдено",
          bullets: ["По этим условиям нет строк. Попробуйте расширить выборку или очистить поиск."],
          severity: "empty",
        };
      }
      if (this.query.attention_summary) {
        return {
          title: this.query.attention_summary.title,
          bullets: this.query.attention_summary.bullets || [],
          severity: this.query.attention_summary.severity || "normal",
        };
      }
      const totals = this.query.totals || {};
      const topRow = this.query.rows[0];
      const pipeline = this.simpleTotals;
      const bullets = [
        `План: ${this.formatMoney(pipeline.plan)}, касса: ${this.formatMoney(pipeline.cash)}.`,
        this.hasProblems ? `Есть ${this.problemRows.length} объектов с проблемами исполнения.` : "Явных проблем исполнения не найдено.",
        `Самый крупный объект: ${topRow.object_name || topRow.object_code || "без названия"}.`,
      ];
      return { title: `На ${this.currentDateLabel || this.filters.date} найдено ${this.query.rows.length} объектов.`, bullets, severity: this.hasProblems ? "warning" : "normal" };
    },

    compareNarrative() {
      if (this.compare.compare_insights) {
        return {
          title: this.compare.compare_insights.title,
          bullets: this.compare.compare_insights.bullets || [],
          severity: this.compare.compare_insights.severity || "normal",
        };
      }
      if (!this.compare.rows.length) {
        return {
          title: "Изменений не найдено",
          bullets: ["По выбранным датам и фильтрам нет сравнимых строк."],
          severity: "empty",
        };
      }
      const bullets = [];
      this.activeMetricCodes.slice(0, 3).forEach((metric) => {
        const delta = this.compare.rows.reduce((sum, row) => sum + Number(row.metrics?.[metric]?.delta || 0), 0);
        bullets.push(`${METRIC_LABELS[metric] || metric}: изменение ${this.formatMoney(delta)}`);
      });
      const top = this.compare.rows[0];
      bullets.push(`Самое большое изменение: ${top.object_name || top.object_code || "без названия"}`);
      return { title: `Изменения за период ${this.filters.base} - ${this.filters.target}`, bullets, severity: "normal" };
    },

    compareInsightSections() {
      const insights = this.compare.compare_insights || {};
      return [
        { code: "new_problem_objects", title: "Новые проблемы", rows: insights.new_problem_objects || [] },
        { code: "improved_objects", title: "Риск снизился", rows: insights.improved_objects || [] },
        { code: "worsened_objects", title: "Риск вырос", rows: insights.worsened_objects || [] },
        { code: "stalled_cash_objects", title: "План вырос, касса не изменилась", rows: insights.stalled_cash_objects || [] },
      ].filter((section) => section.rows.length);
    },

    emptyStateSuggestions() {
      const result = [];
      if (this.filters.budget) result.push({ label: "Убрать бюджет", patch: { budget: "" } });
      if (this.filters.source) result.push({ label: "Искать во всех источниках", patch: { source: "" } });
      if (this.filters.template !== "all" || this.filters.budget || this.filters.source || this.filters.code || this.filters.post_filter) {
        result.push({ label: "Искать во всех данных", patch: { template: "all", budget: "", source: "", code: "", post_filter: "" } });
      }
      if (this.filters.q || this.filters.code) result.push({ label: "Очистить поиск", patch: { q: "", code: "" } });
      if (this.filters.date !== this.activeDateValues.at(-1)) {
        result.push({ label: "Показать последнюю отчетную дату", patch: { date: this.activeDateValues.at(-1) || "" } });
      }
      if (!result.length) result.push({ label: "Сбросить лишние фильтры", patch: { q: "", code: "", budget: "", source: "", template: "all", post_filter: "" } });
      return result;
    },

    demoReadiness() {
      return this.readiness.checks || [];
    },
  },

  watch: {
    // Автозагрузка отключается на время пакетного применения действий,
    // чтобы один клик не запускал несколько одинаковых запросов.
    "filters.template"() {
      if (this.initialized && !this.suppressAutoLoad) this.loadData();
    },
    "filters.budget"() {
      if (this.initialized && !this.suppressAutoLoad) this.loadData();
    },
    "filters.source"() {
      if (this.initialized && !this.suppressAutoLoad) this.loadData();
    },
    "filters.date"() {
      if (this.initialized && !this.suppressAutoLoad && this.mode === "slice") this.loadData();
    },
    "filters.base"() {
      if (this.initialized && !this.suppressAutoLoad && this.mode === "compare") this.loadData();
    },
    "filters.target"() {
      if (this.initialized && !this.suppressAutoLoad && this.mode === "compare") this.loadData();
    },
    selectedMetrics: {
      deep: true,
      handler() {
        if (this.initialized && !this.suppressAutoLoad) this.loadData();
      },
    },
    currentView() {
      nextTick(() => {
        this.drawChart();
        this.drawFunnelChart();
        this.drawRiskChart();
      });
    },
  },

  async mounted() {
    this.theme = localStorage.getItem("analytics-theme") || "dark";
    this.applyTheme();
    this.chartRedraw = debounce(() => {
      this.drawChart();
      this.drawFunnelChart();
      this.drawRiskChart();
    }, 200);
    await this.loadInitialData();
    this.initialized = true;
    await this.loadData();
    window.onresize = this.scheduleChartRedraw;
  },

  beforeUnmount() {
    if (window.onresize === this.scheduleChartRedraw) {
      window.onresize = null;
    }
  },

  methods: {
    async loadInitialData() {
      // Справочники загружаются параллельно: они независимы и нужны до первого query.
      this.loading.meta = true;
      this.error = "";
      try {
        const [meta, templates, metrics, quality, quickActions, reportingDates] = await Promise.all([
          fetchJson("/api/meta"),
          fetchJson("/api/catalog/templates"),
          fetchJson("/api/catalog/metrics"),
          fetchJson("/api/quality"),
          fetchJson("/api/catalog/quick-actions"),
          fetchJson("/api/catalog/reporting-dates"),
        ]);
        this.meta = { records: 0, budgets: [], sources: [], snapshots: [], reporting_dates: reportingDates, objects: [], ...meta, reporting_dates: reportingDates };
        this.templates = templates;
        this.metrics = metrics.map((metric) => ({ ...metric, label: METRIC_LABELS[metric.code] || metric.label }));
        this.quickActions = quickActions;
        this.quality = { issues: [], summary: { warnings: 0, errors: 0 }, ...quality, summary: { warnings: 0, errors: 0, ...(quality.summary || {}) } };
        this.selectedMetrics = this.metrics.map((metric) => metric.code);
        if (this.reportingDates.length) {
          this.filters.date = this.reportingDates.at(-1).date;
          this.filters.base = this.reportingDates[0].date;
          this.filters.target = this.reportingDates.at(-1).date;
        }
      } catch (error) {
        console.error(error);
        this.error = "Не удалось загрузить справочники. Проверьте сервер.";
      } finally {
        this.loading.meta = false;
      }
    },

    async loadData() {
      // В режиме среза основной результат и readiness идут одним набором параметров.
      if (this.mode === "compare") {
        await this.loadCompare();
        return;
      }
      this.loading.query = true;
      this.error = "";
      try {
        const params = this.buildQueryParams(true);
        const [query, readiness] = await Promise.all([
          fetchJson(`/api/query?${params.toString()}`),
          fetchJson(`/api/readiness?${params.toString()}`),
        ]);
        this.query = query;
        this.readiness = readiness;
        await nextTick();
        this.drawChart();
        this.drawFunnelChart();
        this.drawRiskChart();
        await this.scrollToResultsIfNeeded();
      } catch (error) {
        console.error(error);
        this.error = "Не удалось загрузить данные. Проверьте параметры запроса.";
      } finally {
        this.loading.query = false;
      }
    },

    async loadCompare() {
      this.loading.compare = true;
      this.error = "";
      try {
        const params = this.buildQueryParams(false);
        if (this.filters.base) params.set("base", this.filters.base);
        if (this.filters.target) params.set("target", this.filters.target);
        this.compare = await fetchJson(`/api/compare?${params.toString()}`);
        await nextTick();
        await this.scrollToResultsIfNeeded();
      } catch (error) {
        console.error(error);
        this.error = "Не удалось загрузить сравнение. Проверьте параметры запроса.";
      } finally {
        this.loading.compare = false;
      }
    },

    buildQueryParams(includeDates) {
      // Единая сборка query string защищает export, chart и таблицы от расхождения фильтров.
      const params = new URLSearchParams();
      ["q", "code", "template", "budget", "source", "post_filter"].forEach((key) => {
        const value = String(this.filters[key] || "").trim();
        if (value) params.set(key, value);
      });
      if (includeDates) {
        params.set("view", "as_of");
        if (this.filters.date) params.set("date", this.filters.date);
      }
      params.set("metrics", this.activeMetricCodes.join(","));
      return params;
    },

    requestResultScroll() {
      this.pendingResultScroll = true;
    },

    async scrollToResultsIfNeeded() {
      if (!this.pendingResultScroll) return;
      this.pendingResultScroll = false;
      await nextTick();
      this.$refs.resultTabs?.scrollIntoView({
        behavior: "smooth",
        block: "start",
      });
    },

    async runWithSuppressedAutoLoad(callback, shouldScroll = false) {
      // Пакетные изменения фильтров применяются атомарно с одним последующим loadData.
      this.suppressAutoLoad = true;
      try {
        callback();
        await nextTick();
      } finally {
        this.suppressAutoLoad = false;
      }
      if (shouldScroll) this.requestResultScroll();
      await this.loadData();
    },

    applyQuickAction(action) {
      const { code, ...cleanAction } = action;
      return this.applyAction({
        reset_scope: true,
        q: "",
        code: "",
        budget: "",
        source: "",
        post_filter: "",
        ...cleanAction,
      }, { scrollToResults: true });
    },

    onCommandInput() {
      this.command.error = "";
      this.command.suggestions = this.buildCommandSuggestions(this.command.text);
    },

    buildCommandSuggestions(text) {
      // Умная строка не вызывает LLM: она распознает частые бюджетные сценарии
      // и превращает их в те же actions, что быстрые кнопки и assistant.
      const value = String(text || "").trim();
      if (!value) return [];
      const lower = value.toLowerCase();
      const suggestions = [];
      const add = (code, title, description, action) => suggestions.push({ code, title, description, action });
      const fullMetrics = ["limit", "obligation", "cash", "agreement", "contract", "payment", "buau"];
      const dateMatch = value.match(/\b(\d{2})[.](\d{2})[.](20\d{2})\b/);
      const parsedDate = dateMatch ? `${dateMatch[3]}-${dateMatch[2]}-${dateMatch[1]}` : "";
      const requestedTemplate = lower.includes("скк") || lower.includes("6105")
        ? "skk"
        : lower.includes("кик") || lower.includes("978")
          ? "kik"
          : lower.includes("970") || lower.includes("2/3")
            ? "two_thirds"
            : ["окв", "капитал", "капвлож"].some((word) => lower.includes(word))
              ? "okv"
              : this.filters.template;

      if (["сравн", "измен", "динамик"].some((word) => lower.includes(word))) {
        add("apply_compare", "Сравнить две даты", "Показать изменения между первой и последней отчетной датой", { mode: "compare", template: requestedTemplate, base: this.activeDateValues[0] || "", target: this.activeDateValues.at(-1) || "", metrics: this.activeMetricCodes });
      }
      if (lower.includes("нет касс")) {
        add("apply_no_cash", "Найти объекты без кассы", "Показать объекты с планом и нулевой кассой", { mode: "slice", template: requestedTemplate, q: "", post_filter: "no_cash", metrics: fullMetrics });
      }
      if (lower.includes("нет оплат") || lower.includes("нет платеж")) {
        add("apply_no_payments", "Найти объекты без оплат", "Показать документы без оплат", { mode: "slice", template: requestedTemplate, q: "", post_filter: "no_payments", metrics: fullMetrics });
      }
      if (lower.includes("нет документ")) {
        add("apply_no_documents", "Найти объекты без документов", "Показать план без документов", { mode: "slice", template: requestedTemplate, q: "", post_filter: "no_documents", metrics: fullMetrics });
      }
      if (lower.includes("низк") && lower.includes("исполн")) {
        add("apply_low_cash", "Найти низкую кассу", "Касса меньше 25% от плана", { mode: "slice", template: requestedTemplate, q: "", post_filter: "low_cash", metrics: fullMetrics });
      }
      if (parsedDate) {
        add("apply_date", `Показать на ${dateMatch[0]}`, "Применить выбранную отчетную дату", { mode: "slice", template: requestedTemplate, q: this.cleanSmartText(value), date: parsedDate, metrics: this.activeMetricCodes });
      }

      if (lower.includes("6105") || lower.includes("скк")) {
        add("apply_skk", "Похоже, вы ищете СКК", "Применить готовый отчёт СКК", { mode: "slice", template: "skk", q: "", code: "", metrics: fullMetrics });
      }
      if (lower.includes("978") || lower.includes("кик")) {
        add("apply_kik", "Похоже, вы ищете КИК", "Применить готовый отчёт КИК", { mode: "slice", template: "kik", q: "", code: "", metrics: ["limit", "obligation", "cash", "agreement", "contract", "payment"] });
      }
      if (lower.includes("970") || lower.includes("2/3")) {
        add("apply_two_thirds", "Похоже, вы ищете 2/3", "Применить отчёт по высвобождаемым средствам", { mode: "slice", template: "two_thirds", q: "", code: "", metrics: ["limit", "obligation", "cash", "agreement"] });
      }
      if (["окв", "капитал", "капвлож"].some((word) => lower.includes(word))) {
        add("apply_okv", "Похоже, вы ищете ОКВ", "Показать объекты капитальных вложений", { mode: "slice", template: "okv", q: "", code: "", metrics: ["limit", "obligation", "cash", "contract", "payment", "buau"] });
      }
      if (["касс", "исполн", "платеж"].some((word) => lower.includes(word))) {
        add("apply_cash", "Показать исполнение и оплаты", "Оставить кассу, платежи и выплаты БУ/АУ", { mode: this.mode, template: this.filters.template, q: value, metrics: ["cash", "payment", "buau"] });
      }
      if (["лимит", "план"].some((word) => lower.includes(word))) {
        add("apply_plan", "Показать плановые суммы", "Оставить лимиты и обязательства", { mode: this.mode, template: this.filters.template, q: value, metrics: ["limit", "obligation"] });
      }
      add("apply_search", "Искать по введённому тексту", "Найти объект, бюджет, получателя или документ", { mode: "slice", template: this.filters.template, q: value, code: /^\d+$/.test(value) ? value : "", metrics: this.activeMetricCodes });
      return suggestions;
    },

    async runCommand() {
      const text = String(this.command.text || "").trim();
      if (!text) return;
      this.command.loading = true;
      this.command.error = "";
      this.command.response = null;
      try {
        const response = await fetchJson("/api/assistant", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            message: text,
            context: {
              mode: this.mode,
              template: this.filters.template,
              date: this.filters.date,
              selected_metrics: this.activeMetricCodes,
              available_dates: this.activeDateValues,
            },
          }),
        });
        await this.applyCommandResponse(response);
      } catch (error) {
        console.error(error);
        this.command.suggestions = this.buildCommandSuggestions(text);
        if (this.command.suggestions[0]) {
          await this.applyCommandSuggestion(this.command.suggestions[0]);
        } else {
          this.command.error = "Не удалось разобрать запрос. Попробуйте уточнить объект, код или сценарий.";
        }
      } finally {
        this.command.loading = false;
      }
    },

    async applyCommandResponse(response) {
      this.command.response = response;
      this.command.source = response?.mode || "";
      this.assistant.response = response;
      this.assistant.history.unshift({ question: this.command.text, response });
      await this.applyAction(response?.action || {}, { scrollToResults: true });
    },

    applyCommandSuggestion(suggestion) {
      return this.applyAction(suggestion.action || {}, { scrollToResults: true });
    },

    onSmartInput() {
      this.onCommandInput();
    },

    buildSmartSuggestions(text) {
      return this.buildCommandSuggestions(text);
    },

    applyFirstSuggestion() {
      return this.runCommand();
    },

    applySmartSuggestion(suggestion) {
      return this.applyCommandSuggestion(suggestion);
    },

    cleanSmartText(value) {
      return String(value || "")
        .replace(/\b\d{2}[.]\d{2}[.]20\d{2}\b/g, "")
        .replace(/\b(на|сравни|сравнить|где|нет|кассы|оплат|платежей|документов|низкое|исполнение)\b/gi, " ")
        .trim();
    },

    async askAssistant() {
      this.command.text = this.assistant.message || this.command.text;
      return this.runCommand();
    },

    applyAssistantAction(action) {
      return this.applyAction(action || {}, { scrollToResults: true });
    },

    runFollowup(action) {
      if (!action) return Promise.resolve();
      if (action.download === "excel") {
        this.exportExcel();
        return Promise.resolve();
      }
      if (action.open === "top_risk" && this.topRisks[0]) {
        return this.openObject(this.topRisks[0]);
      }
      if (action.open_view) {
        this.setView(action.open_view);
      }
      if (action.post_filter) {
        return this.applyProblemFilter(action.post_filter);
      }
      return Promise.resolve();
    },

    async explainResult() {
      this.loading.explain = true;
      this.error = "";
      try {
        const payload = {
          attention_summary: this.query.attention_summary || {},
          compare_insights: this.compare.compare_insights || {},
          readiness: this.readiness || {},
          top_risks: this.topRisks.slice(0, 5).map((item) => ({
            object_key: item.object_key,
            object_name: item.object_name,
            risk_score: item.risk_score,
            risk_label: item.risk_label,
            plan: item.plan,
            reasons: item.reasons || item.problem_reasons || [],
          })),
          filters: {
            mode: this.mode,
            template: this.filters.template,
            date: this.filters.date,
            base: this.filters.base,
            target: this.filters.target,
            post_filter: this.filters.post_filter,
          },
        };
        this.explanation = await fetchJson("/api/explain", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ kind: this.mode === "compare" ? "compare" : "query", payload }),
        });
      } catch (error) {
        console.error(error);
        this.error = "Не удалось получить простое объяснение.";
      } finally {
        this.loading.explain = false;
      }
    },

    async applyAction(action, options = {}) {
      const shouldScroll = options.scrollToResults !== false;
      await this.runWithSuppressedAutoLoad(() => {
        if (action.reset_scope) {
          this.filters.code = "";
          this.filters.budget = "";
          this.filters.source = "";
          this.filters.post_filter = "";
        }
        this.demoMode = action.demo_mode === "skk_risks" ? "skk_risks" : "";
        if (action.mode) this.mode = action.mode;
        if (action.template) this.filters.template = action.template;
        if (action.q !== undefined) this.filters.q = action.q;
        if (action.code !== undefined) this.filters.code = action.code;
        if (action.budget !== undefined) this.filters.budget = action.budget;
        if (action.source !== undefined) this.filters.source = action.source;
        this.filters.post_filter = action.post_filter || "";
        if (action.metrics?.length) this.selectedMetrics = action.metrics;
        if (this.mode === "compare") {
          this.filters.base = action.base || this.activeDateValues[0] || "";
          this.filters.target = action.target || this.activeDateValues.at(-1) || "";
          this.currentView = "changes";
        } else {
          this.filters.date = action.date || this.filters.date || this.activeDateValues.at(-1) || "";
          this.currentView = action.open_view || (action.post_filter ? "problems" : "overview");
        }
        this.explanation = null;
      }, shouldScroll);
    },

    async applyEmptySuggestion(item) {
      await this.runWithSuppressedAutoLoad(() => {
        Object.assign(this.filters, item.patch || {});
      }, true);
    },

    applyProblemFilter(postFilter) {
      return this.applyAction({ mode: "slice", post_filter: postFilter, metrics: ["limit", "obligation", "cash", "agreement", "contract", "payment", "buau"] }, { scrollToResults: true });
    },

    setProblemRiskFilter(filter) {
      this.problemRiskFilter = filter;
    },

    scheduleLoad() {
      clearTimeout(this.debounceTimer);
      this.debounceTimer = setTimeout(() => this.loadData(), 250);
    },

    scheduleChartRedraw() {
      if (this.chartRedraw) this.chartRedraw();
    },

    toggleTheme() {
      this.theme = this.theme === "dark" ? "light" : "dark";
      localStorage.setItem("analytics-theme", this.theme);
      this.applyTheme();
      nextTick(() => {
        this.drawChart();
        this.drawFunnelChart();
        this.drawRiskChart();
      });
    },

    applyTheme() {
      document.documentElement.dataset.theme = this.theme;
    },

    drawChart() {
      // Canvas рисуется вручную, чтобы не добавлять сборку или внешнюю chart-библиотеку.
      const canvas = this.$refs.chart;
      if (!canvas || this.mode !== "slice") return;
      const styles = getComputedStyle(document.documentElement);
      const chartGrid = styles.getPropertyValue("--chart-grid").trim() || "#d9e0e4";
      const chartAxis = styles.getPropertyValue("--chart-axis").trim() || "#63717b";
      const chartText = styles.getPropertyValue("--chart-text").trim() || "#63717b";
      const chartLine = styles.getPropertyValue("--chart-line").trim() || "#0f766e";
      const ratio = window.devicePixelRatio || 1;
      const width = canvas.clientWidth || canvas.parentElement?.clientWidth || 640;
      const height = canvas.clientHeight || Number(canvas.getAttribute("height")) || 280;
      canvas.width = Math.floor(width * ratio);
      canvas.height = Math.floor(height * ratio);
      const ctx = canvas.getContext("2d");
      ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
      ctx.clearRect(0, 0, width, height);
      const points = (this.query.timeline || []).map((row) => ({
        date: row.date,
        value: this.activeMetricCodes.reduce((sum, metric) => sum + Number(row[metric] || 0), 0),
      }));
      if (!points.length) {
        ctx.fillStyle = chartText;
        ctx.font = "14px Arial";
        ctx.fillText("Нет данных", 24, 36);
        return;
      }
      const pad = { left: 72, right: 20, top: 20, bottom: 38 };
      const max = Math.max(1, ...points.map((point) => point.value));
      ctx.strokeStyle = chartAxis;
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(pad.left, pad.top);
      ctx.lineTo(pad.left, height - pad.bottom);
      ctx.lineTo(width - pad.right, height - pad.bottom);
      ctx.stroke();
      [0, 0.5, 1].forEach((tick) => {
        const y = height - pad.bottom - tick * (height - pad.top - pad.bottom);
        ctx.fillStyle = chartText;
        ctx.font = "12px Arial";
        ctx.fillText(this.formatMoney(max * tick), 8, y + 4);
        ctx.strokeStyle = chartGrid;
        ctx.beginPath();
        ctx.moveTo(pad.left, y);
        ctx.lineTo(width - pad.right, y);
        ctx.stroke();
      });
      this.drawLine(ctx, points, chartLine, pad, width, height, max);
      ctx.fillStyle = chartText;
      ctx.font = "12px Arial";
      ctx.fillText(points[0].date, pad.left, height - 12);
      ctx.fillText(points[points.length - 1].date, Math.max(pad.left, width - pad.right - 78), height - 12);
    },

    drawLine(ctx, points, color, pad, width, height, max) {
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
    },

    prepareCanvas(canvas, fallbackHeight = 220) {
      if (!canvas) return null;
      const ratio = window.devicePixelRatio || 1;
      const width = canvas.clientWidth || canvas.parentElement?.clientWidth || 420;
      const height = canvas.clientHeight || Number(canvas.getAttribute("height")) || fallbackHeight;
      canvas.width = Math.floor(width * ratio);
      canvas.height = Math.floor(height * ratio);
      const ctx = canvas.getContext("2d");
      ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
      ctx.clearRect(0, 0, width, height);
      return { ctx, width, height };
    },

    drawNoData(ctx, width, height) {
      const styles = getComputedStyle(document.documentElement);
      ctx.fillStyle = styles.getPropertyValue("--chart-text").trim() || "#63717b";
      ctx.font = "14px Arial";
      ctx.textAlign = "center";
      ctx.fillText("Нет данных", width / 2, height / 2);
      ctx.textAlign = "left";
    },

    drawFunnelChart() {
      const prepared = this.prepareCanvas(this.$refs.funnelChart, 210);
      if (!prepared || this.mode !== "slice" || this.currentView !== "overview") return;
      const { ctx, width, height } = prepared;
      const styles = getComputedStyle(document.documentElement);
      const text = styles.getPropertyValue("--chart-text").trim() || "#63717b";
      const grid = styles.getPropertyValue("--chart-grid").trim() || "#d9e0e4";
      const accent = styles.getPropertyValue("--accent").trim() || "#0f766e";
      const blue = styles.getPropertyValue("--blue").trim() || "#2563eb";
      const warn = styles.getPropertyValue("--warn").trim() || "#d97706";
      const values = [
        ["План", Number(this.funnelValues.plan || 0)],
        ["Документы", Number(this.funnelValues.documents || 0)],
        ["Оплачено", Number(this.funnelValues.paid || 0)],
        ["Касса", Number(this.funnelValues.cash || 0)],
      ];
      const max = Math.max(...values.map((item) => item[1]));
      if (!max) {
        this.drawNoData(ctx, width, height);
        return;
      }
      const colors = [accent, accent, warn, blue];
      const left = 112;
      const right = 28;
      const top = 18;
      const barHeight = 18;
      const gap = 25;
      const usable = Math.max(120, width - left - right);
      ctx.strokeStyle = grid;
      ctx.lineWidth = 1;
      [0, 0.5, 1].forEach((tick) => {
        const x = left + usable * tick;
        ctx.beginPath();
        ctx.moveTo(x, top - 6);
        ctx.lineTo(x, top + values.length * (barHeight + gap) - gap + 9);
        ctx.stroke();
      });
      values.forEach(([label, value], index) => {
        const y = top + index * (barHeight + gap);
        const barWidth = Math.max(value ? 4 : 0, usable * (value / max));
        const percent = max ? Math.round((value / max) * 100) : 0;
        ctx.fillStyle = "rgba(148, 163, 184, 0.16)";
        ctx.fillRect(left, y, usable, barHeight);
        ctx.fillStyle = colors[index];
        ctx.fillRect(left, y, barWidth, barHeight);
        ctx.fillStyle = text;
        ctx.font = "600 12px Arial";
        ctx.fillText(label, 14, y + 14);
        ctx.font = "12px Arial";
        ctx.fillText(`${percent}%`, left + usable - 34, y + 14);
        ctx.fillText(this.formatMoney(value), left, y + barHeight + 16);
      });
    },

    drawRiskChart() {
      const prepared = this.prepareCanvas(this.$refs.riskChart, 210);
      if (!prepared || this.mode !== "slice" || this.currentView !== "overview") return;
      const { ctx, width, height } = prepared;
      const styles = getComputedStyle(document.documentElement);
      const text = styles.getPropertyValue("--chart-text").trim() || "#63717b";
      const grid = styles.getPropertyValue("--chart-grid").trim() || "#d9e0e4";
      const colors = {
        critical: styles.getPropertyValue("--danger").trim() || "#dc2626",
        high: "#f97316",
        medium: styles.getPropertyValue("--warn").trim() || "#d97706",
        low: styles.getPropertyValue("--accent").trim() || "#0f766e",
      };
      const labels = [
        ["critical", "Критичные"],
        ["high", "Высокие"],
        ["medium", "Средние"],
        ["low", "Низкие"],
      ];
      const values = labels.map(([key, label]) => [key, label, Number(this.riskDistribution[key] || 0)]);
      const total = values.reduce((sum, item) => sum + item[2], 0);
      if (!total) {
        this.drawNoData(ctx, width, height);
        return;
      }
      const left = 116;
      const right = 34;
      const top = 20;
      const barHeight = 20;
      const gap = 22;
      const usable = Math.max(120, width - left - right);
      const max = Math.max(...values.map((item) => item[2]), 1);
      ctx.strokeStyle = grid;
      ctx.lineWidth = 1;
      [0, 0.5, 1].forEach((tick) => {
        const x = left + usable * tick;
        ctx.beginPath();
        ctx.moveTo(x, top - 6);
        ctx.lineTo(x, top + values.length * (barHeight + gap) - gap + 8);
        ctx.stroke();
      });
      values.forEach(([key, label, value], index) => {
        const y = top + index * (barHeight + gap);
        const barWidth = Math.max(value ? 4 : 0, usable * (value / max));
        const percent = Math.round((value / total) * 100);
        ctx.fillStyle = "rgba(148, 163, 184, 0.16)";
        ctx.fillRect(left, y, usable, barHeight);
        ctx.fillStyle = colors[key];
        ctx.fillRect(left, y, barWidth, barHeight);
        ctx.fillStyle = text;
        ctx.font = "600 12px Arial";
        ctx.fillText(label, 14, y + 15);
        ctx.font = "12px Arial";
        ctx.fillText(`${value} · ${percent}%`, left + Math.min(usable - 58, Math.max(barWidth + 8, 8)), y + 15);
      });
    },

    setMode(mode) {
      if (!["slice", "compare"].includes(mode) || this.mode === mode) return;
      this.mode = mode;
      this.currentView = mode === "slice" ? "overview" : "changes";
      this.loadData();
    },

    setView(view) {
      this.currentView = view;
      nextTick(() => {
        this.drawChart();
        this.drawFunnelChart();
        this.drawRiskChart();
      });
    },

    resetFilters() {
      this.filters.q = "";
      this.filters.code = "";
      this.filters.template = "all";
      this.filters.budget = "";
      this.filters.source = "";
      this.filters.post_filter = "";
      this.command.text = "";
      this.command.suggestions = [];
      this.command.response = null;
      this.command.source = "";
      this.explanation = null;
      this.demoMode = "";
      this.loadData();
    },

    async openTrace(id) {
      this.loading.trace = true;
      this.error = "";
      this.trace = { id: "", source: "", source_file: "", source_row: "", human_summary: { amount_fields: [] }, raw: {}, normalized: {} };
      this.$refs.traceDialog?.showModal();
      try {
        this.trace = await fetchJson(`/api/trace?id=${encodeURIComponent(id)}`);
      } catch (error) {
        console.error(error);
        this.error = "Не удалось загрузить источник цифры.";
      } finally {
        this.loading.trace = false;
      }
    },

    async openObject(row) {
      if (!row?.object_key) return;
      this.loading.object = true;
      this.objectRowsOpen = false;
      this.objectCard = null;
      this.$refs.objectDialog?.showModal();
      try {
        const params = new URLSearchParams();
        params.set("date", this.mode === "compare" ? this.filters.target : this.filters.date);
        params.set("template", this.filters.template);
        params.set("object_key", row.object_key);
        if (row.budget) params.set("budget", row.budget);
        this.objectCard = await fetchJson(`/api/object?${params.toString()}`);
      } catch (error) {
        console.error(error);
        this.error = "Не удалось открыть карточку объекта.";
      } finally {
        this.loading.object = false;
      }
    },

    exportExcel() {
      const params = this.buildQueryParams(this.mode !== "compare");
      if (this.mode === "compare") {
        params.set("mode", "compare");
        if (this.filters.base) params.set("base", this.filters.base);
        if (this.filters.target) params.set("target", this.filters.target);
      } else if (this.filters.date) {
        params.set("date", this.filters.date);
      }
      window.location.href = `/api/export.xlsx?${params.toString()}`;
    },

    exportCsv() {
      const createdAt = new Date().toLocaleString("ru-RU");
      const header = [
        ["Отчёт", this.activeTemplateLabel],
        ["Дата формирования", createdAt],
        [this.mode === "compare" ? "Сравнение" : "На дату", this.mode === "compare" ? `${this.filters.base} - ${this.filters.target}` : this.currentDateLabel],
        ["Показатели", "План, Документы, Оплачено, Касса, Статус"],
        [],
      ];
      let rows;
      if (this.mode === "compare") {
        const metricCodes = this.activeMetricCodes;
        const metricHeaders = this.activeMetricObjects.flatMap((metric) => [`${metric.label} база`, `${metric.label} цель`, `${metric.label} изменение`]);
        rows = [["Код", "Объект", "Бюджет", ...metricHeaders], ...this.compare.rows.map((row) => [row.object_code, row.object_name, row.budget, ...metricCodes.flatMap((metric) => {
          const values = row.metrics?.[metric] || {};
          return [values.base, values.target, values.delta];
        })])];
      } else {
        rows = [["Объект", "План", "Документы", "Оплачено", "Касса", "Статус"], ...this.simpleRows.map((row) => [
          row.object_name || row.object_code,
          row.plan,
          row.documents,
          row.paid,
          row.cash,
          row.statusLabel,
        ])];
      }
      const csv = [...header, ...rows].map((line) => line.map(csvCell).join(";")).join("\n");
      const blob = new Blob(["\ufeff", csv], { type: "text/csv;charset=utf-8" });
      const link = document.createElement("a");
      link.href = URL.createObjectURL(blob);
      const suffix = this.mode === "compare" ? `compare_${this.filters.template}_${this.filters.base}_${this.filters.target}` : `${this.filters.template}_${this.filters.date || "as_of"}`;
      link.download = `analytics_${suffix}.csv`;
      link.click();
      URL.revokeObjectURL(link.href);
    },

    rowStatus(row) {
      // Статус в таблице следует тем же проблемным причинам, что и backend-фильтры.
      const reasons = row.problem_reasons || [];
      if (reasons.includes("no_documents")) return "no_documents";
      if (reasons.includes("no_payments")) return "no_payments";
      if (reasons.includes("no_cash")) return "no_cash";
      if (reasons.includes("low_cash")) return "low_cash";
      if (reasons.includes("data_gap")) return "data_gap";
      const pipeline = row.pipeline || this.buildPipeline(row);
      if (!reasons.length) {
        if (pipeline.plan > 0 && pipeline.documents === 0) return "no_documents";
        if (pipeline.documents > 0 && pipeline.paid === 0) return "no_payments";
        if (pipeline.plan > 0 && pipeline.cash === 0) return "no_cash";
        if (pipeline.plan > 0 && pipeline.cash / pipeline.plan < 0.25) return "low_cash";
      }
      if (pipeline.plan > 0 && pipeline.cash >= pipeline.plan) return "executed";
      return "normal";
    },

    rowStatusLabel(status) {
      return {
        no_documents: "Нет документов",
        no_payments: "Нет оплат",
        no_cash: "Нет кассы",
        low_cash: "Низкая касса",
        data_gap: "Разрыв данных",
        executed: "Исполнено",
        normal: "Без явных проблем",
      }[status] || "Без явных проблем";
    },

    rowStatusClass(status) {
      return {
        no_documents: "danger",
        no_payments: "danger",
        no_cash: "danger",
        low_cash: "warning",
        data_gap: "warning",
        executed: "ok",
        normal: "neutral",
      }[status] || "neutral";
    },

    buildPipeline(row) {
      const plan = Number(row.limit || 0) + Number(row.obligation || 0);
      const documents = Number(row.agreement || 0) + Number(row.contract || 0);
      const paid = Number(row.payment || 0) + Number(row.buau || 0);
      const cash = Number(row.cash || 0);
      return { plan, documents, paid, cash, missing_steps: [] };
    },

    togglePipeline(row) {
      this.expandedPipelines[row.rowKey] = !this.expandedPipelines[row.rowKey];
    },

    problemReasonLabel(reason) {
      return {
        no_documents: "Нет документов",
        no_payments: "Нет оплат",
        no_cash: "Нет кассы",
        low_cash: "Низкая касса",
        data_gap: "Разрыв данных",
      }[reason] || "Проблема данных";
    },

    riskLabel(level) {
      return {
        critical: "Критичный",
        high: "Высокий",
        medium: "Средний",
        low: "Низкий",
      }[level] || "Низкий";
    },

    riskClass(level) {
      return {
        critical: "danger",
        high: "high",
        medium: "warning",
        low: "neutral",
      }[level] || "neutral";
    },

    hasSource(fragment) {
      const lower = fragment.toLowerCase();
      return (this.meta.sources || []).some((source) => String(source).toLowerCase().includes(lower));
    },

    formatMoney(value) {
      return money.format(Math.round(Number(value || 0)));
    },

    formatPercent(value) {
      if (value === null || value === undefined || Number.isNaN(Number(value))) return "0%";
      return `${percent.format(Number(value))}%`;
    },

    metricHint(code) {
      return {
        limit: "сколько доведено",
        obligation: "сколько принято обязательств",
        cash: "сколько фактически выбыло",
        agreement: "суммы соглашений",
        contract: "суммы договоров",
        payment: "фактические оплаты",
        buau: "выплаты учреждений",
      }[code] || "показатель";
    },

    detailAmount(row) {
      return this.activeMetricCodes.reduce((sum, key) => sum + Number(row[key] || 0), 0);
    },

    deltaClass(value) {
      const number = Number(value || 0);
      if (number > 0) return "positive";
      if (number < 0) return "negative";
      return "zero";
    },
  },
}).mount("#app");
