const { createApp, nextTick } = Vue;

const money = new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 0 });

function csvCell(value) {
  return `"${String(value ?? "").replaceAll('"', '""')}"`;
}

function debounce(fn, delay = 250) {
  let timer;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), delay);
  };
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}

createApp({
  data() {
    return {
      mode: "slice",
      theme: "dark",
      currentView: "overview",

      meta: {
        records: 0,
        budgets: [],
        sources: [],
        snapshots: [],
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
        q: "",
        code: "",
        template: "all",
        budget: "",
        source: "",
        start: "",
        end: "",
        base: "",
        target: "",
      },

      selectedMetrics: [],

      query: {
        totals: {},
        rows: [],
        details: [],
        timeline: [],
        count: 0,
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
        raw: {},
        normalized: {},
      },

      loading: {
        meta: false,
        query: false,
        compare: false,
        trace: false,
      },

      error: "",
      debounceTimer: null,
      chartRedraw: null,
      initialized: false,
    };
  },

  computed: {
    activeTemplateLabel() {
      const template = this.templates.find((item) => item.code === this.filters.template);
      return template ? template.label : "Все данные";
    },

    activeMetricObjects() {
      if (!this.selectedMetrics.length) {
        return this.metrics;
      }
      const selected = new Set(this.selectedMetrics);
      return this.metrics.filter((metric) => selected.has(metric.code));
    },

    activeMetricCodes() {
      const codes = this.activeMetricObjects.map((metric) => metric.code);
      return codes.length ? codes : this.metrics.map((metric) => metric.code);
    },

    metricLabels() {
      return {
        limit: "Лимиты",
        obligation: "БО",
        cash: "Касса",
        agreement: "Соглашения",
        contract: "Контракты",
        payment: "Платежи",
        buau: "БУ/АУ",
      };
    },

    prettyTrace() {
      return JSON.stringify(this.trace, null, 2);
    },

    isCompareMode() {
      return this.mode === "compare";
    },

    viewTabs() {
      if (this.mode === "compare") {
        return [{ code: "changes", label: "Изменения", count: this.compare.rows.length }];
      }
      return [
        { code: "overview", label: "Главное", count: this.query.count || 0 },
        { code: "summary", label: "Итоги", count: this.query.rows.length },
        { code: "records", label: "Записи", count: this.query.details.length },
      ];
    },
  },

  watch: {
    "filters.template"() {
      if (!this.initialized) return;
      this.loadData();
    },
    "filters.budget"() {
      if (!this.initialized) return;
      this.loadData();
    },
    "filters.source"() {
      if (!this.initialized) return;
      this.loadData();
    },
    "filters.start"() {
      if (!this.initialized) return;
      if (this.mode === "slice") this.loadData();
    },
    "filters.end"() {
      if (!this.initialized) return;
      if (this.mode === "slice") this.loadData();
    },
    "filters.base"() {
      if (!this.initialized) return;
      if (this.mode === "compare") this.loadData();
    },
    "filters.target"() {
      if (!this.initialized) return;
      if (this.mode === "compare") this.loadData();
    },
    selectedMetrics: {
      deep: true,
      handler() {
        if (!this.initialized) return;
        this.loadData();
      },
    },
    currentView() {
      nextTick(() => this.drawChart());
    },
  },

  async mounted() {
    this.theme = localStorage.getItem("analytics-theme") || "dark";
    this.applyTheme();
    this.chartRedraw = debounce(() => this.drawChart(), 200);
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
      this.loading.meta = true;
      this.error = "";
      try {
        const [meta, templates, metrics, quality] = await Promise.all([
          fetchJson("/api/meta"),
          fetchJson("/api/catalog/templates"),
          fetchJson("/api/catalog/metrics"),
          fetchJson("/api/quality"),
        ]);

        this.meta = {
          records: 0,
          budgets: [],
          sources: [],
          snapshots: [],
          objects: [],
          ...meta,
        };
        this.templates = templates;
        this.metrics = metrics;
        this.quality = {
          issues: [],
          summary: { warnings: 0, errors: 0 },
          ...quality,
          summary: { warnings: 0, errors: 0, ...(quality.summary || {}) },
        };
        this.selectedMetrics = metrics.map((metric) => metric.code);

        if (this.meta.snapshots.length) {
          this.filters.start = this.meta.snapshots[0];
          this.filters.end = this.meta.snapshots[this.meta.snapshots.length - 1];
          this.filters.base = this.meta.snapshots[0];
          this.filters.target = this.meta.snapshots[this.meta.snapshots.length - 1];
        }

        if (templates.some((template) => template.code === "all")) {
          this.filters.template = "all";
        }
      } catch (error) {
        console.error(error);
        this.error = "Не удалось загрузить справочники. Проверьте сервер и параметры запроса.";
      } finally {
        this.loading.meta = false;
      }
    },

    async loadData() {
      if (this.mode === "compare") {
        await this.loadCompare();
        return;
      }

      this.loading.query = true;
      this.error = "";
      try {
        const params = this.buildQueryParams(true);
        this.query = await fetchJson(`/api/query?${params.toString()}`);
        await nextTick();
        this.drawChart();
      } catch (error) {
        console.error(error);
        this.error = "Не удалось загрузить данные. Проверьте сервер и параметры запроса.";
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
      } catch (error) {
        console.error(error);
        this.error = "Не удалось загрузить сравнение. Проверьте сервер и параметры запроса.";
      } finally {
        this.loading.compare = false;
      }
    },

    buildQueryParams(includeDates) {
      const params = new URLSearchParams();

      ["q", "code", "template", "budget", "source"].forEach((key) => {
        const value = String(this.filters[key] || "").trim();
        if (value) params.set(key, value);
      });

      if (includeDates) {
        if (this.filters.start) params.set("start", this.filters.start);
        if (this.filters.end) params.set("end", this.filters.end);
      }

      params.set("metrics", this.activeMetricCodes.join(","));
      return params;
    },

    scheduleLoad() {
      clearTimeout(this.debounceTimer);
      this.debounceTimer = setTimeout(() => this.loadData(), 250);
    },

    scheduleChartRedraw() {
      if (this.chartRedraw) {
        this.chartRedraw();
      }
    },

    toggleTheme() {
      this.theme = this.theme === "dark" ? "light" : "dark";
      localStorage.setItem("analytics-theme", this.theme);
      this.applyTheme();
      nextTick(() => this.drawChart());
    },

    applyTheme() {
      document.documentElement.dataset.theme = this.theme;
    },

    drawChart() {
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
      ctx.fillText("Выбранные метрики", Math.max(84, width - 180), 16);
      ctx.fillStyle = chartText;
      ctx.fillText(points[0].date, pad.left, height - 12);
      ctx.fillText(points[points.length - 1].date, Math.max(pad.left, width - pad.right - 78), height - 12);
    },

    drawLine(ctx, points, color, pad, width, height, max) {
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

      ctx.fillStyle = color;
      points.forEach((point, index) => {
        const x = pad.left + (points.length === 1 ? 0 : (index / (points.length - 1)) * chartWidth);
        const y = height - pad.bottom - (point.value / max) * chartHeight;
        ctx.beginPath();
        ctx.arc(x, y, 3, 0, Math.PI * 2);
        ctx.fill();
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
      nextTick(() => this.drawChart());
    },

    resetFilters() {
      this.filters.q = "";
      this.filters.code = "";
      this.filters.template = "all";
      this.filters.budget = "";
      this.filters.source = "";
      this.loadData();
    },

    async openTrace(id) {
      this.loading.trace = true;
      this.error = "";
      this.trace = {
        id: "",
        source: "",
        source_file: "",
        source_row: "",
        raw: {},
        normalized: {},
      };
      this.$refs.traceDialog?.showModal();

      try {
        this.trace = await fetchJson(`/api/trace?id=${encodeURIComponent(id)}`);
      } catch (error) {
        console.error(error);
        this.error = "Не удалось загрузить исходную строку.";
      } finally {
        this.loading.trace = false;
      }
    },

    exportCsv() {
      const createdAt = new Date().toISOString();
      const metricCodes = this.activeMetricCodes;
      let lines;

      if (this.mode === "compare") {
        const metricHeaders = this.activeMetricObjects.flatMap((metric) => [
          `${metric.label} база`,
          `${metric.label} цель`,
          `${metric.label} дельта`,
        ]);
        lines = [
          [
            "Дата формирования",
            "Режим",
            "Шаблон",
            "База",
            "Цель",
            "Метрики",
            "Код",
            "Объект",
            "Бюджет",
            ...metricHeaders,
          ],
          ...this.compare.rows.map((row) => [
            createdAt,
            "Сравнение",
            this.activeTemplateLabel,
            this.filters.base,
            this.filters.target,
            metricCodes.join(","),
            row.object_code,
            row.object_name,
            row.budget,
            ...metricCodes.flatMap((metric) => {
              const values = row.metrics?.[metric] || {};
              return [values.base, values.target, values.delta];
            }),
          ]),
        ];
      } else {
        lines = [
          [
            "Дата формирования",
            "Режим",
            "Шаблон",
            "Метрики",
            "Код",
            "Объект",
            "Бюджет",
            "Источники",
            ...this.activeMetricObjects.map((metric) => metric.label),
          ],
          ...this.query.rows.map((row) => [
            createdAt,
            "Срез",
            this.activeTemplateLabel,
            metricCodes.join(","),
            row.object_code,
            row.object_name,
            row.budget,
            row.sources,
            ...metricCodes.map((metric) => row[metric]),
          ]),
        ];
      }

      const csv = lines.map((line) => line.map(csvCell).join(";")).join("\n");
      const blob = new Blob(["\ufeff", csv], { type: "text/csv;charset=utf-8" });
      const link = document.createElement("a");
      link.href = URL.createObjectURL(blob);
      link.download = "analytics_selection.csv";
      link.click();
      URL.revokeObjectURL(link.href);
    },

    formatMoney(value) {
      return money.format(Math.round(Number(value || 0)));
    },

    formatPercent(value) {
      if (value === null || value === undefined || Number.isNaN(Number(value))) {
        return "0%";
      }
      return `${new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 1 }).format(Number(value))}%`;
    },

    metricHint(code) {
      return {
        limit: "Доведенные лимиты",
        obligation: "Принятые обязательства",
        cash: "Кассовые выплаты",
        agreement: "Суммы соглашений",
        contract: "Контракты и договоры",
        payment: "Факты оплаты",
        buau: "Выплаты учреждений",
      }[code] || "Показатель выборки";
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
