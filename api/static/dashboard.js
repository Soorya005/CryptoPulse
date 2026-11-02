/**
 * dashboard.js — Logic & State Controller for the Detector Comparison Dashboard.
 */

// State storage
let comparisonData = null;
let delayStatsData = null;
let alertStatsData = null;
let currentDetectorFilter = "all";
let currentScenarioFilter = "all";
let currentSearchTerm = "";
let sortColumn = "scenario";
let sortDirection = "asc";

document.addEventListener("DOMContentLoaded", () => {
  initDashboard();
  setupEventListeners();
});

/**
 * Initialize dashboard by loading all API data concurrently.
 */
async function initDashboard() {
  const refreshBtn = document.getElementById("refreshBtn");
  if (refreshBtn) refreshBtn.classList.add("loading");

  try {
    const [compRes, delayRes, statsRes] = await Promise.allSettled([
      fetch("/detectors/comparison").then((r) => (r.ok ? r.json() : null)),
      fetch("/metrics/delay").then((r) => (r.ok ? r.json() : null)),
      fetch("/alerts/stats").then((r) => (r.ok ? r.json() : null)),
    ]);

    if (compRes.status === "fulfilled" && compRes.value) {
      comparisonData = compRes.value;
      renderExecutiveCards(comparisonData);
      renderComparisonTable(comparisonData);
      renderArchitecturalCards(comparisonData);
      renderRecommendations(comparisonData);
      document.getElementById("lastUpdated").textContent = new Date(
        comparisonData.timestamp
      ).toLocaleTimeString();
    }

    if (delayRes.status === "fulfilled" && delayRes.value) {
      delayStatsData = delayRes.value;
      renderOperationalDelay(delayStatsData);
    }

    if (statsRes.status === "fulfilled" && statsRes.value) {
      alertStatsData = statsRes.value;
      renderLiveAlertStats(alertStatsData);
    }
  } catch (err) {
    console.error("Dashboard initialization error:", err);
  } finally {
    if (refreshBtn) refreshBtn.classList.remove("loading");
  }
}

/**
 * Wire up toolbar and interactive filter buttons.
 */
function setupEventListeners() {
  // Detector filter buttons
  document.querySelectorAll(".btn-filter").forEach((btn) => {
    btn.addEventListener("click", (e) => {
      document
        .querySelectorAll(".btn-filter")
        .forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      currentDetectorFilter = btn.dataset.filter;
      applyFiltersAndRender();
    });
  });

  // Scenario select dropdown
  const scenarioSelect = document.getElementById("scenarioSelect");
  if (scenarioSelect) {
    scenarioSelect.addEventListener("change", (e) => {
      currentScenarioFilter = e.target.value;
      applyFiltersAndRender();
    });
  }

  // Search input
  const searchInput = document.getElementById("searchInput");
  if (searchInput) {
    searchInput.addEventListener("input", (e) => {
      currentSearchTerm = e.target.value.toLowerCase().trim();
      applyFiltersAndRender();
    });
  }

  // Refresh button
  const refreshBtn = document.getElementById("refreshBtn");
  if (refreshBtn) {
    refreshBtn.addEventListener("click", () => {
      initDashboard();
    });
  }

  // Simulator Run button
  const simRunBtn = document.getElementById("simRunBtn");
  if (simRunBtn) {
    simRunBtn.addEventListener("click", handleRunSimulation);
  }

  // Table column sorting
  document.querySelectorAll("th.sortable").forEach((th) => {
    th.addEventListener("click", () => {
      const col = th.dataset.col;
      if (sortColumn === col) {
        sortDirection = sortDirection === "asc" ? "desc" : "asc";
      } else {
        sortColumn = col;
        sortDirection = "asc";
      }
      applyFiltersAndRender();
    });
  });
}

/**
 * Render Executive Metric Hero Cards.
 */
function renderExecutiveCards(data) {
  if (!data || !data.detectors) return;

  const tDet = data.detectors.find((d) => d.name === "FixedThreshold");
  const zDet = data.detectors.find((d) => d.name === "ZScore");

  if (tDet) {
    document.getElementById("tPrecision").textContent = `${(
      tDet.overall_precision * 100
    ).toFixed(1)}%`;
    document.getElementById("tRecall").textContent = `${(
      tDet.overall_recall * 100
    ).toFixed(1)}%`;
    document.getElementById("tF1").textContent = tDet.overall_f1.toFixed(3);
    document.getElementById("tDelay").textContent = `${tDet.avg_delay_seconds.toFixed(1)}s (${tDet.avg_delay_windows.toFixed(1)}w)`;
    document.getElementById("tBar").style.width = `${tDet.overall_recall * 100}%`;
  }

  if (zDet) {
    document.getElementById("zPrecision").textContent = `${(
      zDet.overall_precision * 100
    ).toFixed(1)}%`;
    document.getElementById("zRecall").textContent = `${(
      zDet.overall_recall * 100
    ).toFixed(1)}%`;
    document.getElementById("zF1").textContent = zDet.overall_f1.toFixed(3);
    document.getElementById("zDelay").textContent = `${zDet.avg_delay_seconds.toFixed(1)}s (${zDet.avg_delay_windows.toFixed(1)}w)`;
    document.getElementById("zBar").style.width = `${zDet.overall_recall * 100}%`;
  }

  document.getElementById("totalScenariosCount").textContent =
    data.scenario_matrix ? data.scenario_matrix.length : 6;
  document.getElementById("totalWindowsCount").textContent =
    data.macro_comparison ? data.macro_comparison.total_windows_evaluated : 126;
}

/**
 * Format score as HTML badge with appropriate color styling.
 */
function formatScoreBadge(score) {
  const pct = (score * 100).toFixed(0);
  let cls = "score-low";
  if (score >= 0.99) cls = "score-perfect";
  else if (score >= 0.8) cls = "score-good";
  else if (score >= 0.5) cls = "score-mid";

  return `<span class="metric-badge ${cls}">${pct}%</span>`;
}

/**
 * Format delay as an interactive badge.
 */
function formatDelayChip(delaySeconds, delayWindows) {
  if (delaySeconds === null || delaySeconds === undefined) {
    return `<span class="delay-chip" style="background: rgba(255,255,255,0.05); color: #94a3b8;">N/A (Missed)</span>`;
  }
  if (delaySeconds === 0) {
    return `<span class="delay-chip delay-zero">0.0s <small>(Instant)</small></span>`;
  }
  return `<span class="delay-chip delay-warn">${delaySeconds.toFixed(1)}s <small>(+${delayWindows}w)</small></span>`;
}

/**
 * Format winner badge.
 */
function formatWinnerTag(winner) {
  if (winner === "Z-Score") {
    return `<span class="winner-tag winner-zscore">⚡ Z-Score</span>`;
  }
  if (winner === "Threshold") {
    return `<span class="winner-tag winner-threshold">🛡️ Threshold</span>`;
  }
  return `<span class="winner-tag winner-tie">🤝 Parity</span>`;
}

/**
 * Filter, sort, and render comparison table rows.
 */
function applyFiltersAndRender() {
  if (!comparisonData || !comparisonData.scenario_matrix) return;

  const rows = [];
  comparisonData.scenario_matrix.forEach((item) => {
    // Check scenario filter
    if (
      currentScenarioFilter !== "all" &&
      item.scenario !== currentScenarioFilter
    ) {
      return;
    }

    // Check search term
    const matchesSearch =
      item.scenario_display.toLowerCase().includes(currentSearchTerm) ||
      item.description.toLowerCase().includes(currentSearchTerm) ||
      item.notes.toLowerCase().includes(currentSearchTerm);
    if (!matchesSearch) return;

    // FixedThreshold entry
    if (
      currentDetectorFilter === "all" ||
      currentDetectorFilter === "threshold"
    ) {
      rows.push({
        scenario: item.scenario,
        scenario_display: item.scenario_display,
        description: item.description,
        expected: item.expected_anomalies,
        detector: "FixedThreshold",
        detector_display: "FixedThreshold",
        fired: item.threshold_fired,
        tp: item.threshold_tp,
        fp: item.threshold_fp,
        fn: item.threshold_fn,
        precision: item.threshold_precision,
        recall: item.threshold_recall,
        f1: item.threshold_f1,
        delay_s: item.threshold_delay_s,
        delay_win: item.threshold_delay_win,
        winner: item.winner,
        notes: item.notes,
      });
    }

    // ZScore entry
    if (currentDetectorFilter === "all" || currentDetectorFilter === "zscore") {
      rows.push({
        scenario: item.scenario,
        scenario_display: item.scenario_display,
        description: item.description,
        expected: item.expected_anomalies,
        detector: "ZScore",
        detector_display: "ZScore",
        fired: item.zscore_fired,
        tp: item.zscore_tp,
        fp: item.zscore_fp,
        fn: item.zscore_fn,
        precision: item.zscore_precision,
        recall: item.zscore_recall,
        f1: item.zscore_f1,
        delay_s: item.zscore_delay_s,
        delay_win: item.zscore_delay_win,
        winner: item.winner,
        notes: item.notes,
      });
    }
  });

  // Sort rows
  rows.sort((a, b) => {
    let valA = a[sortColumn];
    let valB = b[sortColumn];
    if (valA === undefined || valA === null) valA = -1;
    if (valB === undefined || valB === null) valB = -1;

    if (typeof valA === "string") {
      return sortDirection === "asc"
        ? valA.localeCompare(valB)
        : valB.localeCompare(valA);
    }
    return sortDirection === "asc" ? valA - valB : valB - valA;
  });

  renderTableRows(rows);
}

/**
 * Write rows into table body.
 */
function renderTableRows(rows) {
  const tbody = document.getElementById("comparisonTableBody");
  if (!tbody) return;

  if (rows.length === 0) {
    tbody.innerHTML = `
      <tr>
        <td colspan="10" style="text-align: center; padding: 2.5rem; color: var(--text-dim);">
          No scenario records match your filter criteria.
        </td>
      </tr>
    `;
    return;
  }

  tbody.innerHTML = rows
    .map((r) => {
      const isZScore = r.detector === "ZScore";
      const pillClass = isZScore ? "zscore" : "threshold";
      const icon = isZScore ? "📈" : "🎯";

      return `
      <tr>
        <td>
          <div class="scenario-name">
            <span class="scenario-badge" style="background: ${isZScore ? "rgba(6,182,212,0.15)" : "rgba(99,102,241,0.15)"}; color: ${isZScore ? "var(--accent-cyan)" : "var(--accent-indigo)"};">${icon}</span>
            <div>
              <strong>${r.scenario_display}</strong>
              <div class="scenario-desc">${r.description}</div>
            </div>
          </div>
        </td>
        <td>
          <span class="detector-pill ${pillClass}">
            ${r.detector_display}
          </span>
        </td>
        <td style="font-family: var(--font-mono); font-weight: 600;">
          ${r.expected}
        </td>
        <td style="font-family: var(--font-mono); font-weight: 600; color: ${r.fired > 0 ? "var(--accent-cyan)" : "var(--text-dim)"};">
          ${r.fired}
        </td>
        <td>
          <span style="font-family: var(--font-mono); font-size: 0.8rem; color: #cbd5e1;">
            <strong style="color: var(--accent-emerald);">${r.tp}</strong> / 
            <strong style="color: var(--accent-amber);">${r.fp}</strong> / 
            <strong style="color: var(--accent-rose);">${r.fn}</strong>
          </span>
        </td>
        <td>${formatScoreBadge(r.precision)}</td>
        <td>${formatScoreBadge(r.recall)}</td>
        <td style="font-family: var(--font-mono); font-weight: 700; color: #f8fafc;">
          ${r.f1.toFixed(3)}
        </td>
        <td>${formatDelayChip(r.delay_s, r.delay_win)}</td>
        <td>${formatWinnerTag(r.winner)}</td>
      </tr>
    `;
    })
    .join("");
}

function renderComparisonTable(data) {
  applyFiltersAndRender();
}

/**
 * Render Head-to-Head Architectural Trade-offs.
 */
function renderArchitecturalCards(data) {
  if (!data || !data.detectors) return;

  const tDet = data.detectors.find((d) => d.name === "FixedThreshold");
  const zDet = data.detectors.find((d) => d.name === "ZScore");

  if (tDet) {
    const listHtml = tDet.strengths
      .map(
        (s) => `
      <li>
        <svg class="icon-check" viewBox="0 0 20 20" fill="currentColor">
          <path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clip-rule="evenodd" />
        </svg>
        <span>${s}</span>
      </li>
    `
      )
      .join("");

    const weakHtml = tDet.weaknesses
      .map(
        (w) => `
      <li>
        <svg class="icon-warn" viewBox="0 0 20 20" fill="currentColor">
          <path fill-rule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clip-rule="evenodd" />
        </svg>
        <span>${w}</span>
      </li>
    `
      )
      .join("");

    document.getElementById("thresholdStrengths").innerHTML =
      listHtml + weakHtml;
  }

  if (zDet) {
    const listHtml = zDet.strengths
      .map(
        (s) => `
      <li>
        <svg class="icon-check" viewBox="0 0 20 20" fill="currentColor">
          <path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clip-rule="evenodd" />
        </svg>
        <span>${s}</span>
      </li>
    `
      )
      .join("");

    const weakHtml = zDet.weaknesses
      .map(
        (w) => `
      <li>
        <svg class="icon-warn" viewBox="0 0 20 20" fill="currentColor">
          <path fill-rule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clip-rule="evenodd" />
        </svg>
        <span>${w}</span>
      </li>
    `
      )
      .join("");

    document.getElementById("zscoreStrengths").innerHTML = listHtml + weakHtml;
  }
}

/**
 * Render production deployment recommendations.
 */
function renderRecommendations(data) {
  if (!data || !data.recommendations) return;
  const rec = data.recommendations;
  document.getElementById("recProduction").textContent =
    rec.production_strategy || "";
  document.getElementById("recVolatility").textContent =
    rec.high_volatility_pairs || "";
  document.getElementById("recCircuit").textContent =
    rec.circuit_breaker_protection || "";
}

/**
 * Render ClickHouse live operational delay statistics.
 */
function renderOperationalDelay(stats) {
  if (!stats) return;
  document.getElementById("opTotalAlerts").textContent =
    stats.total_alerts || 0;
  document.getElementById("opAvgDelay").textContent = `${(
    stats.avg_delay_seconds || 0
  ).toFixed(2)}s`;
  document.getElementById("opMedianDelay").textContent = `${(
    stats.median_delay_seconds || 0
  ).toFixed(2)}s`;
  document.getElementById("opP95Delay").textContent = `${(
    stats.p95_delay_seconds || 0
  ).toFixed(2)}s`;
}

/**
 * Render recent alert counts.
 */
function renderLiveAlertStats(stats) {
  if (!stats) return;
  const badge = document.getElementById("liveAlerts1h");
  if (badge) {
    badge.textContent = `${stats.recent_alerts_1h || 0} in last 1h`;
  }
}

/**
 * Interactive Simulation Execution.
 */
async function handleRunSimulation() {
  const scenarioVal = document.getElementById("simScenarioSelect").value;
  const resultBox = document.getElementById("simResultOutput");
  resultBox.textContent = `Running simulation for '${scenarioVal}' across FixedThreshold and ZScore detectors...`;

  try {
    const res = await fetch(`/metrics/evaluation?scenario=${scenarioVal}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();

    resultBox.innerHTML = `
      <div style="color: var(--accent-emerald); font-weight: 700; margin-bottom: 0.5rem;">
        ✓ Simulation Successful (${new Date(data.timestamp).toLocaleTimeString()})
      </div>
      <div><strong>Macro Precision:</strong> ${(data.overall_precision * 100).toFixed(1)}%</div>
      <div><strong>Macro Recall:</strong> ${(data.overall_recall * 100).toFixed(1)}%</div>
      <div><strong>Macro F1:</strong> ${data.overall_f1_score.toFixed(3)}</div>
      <div><strong>Avg Detection Delay:</strong> ${data.avg_detection_delay_seconds.toFixed(2)}s (${data.avg_detection_delay_windows} windows)</div>
      <div style="margin-top: 0.5rem; border-top: 1px solid rgba(255,255,255,0.1); padding-top: 0.5rem;">
        ${data.evaluations
          .map(
            (e) => `
          <div><strong>[${e.detector_name}]</strong> TP: ${e.true_positives}, FP: ${e.false_positives}, FN: ${e.false_negatives} | Precision: ${(e.precision * 100).toFixed(1)}% | Recall: ${(e.recall * 100).toFixed(1)}% | Delay: ${e.mean_delay_seconds !== null ? e.mean_delay_seconds + "s" : "N/A"}</div>
        `
          )
          .join("")}
      </div>
    `;
  } catch (err) {
    resultBox.innerHTML = `<span style="color: var(--accent-rose);">Simulation failed: ${err.message}</span>`;
  }
}
