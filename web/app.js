'use strict';

const API = '';

async function api(path, opts = {}) {
  const r = await fetch(API + '/api' + path, opts);
  if (!r.ok) throw new Error(`API ${path} -> ${r.status}`);
  return r.json();
}

async function apiPost(path, body = {}) {
  return api(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
}

async function apiDelete(path) {
  const r = await fetch(API + '/api' + path, { method: 'DELETE' });
  if (!r.ok) throw new Error(`DELETE ${path} -> ${r.status}`);
  return r.json();
}

function el(id) { return document.getElementById(id); }

function fmt(n) {
  if (n == null) return '-';
  return n.toLocaleString();
}

function fmtDate(s) {
  if (!s) return 'undated';
  return s.substring(0, 10);
}

function showToast(message, type = 'info') {
  const root = el('toast-root');
  if (!root) return;

  const toast = document.createElement('div');
  toast.className = `toast ${type === 'ok' ? 'toast--ok' : ''} ${type === 'err' ? 'toast--err' : ''}`.trim();
  toast.textContent = message;
  root.appendChild(toast);

  setTimeout(() => toast.remove(), 2800);
}

function setBusy(node, busy) {
  if (!node) return;
  node.classList.toggle('is-loading', busy);
}

function setEmpty(containerId, message, isError = false) {
  const node = el(containerId);
  if (!node) return;
  node.innerHTML = `<div class="empty-state ${isError ? 'red' : ''}">${escHtml(message)}</div>`;
}

function setSkeleton(containerId, variant = 'thumb', count = 8) {
  const node = el(containerId);
  if (!node) return;
  const blocks = [];
  for (let i = 0; i < count; i += 1) {
    blocks.push(`<div class="skeleton skeleton-${variant}" aria-hidden="true"></div>`);
  }
  node.innerHTML = blocks.join('');
}

const tabBtns = document.querySelectorAll('.tab-btn');
const tabPanes = document.querySelectorAll('.tab-pane');
let activeTab = 'dashboard';

function switchTab(tabId) {
  activeTab = tabId;
  tabBtns.forEach(b => b.classList.remove('active'));
  tabPanes.forEach(p => p.classList.remove('active'));

  const btn = document.querySelector(`.tab-btn[data-tab="${tabId}"]`);
  if (btn) btn.classList.add('active');

  const pane = el(`tab-${tabId}`);
  if (pane) pane.classList.add('active');
  updateSidebarActiveTab(tabId);
  if (tabId === 'dashboard') updateLogTail();

  switch (tabId) {
    case 'clusters': loadClusters(); break;
    case 'objects': loadTagBrowser(); break;
    case 'photos':
      refreshPhotoFilters().finally(() => loadPhotos(1));
      break;
    case 'settings': loadSettings(); break;
    default: break;
  }
}

tabBtns.forEach(btn => {
  btn.addEventListener('click', () => {
    switchTab(btn.dataset.tab);
  });
});

const PHASE_DEFS = [
  { id: 'preflight', num: 0, name: 'Preflight' },
  { id: 'pull', num: 1, name: 'Pull' },
  { id: 'process', num: 2, name: 'Process' },
  { id: 'cluster', num: 3, name: 'Cluster' },
  { id: 'organize', num: 4, name: 'Organize' },
  { id: 'tag', num: 5, name: 'Tag' },
  { id: 'push', num: 6, name: 'Push' },
  { id: 'verify', num: 7, name: 'Verify' },
];

let autoRefreshTimer = null;
let statusData = null;
let _prevProgress = {};
let _etaCache = {};
const ETA_CACHE_TTL_MS = 90 * 1000;
const ETA_MIN_ITEMS_FOR_CONFIDENCE = 40;
const ETA_MIN_ELAPSED_MIN_FOR_CONFIDENCE = 4;
const TAB_LABELS = {
  dashboard: 'Dashboard',
  clusters: 'Cluster Review',
  objects: 'Objects & Pets',
  photos: 'Photo Browser',
  settings: 'Settings',
};
const SIDEBAR_SHORTCUTS = {
  shared: [
    { keys: 'Ctrl+K', label: 'command palette' },
    { keys: '1..5', label: 'switch tabs' },
    { keys: 'Esc', label: 'close overlay' },
  ],
  dashboard: [
    { keys: 'Run', label: 'kick off next phase' },
  ],
  clusters: [
    { keys: 'Enter', label: 'approve cluster' },
    { keys: 'U', label: 'untag selection or cluster' },
    { keys: 'N', label: 'mark cluster as noise' },
    { keys: 'Shift+Click', label: 'range select faces' },
  ],
  objects: [
    { keys: 'Click', label: 'open a tag gallery' },
  ],
  photos: [
    { keys: '/', label: 'focus photo query' },
    { keys: 'Enter', label: 'run quick query' },
  ],
  settings: [
    { keys: 'Save', label: 'apply config updates' },
  ],
};

function runningBackgroundJobs(jobs = []) {
  return jobs.filter(job => job.status === 'running');
}

function backgroundJobErrors(jobs = []) {
  return jobs.filter(job => job.status === 'error');
}

function backgroundJobLabel(job) {
  if (!job) return '-';
  return job.detail || String(job.job_name || '').replace(/_/g, ' ');
}

function backgroundJobProgress(job) {
  if (!job) return '-';
  if (job.progress_total > 0) {
    const pct = Math.round((job.progress_current / job.progress_total) * 100);
    return `${pct}% (${fmt(job.progress_current)} / ${fmt(job.progress_total)})`;
  }
  return job.status ? job.status.toUpperCase() : '-';
}

function normalizeStatusSnapshot(snapshotOrPhases, bgJobs = []) {
  if (Array.isArray(snapshotOrPhases)) {
    return {
      phases: snapshotOrPhases,
      background_jobs: bgJobs || [],
      counts: {},
      sidebar: {},
      nvidia: null,
    };
  }
  return {
    phases: snapshotOrPhases?.phases || [],
    background_jobs: snapshotOrPhases?.background_jobs || bgJobs || [],
    counts: snapshotOrPhases?.counts || {},
    sidebar: snapshotOrPhases?.sidebar || {},
    nvidia: snapshotOrPhases?.nvidia || null,
  };
}

function activePhaseBadgeText(running, runningJobs = []) {
  if (running) return running.phase.toUpperCase();
  if (runningJobs[0]) return 'BACKGROUND';
  return 'IDLE';
}

function sidebarMachineStateLabel(running, runningJobs = [], errors = 0) {
  if (running || runningJobs.length) return 'BUSY';
  if (errors > 0) return 'DEGRADED';
  return 'READY';
}

function railContextRow(label, value, meta = '') {
  const valueText = value || '-';
  const longValue = String(valueText).length > 26;
  return `
    <div class="rail-context-row">
      <div class="rail-context-head">
        <span class="rail-context-label">${escHtml(label)}</span>
        <span class="rail-context-value${longValue ? ' rail-context-value--long' : ''}">${escHtml(valueText)}</span>
      </div>
      ${meta ? `<div class="rail-context-meta">${escHtml(meta)}</div>` : ''}
    </div>
  `;
}

function currentPhotoFilterSummary() {
  if (_lastSearchQuery) return `Semantic: ${_lastSearchQuery}`;

  const filters = [];
  const person = el('filter-person')?.value;
  const tag = el('filter-tag')?.value;
  const year = el('filter-year')?.value;
  const month = el('filter-month')?.value;
  const undated = !!el('filter-undated')?.checked;

  if (person) filters.push(`person ${person}`);
  if (tag) filters.push(`tag ${tag}`);
  if (year) filters.push(`year ${year}`);
  if (month) filters.push(`month ${month}`);
  if (undated) filters.push('undated');

  const quickQuery = (el('quick-search')?.value || '').trim();
  if (filters.length) return filters.join(' | ');
  if (quickQuery) return `Quick: ${quickQuery}`;
  return 'All photos';
}

function dashboardContextRows(snapshot) {
  const phases = snapshot.phases || [];
  const counts = snapshot.counts || {};
  const sidebar = snapshot.sidebar || {};
  const running = phases.find(phase => phase.status === 'running');
  const stalePhases = phases.filter(phase => phase.is_stale);
  const nextPhase = phases.find(phase => phase.status !== 'complete');
  const nextPhaseDef = PHASE_DEFS.find(def => def.id === (nextPhase?.phase || ''));
  const nextValue = running
    ? `${running.phase.toUpperCase()} live`
    : nextPhase
      ? (nextPhaseDef?.name || nextPhase.phase || 'Pending').toUpperCase()
      : 'All clear';
  const nextMeta = running
    ? _phaseCountLine(running.phase, running, counts)
    : nextPhase?.stale_reason || (stalePhases.length ? `${stalePhases.length} stale downstream outputs need reruns.` : 'No active blockers.');
  const batchValue = sidebar.batch_manifest_name || 'Full library';
  const batchMeta = sidebar.test_year_scope
    ? `Year scope ${sidebar.test_year_scope} | active manifest`
    : sidebar.batch_manifest_active
      ? 'Manifest staging is active'
      : 'Processing the default library scope';
  const backlogValue = `${fmt(counts.pending_clusters || 0)} pending | ${fmt(counts.noise_clusters || 0)} noise`;
  const backlogMeta = `${fmt(counts.labeled_people || 0)} people labeled | ${fmt(counts.total_faces || 0)} faces indexed`;
  const searchValue = sidebar.search_layer_enabled ? 'Online' : 'Off';
  const searchMeta = sidebar.search_layer_enabled
    ? `OCR ${sidebar.search_ocr_enabled ? 'enabled' : 'disabled'} | Photo search ready`
    : 'Semantic search is disabled';
  return [
    railContextRow('Next Action', nextValue, nextMeta),
    railContextRow('Batch Scope', batchValue, batchMeta),
    railContextRow('Review Backlog', backlogValue, backlogMeta),
    railContextRow('Search Layer', searchValue, searchMeta),
  ];
}

function clusterContextRows(snapshot) {
  const counts = snapshot.counts || {};
  const cluster = typeof selectedClusterRecord === 'function' ? selectedClusterRecord() : null;
  const prototypeGroup = Array.isArray(prototypeGroups)
    ? prototypeGroups.find(group => prototypeLabelKey(group.person_label) === prototypeLabelKey(selectedPrototypeLabel))
    : null;
  const modeValue = isPrototypeReviewMode()
    ? `By Person${Number.isFinite(prototypeSourceClusterId) ? ' scoped' : ''}`
    : isPersonReviewMode()
      ? 'Whole File'
      : 'By Cluster';
  const modeMeta = isPrototypeReviewMode()
    ? Number.isFinite(prototypeSourceClusterId)
      ? `Splitting cluster ${prototypeSourceClusterId} by best prototype match`
      : 'Global prototype triage across unlabeled and noise faces'
    : isPersonReviewMode()
      ? 'Worst matches first for cleanup'
      : 'Cluster-by-cluster review flow';

  let focusValue = 'No cluster selected';
  let focusMeta = 'Pick a cluster to begin review.';
  if (isPrototypeReviewMode() && prototypeGroup) {
    focusValue = prototypeGroup.display_label || prototypeGroup.person_label || 'Unknown';
    focusMeta = `${fmt(prototypeGroup.face_count || 0)} faces | avg ${Number(prototypeGroup.avg_similarity || 0).toFixed(2)} similarity`;
  } else if (isPersonReviewMode()) {
    focusValue = clusterReviewScope.personLabel || personReviewData?.person_label || cluster?.person_label || 'Whole file';
    focusMeta = personReviewData
      ? `${fmt(personReviewData.face_count || 0)} faces across ${fmt(personReviewData.cluster_count || 0)} clusters`
      : 'Loading person-wide review...';
  } else if (cluster) {
    focusValue = cluster.person_label || `Cluster ${cluster.cluster_id}`;
    focusMeta = clusterMetaLine(cluster) || (cluster.is_noise ? 'Noise cluster review' : 'Unlabeled review cluster');
  }

  const selectionValue = isPersonReviewMode()
    ? `${selectedFaceIds.size} marked`
    : `${selectedFaceIds.size} selected`;
  const selectionMeta = selectedFaceIds.size > 0
    ? 'Use Move Selected Faces or Untag to apply the current selection.'
    : 'Shift+Click ranges work in the crop grid.';
  const queueValue = `${fmt(counts.pending_clusters || 0)} pending | ${fmt(counts.noise_clusters || 0)} noise`;
  const queueMeta = `${fmt(counts.labeled_clusters || 0)} / ${fmt(counts.total_clusters || 0)} labeled | ${fmt(counts.labeled_people || 0)} people`;

  return [
    railContextRow('Review Mode', modeValue, modeMeta),
    railContextRow('Focus', focusValue, focusMeta),
    railContextRow('Selection', selectionValue, selectionMeta),
    railContextRow('Queue', queueValue, queueMeta),
  ];
}

function objectContextRows(snapshot) {
  const counts = snapshot.counts || {};
  const tagValue = selectedTag || 'No tag selected';
  const tagMeta = el('obj-count')?.textContent || 'Choose a tag to load its gallery.';
  const catalogValue = `${fmt(counts.total_photos || 0)} photos`;
  const catalogMeta = `${fmt(counts.total_detections || 0)} detections indexed | approved tags only`;
  const focusValue = selectedTag ? 'Tag gallery live' : 'Browse approved tags';
  const focusMeta = selectedTag ? 'Click photos to inspect larger previews.' : 'Choose a tag from the left rail to load photos.';
  return [
    railContextRow('Tag Focus', tagValue, tagMeta),
    railContextRow('Catalog', catalogValue, catalogMeta),
    railContextRow('Mode', focusValue, focusMeta),
  ];
}

function photoContextRows(snapshot) {
  const counts = snapshot.counts || {};
  const browseValue = _lastSearchQuery ? 'Semantic search' : 'Photo browser';
  const browseMeta = currentPhotoFilterSummary();
  const resultValue = el('search-result-count')?.textContent || el('photo-count')?.textContent || `${fmt(counts.total_photos || 0)} photos`;
  const resultMeta = _lastSearchQuery
    ? 'Semantic results are shown in the main grid.'
    : 'Structured filters and quick query drive the current view.';
  const filterValue = currentPhotoFilterSummary();
  const filterMeta = el('filter-undated')?.checked ? 'Undated-only filter is active.' : 'Use quick query or semantic search to narrow faster.';
  return [
    railContextRow('Browse Mode', browseValue, browseMeta),
    railContextRow('Result Set', resultValue, resultMeta),
    railContextRow('Filters', filterValue, filterMeta),
  ];
}

function settingsContextRows(snapshot) {
  const sidebar = snapshot.sidebar || {};
  const nvidia = snapshot.nvidia || {};
  const batchValue = sidebar.batch_manifest_name || 'Full library';
  const batchMeta = sidebar.batch_manifest_path || 'No batch manifest is currently active.';
  const yearValue = sidebar.test_year_scope || 'All years';
  const yearMeta = sidebar.test_year_scope ? 'Test scope is constrained.' : 'Pipeline can span the full library.';
  const searchValue = sidebar.search_layer_enabled ? 'Enabled' : 'Disabled';
  const searchMeta = sidebar.search_layer_enabled
    ? `OCR ${sidebar.search_ocr_enabled ? 'enabled' : 'disabled'}`
    : 'Semantic photo search is off.';
  const burstValue = nvidia.label || 'Unavailable';
  const burstMeta = Number.isFinite(nvidia.requests_remaining)
    ? `${fmt(nvidia.requests_remaining)} req left | ${fmt(nvidia.tokens_remaining)} tok left`
    : 'Burst usage is not available.';
  return [
    railContextRow('Batch Scope', batchValue, batchMeta),
    railContextRow('Year Scope', yearValue, yearMeta),
    railContextRow('Search Layer', searchValue, searchMeta),
    railContextRow('NVIDIA Burst', burstValue, burstMeta),
  ];
}

function renderRailContext(snapshotOrPhases, bgJobs = []) {
  const snapshot = normalizeStatusSnapshot(snapshotOrPhases, bgJobs);
  const body = el('rail-context-body');
  if (!body) return;

  let rows = [];
  switch (activeTab) {
    case 'clusters':
      rows = clusterContextRows(snapshot);
      break;
    case 'objects':
      rows = objectContextRows(snapshot);
      break;
    case 'photos':
      rows = photoContextRows(snapshot);
      break;
    case 'settings':
      rows = settingsContextRows(snapshot);
      break;
    default:
      rows = dashboardContextRows(snapshot);
      break;
  }

  body.innerHTML = rows.length
    ? rows.join('')
    : '<div class="rail-context-empty">No context available yet.</div>';
}

function renderRailShortcuts(tabId = activeTab) {
  const node = el('rail-shortcuts');
  if (!node) return;
  const items = [...(SIDEBAR_SHORTCUTS.shared || []), ...(SIDEBAR_SHORTCUTS[tabId] || [])];
  node.innerHTML = items.map(item => `<p><kbd>${escHtml(item.keys)}</kbd> ${escHtml(item.label)}</p>`).join('');
}

function renderRailNvidia(nvidia) {
  const pill = el('rail-nvidia-pill');
  const labelNode = el('rail-nvidia-label');
  const usageNode = el('rail-nvidia-usage');
  if (!pill || !labelNode || !usageNode) return;

  const tone = nvidia?.tone === 'ok'
    ? 'ok'
    : nvidia?.tone === 'bad'
      ? 'bad'
      : nvidia?.tone === 'warn' || nvidia?.tone === 'neutral'
        ? 'warn'
        : 'off';
  pill.className = `rail-nvidia-pill rail-nvidia-pill--${tone}`;
  labelNode.textContent = nvidia?.label || 'NVIDIA Off';

  if (!nvidia) {
    usageNode.textContent = 'Burst status unavailable';
    pill.title = 'NVIDIA burst status unavailable.';
    return;
  }

  if (!nvidia.feature_enabled) {
    usageNode.textContent = 'Burst disabled in config';
  } else if (!nvidia.api_key_present) {
    usageNode.textContent = 'API key missing';
  } else if (Number.isFinite(nvidia.requests_used) && Number.isFinite(nvidia.tokens_used)) {
    usageNode.textContent = `${fmt(nvidia.requests_used)} / ${fmt(nvidia.requests_cap)} req | ${fmt(nvidia.tokens_used)} / ${fmt(nvidia.tokens_cap)} tok`;
  } else {
    usageNode.textContent = 'Usage unavailable';
  }

  const titleBits = [
    nvidia.server_url || '',
    nvidia.last_checked_at ? `Checked ${nvidia.last_checked_at}` : '',
    nvidia.last_error ? `Last error: ${nvidia.last_error}` : '',
  ].filter(Boolean);
  pill.title = titleBits.join('\n') || 'NVIDIA burst status';
}

function refreshRailContext() {
  renderRailContext(statusData || PHASE_DEFS.map(p => ({ phase: p.id, status: 'pending' })));
  renderRailShortcuts(activeTab);
}

function renderPhaseGrid(phases, counts) {
  const grid = el('phase-grid');
  if (!grid) return;
  grid.innerHTML = '';

  const phaseMap = {};
  phases.forEach(p => { phaseMap[p.phase] = p; });

  PHASE_DEFS.forEach((def, i) => {
    const p = phaseMap[def.id] || { status: 'pending', progress_current: 0, progress_total: 0 };
    const prevDef = i > 0 ? PHASE_DEFS[i - 1] : null;
    const prevPhase = prevDef ? phaseMap[prevDef.id] : null;

    const prevOk = !prevPhase || prevPhase.status === 'complete';
    const canRun = (def.id === 'process' || prevOk) && p.status !== 'running';
    const isPush = def.id === 'push';
    const isCluster = def.id === 'cluster';
    const isStale = !!p.is_stale;

    const pct = isStale
      ? 0
      : p.progress_total > 0
      ? Math.round((p.progress_current / p.progress_total) * 100)
      : (p.status === 'complete' ? 100 : 0);

    const card = document.createElement('div');
    const cardClasses = ['phase-card'];
    if (p.status === 'running') cardClasses.push('phase-card--running');
    if (isStale) cardClasses.push('phase-card--stale');
    card.className = cardClasses.join(' ');
    const canReset = p.status !== 'running';
    card.innerHTML = `
      <div class="phase-card-header">
        <div><span class="phase-num">[${def.num}]</span><span class="phase-name">${def.name}</span></div>
        <span class="badge badge-${isStale ? 'pending' : p.status}">${isStale ? 'STALE' : p.status.toUpperCase()}</span>
      </div>
      <div class="phase-progress"><div class="phase-progress-bar" style="width:${pct}%"></div></div>
      <div class="phase-count">${_phaseCountLine(def.id, p, counts)}</div>
      ${isStale ? `<div class="phase-error-msg">${escHtml(p.stale_reason || 'Upstream output changed. Rerun required.')}</div>` : ''}
      ${p.error_message ? `<div class="phase-error-msg">${escHtml(p.error_message)}</div>` : ''}
      <div class="phase-actions">
        ${isPush ? `
          <label class="push-confirm">
            <input type="checkbox" id="push-confirm-cb" />
            I've reviewed output - ready to push
          </label>
        ` : ''}
        <button class="btn btn-accent run-btn" data-phase="${def.id}" ${canRun && !isPush ? '' : 'disabled'} ${isPush ? 'id="btn-run-push"' : ''}>
          Run
        </button>
        ${p.status === 'running' ? `<button class="btn btn-danger stop-btn" data-phase="${def.id}">Stop</button>` : ''}
        <button class="btn reset-btn" data-phase="${def.id}" ${canReset ? '' : 'disabled'} title="Reset this phase (and downstream) to pending">Reset</button>
        ${def.id === 'process' && !prevOk ? '<span class="note">Can run from existing local originals</span>' : ''}
        ${isCluster ? '<span class="note">Review clusters before Organize</span>' : ''}
      </div>
    `;
    grid.appendChild(card);

    if (isPush) {
      const cb = card.querySelector('#push-confirm-cb');
      const runBtn = card.querySelector('#btn-run-push');
      if (cb && runBtn) {
        cb.addEventListener('change', () => {
          runBtn.disabled = !(cb.checked && prevOk);
        });
      }
    }
  });

  grid.querySelectorAll('.run-btn').forEach(btn => {
    if (!btn.disabled) {
      btn.addEventListener('click', () => {
        const phase = btn.dataset.phase;
        if (phase === 'process') {
          const ok = confirm('Start Process phase now? This can run for hours and use significant GPU resources.');
          if (!ok) return;
        }
        triggerPhase(phase);
      });
    }
  });

  grid.querySelectorAll('.stop-btn').forEach(btn => {
    btn.addEventListener('click', stopPipeline);
  });

  grid.querySelectorAll('.reset-btn').forEach(btn => {
    if (!btn.disabled) {
      btn.addEventListener('click', async () => {
        const phase = btn.dataset.phase;
        if (!confirm(`Reset '${phase}' and all downstream phases to pending?`)) return;
        try {
          await apiPost(`/pipeline/reset/${phase}`);
          showToast(`Reset ${phase} (cascade).`, 'ok');
          await refreshStatus();
        } catch (e) {
          showToast(`Reset failed: ${e.message}`, 'err');
        }
      });
    }
  });
}

function _fmtEta(minutes) {
  if (!Number.isFinite(minutes) || minutes < 0) return '-';
  if (minutes < 1) return '<1m';
  const h = Math.floor(minutes / 60);
  const m = Math.round(minutes % 60);
  if (h <= 0) return `${m}m`;
  if (m === 0) return `${h}h`;
  return `${h}h ${m}m`;
}

function _clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function _parseIsoMs(value) {
  if (!value) return null;
  const ms = Date.parse(value);
  return Number.isFinite(ms) ? ms : null;
}

function _etaDisplayText(eta) {
  if (!eta) return '-';
  if (eta.state === 'warming') return 'ETA warming up...';

  const rateText = Number.isFinite(eta.ratePerMin) && eta.ratePerMin > 0
    ? `~${Math.round(eta.ratePerMin)}/min`
    : null;
  const etaText = Number.isFinite(eta.etaMinutes)
    ? `ETA ~${_fmtEta(eta.etaMinutes)}`
    : 'ETA calibrating...';

  if (eta.state === 'calibrating') {
    return rateText ? `${rateText} | ${etaText} (settling)` : `${etaText} (settling)`;
  }
  return rateText ? `${rateText} | ${etaText}` : etaText;
}

function _buildEtaEstimate(phase, prev, cached, now) {
  const current = Math.max(0, phase.progress_current || 0);
  const total = Math.max(0, phase.progress_total || 0);
  const remaining = Math.max(0, total - current);
  const startedAtMs = _parseIsoMs(phase.started_at);
  const sameRunAsCache = !!cached && cached.startedAtMs === startedAtMs;
  const elapsedMin = startedAtMs && now > startedAtMs
    ? (now - startedAtMs) / 60000
    : null;

  let overallRatePerMin = null;
  if (current > 0 && Number.isFinite(elapsedMin) && elapsedMin > 0) {
    overallRatePerMin = current / elapsedMin;
  }

  let recentRatePerMin = null;
  if (prev && prev.startedAtMs === startedAtMs) {
    const deltaItems = current - (prev.current || 0);
    const deltaMin = (now - prev.t) / 60000;
    if (deltaItems > 0 && deltaMin > 0) {
      recentRatePerMin = deltaItems / deltaMin;
    }
  }

  if (
    !recentRatePerMin &&
    sameRunAsCache &&
    Number.isFinite(cached.recentRatePerMin) &&
    cached.recentRatePerMin > 0 &&
    (now - (cached.updatedAt || 0)) <= ETA_CACHE_TTL_MS
  ) {
    recentRatePerMin = cached.recentRatePerMin;
  }

  let ratePerMin = null;
  if (overallRatePerMin && recentRatePerMin) {
    const progressMaturity = total > 0 ? _clamp(current / Math.min(total, 300), 0, 1) : 0;
    const elapsedMaturity = Number.isFinite(elapsedMin) ? _clamp(elapsedMin / 20, 0, 1) : 0;
    const idlePenalty = prev && current === prev.current ? 0.1 : 0;
    const recentWeight = _clamp(
      0.2 + (0.25 * progressMaturity) + (0.2 * elapsedMaturity) - idlePenalty,
      0.2,
      0.65,
    );
    ratePerMin = (recentRatePerMin * recentWeight) + (overallRatePerMin * (1 - recentWeight));
  } else if (recentRatePerMin) {
    ratePerMin = recentRatePerMin;
  } else if (overallRatePerMin) {
    ratePerMin = overallRatePerMin;
  } else if (
    sameRunAsCache &&
    Number.isFinite(cached.ratePerMin) &&
    cached.ratePerMin > 0 &&
    (now - (cached.updatedAt || 0)) <= ETA_CACHE_TTL_MS
  ) {
    ratePerMin = cached.ratePerMin;
  }

  if (!Number.isFinite(ratePerMin) || ratePerMin <= 0) {
    return {
      state: 'warming',
      current,
      total,
      startedAtMs,
      updatedAt: now,
    };
  }

  const state = (
    current < Math.min(total, ETA_MIN_ITEMS_FOR_CONFIDENCE) ||
    (Number.isFinite(elapsedMin) && elapsedMin < ETA_MIN_ELAPSED_MIN_FOR_CONFIDENCE)
  )
    ? 'calibrating'
    : 'stable';

  return {
    state,
    current,
    total,
    startedAtMs,
    ratePerMin,
    recentRatePerMin,
    overallRatePerMin,
    etaMinutes: remaining / ratePerMin,
    updatedAt: now,
  };
}

function _updateEtaCache(phases) {
  const now = Date.now();
  const nextPrev = {};
  const nextEta = { ..._etaCache };
  const runningPhases = new Set();

  phases.forEach(p => {
    const current = p.progress_current || 0;
    const total = p.progress_total || 0;
    const startedAtMs = _parseIsoMs(p.started_at);
    nextPrev[p.phase] = { current, t: now, startedAtMs };

    if (p.status !== 'running' || total <= 0) return;
    runningPhases.add(p.phase);

    const prev = _prevProgress[p.phase];
    const cached = nextEta[p.phase];
    const estimate = _buildEtaEstimate(p, prev, cached, now);
    if (estimate) nextEta[p.phase] = estimate;
    else delete nextEta[p.phase];
  });

  Object.keys(nextEta).forEach(phaseId => {
    if (!runningPhases.has(phaseId)) delete nextEta[phaseId];
  });

  _prevProgress = nextPrev;
  _etaCache = nextEta;
}

function _phaseCountLine(id, p, counts) {
  if (!counts) return `${fmt(p.progress_current)} / ${fmt(p.progress_total)}`;

  const eta = _etaCache[id];
  const etaText = p.status === 'running' ? _etaDisplayText(eta) : '-';
  const etaSuffix = etaText && etaText !== '-' ? ` | ${etaText}` : '';

  switch (id) {
    case 'pull':
      {
        const pulled = p.progress_total || p.progress_current || 0;
        const total = counts.total_photos || 0;
        if (pulled > 0 && total > 0 && pulled !== total) {
          return `${fmt(pulled)} pulled | ${fmt(total)} total${etaSuffix}`;
        }
        return `${fmt(total || pulled)} photos${etaSuffix}`;
      }
    case 'process':
      return `${fmt(p.progress_current)} / ${fmt(p.progress_total)} photos | ${fmt(counts.total_faces)} faces${etaSuffix}`;
    case 'cluster':
      if (p.progress_total > 0) {
        return `${fmt(p.progress_current)} / ${fmt(p.progress_total)} steps | ${fmt(counts.total_clusters)} clusters | ${fmt(counts.labeled_clusters)} labeled${etaSuffix}`;
      }
      return `${fmt(counts.total_clusters)} clusters | ${fmt(counts.labeled_clusters)} labeled${etaSuffix}`;
    case 'organize':
      return `${fmt(p.progress_current)} / ${fmt(p.progress_total)} organized${etaSuffix}`;
    case 'tag':
      return `${fmt(p.progress_current)} / ${fmt(p.progress_total)} photos | ${fmt(counts.total_detections)} detections${etaSuffix}`;
    default:
      return `${fmt(p.progress_current)} / ${fmt(p.progress_total)}${etaSuffix}`;
  }
}

async function triggerPhase(phase) {
  try {
    await apiPost(`/pipeline/run/${phase}`);
    showToast(`Started ${phase}.`, 'ok');
    await refreshStatus();
    scheduleAutoRefresh();
  } catch (e) {
    showToast(`Failed to start ${phase}.`, 'err');
  }
}

async function stopPipeline() {
  try {
    await apiPost('/pipeline/stop');
    showToast('Stop requested. Waiting for current phase to halt...', 'ok');
    await refreshStatus();
    scheduleAutoRefresh();
  } catch (e) {
    showToast('Failed to request stop.', 'err');
  }
}

async function refreshStatus() {
  try {
    const data = await api('/status');
    const bgJobs = data.background_jobs || [];
    statusData = data;
    _updateEtaCache(data.phases);

    renderPhaseGrid(data.phases, data.counts);
    updateHeaderStatus(data.phases, bgJobs);
    renderSidebarSnapshot(data);
    renderBackgroundJobs(bgJobs);
    if (activeTab === 'dashboard') {
      updateLogTail();
    }

    const anyRunning = data.phases.some(p => p.status === 'running') || bgJobs.some(j => j.status === 'running');
    if (anyRunning) scheduleAutoRefresh();
    else clearAutoRefresh();
  } catch (e) {
    console.error('Status refresh failed:', e);
  }
}

function updateHeaderStatus(phases, bgJobs = []) {
  const running = phases.find(p => p.status === 'running');
  const errors = phases.filter(p => p.status === 'error');
  const runningJobs = runningBackgroundJobs(bgJobs);
  const bgErrors = backgroundJobErrors(bgJobs);

  let txt = 'READY';
  if (running) txt = `${running.phase.toUpperCase()} RUNNING`;
  else if (runningJobs.length === 1) txt = `${backgroundJobLabel(runningJobs[0]).toUpperCase()} RUNNING`;
  else if (runningJobs.length > 1) txt = `${runningJobs.length} BACKGROUND TASKS`;
  else if (errors.length || bgErrors.length) txt = `${errors.length + bgErrors.length} ERROR(S)`;

  const statusNode = el('header-status');
  if (statusNode) statusNode.textContent = txt;

  const sideStatus = el('sidebar-status');
  if (sideStatus) sideStatus.textContent = txt;
}

function renderSidebarSnapshot(snapshotOrPhases, bgJobs = []) {
  const snapshot = normalizeStatusSnapshot(snapshotOrPhases, bgJobs);
  const phases = snapshot.phases || [];
  const bgJobList = snapshot.background_jobs || [];
  const running = phases.find(p => p.status === 'running');
  const runningJobs = runningBackgroundJobs(bgJobList);
  const bgErrors = backgroundJobErrors(bgJobList);
  const complete = phases.filter(p => p.status === 'complete').length;
  const errors = phases.filter(p => p.status === 'error').length + bgErrors.length;
  const machineState = sidebarMachineStateLabel(running, runningJobs, errors);

  const runNode = el('sidebar-running-phase');
  if (runNode) {
    runNode.textContent = running
      ? running.phase.toUpperCase()
      : (runningJobs[0] ? backgroundJobLabel(runningJobs[0]).toUpperCase() : '-');
  }

  const activeTabNode = el('sidebar-active-tab');
  if (activeTabNode) activeTabNode.textContent = TAB_LABELS[activeTab] || activeTab;

  const completeNode = el('sidebar-complete-count');
  if (completeNode) completeNode.textContent = `${complete} / ${PHASE_DEFS.length}`;

  const errNode = el('sidebar-error-count');
  if (errNode) errNode.textContent = String(errors);

  const machineNode = el('sidebar-machine-state');
  if (machineNode) machineNode.textContent = machineState;

  const machinePill = el('rail-machine-pill');
  if (machinePill) {
    machinePill.textContent = machineState;
    machinePill.classList.remove('rail-pill--busy', 'rail-pill--ready', 'rail-pill--warn');
    machinePill.classList.add(
      machineState === 'BUSY'
        ? 'rail-pill--busy'
        : machineState === 'DEGRADED'
          ? 'rail-pill--warn'
          : 'rail-pill--ready',
    );
  }

  const bgCountNode = el('sidebar-background-count');
  if (bgCountNode) bgCountNode.textContent = `${runningJobs.length} running`;

  const tickerPhase = el('ticker-phase');
  if (tickerPhase) {
    tickerPhase.textContent = running
      ? running.phase.toUpperCase()
      : (runningJobs[0] ? backgroundJobLabel(runningJobs[0]).toUpperCase() : 'Idle');
  }

  const phaseBadge = el('rail-phase-badge');
  if (phaseBadge) phaseBadge.textContent = activePhaseBadgeText(running, runningJobs);

  const tickerProgress = el('ticker-progress');
  if (tickerProgress) {
    if (running && running.progress_total > 0) {
      const pct = Math.round((running.progress_current / running.progress_total) * 100);
      tickerProgress.textContent = `${pct}% (${fmt(running.progress_current)} / ${fmt(running.progress_total)})`;
    } else if (runningJobs[0]) {
      tickerProgress.textContent = backgroundJobProgress(runningJobs[0]);
    } else {
      tickerProgress.textContent = '-';
    }
  }

  const tickerThroughput = el('ticker-throughput');
  if (tickerThroughput) {
    const eta = running ? _etaCache[running.phase] : null;
    if (eta) {
      tickerThroughput.textContent = _etaDisplayText(eta);
    } else if (runningJobs[0]) {
      tickerThroughput.textContent = 'Background indexing active';
    } else {
      tickerThroughput.textContent = '-';
    }
  }

  renderRailNvidia(snapshot.nvidia);
  renderRailContext(snapshot);
  renderRailShortcuts(activeTab);
}

function renderBackgroundJobs(bgJobs = []) {
  const node = el('background-job-list');
  if (!node) return;

  const visibleJobs = bgJobs.filter(job => job.status === 'running' || job.status === 'error');
  if (!visibleJobs.length) {
    node.innerHTML = '<div class="bg-job bg-job--empty">No heavy background jobs</div>';
    return;
  }

  node.innerHTML = visibleJobs.map(job => `
    <div class="bg-job">
      <div class="bg-job__head">
        <span class="bg-job__name">${escHtml(backgroundJobLabel(job))}</span>
        <span class="badge badge-${job.status}">${escHtml(String(job.status || '').toUpperCase())}</span>
      </div>
      <div class="bg-job__meta">
        <span>${escHtml(backgroundJobProgress(job))}</span>
        <span>${job.updated_at ? escHtml(fmtDate(job.updated_at)) : '-'}</span>
      </div>
      ${job.error_message ? `<div class="bg-job__detail">${escHtml(job.error_message)}</div>` : ''}
    </div>
  `).join('');
}

function updateSidebarActiveTab(tabId) {
  const node = el('sidebar-active-tab');
  if (node) node.textContent = TAB_LABELS[tabId] || tabId;
  refreshRailContext();
}

function scheduleAutoRefresh() {
  clearAutoRefresh();
  autoRefreshTimer = setInterval(refreshStatus, 5000);
}

function clearAutoRefresh() {
  if (autoRefreshTimer) {
    clearInterval(autoRefreshTimer);
    autoRefreshTimer = null;
  }
}

function _logLineClass(line) {
  if (/\b(ERROR|CRITICAL|EXCEPTION)\b/.test(line)) return 'log-line--error';
  if (/\b(WARNING|WARN)\b/.test(line)) return 'log-line--warn';
  if (/\bDEBUG\b/.test(line)) return 'log-line--debug';
  return 'log-line--info';
}

async function updateLogTail() {
  try {
    const data = await api('/pipeline/log-tail?lines=50');
    const box = el('log-tail');
    if (!box) return;

    if (data.lines && data.lines.length > 0) {
      box.innerHTML = data.lines
        .map(line => `<span class="log-line ${_logLineClass(line)}">${escHtml(line)}</span>`)
        .join('\n');
      box.scrollTop = box.scrollHeight;
    } else {
      box.textContent = '[ No log output yet ]';
    }
  } catch (_) {
    // optional
  }
}

let clusters = [];
let selectedClusterIdx = 0;
let selectedClusterId = null;
let selectedFaceIds = new Set();
let lastSelectedFaceIndex = null;
let clusterSidebarMode = 'by-cluster';
let prototypeGroups = [];
let selectedPrototypeLabel = null;
let prototypeSourceClusterId = null;
let prototypeScopedClusterMode = false;
let clusterReviewScope = { mode: 'cluster', personLabel: null };
let personReviewData = null;
const collapsedClusterGroups = new Set();
const collapsedUntaggedGroups = new Set();

function isPersonReviewMode() {
  return clusterReviewScope.mode === 'person' && !!clusterReviewScope.personLabel;
}

function isPrototypeReviewMode() {
  return clusterSidebarMode === 'by-person-prototype';
}

function prototypeLabelKey(label) {
  return String(label || '').trim().toLowerCase();
}

function activePrototypeScopeQuery() {
  return Number.isFinite(prototypeSourceClusterId)
    ? `?cluster_id=${encodeURIComponent(prototypeSourceClusterId)}`
    : '';
}

function selectedClusterRecord() {
  return clusters.find(cluster => cluster.cluster_id === selectedClusterId) || clusters[selectedClusterIdx] || null;
}

function resolvePrototypeSourceClusterId() {
  const cluster = selectedClusterRecord();
  if (!cluster) return null;
  return (!cluster.person_label || cluster.is_noise) ? Number(cluster.cluster_id) : null;
}

function updateClusterColumnHeaders(taggedLabel = 'Tagged', untaggedLabel = 'Untagged') {
  const headers = document.querySelectorAll('#tab-clusters .cluster-column-header');
  if (headers[0]) headers[0].textContent = taggedLabel;
  if (headers[1]) headers[1].textContent = untaggedLabel;
}

function updateSidebarModeToggle() {
  el('btn-sidebar-by-cluster')?.classList.toggle('active', clusterSidebarMode === 'by-cluster');
  el('btn-sidebar-by-person')?.classList.toggle('active', clusterSidebarMode === 'by-person-prototype');
  if (clusterSidebarMode === 'by-person-prototype') {
    updateClusterColumnHeaders('Matched', 'Unknown');
  } else {
    updateClusterColumnHeaders('Tagged', 'Untagged');
  }
}

function setClusterReviewScope(mode = 'cluster', personLabel = null) {
  clusterReviewScope = {
    mode: mode === 'person' ? 'person' : 'cluster',
    personLabel: mode === 'person' && personLabel ? String(personLabel).trim() : null,
  };
  personReviewData = null;
}

function updateClusterReviewChrome() {
  const cluster = clusters[selectedClusterIdx];
  const personLabel = (cluster?.person_label || '').trim();
  const personMode = isPersonReviewMode() && !!personLabel;
  const prototypeMode = isPrototypeReviewMode();
  const prototypeGroup = prototypeGroups.find(group =>
    prototypeLabelKey(group.person_label) === prototypeLabelKey(selectedPrototypeLabel));
  const reviewBtn = el('btn-review-person');
  const scopeNote = el('cluster-review-scope');
  const nameInput = el('cluster-name-input');
  const approveBtn = el('btn-approve-cluster');
  const noiseBtn = el('btn-noise-cluster');
  const mergeTarget = el('merge-target');
  const mergeBtn = el('btn-merge-cluster');
  const reassignTarget = el('reassign-target');
  const reassignName = el('reassign-name-input');
  const reassignBtn = el('btn-reassign-faces');
  const selectAllBtn = el('btn-select-all-faces');
  const untagBtn = el('btn-untag-cluster');
  const toolbarTip = document.querySelector('#cluster-toolbar .toolbar-tip');

  if (reviewBtn) {
    reviewBtn.disabled = prototypeMode || !personLabel;
    reviewBtn.textContent = personMode ? 'Back to Cluster' : 'Review Whole File';
  }
  if (nameInput) {
    nameInput.disabled = personMode || prototypeMode;
    if (prototypeMode) nameInput.value = '';
  }
  if (approveBtn) approveBtn.disabled = personMode || prototypeMode;
  if (noiseBtn) noiseBtn.disabled = personMode || prototypeMode;
  if (mergeTarget) mergeTarget.disabled = personMode || prototypeMode;
  if (mergeBtn) mergeBtn.disabled = personMode || prototypeMode;
  if (reassignTarget) reassignTarget.disabled = personMode;
  if (reassignName) reassignName.disabled = personMode;
  if (reassignBtn) reassignBtn.disabled = personMode || selectedFaceIds.size === 0;
  if (selectAllBtn) selectAllBtn.disabled = false;
  if (untagBtn) {
    untagBtn.textContent = personMode ? 'Remove Selected' : 'Untag';
    untagBtn.disabled = prototypeMode;
  }
  if (toolbarTip) {
    toolbarTip.textContent = prototypeMode
      ? '[Ctrl/Cmd+A]=select all [Esc]=clear selection [Shift+Click]=range select'
      : '[Enter]=approve [U]=untag selected (or whole cluster if none selected) [N]=noise [Arrows]=nav [Shift+Click]=range select [Esc]=clear selection';
  }

  refreshRailContext();

  if (!scopeNote) return;
  if (prototypeMode && prototypeGroup) {
    const isUnknown = prototypeLabelKey(prototypeGroup.person_label) === '__unknown__';
    const scopePrefix = Number.isFinite(prototypeSourceClusterId)
      ? `cluster ${prototypeSourceClusterId} | `
      : '';
    const meta = isUnknown
      ? `${scopePrefix}${fmt(prototypeGroup.face_count)} faces | below ${Number(prototypeGroup.threshold || 0).toFixed(2)} threshold`
      : `${scopePrefix}${fmt(prototypeGroup.face_count)} faces | avg ${Number(prototypeGroup.avg_similarity || 0).toFixed(2)} similarity | ${fmt(prototypeGroup.prototype_support_faces || 0)} support faces`;
    const tip = Number.isFinite(prototypeSourceClusterId)
      ? `Prototype triage is scoped to cluster ${prototypeSourceClusterId} and buckets faces by their best prototype match so you can split this cluster quickly.`
      : 'Prototype triage groups unlabeled and noise faces by closest known person so you can multi-select and move them into the right cluster.';
    scopeNote.innerHTML = `
      <span class="cluster-review-scope-title">${escHtml(prototypeGroup.display_label || prototypeGroup.person_label || 'Unknown')}</span>
      <span class="cluster-review-scope-meta">${escHtml(meta)}</span>
      <span class="cluster-review-scope-tip">${escHtml(tip)}</span>
    `;
    return;
  }
  if (prototypeMode) {
    scopeNote.textContent = 'Loading prototype group...';
    return;
  }
  if (personMode && personReviewData) {
    scopeNote.innerHTML = `
      <span class="cluster-review-scope-title">${escHtml(personReviewData.person_label)}</span>
      <span class="cluster-review-scope-meta">
        ${fmt(personReviewData.face_count)} faces | ${fmt(personReviewData.cluster_count)} clusters | worst matches first
      </span>
      <span class="cluster-review-scope-tip">Select one or more faces and press Remove Selected to pull them out into new unlabeled cluster(s).</span>
    `;
    return;
  }
  if (personMode) {
    scopeNote.textContent = `Loading ${personLabel} full review file...`;
    return;
  }
  if (cluster?.is_noise) {
    scopeNote.textContent = 'Noise cluster selected. Faces are ranked by best prototype match so likely rescues rise to the top.';
    return;
  }
  scopeNote.textContent = personLabel
    ? `Cluster ${cluster?.cluster_id} selected. Use Review Whole File to rank ${personLabel} from worst to best.`
    : 'Select a labeled cluster to review the full person file.';
}

function updateFaceSelectionCount() {
  const node = el('face-selection-count');
  if (!node) return;
  if (isPersonReviewMode()) {
    node.textContent = selectedFaceIds.size > 0
      ? `${selectedFaceIds.size} marked for removal`
      : 'Select face(s) to remove';
  } else if (isPrototypeReviewMode()) {
    node.textContent = `${selectedFaceIds.size} selected`;
  } else {
    node.textContent = `${selectedFaceIds.size} selected`;
  }
  updateClusterReviewChrome();
}

function clearFaceSelection() {
  selectedFaceIds.clear();
  lastSelectedFaceIndex = null;
  document.querySelectorAll('#crop-grid .crop-tile.selected').forEach(tile => {
    tile.classList.remove('selected');
  });
  updateFaceSelectionCount();
}

function refreshReassignTargets(currentClusterId = null) {
  const sel = el('reassign-target');
  if (!sel) return;
  const previous = sel.value;
  sel.innerHTML = '<option value="">Move faces to person...</option>';

  const canonicalPeople = new Map();
  clusters.forEach(cluster => {
    const personLabel = String(cluster.person_label || '').trim();
    if (!personLabel || cluster.is_noise) return;

    const key = personLabel.toLowerCase();
    const candidate = {
      personLabel,
      clusterId: Number(cluster.cluster_id),
      approved: !!cluster.approved,
      faceCount: Number(cluster.face_count) || 0,
    };
    const existing = canonicalPeople.get(key);
    if (!existing) {
      canonicalPeople.set(key, candidate);
      return;
    }

    const candidateWins = (
      Number(candidate.approved) > Number(existing.approved)
      || (
        candidate.approved === existing.approved
        && (
          candidate.faceCount > existing.faceCount
          || (
            candidate.faceCount === existing.faceCount
            && candidate.clusterId < existing.clusterId
          )
        )
      )
    );
    if (candidateWins) canonicalPeople.set(key, candidate);
  });

  Array.from(canonicalPeople.values())
    .filter(person => person.clusterId !== currentClusterId)
    .sort((a, b) => a.personLabel.localeCompare(b.personLabel, undefined, { sensitivity: 'base' }))
    .forEach(person => {
      const opt = document.createElement('option');
      opt.value = person.personLabel;
      opt.textContent = person.personLabel;
      sel.appendChild(opt);
    });

  if (previous && Array.from(sel.options).some(o => o.value === previous)) {
    sel.value = previous;
  }
}

function getReassignInputs() {
  const personSelect = el('reassign-target');
  const nameInput = el('reassign-name-input');
  const selectedPersonLabel = (personSelect?.value || '').trim();
  const typedPersonLabel = (nameInput?.value || '').trim();
  return {
    targetClusterId: null,
    selectedPersonLabel,
    typedPersonLabel,
    targetPersonLabel: selectedPersonLabel || typedPersonLabel,
  };
}

function renderClusterSuggestions(data, clusterId) {
  const panel = el('cluster-suggestions');
  if (!panel) return;
  panel.innerHTML = '';

  if (data?.source_pool === 'approved_plus_labeled'
      || data?.source_pool === 'approved_plus_memory_plus_labeled'
      || data?.source_pool === 'memory_plus_labeled') {
    const note = document.createElement('div');
    note.className = 'dim';
    note.textContent = 'Suggestions include thinner-support labels to widen name options.';
    panel.appendChild(note);
  }

  if (!data || !Array.isArray(data.suggestions) || !data.suggestions.length) {
    const reason = data?.reason ? ` (${data.reason.replace(/_/g, ' ')})` : '';
    panel.innerHTML = `<div class="dim">No suggestions available${reason}.</div>`;
    return;
  }

  data.suggestions.forEach(s => {
    const sourceState = s.usable_label ? 'usable' : 'thin support';
    const prototypeOrigin = s.prototype_source === 'memory' ? 'saved memory' : 'current album';
    const prototypeMeta = s.support_clusters > 1
      ? `${prototypeOrigin} prototype from ${s.support_clusters} clusters`
      : (s.source_cluster_id != null ? `${prototypeOrigin} cluster ${s.source_cluster_id}` : `${prototypeOrigin} prototype`);
    const supportMeta = s.clean_approved_faces != null && data?.usable_min_approved_faces
      ? `${s.clean_approved_faces} / ${data.usable_min_approved_faces} clean approved faces`
      : `${s.support_faces} faces`;
    const card = document.createElement('div');
    card.className = 'suggest-card';
    card.innerHTML = `
      <div class="suggest-head">
        <span class="suggest-name">${escHtml(s.person_label)}</span>
        <span class="suggest-score">${Number(s.score).toFixed(2)}</span>
      </div>
      <div class="suggest-meta">${prototypeMeta} | ${supportMeta} | ${sourceState}</div>
      <button class="btn" data-accept-suggestion="1" data-cluster-id="${clusterId}">Use Name</button>
    `;
    const useBtn = card.querySelector('button[data-accept-suggestion]');
    if (useBtn) useBtn.dataset.name = s.person_label;
    panel.appendChild(card);
  });
}

async function loadClusterSuggestions(clusterId) {
  const panel = el('cluster-suggestions');
  if (!panel) return;
  panel.innerHTML = '<div class="dim">Loading suggestions...</div>';
  try {
    const data = await api(`/clusters/${clusterId}/suggestions`);
    renderClusterSuggestions(data, clusterId);
  } catch (_) {
    panel.innerHTML = '<div class="dim red">Suggestions unavailable.</div>';
  }
}

function renderReviewFaces(faces, { personMode = false, prototypeMode = false } = {}) {
  const grid = el('crop-grid');
  if (!grid) return;

  grid.innerHTML = '';
  if (!faces.length) {
    setEmpty(
      'crop-grid',
      personMode
        ? 'No faces found for this person.'
        : (prototypeMode ? 'No faces found for this prototype group.' : 'No crops for this cluster.'),
    );
    return;
  }

  faces.forEach(face => {
    const tile = document.createElement('div');
    tile.className = 'crop-tile';
    tile.dataset.faceId = String(face.face_id);
    if (face.cluster_id != null) tile.dataset.clusterId = String(face.cluster_id);
    tile.dataset.faceIndex = String(grid.children.length);
    const scoreLine = personMode
      ? `match ${Number(face.match_score || 0).toFixed(2)} | det ${Number(face.detection_score || 0).toFixed(2)}`
      : prototypeMode
        ? `${face.matched_person || 'Unknown'} ${Number(face.similarity || 0).toFixed(2)} | det ${Number(face.detection_score || 0).toFixed(2)}`
      : face.predicted_label
        ? `${face.predicted_label} ${Number(face.best_match_score || 0).toFixed(2)} | det ${Number(face.detection_score || 0).toFixed(2)}`
        : `${Number(face.detection_score || 0).toFixed(2)}`;
    const titleBits = [
      face.filename || '',
      personMode ? `cluster ${face.cluster_id}` : '',
      prototypeMode ? `cluster ${face.cluster_id}` : '',
      personMode ? `rank ${face.review_rank}` : '',
      prototypeMode && face.matched_person ? `closest ${face.matched_person}` : '',
      prototypeMode ? `sim ${Number(face.similarity || 0).toFixed(2)}` : '',
      face.predicted_label ? `best ${face.predicted_label}` : '',
      face.best_match_score != null ? `proto ${Number(face.best_match_score).toFixed(2)}` : '',
      personMode ? `match ${Number(face.match_score || 0).toFixed(2)}` : '',
      `det ${Number(face.detection_score || 0).toFixed(2)}`,
    ].filter(Boolean);
    tile.title = titleBits.join(' | ');
    tile.innerHTML = `
      <img src="${face.crop_url || ''}" alt="" />
      <div class="pick-indicator">&#10003;</div>
      <div class="score">${escHtml(scoreLine)}</div>
    `;
    tile.addEventListener('click', evt => {
      const faceId = Number(tile.dataset.faceId);
      const faceIndex = Number(tile.dataset.faceIndex);
      if (!faceId) return;

      if (evt.shiftKey && lastSelectedFaceIndex != null) {
        const lo = Math.min(lastSelectedFaceIndex, faceIndex);
        const hi = Math.max(lastSelectedFaceIndex, faceIndex);
        document.querySelectorAll('#crop-grid .crop-tile').forEach(node => {
          const idx = Number(node.dataset.faceIndex);
          const id = Number(node.dataset.faceId);
          if (idx >= lo && idx <= hi && id) {
            selectedFaceIds.add(id);
            node.classList.add('selected');
          }
        });
      } else if (selectedFaceIds.has(faceId)) {
        selectedFaceIds.delete(faceId);
        tile.classList.remove('selected');
      } else {
        selectedFaceIds.add(faceId);
        tile.classList.add('selected');
      }
      lastSelectedFaceIndex = faceIndex;
      updateFaceSelectionCount();
    });
    grid.appendChild(tile);
  });
}

async function loadActiveClusterFaces() {
  const cluster = clusters[selectedClusterIdx];
  if (!cluster) return;

  personReviewData = null;
  updateClusterReviewChrome();
  setSkeleton('crop-grid', 'crop', 12);

  try {
    if (isPersonReviewMode() && cluster.person_label) {
      personReviewData = await api(`/clusters/${cluster.cluster_id}/person-review`);
      updateClusterReviewChrome();
      renderReviewFaces(personReviewData.faces || [], { personMode: true });
      return;
    }

    const crops = await api(`/clusters/${cluster.cluster_id}/crops`);
    updateClusterReviewChrome();
    renderReviewFaces(crops, { personMode: false });
  } catch (_) {
    setEmpty(
      'crop-grid',
      isPersonReviewMode() ? 'Error loading full person review.' : 'Error loading crops.',
      true,
    );
  }
}

function validateReassignDestination() {
  const { selectedPersonLabel, typedPersonLabel, targetPersonLabel } = getReassignInputs();
  if (selectedPersonLabel && typedPersonLabel) {
    showToast('Pick a person from the list or type a new name, not both.', 'err');
    return null;
  }
  if (!targetPersonLabel) {
    showToast('Pick a target person or type a new person name.', 'err');
    return null;
  }
  return { targetClusterId: null, targetPersonLabel };
}

function renderPrototypeGroupList(data) {
  const taggedList = el('cluster-list-tagged');
  const untaggedList = el('cluster-list-untagged');
  if (!taggedList || !untaggedList) return;
  taggedList.innerHTML = '';
  untaggedList.innerHTML = '';

  const groups = Array.isArray(data?.groups) ? data.groups : [];
  prototypeGroups = groups.map(group => ({
    ...group,
    threshold: Number(data?.threshold || 0),
  }));
  prototypeSourceClusterId = data?.scope_cluster_id != null && Number.isFinite(Number(data.scope_cluster_id))
    ? Number(data.scope_cluster_id)
    : null;
  prototypeScopedClusterMode = !!data?.scoped_cluster_mode;

  const progressNode = el('cluster-progress');
  if (progressNode) {
    const scopeSuffix = Number.isFinite(prototypeSourceClusterId)
      ? ` | cluster ${prototypeSourceClusterId}`
      : '';
    const verb = prototypeScopedClusterMode ? 'grouped' : 'matched';
    progressNode.textContent = `${fmt(data?.total_matched_faces || 0)} / ${fmt(data?.total_untagged_faces || 0)} ${verb} | ${fmt(data?.prototype_count || 0)} prototypes${scopeSuffix}`;
  }

  const renderItem = (group, mountNode, dotClass) => {
    const div = document.createElement('div');
    div.className = 'sidebar-item' + (
      prototypeLabelKey(group.person_label) === prototypeLabelKey(selectedPrototypeLabel) ? ' active' : ''
    );
    div.dataset.prototypeLabel = String(group.person_label || '');
    const isUnknown = prototypeLabelKey(group.person_label) === '__unknown__';
    const meta = isUnknown
      ? `below ${Number(group.threshold || 0).toFixed(2)} threshold`
      : `avg ${Number(group.avg_similarity || 0).toFixed(2)} | ${fmt(group.prototype_support_faces || 0)} support`;
    div.innerHTML = `
      <span class="status-dot ${dotClass}"></span>
      <span class="item-main">
        <span class="item-label">${escHtml(group.display_label || group.person_label || 'Unknown')}</span>
        <span class="item-meta">${escHtml(meta)}</span>
      </span>
      <span class="item-count">${fmt(group.face_count || 0)}</span>
    `;
    div.addEventListener('click', () => selectPrototypeGroup(group.person_label));
    mountNode.appendChild(div);
  };

  const matchedGroups = prototypeGroups.filter(group => prototypeLabelKey(group.person_label) !== '__unknown__');
  const unknownGroup = prototypeGroups.find(group => prototypeLabelKey(group.person_label) === '__unknown__');

  if (!matchedGroups.length) {
    setEmpty('cluster-list-tagged', 'No matched prototype groups.');
  } else {
    matchedGroups.forEach(group => renderItem(group, taggedList, 'dot-labeled'));
  }

  if (!unknownGroup) {
    setEmpty('cluster-list-untagged', 'No unknown faces.');
  } else {
    renderItem(unknownGroup, untaggedList, 'dot-unlabeled');
  }
}

async function selectPrototypeGroup(label) {
  if (!label) return;
  selectedPrototypeLabel = String(label).trim();
  clearFaceSelection();
  refreshReassignTargets(null);
  updateClusterReviewChrome();

  document.querySelectorAll('.cluster-column .sidebar-item').forEach(node => {
    node.classList.toggle(
      'active',
      prototypeLabelKey(node.dataset.prototypeLabel) === prototypeLabelKey(selectedPrototypeLabel),
    );
  });

  const panel = el('cluster-suggestions');
  if (panel) {
    panel.innerHTML = Number.isFinite(prototypeSourceClusterId)
      ? `<div class="dim">Prototype triage is scoped to cluster ${prototypeSourceClusterId}. These buckets use each face's best prototype match, even when the similarity is soft.</div>`
      : '<div class="dim">Prototype triage uses cosine similarity against known person prototypes. Move selected faces into the right cluster when a group looks correct.</div>';
  }

  setSkeleton('crop-grid', 'crop', 12);
  try {
    const faces = await api(`/clusters/by-person-prototype/${encodeURIComponent(selectedPrototypeLabel)}/faces${activePrototypeScopeQuery()}`);
    renderReviewFaces(faces, { prototypeMode: true });
    updateClusterReviewChrome();
  } catch (e) {
    setEmpty('crop-grid', 'Error loading prototype group.', true);
  }
}

async function loadByPersonPrototype() {
  setEmpty('cluster-list-tagged', 'Loading prototype groups...');
  setEmpty('cluster-list-untagged', 'Loading prototype groups...');
  setSkeleton('crop-grid', 'crop', 12);
  setClusterReviewScope('cluster');
  personReviewData = null;

  const toolbar = el('cluster-toolbar');
  if (toolbar) toolbar.style.display = 'flex';

  try {
    const clusterData = await api('/clusters?sort=review');
    clusters = clusterData;
    if (Number.isFinite(prototypeSourceClusterId) && !clusters.some(cluster => cluster.cluster_id === prototypeSourceClusterId)) {
      prototypeSourceClusterId = null;
    }
    const groupData = await api(`/clusters/by-person-prototype${activePrototypeScopeQuery()}`);
    renderPrototypeGroupList(groupData);
    refreshReassignTargets(null);
    updateClusterReviewChrome();

    if (prototypeGroups.length > 0) {
      const nextLabel = prototypeGroups.some(group =>
        prototypeLabelKey(group.person_label) === prototypeLabelKey(selectedPrototypeLabel))
        ? selectedPrototypeLabel
        : prototypeGroups[0].person_label;
      await selectPrototypeGroup(nextLabel);
    } else {
      selectedPrototypeLabel = null;
      const panel = el('cluster-suggestions');
      if (panel) {
        panel.innerHTML = '<div class="dim">No faces are waiting in prototype triage right now.</div>';
      }
      setEmpty('crop-grid', 'No faces available for prototype triage.');
    }
  } catch (e) {
    setEmpty('cluster-list-tagged', `Error: ${e.message}`, true);
    setEmpty('cluster-list-untagged', `Error: ${e.message}`, true);
    setEmpty('crop-grid', 'Unable to load prototype triage.', true);
  }
}

async function reassignSelectedPrototypeFaces(destination) {
  const selectedTiles = Array.from(document.querySelectorAll('#crop-grid .crop-tile.selected'));
  const faceGroups = new Map();

  selectedTiles.forEach(tile => {
    const faceId = Number(tile.dataset.faceId);
    const clusterId = Number(tile.dataset.clusterId);
    if (!faceId || !clusterId) return;
    if (!faceGroups.has(clusterId)) faceGroups.set(clusterId, []);
    faceGroups.get(clusterId).push(faceId);
  });

  if (!faceGroups.size) {
    throw new Error('No valid face selections found.');
  }
  if (destination.targetClusterId && faceGroups.has(destination.targetClusterId)) {
    throw new Error('Target cluster cannot also be one of the selected source clusters.');
  }

  let resolvedTargetClusterId = destination.targetClusterId || null;
  let totalMoved = 0;
  for (const [sourceClusterId, faceIds] of faceGroups.entries()) {
    const payload = {
      source_cluster_id: sourceClusterId,
      face_ids: faceIds,
      target_cluster_id: resolvedTargetClusterId,
      target_person_label: resolvedTargetClusterId ? null : (destination.targetPersonLabel || null),
    };
    const result = await apiPost('/clusters/reassign-faces', payload);
    totalMoved += Number(result?.moved_faces || faceIds.length);
    if (!resolvedTargetClusterId && Number.isFinite(result?.target_cluster_id)) {
      resolvedTargetClusterId = result.target_cluster_id;
    }
  }

  return {
    movedFaces: totalMoved,
    sourceClusterCount: faceGroups.size,
    targetClusterId: resolvedTargetClusterId,
  };
}

async function loadClusters() {
  updateSidebarModeToggle();
  if (clusterSidebarMode === 'by-person-prototype') {
    await loadByPersonPrototype();
    return;
  }

  selectedPrototypeLabel = null;
  prototypeGroups = [];
  prototypeSourceClusterId = null;
  prototypeScopedClusterMode = false;
  setEmpty('cluster-list-tagged', 'Loading clusters...');
  setEmpty('cluster-list-untagged', 'Loading clusters...');
  setSkeleton('crop-grid', 'crop', 12);

  try {
    clusters = await api('/clusters?sort=review');
    if (selectedClusterId == null || !clusters.some(c => c.cluster_id === selectedClusterId)) {
      selectedClusterId = clusters.length ? clusters[0].cluster_id : null;
    }
    renderClusterList();

    if (clusters.length > 0) {
      await selectClusterById(selectedClusterId);
    } else {
      setClusterReviewScope('cluster');
      clearFaceSelection();
      updateClusterReviewChrome();
      setEmpty('crop-grid', 'No clusters yet. Run Process and Cluster phases.');
    }
  } catch (e) {
    setEmpty('cluster-list-tagged', `Error: ${e.message}`, true);
    setEmpty('cluster-list-untagged', `Error: ${e.message}`, true);
    setEmpty('crop-grid', 'Unable to load cluster crops.', true);
  }
}

function clusterDotClass(c) {
  if (c.is_noise) return 'dot-noise';
  if (c.approved) return 'dot-approved';
  if (c.person_label) return 'dot-labeled';
  return 'dot-unlabeled';
}

function clusterPriorityChip(c) {
  if (c.is_noise) return { label: 'Noise', tone: 'noise' };
  if (c.review_state === 'approved') return { label: 'Done', tone: 'done' };
  if (c.review_priority_bucket === 'high') return { label: 'Now', tone: 'high' };
  if (c.review_priority_bucket === 'medium') return { label: 'Soon', tone: 'medium' };
  return { label: 'Later', tone: 'low' };
}

function clusterMetaLine(c) {
  const parts = [];
  const personRank = Number(c.person_cluster_rank);
  const personCount = Number(c.person_cluster_count);
  const personMatch = Number(c.person_match_score);
  if (c.person_label && !c.is_noise && Number.isFinite(personRank) && personCount > 1) {
    parts.push(personRank === 1 ? 'best match' : `#${personRank} of ${personCount}`);
    if (Number.isFinite(personMatch)) {
      parts.push(`${Math.round(personMatch * 100)}% person match`);
    }
  }
  const rank = Number(c.review_priority_rank);
  if (!c.is_noise && c.review_state !== 'approved' && Number.isFinite(rank)) {
    parts.push(`Q${rank}`);
  }
  if (c.review_state === 'labeled_pending') {
    parts.push('awaiting approval');
  } else if (c.review_state === 'unlabeled') {
    parts.push('needs label');
  }
  if (c.is_mega_cluster) {
    parts.push('mega');
  }
  const conf = Number(c.avg_detection_score);
  if (Number.isFinite(conf) && conf > 0) {
    parts.push(`${Math.round(conf * 100)}% conf`);
  }
  return parts.join(' | ');
}

function renderClusterList() {
  const taggedList = el('cluster-list-tagged');
  const untaggedList = el('cluster-list-untagged');
  if (!taggedList || !untaggedList) return;
  taggedList.innerHTML = '';
  untaggedList.innerHTML = '';

  const labeled = clusters.filter(c => c.person_label && !c.is_noise).length;
  const total = clusters.filter(c => !c.is_noise).length;
  const queued = clusters.filter(c => !c.is_noise && c.review_state !== 'approved').length;
  const progressNode = el('cluster-progress');
  if (progressNode) progressNode.textContent = `${labeled} / ${total} labeled | ${queued} queued`;

  if (clusters.length === 0) {
    setEmpty('cluster-list-tagged', 'No tagged clusters.');
    setEmpty('cluster-list-untagged', 'No clusters found.');
    return;
  }

  const tagged = clusters
    .filter(c => c.person_label && !c.is_noise)
    .sort((a, b) => {
      const nameCmp = String(a.person_label || '').localeCompare(String(b.person_label || ''), undefined, {
        sensitivity: 'base',
      });
      if (nameCmp !== 0) return nameCmp;
      return (Number(b.face_count) || 0) - (Number(a.face_count) || 0)
        || (Number(a.cluster_id) || 0) - (Number(b.cluster_id) || 0);
    });
  const untagged = clusters.filter(c => !c.person_label || c.is_noise);

  const renderItem = (c, mountNode) => {
    const div = document.createElement('div');
    div.className = 'sidebar-item' + (c.cluster_id === selectedClusterId ? ' active' : '');
    div.dataset.clusterId = String(c.cluster_id);
    const meta = clusterMetaLine(c);
    const chip = clusterPriorityChip(c);
    div.innerHTML = `
      <span class="status-dot ${clusterDotClass(c)}"></span>
      <span class="item-main">
        <span class="item-label">${escHtml(c.person_label || `Cluster ${c.cluster_id}`)}</span>
        ${meta ? `<span class="item-meta">${escHtml(meta)}</span>` : ''}
      </span>
      ${chip ? `<span class="priority-chip priority-chip--${chip.tone}">${escHtml(chip.label)}</span>` : ''}
      <span class="item-count">${c.face_count}</span>
    `;
    div.addEventListener('click', () => selectClusterById(c.cluster_id));
    mountNode.appendChild(div);
  };

  const taggedGroups = new Map();
  tagged.forEach(c => {
    const name = (c.person_label || '').trim();
    const key = name.toLowerCase();
    if (!taggedGroups.has(key)) taggedGroups.set(key, { name, items: [] });
    taggedGroups.get(key).items.push(c);
  });

  Array.from(taggedGroups.entries())
    .sort((a, b) => a[1].name.localeCompare(b[1].name, undefined, { sensitivity: 'base' }))
    .forEach(([key, group]) => {
      group.items.sort((a, b) => {
        const rankA = Number(a.person_cluster_rank);
        const rankB = Number(b.person_cluster_rank);
        const hasRankA = Number.isFinite(rankA);
        const hasRankB = Number.isFinite(rankB);
        if (hasRankA && hasRankB && rankA !== rankB) return rankA - rankB;
        if (hasRankA !== hasRankB) return hasRankA ? -1 : 1;
        const matchA = Number(a.person_match_score);
        const matchB = Number(b.person_match_score);
        if (Number.isFinite(matchA) && Number.isFinite(matchB) && matchA !== matchB) return matchB - matchA;
        return (Number(b.face_count) || 0) - (Number(a.face_count) || 0)
          || (Number(a.cluster_id) || 0) - (Number(b.cluster_id) || 0);
      });

      const wrap = document.createElement('section');
      wrap.className = 'cluster-group';

      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'cluster-group-toggle';
      const isCollapsed = collapsedClusterGroups.has(key);
      button.setAttribute('aria-expanded', isCollapsed ? 'false' : 'true');
      button.innerHTML = `
        <span class="cluster-group-title">${escHtml(group.name || 'Unnamed')}</span>
        <span class="cluster-group-meta">${group.items.length}</span>
      `;
      button.addEventListener('click', () => {
        if (collapsedClusterGroups.has(key)) {
          collapsedClusterGroups.delete(key);
        } else {
          collapsedClusterGroups.add(key);
        }
        renderClusterList();
      });
      wrap.appendChild(button);

      const body = document.createElement('div');
      body.className = 'cluster-group-body' + (isCollapsed ? ' collapsed' : '');
      group.items.forEach(c => renderItem(c, body));
      wrap.appendChild(body);
      taggedList.appendChild(wrap);
    });

  const pendingClusters = untagged
    .filter(c => !c.is_noise)
    .sort((a, b) => {
      return (Number(a.review_priority_rank) || Number.MAX_SAFE_INTEGER)
        - (Number(b.review_priority_rank) || Number.MAX_SAFE_INTEGER)
        || (Number(b.face_count) || 0) - (Number(a.face_count) || 0)
        || (Number(a.cluster_id) || 0) - (Number(b.cluster_id) || 0);
    });
  const noiseClusters = untagged
    .filter(c => c.is_noise)
    .sort((a, b) => (Number(b.face_count) || 0) - (Number(a.face_count) || 0)
      || (Number(a.cluster_id) || 0) - (Number(b.cluster_id) || 0));

  const untaggedGroups = [
    { key: 'pending', name: 'Pending Queue', items: pendingClusters },
    { key: 'noise', name: 'Noise', items: noiseClusters },
  ];

  untaggedGroups.forEach(group => {
    const wrap = document.createElement('section');
    wrap.className = 'cluster-group';

    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'cluster-group-toggle';
    const isCollapsed = collapsedUntaggedGroups.has(group.key);
    button.setAttribute('aria-expanded', isCollapsed ? 'false' : 'true');
    button.innerHTML = `
      <span class="cluster-group-title">${escHtml(group.name)}</span>
      <span class="cluster-group-meta">${group.items.length}</span>
    `;
    button.addEventListener('click', () => {
      if (collapsedUntaggedGroups.has(group.key)) {
        collapsedUntaggedGroups.delete(group.key);
      } else {
        collapsedUntaggedGroups.add(group.key);
      }
      renderClusterList();
    });
    wrap.appendChild(button);

    const body = document.createElement('div');
    body.className = 'cluster-group-body' + (isCollapsed ? ' collapsed' : '');
    if (group.items.length === 0) {
      const empty = document.createElement('div');
      empty.className = 'cluster-group-empty';
      empty.textContent = `No ${group.name.toLowerCase()} clusters.`;
      body.appendChild(empty);
    } else {
      group.items.forEach(c => renderItem(c, body));
    }
    wrap.appendChild(body);
    untaggedList.appendChild(wrap);
  });

  if (!tagged.length) setEmpty('cluster-list-tagged', 'No tagged clusters.');
  if (!untagged.length) {
    collapsedUntaggedGroups.delete('pending');
    collapsedUntaggedGroups.delete('noise');
  }

  const sel = el('merge-target');
  if (sel) {
    sel.innerHTML = '<option value="">Merge into...</option>';
    clusters.forEach(c => {
      if (!c.is_noise) {
        const opt = document.createElement('option');
        opt.value = c.cluster_id;
        opt.textContent = c.person_label || `Cluster ${c.cluster_id}`;
        sel.appendChild(opt);
      }
    });
  }
  refreshReassignTargets(selectedClusterId);
}

async function selectClusterById(clusterId) {
  if (clusterId == null) return;
  const idx = clusters.findIndex(c => c.cluster_id === clusterId);
  if (idx < 0) return;
  await selectCluster(idx);
}

async function selectCluster(idx) {
  selectedClusterIdx = idx;
  const c = clusters[idx];
  if (!c) return;
  selectedClusterId = c.cluster_id;
  setClusterReviewScope('cluster');

  document.querySelectorAll('.cluster-column .sidebar-item').forEach(node => {
    node.classList.toggle('active', node.dataset.clusterId === String(selectedClusterId));
  });

  const toolbar = el('cluster-toolbar');
  if (toolbar) toolbar.style.display = 'flex';

  const nameInput = el('cluster-name-input');
  if (nameInput) nameInput.value = c.person_label || '';
  clearFaceSelection();
  refreshReassignTargets(c.cluster_id);
  updateClusterReviewChrome();
  loadClusterSuggestions(c.cluster_id);
  await loadActiveClusterFaces();
}

el('btn-sidebar-by-cluster')?.addEventListener('click', async () => {
  if (clusterSidebarMode === 'by-cluster') return;
  clusterSidebarMode = 'by-cluster';
  prototypeSourceClusterId = null;
  prototypeScopedClusterMode = false;
  clearFaceSelection();
  setClusterReviewScope('cluster');
  await loadClusters();
});

el('btn-sidebar-by-person')?.addEventListener('click', async () => {
  if (clusterSidebarMode === 'by-person-prototype') return;
  prototypeSourceClusterId = resolvePrototypeSourceClusterId();
  clusterSidebarMode = 'by-person-prototype';
  clearFaceSelection();
  setClusterReviewScope('cluster');
  await loadClusters();
});

el('btn-approve-cluster').addEventListener('click', async () => {
  if (isPrototypeReviewMode()) return;
  const c = clusters[selectedClusterIdx];
  if (!c) return;

  const button = el('btn-approve-cluster');
  setBusy(button, true);

  try {
    const name = el('cluster-name-input').value.trim();
    if (name) {
      await apiPost(`/clusters/${c.cluster_id}/label`, { person_label: name });
    }
    await apiPost(`/clusters/${c.cluster_id}/approve`);
    await loadClusters();
    showToast('Cluster approved.', 'ok');
  } catch (e) {
    showToast('Failed to approve cluster.', 'err');
  } finally {
    setBusy(button, false);
  }
});

el('btn-review-person')?.addEventListener('click', async () => {
  if (isPrototypeReviewMode()) return;
  const c = clusters[selectedClusterIdx];
  const personLabel = (c?.person_label || '').trim();
  if (!c || !personLabel) {
    showToast('Pick a labeled cluster first.', 'err');
    return;
  }

  clearFaceSelection();
  if (isPersonReviewMode()) {
    setClusterReviewScope('cluster');
  } else {
    setClusterReviewScope('person', personLabel);
  }
  updateClusterReviewChrome();
  await loadActiveClusterFaces();
});

el('btn-clear-face-selection').addEventListener('click', () => clearFaceSelection());

el('btn-select-all-faces').addEventListener('click', () => {
  document.querySelectorAll('#crop-grid .crop-tile').forEach(tile => {
    const faceId = Number(tile.dataset.faceId);
    if (!faceId) return;
    selectedFaceIds.add(faceId);
    tile.classList.add('selected');
  });
  updateFaceSelectionCount();
});

el('btn-reassign-faces').addEventListener('click', async () => {
  const prototypeMode = isPrototypeReviewMode();
  const selectedCount = selectedFaceIds.size;
  if (!selectedFaceIds.size) {
    showToast('Select one or more faces first.', 'err');
    return;
  }

  const destination = validateReassignDestination();
  if (!destination) return;

  const button = el('btn-reassign-faces');
  setBusy(button, true);
  try {
    let result = null;
    const destinationLabel = `"${destination.targetPersonLabel}"`;
    const confirmationText = prototypeMode
      ? `Move ${selectedCount} face(s) from the selected prototype group to ${destinationLabel}?`
      : `Move ${selectedCount} face(s) from cluster ${clusters[selectedClusterIdx]?.cluster_id} to ${destinationLabel}?`;
    const confirmed = window.confirm(confirmationText);
    if (!confirmed) return;

    if (prototypeMode) {
      result = await reassignSelectedPrototypeFaces(destination);
    } else {
      const c = clusters[selectedClusterIdx];
      if (!c) return;
      const payload = {
        source_cluster_id: c.cluster_id,
        face_ids: Array.from(selectedFaceIds),
        target_cluster_id: destination.targetClusterId,
        target_person_label: destination.targetPersonLabel || null,
      };
      result = await apiPost('/clusters/reassign-faces', payload);
    }

    clearFaceSelection();
    if (el('reassign-name-input')) el('reassign-name-input').value = '';
    if (el('reassign-target')) el('reassign-target').value = '';
    await loadClusters();
    if (!prototypeMode && result && Number.isFinite(result.target_cluster_id)) {
      await selectClusterById(result.target_cluster_id);
    }
    showToast(
      prototypeMode
        ? `Moved ${result?.movedFaces || selectedCount} face(s) to ${destination.targetPersonLabel}.`
        : `Moved ${selectedCount} face(s) to ${destination.targetPersonLabel}.`,
      'ok',
    );
  } catch (e) {
    showToast(e?.message || 'Failed to reassign selected faces.', 'err');
  } finally {
    setBusy(button, false);
  }
});

el('btn-undo-last-move').addEventListener('click', async () => {
  const confirmed = window.confirm('Undo the most recent face reassignment?');
  if (!confirmed) return;

  const button = el('btn-undo-last-move');
  setBusy(button, true);
  try {
    const r = await apiPost('/clusters/reassign-faces/undo-last');
    await loadClusters();
    if (!isPrototypeReviewMode() && r && Number.isFinite(r.source_cluster_id)) {
      await selectClusterById(r.source_cluster_id);
    }
    showToast(`Undid move ${r.move_id} (${r.moved_faces} face(s)).`, 'ok');
  } catch (_) {
    showToast('No move available to undo.', 'err');
  } finally {
    setBusy(button, false);
  }
});

function selectedFaceGroupsFromGrid() {
  const groups = new Map();
  document.querySelectorAll('#crop-grid .crop-tile.selected').forEach(tile => {
    const faceId = Number(tile.dataset.faceId);
    const clusterId = Number(tile.dataset.clusterId);
    if (!faceId || !Number.isFinite(clusterId)) return;
    if (!groups.has(clusterId)) groups.set(clusterId, []);
    groups.get(clusterId).push(faceId);
  });
  return groups;
}

el('btn-untag-cluster').addEventListener('click', async () => {
  if (isPrototypeReviewMode()) return;
  const c = clusters[selectedClusterIdx];
  if (!c) return;

  const button = el('btn-untag-cluster');
  setBusy(button, true);

  try {
    if (selectedFaceIds.size > 0) {
      if (isPersonReviewMode()) {
        const personLabel = clusterReviewScope.personLabel || c.person_label || 'this person';
        const faceGroups = selectedFaceGroupsFromGrid();
        const totalSelected = selectedFaceIds.size;
        const sourceClusterCount = faceGroups.size;
        if (!faceGroups.size) {
          showToast('No valid face selections found.', 'err');
          return;
        }
        const confirmed = window.confirm(
          `Remove ${totalSelected} selected face(s) from ${personLabel} and move them into new unlabeled cluster(s)?`
        );
        if (!confirmed) return;

        let totalRemoved = 0;
        for (const [sourceClusterId, faceIds] of faceGroups.entries()) {
          const result = await apiPost(`/clusters/${sourceClusterId}/untag-faces`, {
            face_ids: faceIds,
          });
          totalRemoved += Number(result?.moved_faces || faceIds.length);
        }
        clearFaceSelection();
        await loadClusters();
        const nextCluster = clusters.find(item =>
          String(item.person_label || '').trim().toLowerCase() === String(personLabel).trim().toLowerCase());
        if (nextCluster) {
          await selectClusterById(nextCluster.cluster_id);
          setClusterReviewScope('person', personLabel);
          updateClusterReviewChrome();
          await loadActiveClusterFaces();
        }
        showToast(
          sourceClusterCount > 1
            ? `Removed ${totalRemoved} face(s) from ${personLabel} across ${sourceClusterCount} clusters.`
            : `Removed ${totalRemoved} face(s) from ${personLabel}.`,
          'ok',
        );
        return;
      }

      const count = selectedFaceIds.size;
      const confirmed = window.confirm(
        `Untag ${count} selected face(s) and move them to a new unlabeled cluster?`
      );
      if (!confirmed) return;

      const result = await apiPost(`/clusters/${c.cluster_id}/untag-faces`, {
        face_ids: Array.from(selectedFaceIds),
      });
      clearFaceSelection();
      await loadClusters();
      if (result && Number.isFinite(result.target_cluster_id)) {
        await selectClusterById(result.target_cluster_id);
      }
      showToast(`Untagged ${result?.moved_faces || count} selected face(s).`, 'ok');
      return;
    }

    const confirmed = window.confirm(
      `No faces selected. Untag the entire cluster ${c.cluster_id}?`
    );
    if (!confirmed) return;

    await apiPost(`/clusters/${c.cluster_id}/untag`);
    await loadClusters();
    showToast('Cluster untagged.', 'ok');
  } catch (e) {
    showToast('Failed to untag cluster.', 'err');
  } finally {
    setBusy(button, false);
  }
});

el('btn-noise-cluster').addEventListener('click', async () => {
  if (isPrototypeReviewMode()) return;
  const c = clusters[selectedClusterIdx];
  if (!c) return;

  const button = el('btn-noise-cluster');
  setBusy(button, true);

  try {
    await apiPost(`/clusters/${c.cluster_id}/noise`);
    await loadClusters();
    showToast('Cluster marked as noise.', 'ok');
  } catch (e) {
    showToast('Failed to update cluster.', 'err');
  } finally {
    setBusy(button, false);
  }
});

el('btn-merge-cluster').addEventListener('click', async () => {
  if (isPrototypeReviewMode()) return;
  const c = clusters[selectedClusterIdx];
  if (!c) return;

  const targetId = parseInt(el('merge-target').value, 10);
  if (!targetId || targetId === c.cluster_id) {
    showToast('Select a valid merge target.', 'err');
    return;
  }

  const button = el('btn-merge-cluster');
  setBusy(button, true);

  try {
    await apiPost('/clusters/merge', {
      source_cluster_id: c.cluster_id,
      target_cluster_id: targetId,
    });
    await loadClusters();
    showToast('Clusters merged.', 'ok');
  } catch (e) {
    showToast('Failed to merge clusters.', 'err');
  } finally {
    setBusy(button, false);
  }
});

document.addEventListener('keydown', e => {
  if (isPaletteOpen()) return;
  if (!el('tab-clusters').classList.contains('active')) return;
  if (e.key === 'Escape') {
    clearFaceSelection();
    return;
  }
  if ((e.ctrlKey || e.metaKey) && (e.key === 'a' || e.key === 'A')) {
    e.preventDefault();
    el('btn-select-all-faces').click();
    return;
  }
  if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
  if (clusterSidebarMode !== 'by-cluster') return;

  switch (e.key) {
    case 'Enter':
      el('btn-approve-cluster').click();
      break;
    case 'u':
    case 'U':
      el('btn-untag-cluster').click();
      break;
    case 'n':
    case 'N':
      el('btn-noise-cluster').click();
      break;
    case 'ArrowRight':
      if (selectedClusterIdx < clusters.length - 1) selectCluster(selectedClusterIdx + 1);
      break;
    case 'ArrowLeft':
      if (selectedClusterIdx > 0) selectCluster(selectedClusterIdx - 1);
      break;
    default:
      break;
  }
});

el('reassign-target')?.addEventListener('change', () => {
  if ((el('reassign-target')?.value || '') && el('reassign-name-input')) {
    el('reassign-name-input').value = '';
  }
});

el('reassign-name-input')?.addEventListener('input', () => {
  if ((el('reassign-name-input')?.value || '').trim() && el('reassign-target')) {
    el('reassign-target').value = '';
  }
});

el('cluster-suggestions')?.addEventListener('click', async (evt) => {
  const btn = evt.target.closest('button[data-accept-suggestion]');
  if (!btn) return;
  const clusterId = parseInt(btn.dataset.clusterId, 10);
  const personLabel = btn.dataset.name || '';
  if (!clusterId || !personLabel) return;

  const confirmed = window.confirm(`Use suggested name "${personLabel}" for cluster ${clusterId}?`);
  if (!confirmed) return;

  setBusy(btn, true);
  try {
    await apiPost(`/clusters/${clusterId}/accept-suggestion`, { person_label: personLabel });
    await loadClusters();
    await selectClusterById(clusterId);
    showToast(`Applied suggestion: ${personLabel}`, 'ok');
  } catch (_) {
    showToast('Failed to apply suggestion.', 'err');
  } finally {
    setBusy(btn, false);
  }
});

let paletteSelection = 0;
let paletteCommands = [];

function isPaletteOpen() {
  const overlay = el('command-palette');
  return overlay ? overlay.classList.contains('open') : false;
}

function getCommandCatalog() {
  const tabCommands = [
    { id: 'tab-dashboard', label: 'Go to Dashboard', keywords: 'tab dashboard home', run: () => switchTab('dashboard') },
    { id: 'tab-clusters', label: 'Go to Cluster Review', keywords: 'tab clusters people faces', run: () => switchTab('clusters') },
    { id: 'tab-objects', label: 'Go to Objects & Pets', keywords: 'tab objects tags pets', run: () => switchTab('objects') },
    { id: 'tab-photos', label: 'Go to Photo Browser', keywords: 'tab photos browse filter', run: () => switchTab('photos') },
    { id: 'tab-settings', label: 'Go to Settings', keywords: 'tab settings config', run: () => switchTab('settings') },
  ];

  const phaseCommands = PHASE_DEFS.map(phase => ({
    id: `run-${phase.id}`,
    label: `Run Phase: ${phase.name}`,
    keywords: `run ${phase.id} pipeline phase`,
    run: () => triggerPhase(phase.id),
  }));

  const queryCommands = [
    {
      id: 'query-undated',
      label: 'Quick Query: undated',
      keywords: 'query undated no-date',
      run: () => {
        switchTab('photos');
        el('quick-search').value = 'undated';
        applyQuickQuery('undated');
        loadPhotos(1);
      },
    },
    {
      id: 'query-recent',
      label: 'Quick Query: year:2025',
      keywords: 'query year recent',
      run: () => {
        switchTab('photos');
        el('quick-search').value = 'year:2025';
        applyQuickQuery('year:2025');
        loadPhotos(1);
      },
    },
  ];

  return [...tabCommands, ...phaseCommands, ...queryCommands];
}

function renderCommandResults(commands) {
  const box = el('cmd-results');
  if (!box) return;

  if (!commands.length) {
    box.innerHTML = '<div class="empty-state">No matching commands.</div>';
    return;
  }

  box.innerHTML = '';
  commands.forEach((cmd, i) => {
    const row = document.createElement('button');
    row.className = `cmd-row ${i === paletteSelection ? 'active' : ''}`;
    row.type = 'button';
    row.innerHTML = `<span>${escHtml(cmd.label)}</span>`;
    row.addEventListener('click', () => {
      executeCommand(cmd);
    });
    box.appendChild(row);
  });
}

function refreshCommandPalette() {
  const input = el('cmd-input');
  const q = (input?.value || '').trim().toLowerCase();
  const catalog = getCommandCatalog();

  paletteCommands = !q
    ? catalog
    : catalog.filter(c => c.label.toLowerCase().includes(q) || c.keywords.includes(q));

  if (paletteSelection >= paletteCommands.length) paletteSelection = 0;
  renderCommandResults(paletteCommands);
}

function openCommandPalette(prefill = '') {
  const overlay = el('command-palette');
  const input = el('cmd-input');
  if (!overlay || !input) return;

  overlay.classList.add('open');
  input.value = prefill;
  paletteSelection = 0;
  refreshCommandPalette();
  setTimeout(() => input.focus(), 10);
}

function closeCommandPalette() {
  const overlay = el('command-palette');
  if (overlay) overlay.classList.remove('open');
}

function executeCommand(cmd) {
  closeCommandPalette();
  if (cmd && typeof cmd.run === 'function') cmd.run();
}

let selectedTag = null;
let objPage = 1;

async function loadTagBrowser() {
  setEmpty('tag-list', 'Loading tags...');
  setEmpty('obj-photo-grid', 'Choose a tag to browse photos.');
  const countNode = el('obj-count');
  if (countNode) countNode.textContent = '';
  refreshRailContext();

  try {
    const grouped = await api('/objects/tags');
    renderTagBrowser(grouped);
    refreshRailContext();
  } catch (e) {
    setEmpty('tag-list', `Error: ${e.message}`, true);
    refreshRailContext();
  }
}

function renderTagBrowser(grouped) {
  const list = el('tag-list');
  if (!list) return;
  list.innerHTML = '';

  const groups = Object.entries(grouped);
  if (!groups.length) {
    setEmpty('tag-list', 'No approved tags yet.');
    return;
  }

  groups.forEach(([group, tags]) => {
    const hdr = document.createElement('div');
    hdr.className = 'tag-group-header';
    hdr.textContent = group.toUpperCase();
    list.appendChild(hdr);

    tags.forEach(t => {
      const sources = t.sources || [];
      const hasYolo = sources.includes('yolo');
      const hasClip = sources.includes('clip');
      let dotClass = 'dot-clip';
      if (hasYolo && hasClip) dotClass = 'dot-both';
      else if (hasYolo) dotClass = 'dot-yolo';

      const div = document.createElement('div');
      div.className = 'tag-item' + (t.tag === selectedTag ? ' active' : '');
      div.innerHTML = `
        <span class="source-dot ${dotClass}"></span>
        <span class="tag-name">${escHtml(t.tag)}</span>
        <span class="tag-count">${t.photo_count}</span>
      `;
      div.addEventListener('click', () => {
        selectedTag = t.tag;
        document.querySelectorAll('.tag-item').forEach(x => x.classList.remove('active'));
        div.classList.add('active');

        el('obj-tag-title').textContent = t.tag;
        objPage = 1;
        refreshRailContext();
        loadObjPhotos();
      });
      list.appendChild(div);
    });
  });
}

async function loadObjPhotos() {
  if (!selectedTag) return;

  setSkeleton('obj-photo-grid', 'thumb', 12);
  const pager = el('obj-pagination');
  if (pager) pager.innerHTML = '';

  try {
    const data = await api(`/objects/tags/${encodeURIComponent(selectedTag)}?page=${objPage}&per_page=48`);
    const countNode = el('obj-count');
    if (countNode) countNode.textContent = `${fmt(data.total)} photos`;

    renderPhotoGrid('obj-photo-grid', data.photos);
    renderPagination('obj-pagination', objPage, data.total, 48, p => {
      objPage = p;
      loadObjPhotos();
    });
    refreshRailContext();
  } catch (e) {
    setEmpty('obj-photo-grid', `Error: ${e.message}`, true);
    refreshRailContext();
  }
}

el('btn-vocab-manager').addEventListener('click', async () => {
  const panel = el('vocab-panel');
  if (!panel) return;

  panel.style.display = panel.style.display === 'none' ? 'block' : 'none';
  if (panel.style.display !== 'none') await loadVocab();
});

el('btn-vocab-close').addEventListener('click', () => {
  const panel = el('vocab-panel');
  if (panel) panel.style.display = 'none';
});

async function loadVocab() {
  const tbody = el('vocab-tbody');
  if (!tbody) return;

  tbody.innerHTML = '<tr><td colspan="5" class="dim">Loading vocabulary...</td></tr>';

  try {
    const vocab = await api('/objects/vocabulary');
    tbody.innerHTML = '';

    if (!vocab.length) {
      tbody.innerHTML = '<tr><td colspan="5" class="dim">No vocabulary entries.</td></tr>';
      return;
    }

    vocab.forEach(v => {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td class="mono dim">${escHtml(v.tag_group)}</td>
        <td>${escHtml(v.tag_name)}</td>
        <td class="dim">${escHtml(v.prompts.join(', '))}</td>
        <td><input type="checkbox" ${v.enabled ? 'checked' : ''} disabled /></td>
        <td><button class="btn btn-danger js-delete-vocab" data-vocab-id="${v.vocab_id}">Delete</button></td>
      `;
      tbody.appendChild(tr);
    });
  } catch (e) {
    tbody.innerHTML = '<tr><td colspan="5" class="red">Failed to load vocabulary.</td></tr>';
  }
}

const vocabTbody = el('vocab-tbody');
if (vocabTbody) {
  vocabTbody.addEventListener('click', e => {
    const btn = e.target.closest('.js-delete-vocab');
    if (!btn) return;
    const id = parseInt(btn.dataset.vocabId, 10);
    if (Number.isFinite(id)) deleteVocab(id);
  });
}

async function deleteVocab(id) {
  if (!confirm('Delete this vocabulary entry?')) return;

  try {
    await apiDelete(`/objects/vocabulary/${id}`);
    await loadVocab();
    showToast('Vocabulary entry deleted.', 'ok');
  } catch (e) {
    showToast('Delete failed.', 'err');
  }
}

el('btn-add-vocab').addEventListener('click', async () => {
  const group = el('new-vocab-group').value.trim();
  const name = el('new-vocab-name').value.trim();
  const prompts = el('new-vocab-prompts').value.trim().split('\n').map(x => x.trim()).filter(Boolean);
  const enabled = el('new-vocab-enabled').checked;

  if (!group || !name || !prompts.length) {
    showToast('Fill in group, tag name, and at least one prompt.', 'err');
    return;
  }

  const button = el('btn-add-vocab');
  setBusy(button, true);

  try {
    await apiPost('/objects/vocabulary', { tag_group: group, tag_name: name, prompts, enabled });
    el('new-vocab-group').value = '';
    el('new-vocab-name').value = '';
    el('new-vocab-prompts').value = '';
    await loadVocab();
    showToast('Vocabulary entry added.', 'ok');
  } catch (e) {
    showToast('Failed to add vocabulary.', 'err');
  } finally {
    setBusy(button, false);
  }
});

let photoPage = 1;
let activeModalPhotoId = null;
let modalActionBusy = false;

async function loadPhotos(page = 1) {
  photoPage = page;
  const params = buildPhotoParams();

  setSkeleton('photo-grid', 'thumb', 15);
  const pager = el('photo-pagination');
  if (pager) pager.innerHTML = '';

  try {
    const data = await api(`/photos?${params}&page=${page}&per_page=60`);
    const countNode = el('photo-count');
    if (countNode) countNode.textContent = `${fmt(data.total)} photos`;

    renderPhotoGrid('photo-grid', data.photos);
    renderPagination('photo-pagination', page, data.total, 60, p => loadPhotos(p));
    refreshRailContext();
  } catch (e) {
    setEmpty('photo-grid', `Error: ${e.message}`, true);
    refreshRailContext();
  }
}

function buildPhotoParams() {
  const p = new URLSearchParams();

  const person = el('filter-person').value;
  const tag = el('filter-tag').value;
  const year = el('filter-year').value;
  const month = el('filter-month').value;
  const undated = el('filter-undated').checked;

  if (person) p.set('person', person);
  if (tag) p.set('tag', tag);
  if (year) p.set('year', year);
  if (month) p.set('month', month.padStart(2, '0'));
  if (undated) p.set('undated', 'true');

  return p.toString();
}

function applyQuickQuery(raw) {
  const q = (raw || '').trim();
  if (!q) return;

  let person = '';
  let tag = '';
  let year = '';
  let month = '';
  let undated = false;

  q.split(/\s+/).forEach(token => {
    const [k, ...rest] = token.split(':');
    const value = rest.join(':');
    const key = k.toLowerCase();

    if (key === 'person' && value) person = value;
    if (key === 'tag' && value) tag = value;
    if (key === 'year' && value) year = value;
    if (key === 'month' && value) month = value.padStart(2, '0');
    if (key === 'undated' || key === 'no-date') undated = true;
  });

  if (person) el('filter-person').value = person;
  if (tag) el('filter-tag').value = tag;
  if (year) el('filter-year').value = year;
  if (month) el('filter-month').value = month;

  el('filter-undated').checked = undated;
  ['filter-person', 'filter-tag', 'filter-year', 'filter-month'].forEach(id => {
    el(id).disabled = undated;
  });
}

el('btn-apply-filters').addEventListener('click', () => loadPhotos(1));

el('btn-quick-search').addEventListener('click', () => {
  applyQuickQuery(el('quick-search').value);
  loadPhotos(1);
});

el('quick-search').addEventListener('keydown', e => {
  if (e.key === 'Enter') {
    e.preventDefault();
    applyQuickQuery(el('quick-search').value);
    loadPhotos(1);
  }
});

el('filter-undated').addEventListener('change', () => {
  const undated = el('filter-undated').checked;
  ['filter-person', 'filter-tag', 'filter-year', 'filter-month'].forEach(id => {
    el(id).disabled = undated;
  });
});

async function initPhotoFilters() {
  await refreshPhotoFilters(false);

  const yearSel = el('filter-year');
  const monthSel = el('filter-month');
  const currentYear = new Date().getFullYear();

  for (let y = currentYear; y >= 1970; y -= 1) {
    const opt = document.createElement('option');
    opt.value = y;
    opt.textContent = y;
    yearSel.appendChild(opt);
  }

  for (let m = 1; m <= 12; m += 1) {
    const opt = document.createElement('option');
    opt.value = String(m).padStart(2, '0');
    opt.textContent = new Date(2000, m - 1, 1).toLocaleString('en', { month: 'long' });
    monthSel.appendChild(opt);
  }
}

async function refreshPhotoFilters(preserveSelection = true) {
  try {
    const filters = await api('/photo-filters');
    _refreshPeopleFilter(preserveSelection, filters.people || []);
    _refreshTagsFilter(preserveSelection, filters.tags || []);
  } catch (_) {
    await Promise.all([
      _refreshPeopleFilter(preserveSelection, null),
      _refreshTagsFilter(preserveSelection, null),
    ]);
  }
}

async function _refreshPeopleFilter(preserveSelection, people = null) {
  const personSel = el('filter-person');
  if (!personSel) return;
  const prev = preserveSelection ? personSel.value : '';

  personSel.innerHTML = '<option value="">All People</option>';

  const renderPeople = (items) => {
    items.forEach(item => {
      if (!item.person) return;
      const opt = document.createElement('option');
      opt.value = item.person;
      opt.textContent = item.photo_count > 0
        ? `${item.person} (${fmt(item.photo_count)})`
        : item.person;
      personSel.appendChild(opt);
    });
  };

  if (Array.isArray(people)) {
    renderPeople(people);
  } else {
    try {
      const cl = await api('/clusters');
      const deduped = new Map();
      cl.filter(c => c.person_label && c.approved && !c.is_noise).forEach(c => {
        const name = String(c.person_label || '').trim();
        if (!name || deduped.has(name)) return;
        deduped.set(name, { person: name, photo_count: 0 });
      });
      renderPeople(Array.from(deduped.values()).sort((a, b) => a.person.localeCompare(b.person)));
    } catch (_) {
      // optional
    }
  }

  if (prev && Array.from(personSel.options).some(o => o.value === prev)) {
    personSel.value = prev;
  }
}

async function _refreshTagsFilter(preserveSelection, tags = null) {
  const tagSel = el('filter-tag');
  if (!tagSel) return;
  const prev = preserveSelection ? tagSel.value : '';

  tagSel.innerHTML = '<option value="">All Tags</option>';

  const renderTags = (items) => {
    items.forEach(t => {
      if (!t.tag) return;
      const opt = document.createElement('option');
      opt.value = t.tag;
      opt.textContent = t.photo_count > 0
        ? `${t.tag} (${fmt(t.photo_count)})`
        : t.tag;
      tagSel.appendChild(opt);
    });
  };

  if (Array.isArray(tags)) {
    renderTags(tags);
  } else {
    try {
      const grouped = await api('/objects/tags');

      Object.values(grouped).forEach(groupTags => {
        groupTags.forEach(t => {
          const opt = document.createElement('option');
          opt.value = t.tag;
          opt.textContent = t.tag;
          tagSel.appendChild(opt);
        });
      });
    } catch (_) {
      // optional
    }
  }

  if (prev && Array.from(tagSel.options).some(o => o.value === prev)) {
    tagSel.value = prev;
  }
}

function renderPhotoGrid(gridId, photos) {
  const grid = el(gridId);
  if (!grid) return;

  grid.innerHTML = '';
  if (!photos || !photos.length) {
    setEmpty(gridId, 'No photos found.');
    return;
  }

  photos.forEach(photo => {
    const thumb = document.createElement('div');
    thumb.className = 'photo-thumb';

    const src = photo.dest_path
      ? `/organized/${photo.dest_path}`
      : `/originals/${photo.source_path}`;

    thumb.innerHTML = `
      <img src="${src}" alt="${escHtml(photo.filename)}" loading="lazy" onerror="this.parentElement.style.background='#ece7dd'" />
      <div class="date-overlay">${fmtDate(photo.exif_date)}</div>
    `;

    thumb.addEventListener('click', () => openPhotoModal(photo.photo_id));
    grid.appendChild(thumb);
  });
}

function renderPagination(containerId, page, total, perPage, onPage) {
  const container = el(containerId);
  if (!container) return;

  const totalPages = Math.ceil(total / perPage);
  container.innerHTML = '';
  if (totalPages <= 1) return;

  const prev = document.createElement('button');
  prev.className = 'btn';
  prev.textContent = '< Prev';
  prev.disabled = page <= 1;
  prev.addEventListener('click', () => onPage(page - 1));
  container.appendChild(prev);

  const info = document.createElement('span');
  info.className = 'page-info';
  info.textContent = `Page ${page} of ${totalPages}`;
  container.appendChild(info);

  const next = document.createElement('button');
  next.className = 'btn';
  next.textContent = 'Next >';
  next.disabled = page >= totalPages;
  next.addEventListener('click', () => onPage(page + 1));
  container.appendChild(next);
}

el('modal-close').addEventListener('click', () => {
  el('photo-modal').classList.remove('open');
  activeModalPhotoId = null;
});

el('photo-modal').addEventListener('click', e => {
  if (e.target === el('photo-modal')) {
    el('photo-modal').classList.remove('open');
    activeModalPhotoId = null;
  }
});

async function openPhotoModal(photoId) {
  const canvas = el('modal-canvas');
  const ctx = canvas.getContext('2d');
  const info = el('modal-info');

  activeModalPhotoId = photoId;
  el('photo-modal').classList.add('open');
  el('modal-filename').textContent = 'Loading...';
  info.innerHTML = '<div class="empty-state">Loading photo details...</div>';

  ctx.clearRect(0, 0, canvas.width, canvas.height);
  ctx.fillStyle = '#f4f1ea';
  ctx.fillRect(0, 0, canvas.width, canvas.height);
  ctx.fillStyle = '#4a5a67';
  ctx.font = '13px monospace';
  ctx.fillText('Loading preview...', 16, 28);

  try {
    const photo = await api(`/photos/${photoId}`);
    el('modal-filename').textContent = photo.filename;

    const img = new Image();
    img.crossOrigin = 'anonymous';
    img.onload = () => {
      const maxW = 560;
      const maxH = 420;
      const scale = Math.min(maxW / img.naturalWidth, maxH / img.naturalHeight, 1);

      canvas.width = Math.round(img.naturalWidth * scale);
      canvas.height = Math.round(img.naturalHeight * scale);
      ctx.drawImage(img, 0, 0, canvas.width, canvas.height);

      (photo.faces || []).forEach(f => {
        if (!f.bbox) return;

        const [x1, y1, x2, y2] = f.bbox.map(v => v * scale);
        ctx.strokeStyle = f.cluster_approved ? '#15803d' : '#f59e0b';
        ctx.lineWidth = 2;
        ctx.strokeRect(x1, y1, x2 - x1, y2 - y1);

        if (f.person_label) {
          ctx.fillStyle = 'rgba(20, 22, 24, 0.68)';
          ctx.fillRect(x1, y1 - 16, Math.max(84, x2 - x1), 16);
          ctx.fillStyle = '#f8fafc';
          ctx.font = '11px monospace';
          ctx.fillText(f.person_label, x1 + 4, y1 - 4);
        }
      });

      (photo.detections || []).forEach(d => {
        if (!d.bbox || !d.approved) return;
        const [x1, y1, x2, y2] = d.bbox.map(v => v * scale);
        ctx.strokeStyle = '#2563eb';
        ctx.lineWidth = 1;
        ctx.strokeRect(x1, y1, x2 - x1, y2 - y1);
      });
    };

    img.onerror = () => {
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      ctx.fillStyle = '#f4f1ea';
      ctx.fillRect(0, 0, canvas.width, canvas.height);
      ctx.fillStyle = '#4a5a67';
      ctx.font = '13px monospace';
      ctx.fillText('Preview unavailable.', 16, 28);
    };

    img.src = photo.preview_url;
    renderPhotoInfo(photo);
  } catch (e) {
    el('modal-filename').textContent = 'Preview Error';
    info.innerHTML = '<div class="empty-state red">Could not load photo details.</div>';
    showToast('Failed to load photo details.', 'err');
  }
}

function renderPhotoInfo(photo) {
  const info = el('modal-info');

  const faceTags = (photo.faces || [])
    .filter(f => f.person_label && f.cluster_approved)
    .map(f => f.person_label);

  const tags = (photo.tags || []);

  info.innerHTML = `
    <div class="info-section">
      <h4>Metadata</h4>
      <div class="info-row"><span class="label">Date: </span>${fmtDate(photo.exif_date)} <span class="dim">(${photo.date_source || ''})</span></div>
      <div class="info-row"><span class="label">File: </span><span class="mono">${escHtml(photo.filename)}</span></div>
      <div class="info-row"><span class="label">Source: </span><span class="mono dim">${escHtml(photo.source_path)}</span></div>
      ${photo.dest_path ? `<div class="info-row"><span class="label">Dest: </span><span class="mono dim">${escHtml(photo.dest_path)}</span></div>` : ''}
    </div>

    ${faceTags.length ? `
      <div class="info-section">
        <h4>People</h4>
        <div class="tag-pills">${faceTags.map(t => `<span class="pill pill-face">${escHtml(t)}</span>`).join('')}</div>
      </div>
    ` : ''}

    ${tags.length ? `
      <div class="info-section">
        <h4>Tags</h4>
        <div class="tag-pills">
          ${tags.map(t => {
            const cls = t.source === 'yolo' ? 'pill-yolo' : 'pill-clip';
            return `<span class="pill ${cls}">${escHtml(t.tag)}</span>`;
          }).join('')}
        </div>
      </div>
    ` : ''}

    ${(photo.detections || []).length ? `
      <div class="info-section">
        <h4>Detections</h4>
        ${(photo.detections || []).map(d => `
          <div class="info-row" style="display:flex; justify-content:space-between; align-items:center; gap:8px;">
            <span>${escHtml(d.tag)} <span class="dim">(${d.model})</span></span>
            <span class="mono dim">${(d.confidence || 0).toFixed(2)}</span>
            ${d.approved
              ? `<button class="btn btn-danger js-detection-action" data-action="reject" data-detection-id="${d.detection_id}">Reject</button>`
              : `<button class="btn js-detection-action" data-action="approve" data-detection-id="${d.detection_id}">Approve</button>`}
          </div>
        `).join('')}
      </div>
    ` : ''}

    ${(photo.faces || []).length ? `
      <div class="info-section">
        <h4>Faces (${photo.faces.length})</h4>
        ${(photo.faces || []).map(f => `
          <div class="info-row">
            ${f.person_label
              ? `<span class="green">${escHtml(f.person_label)}</span>${f.cluster_approved ? ' approved' : ' pending'}`
              : `<span class="dim">Cluster ${f.cluster_id ?? '?'} unlabeled</span>`}
            <span class="mono dim" style="margin-left:6px;">${(f.detection_score || 0).toFixed(2)}</span>
          </div>
        `).join('')}
      </div>
    ` : ''}
  `;
}

const modalInfo = el('modal-info');
if (modalInfo) {
  modalInfo.addEventListener('click', e => {
    const btn = e.target.closest('.js-detection-action');
    if (!btn) return;
    const id = parseInt(btn.dataset.detectionId, 10);
    if (!Number.isFinite(id)) return;

    if (btn.dataset.action === 'reject') rejectDetection(id);
    if (btn.dataset.action === 'approve') approveDetection(id);
  });
}

async function rejectDetection(id) {
  if (modalActionBusy) return;
  modalActionBusy = true;

  try {
    await apiPost(`/objects/detections/${id}/reject`);
    if (activeModalPhotoId != null) {
      await openPhotoModal(activeModalPhotoId);
    }
    showToast('Detection rejected.', 'ok');
  } catch (e) {
    showToast('Failed to reject detection.', 'err');
  } finally {
    modalActionBusy = false;
  }
}

async function approveDetection(id) {
  if (modalActionBusy) return;
  modalActionBusy = true;

  try {
    await apiPost(`/objects/detections/${id}/approve`);
    if (activeModalPhotoId != null) {
      await openPhotoModal(activeModalPhotoId);
    }
    showToast('Detection approved.', 'ok');
  } catch (e) {
    showToast('Failed to approve detection.', 'err');
  } finally {
    modalActionBusy = false;
  }
}

function bindSlider(sliderId, displayId, formatter) {
  const s = el(sliderId);
  const d = el(displayId);
  const fmt = formatter || (v => v);
  d.textContent = fmt(s.value);
  s.addEventListener('input', () => { d.textContent = fmt(s.value); });
}

async function loadSettings() {
  try {
    const s = await api('/settings');
    el('s-nas-dir').value = s.nas_source_dir || '';
    el('s-local-base').value = s.local_base || '';

    el('s-yolo-conf').value = s.yolo_conf_threshold ?? 0.45;
    el('s-clip-thresh').value = s.clip_tag_threshold ?? 0.26;
    el('s-max-dim').value = s.max_inference_dim ?? 1920;
    el('s-det-thresh').value = s.det_thresh ?? 0.4;
    el('s-umap-neighbors').value = s.umap_n_neighbors ?? 30;
    el('s-hdbscan-min-cluster').value = s.hdbscan_min_cluster_size ?? 3;
    el('s-hdbscan-min-samples').value = s.hdbscan_min_samples ?? 1;

    bindSlider('s-yolo-conf', 'v-yolo-conf');
    bindSlider('s-clip-thresh', 'v-clip-thresh');
    bindSlider('s-max-dim', 'v-max-dim', v => `${v}px`);
    bindSlider('s-det-thresh', 'v-det-thresh');
    bindSlider('s-umap-neighbors', 'v-umap-neighbors');
    bindSlider('s-hdbscan-min-cluster', 'v-hdbscan-min-cluster');
    bindSlider('s-hdbscan-min-samples', 'v-hdbscan-min-samples');

    const stats = el('stats-rows');
    stats.innerHTML = `
      <div class="stat-row"><span>NVMe Free</span><span class="val">${s.nvme_free_gb} GB</span></div>
      <div class="stat-row"><span>NVMe Total</span><span class="val">${s.nvme_total_gb} GB</span></div>
      <div class="stat-row"><span>DB Size</span><span class="val">${s.db_size_mb} MB</span></div>
      <div class="stat-row"><span>Total Photos</span><span class="val">${fmt(s.total_photos)}</span></div>
      <div class="stat-row"><span>Total Faces</span><span class="val">${fmt(s.total_faces)}</span></div>
    `;

    // Burst Intelligence status (Phase 4, only visible when enabled)
    const burstCard = el('burst-status-card');
    if (burstCard && s.burst && s.burst.enabled) {
      burstCard.style.display = '';
      const pctReq = Math.round((s.burst.requests_used / s.burst.requests_cap) * 100);
      const pctTok = Math.round((s.burst.tokens_used / s.burst.tokens_cap) * 100);
      burstCard.innerHTML = `
        <h3>NVIDIA Burst Intelligence</h3>
        <div class="stat-row"><span>Status</span><span class="val burst-on">Active</span></div>
        <div class="stat-row"><span>Requests Today</span><span class="val">${fmt(s.burst.requests_used)} / ${fmt(s.burst.requests_cap)} (${pctReq}%)</span></div>
        <div class="stat-row"><span>Tokens Today</span><span class="val">${fmt(s.burst.tokens_used)} / ${fmt(s.burst.tokens_cap)} (${pctTok}%)</span></div>
        ${(s.burst.by_type || []).map(t =>
          `<div class="stat-row sub"><span>&nbsp;&nbsp;${t.type}</span><span class="val">${t.requests} req / ${fmt(t.tokens)} tok</span></div>`
        ).join('')}
      `;
    } else if (burstCard) {
      burstCard.style.display = 'none';
    }
  } catch (e) {
    showToast('Failed to load settings.', 'err');
  }
}

el('btn-save-settings').addEventListener('click', async () => {
  const body = {
    nas_source_dir: el('s-nas-dir').value.trim() || null,
    local_base: el('s-local-base').value.trim() || null,
    yolo_conf_threshold: parseFloat(el('s-yolo-conf').value) || null,
    clip_tag_threshold: parseFloat(el('s-clip-thresh').value) || null,
    max_inference_dim: parseInt(el('s-max-dim').value, 10) || null,
    det_thresh: parseFloat(el('s-det-thresh').value) || null,
    umap_n_neighbors: parseInt(el('s-umap-neighbors').value, 10) || null,
    hdbscan_min_cluster_size: parseInt(el('s-hdbscan-min-cluster').value, 10) || null,
    hdbscan_min_samples: parseInt(el('s-hdbscan-min-samples').value, 10) || null,
  };

  const button = el('btn-save-settings');
  setBusy(button, true);

  try {
    const r = await apiPost('/settings', body);
    showToast(r.note || 'Settings saved.', 'ok');
  } catch (e) {
    showToast('Save failed.', 'err');
  } finally {
    setBusy(button, false);
  }
});

el('btn-clear-db').addEventListener('click', async () => {
  const typed = window.prompt('This is destructive. Type CLEAR to reset the database.');
  if (typed !== 'CLEAR') {
    showToast('Clear DB cancelled.', 'err');
    return;
  }

  const button = el('btn-clear-db');
  setBusy(button, true);

  try {
    const r = await apiPost('/settings/clear-db', {});
    showToast(r.note || 'Database cleared.', 'ok');

    if (el('photo-modal').classList.contains('open')) {
      el('photo-modal').classList.remove('open');
      activeModalPhotoId = null;
    }

    // Reset client-side context and refresh all count-driven views.
    selectedTag = null;
    objPage = 1;
    photoPage = 1;
    selectedClusterId = null;
    selectedClusterIdx = 0;
    clearFaceSelection();

    await refreshStatus();
    await loadSettings();
    await refreshPhotoFilters(false);
    if (activeTab === 'photos') await loadPhotos(1);
    if (activeTab === 'objects') await loadTagBrowser();
    if (activeTab === 'clusters') await loadClusters();
  } catch (e) {
    showToast(`Clear DB failed: ${e.message}`, 'err');
  } finally {
    setBusy(button, false);
  }
});

el('btn-command').addEventListener('click', () => {
  openCommandPalette();
});

const logToggleBtn = el('btn-log-toggle');
const logTail = el('log-tail');
if (logToggleBtn && logTail) {
  logToggleBtn.addEventListener('click', () => {
    const expanded = logTail.classList.toggle('expanded');
    logToggleBtn.textContent = expanded ? 'Collapse' : 'Expand';
  });
}

el('command-palette').addEventListener('click', e => {
  if (e.target === el('command-palette')) closeCommandPalette();
});

el('cmd-input').addEventListener('input', () => {
  paletteSelection = 0;
  refreshCommandPalette();
});

el('cmd-input').addEventListener('keydown', e => {
  if (!paletteCommands.length) return;

  if (e.key === 'ArrowDown') {
    e.preventDefault();
    paletteSelection = (paletteSelection + 1) % paletteCommands.length;
    renderCommandResults(paletteCommands);
  } else if (e.key === 'ArrowUp') {
    e.preventDefault();
    paletteSelection = (paletteSelection - 1 + paletteCommands.length) % paletteCommands.length;
    renderCommandResults(paletteCommands);
  } else if (e.key === 'Enter') {
    e.preventDefault();
    executeCommand(paletteCommands[paletteSelection]);
  } else if (e.key === 'Escape') {
    closeCommandPalette();
  }
});

document.addEventListener('keydown', e => {
  const target = e.target;
  const typing = target && (target.tagName === 'INPUT' || target.tagName === 'TEXTAREA');

  if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'k') {
    e.preventDefault();
    if (isPaletteOpen()) closeCommandPalette();
    else openCommandPalette();
    return;
  }

  if (e.key === 'Escape') {
    if (isPaletteOpen()) closeCommandPalette();
    if (el('photo-modal').classList.contains('open')) {
      el('photo-modal').classList.remove('open');
      activeModalPhotoId = null;
    }
    return;
  }

  if (typing || isPaletteOpen()) return;

  if (e.key === '/') {
    e.preventDefault();
    switchTab('photos');
    setTimeout(() => el('quick-search').focus(), 30);
    return;
  }

  if (['1', '2', '3', '4', '5'].includes(e.key)) {
    const map = {
      '1': 'dashboard',
      '2': 'clusters',
      '3': 'objects',
      '4': 'photos',
      '5': 'settings',
    };
    switchTab(map[e.key]);
  }
});

function escHtml(str) {
  if (!str) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// ── Search Layer (gated by settings.search_layer_enabled) ───────────────────

let _searchLayerEnabled = false;
let _lastSearchQuery = '';

async function initSearchLayer() {
  try {
    const settings = await api('/settings');
    if (!settings.search_layer_enabled) return;
    _searchLayerEnabled = true;

    el('semantic-search-bar').style.display = '';

    el('btn-search').addEventListener('click', runSemanticSearch);
    el('search-input').addEventListener('keydown', e => {
      if (e.key === 'Enter') { e.preventDefault(); runSemanticSearch(); }
    });
    el('btn-search-clear').addEventListener('click', clearSemanticSearch);
    el('btn-search-save').addEventListener('click', saveCurrentSearch);

    await refreshSavedSearches();
  } catch (_) { /* search layer not available */ }
}

async function runSemanticSearch() {
  const q = (el('search-input').value || '').trim();
  if (!q) return;
  _lastSearchQuery = q;

  setSkeleton('photo-grid', 'thumb', 15);
  el('photo-pagination').innerHTML = '';
  el('search-result-count').textContent = '';
  el('btn-search-clear').style.display = '';
  el('btn-search-save').style.display = '';

  try {
    const data = await api(`/photos/search?q=${encodeURIComponent(q)}&top_k=60`);
    el('search-result-count').textContent = `${data.total} results`;

    renderPhotoGrid('photo-grid', data.results);
    renderSearchFacets(data.facets || {});
    refreshRailContext();
  } catch (e) {
    setEmpty('photo-grid', `Search error: ${e.message}`, true);
    el('search-facets').style.display = 'none';
    refreshRailContext();
  }
}

function clearSemanticSearch() {
  el('search-input').value = '';
  el('search-result-count').textContent = '';
  el('btn-search-clear').style.display = 'none';
  el('btn-search-save').style.display = 'none';
  el('search-facets').style.display = 'none';
  _lastSearchQuery = '';
  refreshRailContext();
  loadPhotos(1);
}

function renderSearchFacets(facets) {
  const panel = el('search-facets');
  if (!facets || (!facets.tags?.length && !facets.people?.length && !facets.years?.length)) {
    panel.style.display = 'none';
    return;
  }
  panel.style.display = '';

  _renderFacetGroup('facet-people', 'People', facets.people || [], item => {
    el('filter-person').value = item.person;
    loadPhotos(1);
  }, 'person');

  _renderFacetGroup('facet-tags', 'Tags', facets.tags || [], item => {
    el('filter-tag').value = item.tag;
    loadPhotos(1);
  }, 'tag');

  _renderFacetGroup('facet-years', 'Years', facets.years || [], item => {
    el('filter-year').value = item.year;
    loadPhotos(1);
  }, 'year');
}

function _renderFacetGroup(containerId, label, items, onClick, labelKey) {
  const container = el(containerId);
  container.innerHTML = `<h5>${label}</h5>`;
  if (!items.length) { container.style.display = 'none'; return; }
  container.style.display = '';

  items.forEach(item => {
    const row = document.createElement('div');
    row.className = 'facet-item';
    row.innerHTML = `<span>${escHtml(item[labelKey])}</span><span class="facet-count">${item.count}</span>`;
    row.addEventListener('click', () => onClick(item));
    container.appendChild(row);
  });
}

async function refreshSavedSearches() {
  const sel = el('search-saved');
  if (!sel) return;
  try {
    const searches = await api('/searches');
    sel.innerHTML = '<option value="">Saved searches...</option>';
    if (!searches.length) { sel.style.display = 'none'; return; }
    sel.style.display = '';
    searches.forEach(s => {
      const opt = document.createElement('option');
      opt.value = JSON.stringify(s.query);
      opt.textContent = s.name;
      sel.appendChild(opt);
    });
    sel.onchange = () => {
      if (!sel.value) return;
      const query = JSON.parse(sel.value);
      if (query.q) {
        el('search-input').value = query.q;
        runSemanticSearch();
      }
      sel.value = '';
    };
  } catch (_) { sel.style.display = 'none'; }
}

async function saveCurrentSearch() {
  if (!_lastSearchQuery) return;
  const name = prompt('Name this search:', _lastSearchQuery);
  if (!name) return;
  try {
    await apiPost('/searches', { name, query: { q: _lastSearchQuery } });
    await refreshSavedSearches();
  } catch (e) {
    alert(`Failed to save: ${e.message}`);
  }
}

(async () => {
  updateSidebarActiveTab('dashboard');
  renderSidebarSnapshot(PHASE_DEFS.map(p => ({ phase: p.id, status: 'pending' })));
  await refreshStatus();
  await initPhotoFilters();
  await initSearchLayer();
  scheduleAutoRefresh();
})();
