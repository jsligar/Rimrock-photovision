'use strict';

const PHASES = [
  { id: 'preflight', name: 'Preflight', group: 'intake', num: 0 },
  { id: 'pull', name: 'Pull', group: 'intake', num: 1 },
  { id: 'process', name: 'Process', group: 'intake', num: 2 },
  { id: 'cluster', name: 'Cluster', group: 'intake', num: 3 },
  { id: 'organize', name: 'Organize', group: 'delivery', num: 4 },
  { id: 'tag', name: 'Tag', group: 'delivery', num: 5 },
  { id: 'push', name: 'Push', group: 'delivery', num: 6 },
  { id: 'verify', name: 'Verify', group: 'delivery', num: 7 },
  { id: 'ocr', name: 'Documents OCR', group: 'documents', num: 8 },
];

const WORKFLOWS = {
  intake: ['preflight', 'pull', 'process', 'cluster'],
  delivery: ['organize', 'tag', 'push', 'verify'],
  documents: ['ocr'],
};

const state = {
  activeTab: 'intake',
  status: null,
  settings: null,
  clusters: [],
  personFiles: [],
  selectedClusterId: null,
  selectedPersonLabel: null,
  selectedPersonClusterId: null,
  reviewFaces: null,
  reviewMode: 'cluster',
  prototypeGroups: [],
  selectedPrototypeLabel: null,
  prototypeScopeClusterId: null,
  personReviewData: null,
  selectedFaceIds: new Set(),
  lastSelectedFaceIndex: null,
  clusterSuggestions: {},
  clusterLabelDrafts: {},
  reviewSidebarCollapsed: {
    people: false,
    clusters: false,
    noise: false,
    prototype: false,
  },
  photoFiltersLoaded: false,
  settingsLoaded: false,
  photos: [],
  photoPage: 1,
  photoPerPage: 48,
  photoTotal: 0,
  searchLayerEnabled: false,
  semanticSearchActive: false,
  lastSearchQuery: '',
  savedSearches: [],
  selectedTag: null,
  objectPage: 1,
  objectTagsLoaded: false,
  objectTagGroups: {},
  photoModalId: null,
  modalActionBusy: false,
  autoRefreshTimer: null,
  logExpanded: false,
};

function el(id) {
  return document.getElementById(id);
}

function escHtml(value) {
  return String(value == null ? '' : value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

async function api(path, opts = {}) {
  const response = await fetch(`/api${path}`, opts);
  const raw = await response.text();
  let payload = {};
  if (raw) {
    try {
      payload = JSON.parse(raw);
    } catch (_) {
      payload = { raw };
    }
  }
  if (!response.ok) {
    const detail = payload.detail || payload.raw || `${response.status}`;
    throw new Error(detail);
  }
  return payload;
}

function apiPost(path, body) {
  const opts = {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
  };
  if (body !== undefined) {
    opts.body = JSON.stringify(body);
  }
  return api(path, opts);
}

function apiDelete(path) {
  return api(path, { method: 'DELETE' });
}

function fmtNumber(value) {
  if (value == null || Number.isNaN(Number(value))) {
    return '-';
  }
  return Number(value).toLocaleString();
}

function fmtDate(value) {
  if (!value) {
    return 'Undated';
  }
  return String(value).slice(0, 10);
}

function photoThumbSrc(photo) {
  const rawPath = photo?.dest_path || photo?.source_path || '';
  const prefix = photo?.dest_path ? 'organized' : 'originals';
  const parts = String(rawPath).split(/\\|\//).filter(Boolean);
  if (!parts.length) {
    return '';
  }
  return `/${prefix}/${parts.map(encodeURIComponent).join('/')}`;
}

function isTextEntryTarget(target) {
  return Boolean(
    target &&
    (
      target.tagName === 'INPUT' ||
      target.tagName === 'TEXTAREA' ||
      target.tagName === 'SELECT' ||
      target.isContentEditable
    )
  );
}

function showToast(message, tone = 'ok') {
  const stack = el('toast-stack');
  const node = document.createElement('div');
  node.className = `toast ${tone}`;
  node.textContent = message;
  stack.appendChild(node);
  setTimeout(() => node.remove(), 3200);
}

function isBusy() {
  const phases = state.status?.phases || [];
  const workflow = state.status?.workflow;
  return Boolean(workflow?.active || phases.some(phase => phase.status === 'running'));
}

function activePhase() {
  return (state.status?.phases || []).find(phase => phase.status === 'running') || null;
}

function phaseById(phaseId) {
  return (state.status?.phases || []).find(phase => phase.phase === phaseId) || null;
}

function workflowStatusLabel() {
  if (!state.status) {
    return 'Loading';
  }
  if (state.status.workflow?.active) {
    return `Workflow: ${state.status.workflow.name}`;
  }
  const running = activePhase();
  if (running) {
    return `Phase: ${running.phase}`;
  }
  const errors = (state.status.phases || []).filter(phase => phase.status === 'error');
  if (errors.length) {
    return 'Needs attention';
  }
  return 'Ready';
}

function headerToneClass() {
  if (!state.status) {
    return '';
  }
  if (state.status.workflow?.active || activePhase()) {
    return 'running';
  }
  if ((state.status.phases || []).some(phase => phase.status === 'error')) {
    return 'err';
  }
  return 'ok';
}

function openTab(tabId) {
  state.activeTab = tabId;
  document.querySelectorAll('.tab').forEach(node => {
    node.classList.toggle('active', node.dataset.tab === tabId);
  });
  document.querySelectorAll('.page').forEach(node => {
    node.classList.toggle('active', node.id === `page-${tabId}`);
  });

  if (tabId === 'review') {
    loadReview();
  } else if (tabId === 'objects') {
    loadObjectsView();
  } else if (tabId === 'library') {
    if (!state.photoFiltersLoaded) {
      loadPhotoFilters().finally(() => loadLibraryView());
    } else {
      loadLibraryView();
    }
  } else if (tabId === 'settings') {
    if (!state.settingsLoaded) {
      loadSettings();
    }
    loadLog();
  }
}

function renderHeader() {
  el('header-status-chip').textContent = workflowStatusLabel();
  el('header-status-chip').className = `status-chip ${headerToneClass()}`.trim();

  const workflow = state.status?.workflow;
  const running = activePhase();
  const backlog = state.status?.counts?.pending_clusters || 0;
  const docsPending = state.status?.counts?.pending_ocr_documents || 0;
  el('workflow-summary-name').textContent = workflow?.active ? workflow.name : 'Idle';
  el('workflow-summary-phase').textContent = running ? running.phase.toUpperCase() : 'None';
  el('workflow-summary-docs').textContent = fmtNumber(docsPending);
  el('workflow-summary-review').textContent = `${fmtNumber(backlog)} pending`;
  el('header-subtitle').textContent = workflow?.active
    ? `${workflow.name} is running. You can refresh safely; the workflow is tracked server-side.`
    : 'One-click intake through clustering, plus document OCR and review.';

  el('btn-stop-pipeline').disabled = !isBusy();
}

function renderScope() {
  const sidebar = state.status?.sidebar || {};
  const settings = state.settings || {};
  el('scope-batch-manifest').textContent = sidebar.batch_manifest_name || 'Full library';
  el('scope-year').textContent = sidebar.test_year_scope || 'All years';
  el('scope-search-ocr').textContent = sidebar.search_ocr_enabled ? 'Enabled' : 'Disabled';
  el('scope-documents-path').textContent = settings.local_base
    ? `${settings.local_base.replace(/\\/g, '/')}/documents`
    : 'Pending settings load';
}

function phasePercent(phase) {
  if (!phase) {
    return 0;
  }
  if (phase.status === 'complete') {
    return 100;
  }
  if (phase.progress_total > 0) {
    return Math.round((phase.progress_current / phase.progress_total) * 100);
  }
  return phase.status === 'running' ? 10 : 0;
}

function phaseSubcopy(phaseId) {
  const counts = state.status?.counts || {};
  if (phaseId === 'process') {
    return `${fmtNumber(counts.total_photos)} photos tracked`;
  }
  if (phaseId === 'cluster') {
    return `${fmtNumber(counts.pending_clusters)} clusters still need review`;
  }
  if (phaseId === 'organize') {
    return `${fmtNumber(counts.photos_organized)} photos organized`;
  }
  if (phaseId === 'ocr') {
    return `${fmtNumber(counts.pending_ocr_documents)} document photo(s) pending OCR`;
  }
  return 'Manual run available';
}

function renderWorkflowStepList(containerId, workflowName) {
  const container = el(containerId);
  const markup = WORKFLOWS[workflowName].map(phaseId => {
    const phaseDef = PHASES.find(item => item.id === phaseId);
    const phase = phaseById(phaseId);
    const status = phase?.status || 'pending';
    return `
      <div class="workflow-step">
        <div>
          <strong>${phaseDef.name}</strong>
          <div class="phase-meta">${escHtml(phaseSubcopy(phaseId))}</div>
        </div>
        <div class="phase-status">${escHtml(status)}</div>
      </div>
    `;
  }).join('');
  container.innerHTML = markup;
}

function renderPhaseCards() {
  const groups = {
    intake: el('phase-grid-intake'),
    delivery: el('phase-grid-delivery'),
    documents: el('phase-grid-documents'),
  };

  Object.values(groups).forEach(node => {
    node.innerHTML = '';
  });

  PHASES.forEach(def => {
    const phase = phaseById(def.id) || { status: 'pending', progress_current: 0, progress_total: 0 };
    const card = document.createElement('article');
    card.className = `phase-card is-${phase.status}`;
    const percent = phasePercent(phase);
    const disabledAttr = isBusy() ? 'disabled' : '';
    const errorText = phase.error_message
      ? `<div class="phase-error">${escHtml(phase.error_message)}</div>`
      : '';

    card.innerHTML = `
      <div class="phase-head">
        <div>
          <div class="eyebrow">Phase ${def.num}</div>
          <div class="phase-name">${escHtml(def.name)}</div>
        </div>
        <div class="phase-status">${escHtml(phase.status)}</div>
      </div>
      <div class="phase-bar"><div style="width:${percent}%"></div></div>
      <div class="phase-meta">${escHtml(phaseSubcopy(def.id))}</div>
      <div class="phase-meta">${fmtNumber(phase.progress_current)} / ${fmtNumber(phase.progress_total || 0)}</div>
      ${errorText}
      <div class="workflow-actions">
        <button class="btn btn-small" data-run-phase="${def.id}" type="button" ${disabledAttr}>Run</button>
        <button class="btn btn-small" data-reset-phase="${def.id}" type="button" ${disabledAttr}>Reset</button>
      </div>
    `;
    groups[def.group].appendChild(card);
  });

  renderWorkflowStepList('workflow-steps-intake', 'intake');
  renderWorkflowStepList('workflow-steps-delivery', 'delivery');

  const counts = state.status?.counts || {};
  el('ocr-detected-count').textContent = fmtNumber(counts.document_photos);
  el('ocr-complete-count').textContent = fmtNumber(counts.document_photos_ocr_complete);
  el('ocr-pending-count').textContent = fmtNumber(counts.pending_ocr_documents);

  const busy = isBusy();
  el('btn-run-intake').disabled = busy;
  el('btn-run-delivery').disabled = busy;
  el('btn-run-documents').disabled = busy;
}

function clusterDisplayName(cluster) {
  return cluster.person_label || `Cluster ${cluster.cluster_id}`;
}

function selectedCluster() {
  return state.clusters.find(item => item.cluster_id === state.selectedClusterId) || null;
}

function isPrototypeMode() {
  return state.reviewMode === 'prototype';
}

function isPersonMode() {
  return state.reviewMode === 'people';
}

function normalizePrototypeLabel(label) {
  return String(label || '').trim().toLowerCase();
}

function activePrototypeScopeQuery() {
  return Number.isFinite(state.prototypeScopeClusterId)
    ? `?cluster_id=${encodeURIComponent(state.prototypeScopeClusterId)}`
    : '';
}

function derivePrototypeScopeClusterId() {
  const cluster = selectedCluster();
  if (!cluster) {
    return null;
  }
  return (!cluster.person_label || cluster.is_noise) ? cluster.cluster_id : null;
}

function clearFaceSelection() {
  state.selectedFaceIds.clear();
  state.lastSelectedFaceIndex = null;
}

function selectedPersonFile() {
  return state.personFiles.find(item => normalizePrototypeLabel(item.person_label) === normalizePrototypeLabel(state.selectedPersonLabel)) || null;
}

function buildPersonFiles(clusters) {
  const grouped = new Map();

  (clusters || []).forEach(cluster => {
    const personLabel = String(cluster.person_label || '').trim();
    if (!personLabel || cluster.is_noise) {
      return;
    }
    const key = normalizePrototypeLabel(personLabel);
    if (!grouped.has(key)) {
      grouped.set(key, {
        person_label: personLabel,
        face_count: 0,
        photo_count: 0,
        cluster_count: 0,
        approved_cluster_count: 0,
        representative_cluster_id: cluster.cluster_id,
        representative_face_count: cluster.face_count || 0,
      });
    }

    const entry = grouped.get(key);
    entry.face_count += Number(cluster.face_count || 0);
    entry.photo_count += Number(cluster.photo_count || 0);
    entry.cluster_count += 1;
    if (cluster.approved) {
      entry.approved_cluster_count += 1;
    }

    const currentRepApproved = (clusters.find(item => item.cluster_id === entry.representative_cluster_id)?.approved) ? 1 : 0;
    const candidateApproved = cluster.approved ? 1 : 0;
    const candidateFaceCount = Number(cluster.face_count || 0);
    const repFaceCount = Number(entry.representative_face_count || 0);
    if (
      candidateApproved > currentRepApproved ||
      (candidateApproved === currentRepApproved && candidateFaceCount > repFaceCount)
    ) {
      entry.representative_cluster_id = cluster.cluster_id;
      entry.representative_face_count = candidateFaceCount;
    }
  });

  return Array.from(grouped.values()).sort((a, b) => {
    if (b.face_count !== a.face_count) {
      return b.face_count - a.face_count;
    }
    return a.person_label.localeCompare(b.person_label);
  });
}

function currentReviewSelection() {
  return (state.reviewFaces || []).filter(face => state.selectedFaceIds.has(face.face_id));
}

function clusterLabelValue(cluster) {
  if (!cluster) {
    return '';
  }
  if (Object.prototype.hasOwnProperty.call(state.clusterLabelDrafts, cluster.cluster_id)) {
    return state.clusterLabelDrafts[cluster.cluster_id];
  }
  return cluster.person_label || '';
}

function clusterSuggestionState(clusterId) {
  return clusterId ? state.clusterSuggestions[clusterId] || null : null;
}

function maybeAutofillClusterLabel(clusterId) {
  const cluster = state.clusters.find(item => item.cluster_id === clusterId);
  if (!cluster || cluster.person_label) {
    return;
  }
  const existing = String(state.clusterLabelDrafts[clusterId] || '').trim();
  if (existing) {
    return;
  }
  const suggestions = clusterSuggestionState(clusterId)?.suggestions || [];
  if (suggestions.length) {
    state.clusterLabelDrafts[clusterId] = suggestions[0].person_label;
  }
}

function queueClusters() {
  return state.clusters.filter(cluster => !cluster.person_label && !cluster.is_noise);
}

function noiseClusters() {
  return state.clusters.filter(cluster => cluster.is_noise);
}

function firstQueueClusterId() {
  return queueClusters()[0]?.cluster_id || noiseClusters()[0]?.cluster_id || null;
}

function selectedDestinationClusterId() {
  const raw = Number.parseInt(el('review-destination-cluster')?.value || '', 10);
  return Number.isFinite(raw) ? raw : null;
}

function selectedMergeTargetId() {
  const raw = Number.parseInt(el('review-merge-target')?.value || '', 10);
  return Number.isFinite(raw) ? raw : null;
}

function clusterOptionLabel(cluster) {
  if (!cluster) {
    return '';
  }
  const name = cluster.person_label ? `${cluster.person_label} | cluster ${cluster.cluster_id}` : `Cluster ${cluster.cluster_id}`;
  return `${name} | ${fmtNumber(cluster.face_count || 0)} face(s)`;
}

function repopulateClusterSelect(selectId, clusters, placeholder, keepValue = '') {
  const select = el(selectId);
  if (!select) {
    return;
  }
  const previous = keepValue || select.value || '';
  select.innerHTML = `<option value="">${escHtml(placeholder)}</option>`;
  (clusters || []).forEach(cluster => {
    const option = document.createElement('option');
    option.value = String(cluster.cluster_id);
    option.textContent = clusterOptionLabel(cluster);
    select.appendChild(option);
  });
  if (previous && Array.from(select.options).some(option => option.value === previous)) {
    select.value = previous;
  }
}

function refreshReviewTargets(cluster) {
  const clusterOptions = isPrototypeMode() || isPersonMode() || !cluster
    ? []
    : state.clusters.filter(item => item.cluster_id !== cluster.cluster_id);
  const mergeOptions = !cluster || isPrototypeMode() || isPersonMode()
    ? []
    : state.clusters.filter(item => item.cluster_id !== cluster.cluster_id);
  repopulateClusterSelect('review-destination-cluster', clusterOptions, 'Or move into an existing cluster...');
  repopulateClusterSelect('review-merge-target', mergeOptions, 'Choose a merge target...');
}

function currentReviewQueue() {
  if (isPrototypeMode()) {
    return state.prototypeGroups.map(group => ({
      key: normalizePrototypeLabel(group.person_label),
      type: 'prototype',
      label: group.person_label,
    }));
  }
  if (isPersonMode()) {
    return state.personFiles.map(person => ({
      key: normalizePrototypeLabel(person.person_label),
      type: 'person',
      label: person.person_label,
      clusterId: person.representative_cluster_id,
    }));
  }
  return [...queueClusters(), ...noiseClusters()].map(cluster => ({
    key: String(cluster.cluster_id),
    type: 'cluster',
    clusterId: cluster.cluster_id,
  }));
}

async function navigateReviewSelection(delta) {
  const items = currentReviewQueue();
  if (!items.length) {
    return;
  }

  let currentIndex = 0;
  if (isPrototypeMode()) {
    currentIndex = Math.max(
      0,
      items.findIndex(item => item.key === normalizePrototypeLabel(state.selectedPrototypeLabel))
    );
  } else if (isPersonMode()) {
    currentIndex = Math.max(
      0,
      items.findIndex(item => item.key === normalizePrototypeLabel(state.selectedPersonLabel))
    );
  } else {
    currentIndex = Math.max(
      0,
      items.findIndex(item => item.clusterId === state.selectedClusterId)
    );
  }

  const nextIndex = Math.max(0, Math.min(items.length - 1, currentIndex + delta));
  const next = items[nextIndex];
  if (!next || nextIndex === currentIndex) {
    return;
  }

  clearFaceSelection();
  if (next.type === 'prototype') {
    state.selectedPrototypeLabel = next.label;
    renderClusters();
    await loadPrototypeFaces(next.label);
    return;
  }
  if (next.type === 'person') {
    state.reviewMode = 'people';
    state.selectedPersonLabel = next.label;
    state.selectedPersonClusterId = next.clusterId;
    state.selectedClusterId = next.clusterId;
    renderClusters();
    await loadPersonReview(next.clusterId);
    return;
  }

  state.reviewMode = 'cluster';
  state.selectedPersonLabel = null;
  state.selectedPersonClusterId = null;
  state.personReviewData = null;
  state.selectedClusterId = next.clusterId;
  renderClusters();
  await loadClusterCrops(next.clusterId);
}

function isReviewSectionCollapsed(sectionKey) {
  return Boolean(state.reviewSidebarCollapsed[sectionKey]);
}

function toggleReviewSection(sectionKey) {
  state.reviewSidebarCollapsed[sectionKey] = !state.reviewSidebarCollapsed[sectionKey];
  renderClusters();
}

function renderReviewSection(sectionKey, title, count, content, emptyText) {
  const collapsed = isReviewSectionCollapsed(sectionKey);
  return `
    <section class="review-section ${collapsed ? 'collapsed' : ''}">
      <button class="review-section-toggle" data-toggle-review-section="${sectionKey}" type="button">
        <span class="review-section-toggle-icon">${collapsed ? '+' : '-'}</span>
        <span class="review-section-title-row">
          <strong>${escHtml(title)}</strong>
          <span class="review-section-count">${fmtNumber(count)}</span>
        </span>
      </button>
      <div class="review-section-body">
        <div class="review-section-list">
          ${content || `<div class="empty-state empty-state--compact">${escHtml(emptyText)}</div>`}
        </div>
      </div>
    </section>
  `;
}

function renderReviewModeToggle() {
  el('btn-review-mode-cluster').classList.toggle('active', !isPrototypeMode());
  el('btn-review-mode-prototype').classList.toggle('active', isPrototypeMode());
  el('review-sidebar-title').textContent = isPrototypeMode() ? 'Prototype Triage' : 'Review Queue';
  el('review-sidebar-caption').textContent = isPrototypeMode()
    ? (
      Number.isFinite(state.prototypeScopeClusterId)
        ? `Prototype triage is scoped to cluster ${state.prototypeScopeClusterId}.`
        : 'Prototype triage groups unknown faces by the closest known person.'
    )
    : 'People folders, pending clusters, and noise each keep their own scroll area.';
}

function renderClusters() {
  const container = el('cluster-list');
  renderReviewModeToggle();

  if (isPrototypeMode()) {
    const prototypeMarkup = state.prototypeGroups.map(group => {
      const active = normalizePrototypeLabel(state.selectedPrototypeLabel) === normalizePrototypeLabel(group.person_label);
      const isUnknown = normalizePrototypeLabel(group.person_label) === '__unknown__';
      const meta = isUnknown
        ? `Faces ${fmtNumber(group.face_count)} | below threshold`
        : `Faces ${fmtNumber(group.face_count)} | avg ${Number(group.avg_similarity || 0).toFixed(2)} similarity`;
      return `
        <button class="cluster-item ${active ? 'active' : ''}" data-prototype-label="${escHtml(group.person_label)}" type="button">
          <div class="cluster-item-top">
            <div class="cluster-item-title">${escHtml(group.display_label || group.person_label || 'Unknown')}</div>
            <div class="phase-status">${isUnknown ? 'unknown' : 'match'}</div>
          </div>
          <div class="cluster-item-meta">${escHtml(meta)}</div>
          <div class="cluster-item-bottom">
            <span class="cluster-item-meta">${isUnknown ? 'Needs naming' : `${fmtNumber(group.prototype_support_faces || 0)} support faces`}</span>
          </div>
        </button>
      `;
    }).join('');
    container.innerHTML = renderReviewSection(
      'prototype',
      'Prototype Groups',
      state.prototypeGroups.length,
      prototypeMarkup,
      'No faces are waiting in prototype triage.'
    );
    renderSelectedCluster();
    return;
  }

  const peopleMarkup = state.personFiles.map(person => {
    const active = isPersonMode() && normalizePrototypeLabel(state.selectedPersonLabel) === normalizePrototypeLabel(person.person_label);
    return `
      <button class="cluster-item ${active ? 'active' : ''}" data-person-label="${escHtml(person.person_label)}" data-person-cluster-id="${person.representative_cluster_id}" type="button">
        <div class="cluster-item-top">
          <div class="cluster-item-title">${escHtml(person.person_label)}</div>
          <div class="phase-status">${person.approved_cluster_count > 0 ? 'approved' : 'review'}</div>
        </div>
        <div class="cluster-item-meta">Faces ${fmtNumber(person.face_count)} | Clusters ${fmtNumber(person.cluster_count)}</div>
        <div class="cluster-item-bottom">
          <span class="cluster-item-meta">Photos ${fmtNumber(person.photo_count)}</span>
          <span class="cluster-item-meta">Rep cluster ${fmtNumber(person.representative_cluster_id)}</span>
        </div>
      </button>
    `;
  }).join('');

  const clusterMarkup = queueClusters().map(cluster => `
    <button class="cluster-item ${!isPersonMode() && state.selectedClusterId === cluster.cluster_id ? 'active' : ''}" data-cluster-id="${cluster.cluster_id}" type="button">
      <div class="cluster-item-top">
        <div class="cluster-item-title">${escHtml(clusterDisplayName(cluster))}</div>
        <div class="phase-status">${escHtml(cluster.review_state || 'pending')}</div>
      </div>
      <div class="cluster-item-meta">Faces ${fmtNumber(cluster.face_count)} | Photos ${fmtNumber(cluster.photo_count || 0)}</div>
      <div class="cluster-item-bottom">
        <span class="cluster-item-meta">Priority ${escHtml(cluster.review_priority_bucket || 'low')}</span>
        <span class="cluster-item-meta">Rank ${fmtNumber(cluster.review_priority_rank || 0)}</span>
      </div>
    </button>
  `).join('');

  const noiseMarkup = noiseClusters().map(cluster => `
    <button class="cluster-item ${!isPersonMode() && state.selectedClusterId === cluster.cluster_id ? 'active' : ''}" data-cluster-id="${cluster.cluster_id}" type="button">
      <div class="cluster-item-top">
        <div class="cluster-item-title">${escHtml(clusterDisplayName(cluster))}</div>
        <div class="phase-status">noise</div>
      </div>
      <div class="cluster-item-meta">Faces ${fmtNumber(cluster.face_count)} | Photos ${fmtNumber(cluster.photo_count || 0)}</div>
      <div class="cluster-item-bottom">
        <span class="cluster-item-meta">Noise cluster</span>
        <span class="cluster-item-meta">Rank ${fmtNumber(cluster.review_priority_rank || 0)}</span>
      </div>
    </button>
  `).join('');

  container.innerHTML = [
    renderReviewSection('people', 'People Folders', state.personFiles.length, peopleMarkup, 'No labeled person folders yet.'),
    renderReviewSection('clusters', 'Clusters', queueClusters().length, clusterMarkup, 'No pending clusters right now.'),
    renderReviewSection('noise', 'Noise', noiseClusters().length, noiseMarkup, 'No noise buckets yet.'),
  ].join('');
  renderSelectedCluster();
}

function renderSelectionCaption() {
  const count = state.selectedFaceIds.size;
  let text = 'No faces selected.';
  if (isPrototypeMode()) {
    text = count > 0
      ? `${count} face(s) selected. Move them into the correct person.`
      : 'Select face(s) from a matched person bucket and move them into the right person.';
  } else if (isPersonMode()) {
    text = count > 0
      ? `${count} face(s) selected. Remove them from the person file and return them to unlabeled cleanup.`
      : 'Select one or more wrong faces to remove from the person file.';
  } else {
    text = count > 0
      ? `${count} face(s) selected. You can move them into a different person label.`
      : 'Cluster actions work on the whole cluster. Select faces to move or remove only those crops.';
  }
  el('review-selection-caption').textContent = `${text} Shift-click selects a range.`;
}

function renderReviewControls(cluster) {
  const busy = isBusy();
  const selectedCount = state.selectedFaceIds.size;
  const hasLabeledCluster = !!(cluster && cluster.person_label);
  const clusterField = el('review-destination-cluster');
  const clusterFieldWrap = clusterField?.closest('.field');
  const mergeField = el('review-merge-target');
  const mergeFieldWrap = mergeField?.closest('.field');

  refreshReviewTargets(cluster);
  const destinationClusterId = selectedDestinationClusterId();
  const destinationLabel = el('review-destination-input').value.trim();

  el('cluster-label-input').disabled = busy || isPrototypeMode() || isPersonMode() || !cluster;
  el('cluster-label-input').placeholder = isPrototypeMode()
    ? 'Prototype group mode'
    : isPersonMode()
      ? 'Whole file review'
      : 'Enter a person name';

  el('btn-cluster-approve').disabled = busy || !cluster || isPrototypeMode() || isPersonMode();
  el('btn-cluster-save-label').disabled = busy || !cluster || isPrototypeMode() || isPersonMode();
  el('btn-cluster-untag').disabled = busy || !cluster || isPrototypeMode() || isPersonMode();
  el('btn-cluster-noise').disabled = busy || !cluster || isPrototypeMode() || isPersonMode();

  el('btn-review-whole-file').style.display = isPrototypeMode() ? 'none' : '';
  el('btn-review-whole-file').disabled = busy || !cluster || (!hasLabeledCluster && !isPersonMode());
  el('btn-review-whole-file').textContent = isPersonMode() ? 'Back To Queues' : 'Whole File';

  if (clusterFieldWrap) {
    clusterFieldWrap.style.display = isPersonMode() ? 'none' : '';
  }
  clusterField.hidden = isPrototypeMode() || isPersonMode();
  clusterField.disabled = busy || !cluster || isPrototypeMode() || isPersonMode();
  el('selection-destination-label').textContent = isPrototypeMode()
    ? 'Move Selected To Person'
    : 'Move Selected To Person';
  el('review-destination-input').disabled = busy;
  el('review-destination-input').placeholder = isPrototypeMode()
    ? 'Enter a person name'
    : destinationClusterId
      ? 'Or enter a new person name'
      : 'Enter a person name';
  el('btn-review-move-selected').style.display = isPersonMode() ? 'none' : '';
  el('btn-review-move-selected').disabled = busy || selectedCount === 0 || (!destinationClusterId && !destinationLabel);

  if (mergeFieldWrap) {
    mergeFieldWrap.style.display = !cluster || isPrototypeMode() || isPersonMode() ? 'none' : '';
  }
  mergeField.disabled = busy || !cluster || isPrototypeMode() || isPersonMode();
  el('btn-review-merge-cluster').style.display = !cluster || isPrototypeMode() || isPersonMode() ? 'none' : '';
  el('btn-review-merge-cluster').disabled = busy || !selectedMergeTargetId();

  el('btn-review-undo-move').disabled = busy;
  el('btn-review-select-all').disabled = busy || !(state.reviewFaces || []).length;
  el('btn-review-clear-selection').disabled = busy || selectedCount === 0;
  el('btn-review-remove-selected').style.display = isPrototypeMode() ? 'none' : '';
  el('btn-review-remove-selected').disabled = busy || selectedCount === 0;

  renderSelectionCaption();
}

function renderClusterSuggestions(cluster) {
  const container = el('cluster-suggestions');
  if (!container) {
    return;
  }
  if (!cluster || isPrototypeMode() || isPersonMode()) {
    container.hidden = true;
    container.innerHTML = '';
    return;
  }

  container.hidden = false;
  const suggestionState = clusterSuggestionState(cluster.cluster_id);
  if (!suggestionState || suggestionState.loading) {
    container.innerHTML = '<div class="suggestion-caption">Loading top matches...</div>';
    return;
  }
  if (suggestionState.error) {
    container.innerHTML = `<div class="suggestion-caption">${escHtml(suggestionState.error)}</div>`;
    return;
  }
  if (!Array.isArray(suggestionState.suggestions) || !suggestionState.suggestions.length) {
    const reason = suggestionState.reason ? ` (${String(suggestionState.reason).replace(/_/g, ' ')})` : '';
    container.innerHTML = `<div class="suggestion-caption">No suggestion matches available${escHtml(reason)}.</div>`;
    return;
  }

  const note = suggestionState.source_pool === 'approved_plus_labeled'
    ? '<div class="suggestion-caption">Top three matches include pending labels to widen the candidate pool.</div>'
    : '<div class="suggestion-caption">Top three likely matches. Click one to label and approve the cluster in one step.</div>';
  const cards = suggestionState.suggestions.slice(0, 3).map(suggestion => {
    const score = Number(suggestion.score || 0).toFixed(2);
    const support = fmtNumber(suggestion.support_faces || 0);
    const actionLabel = suggestion.recommended ? 'Approve Match' : 'Approve As';
    return `
      <article class="suggestion-card ${suggestion.recommended ? 'recommended' : ''}">
        <div class="suggestion-name">${escHtml(suggestion.person_label)}</div>
        <div class="suggestion-meta">score ${score} | ${support} support faces</div>
        <div class="suggestion-meta">source cluster ${fmtNumber(suggestion.source_cluster_id)} | ${suggestion.source_approved ? 'approved' : 'pending'}</div>
        <button class="btn btn-small ${suggestion.recommended ? 'btn-primary' : ''}" data-approve-suggestion="${escHtml(suggestion.person_label)}" type="button">
          ${actionLabel}
        </button>
      </article>
    `;
  }).join('');
  container.innerHTML = `${note}<div class="suggestion-row">${cards}</div>`;
}

function reviewFaceScoreLine(face) {
  if (isPersonMode()) {
    return `match ${Number(face.match_score || 0).toFixed(2)} | det ${Number(face.detection_score || 0).toFixed(2)}`;
  }
  if (isPrototypeMode()) {
    const who = face.matched_person || face.predicted_label || 'Unknown';
    return `${who} ${Number(face.similarity || 0).toFixed(2)} | det ${Number(face.detection_score || 0).toFixed(2)}`;
  }
  if (face.predicted_label) {
    return `${face.predicted_label} ${Number(face.best_match_score || 0).toFixed(2)} | det ${Number(face.detection_score || 0).toFixed(2)}`;
  }
  return `det ${Number(face.detection_score || 0).toFixed(2)}`;
}

function renderReviewFaces() {
  const grid = el('cluster-crops');
  if (state.reviewFaces === null) {
    grid.innerHTML = '<div class="empty-state">Loading faces...</div>';
    return;
  }
  if (!state.reviewFaces.length) {
    grid.innerHTML = '<div class="empty-state">No faces were returned for this review mode.</div>';
    return;
  }

  grid.innerHTML = state.reviewFaces.map((face, index) => `
    <button class="crop-card selectable ${state.selectedFaceIds.has(face.face_id) ? 'selected' : ''}" data-face-id="${face.face_id}" data-face-index="${index}" type="button">
      <img src="${escHtml(face.crop_url || '')}" alt="${escHtml(face.filename || 'face crop')}" />
      <div class="crop-indicator">&#10003;</div>
      <footer>
        <strong>${escHtml(face.filename || 'Crop')}</strong>
        <span class="phase-meta">${escHtml(reviewFaceScoreLine(face))}</span>
      </footer>
    </button>
  `).join('');
}

function renderSelectedCluster() {
  const cluster = selectedCluster();
  const personFile = selectedPersonFile();
  const prototypeGroup = state.prototypeGroups.find(group =>
    normalizePrototypeLabel(group.person_label) === normalizePrototypeLabel(state.selectedPrototypeLabel)
  ) || null;

  if (!isPrototypeMode() && !isPersonMode() && !cluster) {
    el('cluster-detail-title').textContent = 'Choose a cluster';
    el('cluster-detail-meta').textContent = '-';
    el('review-detail-eyebrow').textContent = isPrototypeMode() ? 'Selected Person Group' : 'Selected Cluster';
    el('review-grid-eyebrow').textContent = 'Crops';
    el('review-grid-title').textContent = isPrototypeMode() ? 'Matched Faces' : 'Cluster Faces';
    el('cluster-label-input').value = '';
    el('cluster-priority-strip').innerHTML = '';
    el('cluster-crops').innerHTML = '<div class="empty-state">Select a cluster to review.</div>';
    renderReviewControls(null);
    renderClusterSuggestions(null);
    return;
  }

  if (isPrototypeMode()) {
    el('review-detail-eyebrow').textContent = 'Selected Person Group';
    el('review-grid-eyebrow').textContent = 'Prototype Triage';
    el('review-grid-title').textContent = 'Matched Faces';
    el('cluster-detail-title').textContent = prototypeGroup
      ? (prototypeGroup.display_label || prototypeGroup.person_label || 'Unknown')
      : 'Choose a person group';
    el('cluster-detail-meta').textContent = prototypeGroup
      ? `${fmtNumber(prototypeGroup.face_count || 0)} faces | avg ${Number(prototypeGroup.avg_similarity || 0).toFixed(2)} similarity`
      : 'Choose a person group';
    el('cluster-label-input').value = '';
    el('cluster-priority-strip').innerHTML = `
      <span class="priority-pill">${Number.isFinite(state.prototypeScopeClusterId) ? `Scoped to cluster ${state.prototypeScopeClusterId}` : 'Global prototype triage'}</span>
      <span class="priority-pill">${prototypeGroup ? `${fmtNumber(prototypeGroup.prototype_support_faces || 0)} support faces` : 'Select a group'}</span>
    `;
    renderReviewControls(cluster);
    renderClusterSuggestions(null);
    renderReviewFaces();
    return;
  }

  if (isPersonMode()) {
    const personLabel = state.personReviewData?.person_label || personFile?.person_label || cluster?.person_label || 'Choose a person';
    el('review-detail-eyebrow').textContent = 'Person File';
    el('review-grid-eyebrow').textContent = 'Person Review';
    el('review-grid-title').textContent = 'Faces Ranked Worst First';
    el('cluster-detail-title').textContent = personLabel;
    el('cluster-detail-meta').textContent = state.personReviewData
      ? `${fmtNumber(state.personReviewData.face_count || 0)} faces across ${fmtNumber(state.personReviewData.cluster_count || 0)} clusters`
      : personFile
        ? `${fmtNumber(personFile.face_count || 0)} faces across ${fmtNumber(personFile.cluster_count || 0)} clusters`
        : 'Loading person file...';
    el('cluster-label-input').value = personLabel || '';
    el('cluster-priority-strip').innerHTML = `
      <span class="priority-pill">Prototype support ${fmtNumber(state.personReviewData?.prototype_support_faces || 0)}</span>
      <span class="priority-pill">${state.personReviewData?.usable_label ? 'Usable label' : 'Thin support'}</span>
    `;
    renderReviewControls(cluster || { person_label: personLabel });
    renderClusterSuggestions(null);
    renderReviewFaces();
    return;
  }

  el('review-detail-eyebrow').textContent = 'Selected Cluster';
  el('review-grid-eyebrow').textContent = 'Crops';
  el('review-grid-title').textContent = 'Cluster Faces';
  el('cluster-detail-title').textContent = clusterDisplayName(cluster);
  el('cluster-detail-meta').textContent = `${fmtNumber(cluster.face_count)} faces | ${cluster.review_state || 'pending'}`;
  el('cluster-label-input').value = clusterLabelValue(cluster);
  el('cluster-priority-strip').innerHTML = `
    <span class="priority-pill ${escHtml(cluster.review_priority_bucket || 'low')}">Priority ${escHtml(cluster.review_priority_bucket || 'low')}</span>
    <span class="priority-pill">Rank ${fmtNumber(cluster.review_priority_rank || 0)}</span>
    <span class="priority-pill">Score ${cluster.review_priority_score ?? '-'}</span>
  `;
  renderReviewControls(cluster);
  renderClusterSuggestions(cluster);
  renderReviewFaces();
}

async function setReviewMode(mode) {
  if (mode === 'prototype') {
    state.reviewMode = 'prototype';
    state.prototypeScopeClusterId = derivePrototypeScopeClusterId();
    state.personReviewData = null;
    state.selectedPersonLabel = null;
    state.selectedPersonClusterId = null;
    clearFaceSelection();
    await loadPrototypeGroups();
    return;
  }

  if (mode === 'people') {
    const cluster = selectedCluster();
    state.reviewMode = 'people';
    state.prototypeScopeClusterId = null;
    state.selectedPrototypeLabel = null;
    state.personReviewData = null;
    if (!state.selectedPersonLabel && cluster && cluster.person_label && !cluster.is_noise) {
      state.selectedPersonLabel = cluster.person_label;
      state.selectedPersonClusterId = cluster.cluster_id;
    }
    clearFaceSelection();
    await loadPeopleMode();
    return;
  }

  state.reviewMode = 'cluster';
  state.personReviewData = null;
  state.prototypeScopeClusterId = null;
  state.selectedPrototypeLabel = null;
  state.selectedPersonLabel = null;
  state.selectedPersonClusterId = null;
  if (!selectedCluster() || (selectedCluster()?.person_label && !selectedCluster()?.is_noise)) {
    state.selectedClusterId = firstQueueClusterId();
  }
  clearFaceSelection();
  await loadClusters();
}

async function showReviewQueues() {
  await setReviewMode('cluster');
}

async function toggleWholeFileReview() {
  const cluster = selectedCluster();
  if (isPersonMode()) {
    await setReviewMode('cluster');
    return;
  }
  if (!cluster) {
    showToast('Pick a cluster first.', 'err');
    return;
  }
  if (!cluster.person_label) {
    showToast('Whole file review needs a labeled cluster.', 'err');
    return;
  }

  clearFaceSelection();
  state.selectedPersonLabel = cluster.person_label;
  state.selectedPersonClusterId = cluster.cluster_id;
  await setReviewMode('people');
}

async function loadReview() {
  if (isPrototypeMode()) {
    await loadPrototypeGroups();
  } else if (isPersonMode()) {
    await loadPeopleMode();
  } else {
    await loadClusters();
  }
}

async function loadClusters() {
  try {
    state.clusters = await api('/clusters?sort=review');
    state.personFiles = buildPersonFiles(state.clusters);
    if (state.selectedClusterId && !state.clusters.some(item => item.cluster_id === state.selectedClusterId)) {
      state.selectedClusterId = null;
    }
    if (
      state.reviewMode === 'cluster' &&
      state.selectedClusterId &&
      state.clusters.some(item => item.cluster_id === state.selectedClusterId && item.person_label && !item.is_noise)
    ) {
      state.selectedClusterId = firstQueueClusterId();
    }
    if (!state.selectedClusterId && state.clusters.length) {
      state.selectedClusterId = firstQueueClusterId();
    }
    if (!state.selectedClusterId && state.personFiles.length) {
      state.reviewMode = 'people';
      state.selectedPersonLabel = state.selectedPersonLabel || state.personFiles[0].person_label;
      state.selectedPersonClusterId = selectedPersonFile()?.representative_cluster_id || state.personFiles[0].representative_cluster_id;
      renderClusters();
      await loadPersonReview(state.selectedPersonClusterId);
      return;
    }
    if (!state.selectedClusterId && state.clusters.length) {
      state.selectedClusterId = state.clusters[0].cluster_id;
    }
    renderClusters();
    if (state.selectedClusterId) {
      await loadClusterCrops(state.selectedClusterId);
    } else {
      state.reviewFaces = [];
      renderSelectedCluster();
    }
  } catch (error) {
    el('cluster-list').innerHTML = `<div class="empty-state">${escHtml(error.message)}</div>`;
    renderSelectedCluster();
  }
}

async function loadPeopleMode() {
  state.reviewFaces = null;
  state.prototypeGroups = [];
  state.selectedPrototypeLabel = null;
  renderReviewModeToggle();
  renderSelectedCluster();

  try {
    state.clusters = await api('/clusters?sort=review');
    state.personFiles = buildPersonFiles(state.clusters);

    if (
      !state.selectedPersonLabel ||
      !state.personFiles.some(item => normalizePrototypeLabel(item.person_label) === normalizePrototypeLabel(state.selectedPersonLabel))
    ) {
      state.selectedPersonLabel = state.personFiles[0]?.person_label || null;
      state.selectedPersonClusterId = state.personFiles[0]?.representative_cluster_id || null;
    } else {
      state.selectedPersonClusterId = selectedPersonFile()?.representative_cluster_id || state.selectedPersonClusterId;
    }
    state.selectedClusterId = Number.isFinite(state.selectedPersonClusterId)
      ? state.selectedPersonClusterId
      : state.selectedClusterId;

    renderClusters();
    if (state.selectedPersonClusterId) {
      await loadPersonReview(state.selectedPersonClusterId);
    } else {
      state.reviewFaces = [];
      renderSelectedCluster();
    }
  } catch (error) {
    el('cluster-list').innerHTML = `<div class="empty-state">${escHtml(error.message)}</div>`;
    state.reviewFaces = [];
    renderSelectedCluster();
  }
}

async function loadPrototypeGroups() {
  state.reviewFaces = null;
  state.personReviewData = null;
  renderReviewModeToggle();
  renderSelectedCluster();

  try {
    state.clusters = await api('/clusters?sort=review');
    if (state.selectedClusterId && !state.clusters.some(item => item.cluster_id === state.selectedClusterId)) {
      state.selectedClusterId = null;
    }
    const data = await api(`/clusters/by-person-prototype${activePrototypeScopeQuery()}`);
    state.prototypeGroups = data.groups || [];
    if (
      !state.selectedPrototypeLabel ||
      !state.prototypeGroups.some(group => normalizePrototypeLabel(group.person_label) === normalizePrototypeLabel(state.selectedPrototypeLabel))
    ) {
      state.selectedPrototypeLabel = state.prototypeGroups[0]?.person_label || null;
    }
    renderClusters();
    if (state.selectedPrototypeLabel) {
      await loadPrototypeFaces(state.selectedPrototypeLabel);
    } else {
      state.reviewFaces = [];
      renderSelectedCluster();
    }
  } catch (error) {
    el('cluster-list').innerHTML = `<div class="empty-state">${escHtml(error.message)}</div>`;
    state.reviewFaces = [];
    renderSelectedCluster();
  }
}

async function loadClusterCrops(clusterId) {
  state.reviewFaces = null;
  state.personReviewData = null;
  if (clusterId) {
    loadClusterSuggestions(clusterId);
  }
  renderSelectedCluster();
  try {
    state.reviewFaces = await api(`/clusters/${clusterId}/crops`);
    renderSelectedCluster();
  } catch (error) {
    el('cluster-crops').innerHTML = `<div class="empty-state">${escHtml(error.message)}</div>`;
  }
}

async function loadClusterSuggestions(clusterId) {
  state.clusterSuggestions[clusterId] = { loading: true, suggestions: [] };
  if (state.selectedClusterId === clusterId && !isPrototypeMode() && !isPersonMode()) {
    renderClusterSuggestions(selectedCluster());
  }
  try {
    const data = await api(`/clusters/${clusterId}/suggestions`);
    state.clusterSuggestions[clusterId] = data;
    maybeAutofillClusterLabel(clusterId);
    if (state.selectedClusterId === clusterId && !isPrototypeMode() && !isPersonMode()) {
      renderSelectedCluster();
    }
  } catch (error) {
    state.clusterSuggestions[clusterId] = { error: error.message, suggestions: [] };
    if (state.selectedClusterId === clusterId && !isPrototypeMode() && !isPersonMode()) {
      renderClusterSuggestions(selectedCluster());
    }
  }
}

async function loadPrototypeFaces(label) {
  state.reviewFaces = null;
  renderSelectedCluster();
  try {
    state.reviewFaces = await api(`/clusters/by-person-prototype/${encodeURIComponent(label)}\/faces${activePrototypeScopeQuery()}`);
    renderSelectedCluster();
  } catch (error) {
    el('cluster-crops').innerHTML = `<div class="empty-state">${escHtml(error.message)}</div>`;
  }
}

async function loadPersonReview(clusterId) {
  state.reviewFaces = null;
  state.personReviewData = null;
  state.selectedClusterId = clusterId;
  renderSelectedCluster();
  try {
    state.personReviewData = await api(`/clusters/${clusterId}/person-review`);
    state.reviewFaces = state.personReviewData.faces || [];
    renderSelectedCluster();
  } catch (error) {
    el('cluster-crops').innerHTML = `<div class="empty-state">${escHtml(error.message)}</div>`;
  }
}

function toggleFaceSelection(faceId, faceIndex = null, shiftKey = false) {
  if (
    shiftKey &&
    faceIndex != null &&
    state.lastSelectedFaceIndex != null &&
    Array.isArray(state.reviewFaces) &&
    state.reviewFaces.length
  ) {
    const lower = Math.min(state.lastSelectedFaceIndex, faceIndex);
    const upper = Math.max(state.lastSelectedFaceIndex, faceIndex);
    for (let index = lower; index <= upper; index += 1) {
      const face = state.reviewFaces[index];
      if (face?.face_id) {
        state.selectedFaceIds.add(face.face_id);
      }
    }
  } else if (state.selectedFaceIds.has(faceId)) {
    state.selectedFaceIds.delete(faceId);
  } else {
    state.selectedFaceIds.add(faceId);
  }
  state.lastSelectedFaceIndex = faceIndex;
  renderSelectedCluster();
}

function selectAllReviewFaces() {
  (state.reviewFaces || []).forEach(face => {
    if (face?.face_id) {
      state.selectedFaceIds.add(face.face_id);
    }
  });
  if ((state.reviewFaces || []).length) {
    state.lastSelectedFaceIndex = state.reviewFaces.length - 1;
  }
  renderSelectedCluster();
}

function clearFaceSelectionAndRender() {
  clearFaceSelection();
  renderSelectedCluster();
}

function resetReviewDestinationInputs() {
  el('review-destination-input').value = '';
  el('review-destination-cluster').value = '';
}

async function moveSelectedFaces() {
  const selectedFaces = currentReviewSelection();
  if (!selectedFaces.length) {
    showToast('Select one or more faces first.', 'err');
    return;
  }

  const targetClusterId = selectedDestinationClusterId();
  const targetPersonLabel = targetClusterId ? '' : el('review-destination-input').value.trim();
  if (!targetClusterId && !targetPersonLabel) {
    showToast('Choose a destination cluster or enter a person label.', 'err');
    return;
  }

  try {
    const destinationLabel = targetClusterId ? `cluster ${targetClusterId}` : targetPersonLabel;
    const confirmed = window.confirm(`Move ${selectedFaces.length} selected face(s) to ${destinationLabel}?`);
    if (!confirmed) {
      return;
    }

    if (isPrototypeMode()) {
      const grouped = new Map();
      let movedFaces = 0;
      let skippedFaces = 0;
      selectedFaces.forEach(face => {
        const clusterId = Number(face.cluster_id);
        if (!Number.isFinite(clusterId)) {
          return;
        }
        if (!grouped.has(clusterId)) {
          grouped.set(clusterId, []);
        }
        grouped.get(clusterId).push(face.face_id);
      });

      for (const [clusterId, faceIds] of grouped.entries()) {
        if (targetClusterId && targetClusterId === clusterId) {
          skippedFaces += faceIds.length;
          continue;
        }
        const result = await apiPost('/clusters/reassign-faces', {
          source_cluster_id: clusterId,
          face_ids: faceIds,
          target_cluster_id: targetClusterId || undefined,
          target_person_label: targetClusterId ? null : targetPersonLabel,
        });
        movedFaces += Number(result.moved_faces || faceIds.length);
      }

      clearFaceSelection();
      resetReviewDestinationInputs();
      showToast(
        skippedFaces
          ? `Moved ${movedFaces} face(s). Skipped ${skippedFaces} already in the target cluster.`
          : `Moved ${movedFaces} face(s) to ${destinationLabel}.`,
        'ok'
      );
      await loadPrototypeGroups();
      await refreshStatus();
      return;
    }

    const cluster = selectedCluster();
    if (!cluster) {
      showToast('Pick a cluster first.', 'err');
      return;
    }

    if (targetClusterId && targetClusterId === cluster.cluster_id) {
      showToast('Choose a different destination cluster.', 'err');
      return;
    }

    const result = await apiPost('/clusters/reassign-faces', {
      source_cluster_id: cluster.cluster_id,
      face_ids: selectedFaces.map(face => face.face_id),
      target_cluster_id: targetClusterId || undefined,
      target_person_label: targetClusterId ? null : targetPersonLabel,
    });
    showToast(`Moved ${selectedFaces.length} face(s) to ${destinationLabel}.`, 'ok');
    clearFaceSelection();
    resetReviewDestinationInputs();
    if (Number.isFinite(result.target_cluster_id)) {
      state.selectedClusterId = result.target_cluster_id;
    }
    await loadClusters();
    await refreshStatus();
  } catch (error) {
    showToast(error.message, 'err');
  }
}

async function removeSelectedFaces() {
  const selectedFaces = currentReviewSelection();
  if (!selectedFaces.length) {
    showToast('Select one or more faces to remove.', 'err');
    return;
  }

  try {
    const confirmed = window.confirm(
      isPersonMode()
        ? `Remove ${selectedFaces.length} face(s) from this person file and send them back to unlabeled review?`
        : `Untag ${selectedFaces.length} selected face(s) into a new unlabeled cluster?`
    );
    if (!confirmed) {
      return;
    }

    const grouped = new Map();
    selectedFaces.forEach(face => {
      const clusterId = Number(face.cluster_id);
      if (!Number.isFinite(clusterId)) {
        return;
      }
      if (!grouped.has(clusterId)) {
        grouped.set(clusterId, []);
      }
      grouped.get(clusterId).push(face.face_id);
    });

    let movedFaces = 0;
    let targetClusterId = null;
    for (const [clusterId, faceIds] of grouped.entries()) {
      const result = await apiPost(`/clusters/${clusterId}/untag-faces`, {
        face_ids: faceIds,
      });
      movedFaces += Number(result.moved_faces || 0);
      if (targetClusterId == null && Number.isFinite(result.target_cluster_id)) {
        targetClusterId = result.target_cluster_id;
      }
    }

    showToast(
      isPersonMode()
        ? `Removed ${movedFaces} face(s) from the person file.`
        : `Untagged ${movedFaces} selected face(s).`,
      'ok'
    );
    clearFaceSelection();
    resetReviewDestinationInputs();
    if (!isPersonMode() && Number.isFinite(targetClusterId)) {
      state.selectedClusterId = targetClusterId;
    }
    if (isPersonMode()) {
      await loadPeopleMode();
    } else {
      await loadClusters();
    }
    await refreshStatus();
  } catch (error) {
    showToast(error.message, 'err');
  }
}

async function undoLastMove() {
  const confirmed = window.confirm('Undo the most recent face move?');
  if (!confirmed) {
    return;
  }

  try {
    const result = await apiPost('/clusters/reassign-faces/undo-last');
    clearFaceSelection();
    state.reviewMode = 'cluster';
    state.selectedPersonLabel = null;
    state.selectedPersonClusterId = null;
    state.selectedPrototypeLabel = null;
    state.prototypeScopeClusterId = null;
    state.personReviewData = null;
    state.selectedClusterId = Number(result.source_cluster_id);
    showToast(`Undid move ${result.move_id}.`, 'ok');
    await loadClusters();
    await refreshStatus();
  } catch (error) {
    showToast(error.message, 'err');
  }
}

async function mergeCluster() {
  const cluster = selectedCluster();
  const targetClusterId = selectedMergeTargetId();
  if (!cluster) {
    showToast('Pick a cluster first.', 'err');
    return;
  }
  if (!targetClusterId) {
    showToast('Choose a merge target.', 'err');
    return;
  }
  if (targetClusterId === cluster.cluster_id) {
    showToast('Choose a different merge target.', 'err');
    return;
  }

  const confirmed = window.confirm(`Merge cluster ${cluster.cluster_id} into cluster ${targetClusterId}?`);
  if (!confirmed) {
    return;
  }

  try {
    await apiPost('/clusters/merge', {
      source_cluster_id: cluster.cluster_id,
      target_cluster_id: targetClusterId,
    });
    clearFaceSelection();
    state.selectedClusterId = targetClusterId;
    showToast('Clusters merged.', 'ok');
    await loadClusters();
    await refreshStatus();
  } catch (error) {
    showToast(error.message, 'err');
  }
}

function renderPhotoFilters(filters) {
  const personSelect = el('filter-person');
  const tagSelect = el('filter-tag');
  const previous = {
    person: personSelect.value,
    tag: tagSelect.value,
  };

  personSelect.innerHTML = '<option value="">All</option>';
  (filters.people || []).forEach(item => {
    const opt = document.createElement('option');
    opt.value = item.person;
    opt.textContent = `${item.person} (${item.photo_count})`;
    personSelect.appendChild(opt);
  });

  tagSelect.innerHTML = '<option value="">All</option>';
  (filters.tags || []).forEach(item => {
    const opt = document.createElement('option');
    opt.value = item.tag;
    opt.textContent = `${item.tag} (${item.photo_count})`;
    tagSelect.appendChild(opt);
  });

  if (previous.person && Array.from(personSelect.options).some(opt => opt.value === previous.person)) {
    personSelect.value = previous.person;
  }
  if (previous.tag && Array.from(tagSelect.options).some(opt => opt.value === previous.tag)) {
    tagSelect.value = previous.tag;
  }
  updateLibraryFieldLocks();
}

function updateLibraryFieldLocks() {
  const undated = el('filter-undated').checked;
  ['filter-person', 'filter-tag', 'filter-year', 'filter-month'].forEach(id => {
    el(id).disabled = undated;
  });
}

function buildPhotoParams(page) {
  const params = new URLSearchParams();
  params.set('page', String(page));
  params.set('per_page', String(state.photoPerPage));

  const person = el('filter-person').value;
  const tag = el('filter-tag').value;
  const year = el('filter-year').value.trim();
  const monthRaw = el('filter-month').value.trim();
  const month = monthRaw ? monthRaw.padStart(2, '0') : '';
  const q = el('filter-query').value.trim();
  const undated = el('filter-undated').checked;

  if (person) params.set('person', person);
  if (tag) params.set('tag', tag);
  if (year) params.set('year', year);
  if (month) params.set('month', month);
  if (q) params.set('q', q);
  if (undated) params.set('undated', 'true');

  return params.toString();
}

function renderPhotoCards(containerId, photos, options = {}) {
  const container = el(containerId);
  if (!container) {
    return;
  }
  if (!photos.length) {
    container.innerHTML = `<div class="empty-state">${escHtml(options.emptyText || 'No photos found.')}</div>`;
    return;
  }

  container.innerHTML = photos.map(photo => {
    const metaBits = [];
    if (options.showScore && photo.score != null) {
      metaBits.push(`score ${Number(photo.score).toFixed(2)}`);
    }
    metaBits.push(fmtDate(photo.exif_date));
    return `
      <button class="photo-card" data-photo-id="${photo.photo_id}" type="button">
        <img src="${escHtml(photoThumbSrc(photo))}" alt="${escHtml(photo.filename)}" loading="lazy" />
        <footer>
          <strong>${escHtml(photo.filename || 'Photo')}</strong>
          <span class="phase-meta">${escHtml(metaBits.join(' | '))}</span>
        </footer>
      </button>
    `;
  }).join('');
}

function renderPager(containerId, page, total, perPage, onPage) {
  const container = el(containerId);
  if (!container) {
    return;
  }
  const totalPages = Math.max(1, Math.ceil(total / perPage));
  container.innerHTML = '';
  if (totalPages <= 1) {
    return;
  }

  const prev = document.createElement('button');
  prev.className = 'btn btn-small';
  prev.textContent = 'Previous';
  prev.disabled = page <= 1;
  prev.addEventListener('click', () => onPage(page - 1));
  container.appendChild(prev);

  const info = document.createElement('span');
  info.textContent = `Page ${page} of ${totalPages}`;
  container.appendChild(info);

  const next = document.createElement('button');
  next.className = 'btn btn-small';
  next.textContent = 'Next';
  next.disabled = page >= totalPages;
  next.addEventListener('click', () => onPage(page + 1));
  container.appendChild(next);
}

function renderPhotos() {
  renderPhotoCards('photo-grid', state.photos, {
    emptyText: state.semanticSearchActive
      ? 'No semantic search results matched this query.'
      : 'No photos matched the current filters.',
    showScore: state.semanticSearchActive,
  });
  el('library-result-count').textContent = state.semanticSearchActive
    ? `${fmtNumber(state.photoTotal)} search results`
    : `${fmtNumber(state.photoTotal)} photos`;
  renderPagination();
}

function renderPagination() {
  if (state.semanticSearchActive) {
    el('library-pagination').innerHTML = '';
    return;
  }
  renderPager('library-pagination', state.photoPage, state.photoTotal, state.photoPerPage, nextPage => loadPhotos(nextPage));
}

function clearSearchFacets() {
  el('search-facets').hidden = true;
  ['facet-people', 'facet-tags', 'facet-years'].forEach(id => {
    el(id).innerHTML = '';
  });
}

function syncSearchLayerVisibility() {
  const enabled = Boolean(state.searchLayerEnabled);
  el('semantic-search-bar').hidden = !enabled;
  el('btn-search-clear').hidden = !enabled || !state.semanticSearchActive;
  el('btn-search-save').hidden = !enabled || !state.semanticSearchActive || !state.lastSearchQuery;
  el('search-saved').hidden = !enabled || !state.savedSearches.length;
  if (!enabled) {
    clearSearchFacets();
    el('search-result-count').textContent = '';
  }
}

function clearSemanticSearchState(options = {}) {
  const preserveInput = options.preserveInput !== false;
  state.semanticSearchActive = false;
  if (!preserveInput) {
    state.lastSearchQuery = '';
    el('search-input').value = '';
  }
  el('search-result-count').textContent = '';
  clearSearchFacets();
  syncSearchLayerVisibility();
}

function applyQuickQuery(raw) {
  const query = String(raw || '').trim();
  el('filter-person').value = '';
  el('filter-tag').value = '';
  el('filter-year').value = '';
  el('filter-month').value = '';
  el('filter-query').value = '';
  el('filter-undated').checked = false;

  if (!query) {
    updateLibraryFieldLocks();
    return;
  }

  const freeText = [];
  query.split(/\s+/).forEach(token => {
    const [rawKey, ...rest] = token.split(':');
    const value = rest.join(':').trim();
    const key = rawKey.toLowerCase();
    if (!rest.length) {
      freeText.push(token);
      return;
    }
    if (key === 'person' && value) el('filter-person').value = value;
    else if (key === 'tag' && value) el('filter-tag').value = value;
    else if (key === 'year' && value) el('filter-year').value = value;
    else if (key === 'month' && value) el('filter-month').value = value.padStart(2, '0');
    else if ((key === 'undated' || key === 'no-date') && (!value || value === 'true')) el('filter-undated').checked = true;
    else if (key === 'q' && value) freeText.push(value);
  });

  if (freeText.length) {
    el('filter-query').value = freeText.join(' ');
  }
  updateLibraryFieldLocks();
}

function renderSearchFacets(facets) {
  const hasFacets = Boolean(
    facets &&
    (
      (facets.people || []).length ||
      (facets.tags || []).length ||
      (facets.years || []).length
    )
  );
  if (!hasFacets) {
    clearSearchFacets();
    return;
  }

  el('search-facets').hidden = false;
  renderFacetGroup('facet-people', facets.people || [], item => {
    clearSemanticSearchState();
    el('filter-person').value = item.person;
    loadPhotos(1);
  }, item => item.person);
  renderFacetGroup('facet-tags', facets.tags || [], item => {
    clearSemanticSearchState();
    el('filter-tag').value = item.tag;
    loadPhotos(1);
  }, item => item.tag);
  renderFacetGroup('facet-years', facets.years || [], item => {
    clearSemanticSearchState();
    el('filter-year').value = item.year;
    loadPhotos(1);
  }, item => item.year);
}

function renderFacetGroup(containerId, items, onSelect, labelFn) {
  const container = el(containerId);
  container.innerHTML = '';
  if (!items.length) {
    container.innerHTML = '<div class="sidebar-caption">No matches</div>';
    return;
  }

  items.forEach(item => {
    const button = document.createElement('button');
    button.className = 'facet-item';
    button.type = 'button';
    button.innerHTML = `<span>${escHtml(labelFn(item))}</span><span class="facet-count">${fmtNumber(item.count)}</span>`;
    button.addEventListener('click', () => onSelect(item));
    container.appendChild(button);
  });
}

async function refreshSavedSearches() {
  if (!state.searchLayerEnabled) {
    state.savedSearches = [];
    syncSearchLayerVisibility();
    return;
  }
  try {
    state.savedSearches = await api('/searches');
  } catch (_) {
    state.savedSearches = [];
  }

  const select = el('search-saved');
  select.innerHTML = '<option value="">Saved searches...</option>';
  state.savedSearches.forEach(search => {
    const option = document.createElement('option');
    option.value = String(search.search_id);
    option.textContent = search.name;
    select.appendChild(option);
  });
  syncSearchLayerVisibility();
}

async function saveCurrentSearch() {
  if (!state.searchLayerEnabled || !state.lastSearchQuery) {
    return;
  }
  const name = window.prompt('Name this search:', state.lastSearchQuery);
  if (!name) {
    return;
  }
  try {
    await apiPost('/searches', { name, query: { q: state.lastSearchQuery } });
    showToast('Search saved.', 'ok');
    await refreshSavedSearches();
  } catch (error) {
    showToast(error.message, 'err');
  }
}

async function runSemanticSearch(options = {}) {
  if (!state.searchLayerEnabled) {
    showToast('Semantic search is disabled in settings.', 'err');
    return;
  }
  const query = String(options.reuseLast ? state.lastSearchQuery : el('search-input').value).trim();
  if (!query) {
    showToast('Enter a search query first.', 'err');
    return;
  }

  state.lastSearchQuery = query;
  state.semanticSearchActive = true;
  state.photoPage = 1;
  el('search-input').value = query;
  syncSearchLayerVisibility();
  el('photo-grid').innerHTML = '<div class="empty-state">Searching photo library...</div>';
  el('library-pagination').innerHTML = '';

  try {
    const data = await api(`/photos/search?q=${encodeURIComponent(query)}&top_k=60`);
    state.photos = data.results || [];
    state.photoTotal = Number(data.total || state.photos.length);
    renderPhotos();
    el('search-result-count').textContent = `${fmtNumber(state.photoTotal)} results`;
    renderSearchFacets(data.facets || {});
    syncSearchLayerVisibility();
  } catch (error) {
    state.photos = [];
    state.photoTotal = 0;
    el('photo-grid').innerHTML = `<div class="empty-state">${escHtml(error.message)}</div>`;
    clearSearchFacets();
    showToast(error.message, 'err');
  }
}

function clearSemanticSearch() {
  clearSemanticSearchState({ preserveInput: false });
  loadPhotos(1);
}

async function loadPhotoFilters() {
  try {
    const filters = await api('/photo-filters');
    renderPhotoFilters(filters);
    state.photoFiltersLoaded = true;
  } catch (error) {
    showToast(`Photo filters failed: ${error.message}`, 'err');
  }
}

async function loadPhotos(page = 1) {
  if (state.semanticSearchActive) {
    clearSemanticSearchState();
  }
  state.photoPage = page;
  el('photo-grid').innerHTML = '<div class="empty-state">Loading photos...</div>';
  try {
    const data = await api(`/photos?${buildPhotoParams(page)}`);
    state.photos = data.photos || [];
    state.photoTotal = Number(data.total || 0);
    renderPhotos();
  } catch (error) {
    state.photos = [];
    state.photoTotal = 0;
    el('photo-grid').innerHTML = `<div class="empty-state">${escHtml(error.message)}</div>`;
    el('library-pagination').innerHTML = '';
  }
}

async function loadLibraryView() {
  syncSearchLayerVisibility();
  if (state.semanticSearchActive && state.lastSearchQuery) {
    await runSemanticSearch({ reuseLast: true });
    return;
  }
  await loadPhotos(state.photoPage || 1);
}

async function loadObjectsView() {
  await loadTagBrowser();
}

async function loadTagBrowser() {
  el('tag-list').innerHTML = '<div class="empty-state">Loading tags...</div>';
  el('obj-photo-grid').innerHTML = '<div class="empty-state">Choose a tag to browse photos.</div>';
  el('obj-count').textContent = '';

  try {
    const grouped = await api('/objects/tags');
    state.objectTagsLoaded = true;
    state.objectTagGroups = grouped || {};
    renderTagBrowser(grouped);
    const validTags = Object.values(grouped).flat().map(item => item.tag);
    if (state.selectedTag && !validTags.includes(state.selectedTag)) {
      state.selectedTag = null;
      state.objectPage = 1;
    }
    if (!state.selectedTag && validTags.length) {
      state.selectedTag = validTags[0];
      state.objectPage = 1;
      renderTagBrowser(grouped);
    }
    if (state.selectedTag) {
      el('obj-tag-title').textContent = state.selectedTag;
      await loadObjectPhotos(state.objectPage || 1);
    }
  } catch (error) {
    el('tag-list').innerHTML = `<div class="empty-state">${escHtml(error.message)}</div>`;
  }
}

function renderTagBrowser(grouped) {
  const list = el('tag-list');
  const groups = Object.entries(grouped || {});
  if (!groups.length) {
    list.innerHTML = '<div class="empty-state">No approved tags yet.</div>';
    return;
  }

  list.innerHTML = groups.map(([group, tags]) => `
    <section class="tag-group">
      <div class="tag-group-header">${escHtml(group)}</div>
      <div class="tag-group-items">
        ${tags.map(tag => {
          const sources = tag.sources || [];
          const dotClass = sources.includes('yolo') && sources.includes('clip')
            ? 'dot-both'
            : sources.includes('yolo')
              ? 'dot-yolo'
              : 'dot-clip';
          return `
            <button class="tag-item ${state.selectedTag === tag.tag ? 'active' : ''}" data-object-tag="${escHtml(tag.tag)}" type="button">
              <span class="source-dot ${dotClass}"></span>
              <span class="tag-name">${escHtml(tag.tag)}</span>
              <span class="tag-count">${fmtNumber(tag.photo_count)}</span>
            </button>
          `;
        }).join('')}
      </div>
    </section>
  `).join('');
}

async function loadObjectPhotos(page = 1) {
  if (!state.selectedTag) {
    el('obj-photo-grid').innerHTML = '<div class="empty-state">Choose a tag to browse photos.</div>';
    el('obj-pagination').innerHTML = '';
    return;
  }
  state.objectPage = page;
  el('obj-tag-title').textContent = state.selectedTag;
  el('obj-photo-grid').innerHTML = '<div class="empty-state">Loading tagged photos...</div>';
  try {
    const data = await api(`/objects/tags/${encodeURIComponent(state.selectedTag)}?page=${page}&per_page=${state.photoPerPage}`);
    el('obj-count').textContent = `${fmtNumber(data.total || 0)} photos`;
    renderPhotoCards('obj-photo-grid', data.photos || [], { emptyText: 'No approved detections for this tag yet.' });
    renderPager('obj-pagination', page, Number(data.total || 0), state.photoPerPage, nextPage => loadObjectPhotos(nextPage));
  } catch (error) {
    el('obj-photo-grid').innerHTML = `<div class="empty-state">${escHtml(error.message)}</div>`;
    el('obj-pagination').innerHTML = '';
  }
}

function toggleVocabPanel(forceOpen = null) {
  const panel = el('vocab-panel');
  const nextHidden = forceOpen == null ? !panel.hidden : !forceOpen;
  panel.hidden = nextHidden;
}

async function loadVocab() {
  const tbody = el('vocab-tbody');
  tbody.innerHTML = '<tr><td colspan="5" class="table-empty">Loading vocabulary...</td></tr>';
  try {
    const vocab = await api('/objects/vocabulary');
    if (!vocab.length) {
      tbody.innerHTML = '<tr><td colspan="5" class="table-empty">No vocabulary entries yet.</td></tr>';
      return;
    }
    tbody.innerHTML = vocab.map(entry => `
      <tr>
        <td>${escHtml(entry.tag_group)}</td>
        <td>${escHtml(entry.tag_name)}</td>
        <td>${escHtml((entry.prompts || []).join(', '))}</td>
        <td class="table-check"><input type="checkbox" ${entry.enabled ? 'checked' : ''} disabled /></td>
        <td><button class="btn btn-small btn-danger" data-delete-vocab="${entry.vocab_id}" type="button">Delete</button></td>
      </tr>
    `).join('');
  } catch (error) {
    tbody.innerHTML = `<tr><td colspan="5" class="table-empty">${escHtml(error.message)}</td></tr>`;
  }
}

async function deleteVocab(vocabId) {
  const confirmed = window.confirm('Delete this vocabulary entry?');
  if (!confirmed) {
    return;
  }
  try {
    await apiDelete(`/objects/vocabulary/${vocabId}`);
    showToast('Vocabulary entry deleted.', 'ok');
    await loadVocab();
  } catch (error) {
    showToast(error.message, 'err');
  }
}

async function addVocab() {
  const payload = {
    tag_group: el('new-vocab-group').value.trim(),
    tag_name: el('new-vocab-name').value.trim(),
    prompts: el('new-vocab-prompts').value
      .split('\n')
      .map(line => line.trim())
      .filter(Boolean),
    enabled: el('new-vocab-enabled').checked,
  };
  if (!payload.tag_group || !payload.tag_name || !payload.prompts.length) {
    showToast('Fill in group, tag name, and at least one prompt.', 'err');
    return;
  }
  try {
    await apiPost('/objects/vocabulary', payload);
    el('new-vocab-group').value = '';
    el('new-vocab-name').value = '';
    el('new-vocab-prompts').value = '';
    el('new-vocab-enabled').checked = true;
    showToast('Vocabulary entry added.', 'ok');
    await loadVocab();
  } catch (error) {
    showToast(error.message, 'err');
  }
}

function renderSettings() {
  const settings = state.settings;
  if (!settings) {
    return;
  }

  el('setting-nas-source-dir').value = settings.nas_source_dir || '';
  el('setting-local-base').value = settings.local_base || '';
  el('setting-yolo-conf').value = settings.yolo_conf_threshold ?? '';
  el('setting-clip-thresh').value = settings.clip_tag_threshold ?? '';
  el('setting-max-dim').value = settings.max_inference_dim ?? '';
  el('setting-det-thresh').value = settings.det_thresh ?? '';
  el('setting-umap-neighbors').value = settings.umap_n_neighbors ?? '';
  el('setting-hdbscan-min-cluster').value = settings.hdbscan_min_cluster_size ?? '';
  el('setting-hdbscan-min-samples').value = settings.hdbscan_min_samples ?? '';

  el('settings-stats').innerHTML = [
    statTile('NVMe Free', `${settings.nvme_free_gb} GB`),
    statTile('DB Size', `${settings.db_size_mb} MB`),
    statTile('Photos', fmtNumber(settings.total_photos)),
    statTile('Faces', fmtNumber(settings.total_faces)),
    statTile('Document Photos', fmtNumber(settings.document_photos)),
    statTile('Docs OCR Complete', fmtNumber(settings.document_photos_ocr_complete)),
    statTile('Docs Still Pending', fmtNumber(settings.pending_ocr_documents)),
    statTile('API Port', fmtNumber(settings.api_port)),
  ].join('');
}

function statTile(label, value) {
  return `
    <div class="stat-tile">
      <span>${escHtml(label)}</span>
      <strong>${escHtml(value)}</strong>
    </div>
  `;
}

async function refreshStatus() {
  try {
    state.status = await api('/status');
    renderHeader();
    renderPhaseCards();
    renderScope();

    if (state.activeTab === 'review') {
      renderSelectedCluster();
    }
  } catch (error) {
    showToast(`Status refresh failed: ${error.message}`, 'err');
  }
}

async function runWorkflow(name) {
  if (
    name === 'delivery' &&
    !window.confirm('Run delivery workflow 4-7? This can reorganize files and push changes into the verified output.')
  ) {
    return;
  }
  try {
    const result = await apiPost(`/pipeline/workflows/${name}`);
    showToast(`Started ${result.started}.`, 'ok');
    await refreshStatus();
  } catch (error) {
    showToast(error.message, 'err');
  }
}

async function runPhase(phaseId) {
  if (
    (
      phaseId === 'process' &&
      !window.confirm('Run Process now? This can perform OCR and image analysis across the current intake scope.')
    ) ||
    (
      phaseId === 'push' &&
      !window.confirm('Run Push now? This can copy approved output into the destination tree.')
    )
  ) {
    return;
  }
  try {
    const result = await apiPost(`/pipeline/run/${phaseId}`);
    showToast(`Started ${result.started}.`, 'ok');
    await refreshStatus();
  } catch (error) {
    showToast(error.message, 'err');
  }
}

async function resetPhase(phaseId) {
  const cascade = phaseId !== 'ocr';
  try {
    await apiPost(`/pipeline/reset/${phaseId}?cascade=${cascade ? 'true' : 'false'}`);
    showToast(`Reset ${phaseId}.`, 'ok');
    await refreshStatus();
  } catch (error) {
    showToast(error.message, 'err');
  }
}

async function stopPipeline() {
  try {
    await apiPost('/pipeline/stop');
    showToast('Stop requested.', 'ok');
    await refreshStatus();
  } catch (error) {
    showToast(error.message, 'err');
  }
}

async function saveClusterLabel() {
  const cluster = state.clusters.find(item => item.cluster_id === state.selectedClusterId);
  if (!cluster) {
    return;
  }
  const personLabel = el('cluster-label-input').value.trim();
  if (!personLabel) {
    showToast('Enter a person label first.', 'err');
    return;
  }
  try {
    await apiPost(`/clusters/${cluster.cluster_id}/label`, { person_label: personLabel });
    showToast('Label saved.', 'ok');
    await loadClusters();
    if (state.photoFiltersLoaded) {
      await loadPhotoFilters();
    }
  } catch (error) {
    showToast(error.message, 'err');
  }
}

async function approveCluster() {
  const cluster = state.clusters.find(item => item.cluster_id === state.selectedClusterId);
  if (!cluster) {
    return;
  }
  const personLabel = el('cluster-label-input').value.trim();
  try {
    if (personLabel && personLabel !== (cluster.person_label || '')) {
      await apiPost(`/clusters/${cluster.cluster_id}/label`, { person_label: personLabel });
    }
    await apiPost(`/clusters/${cluster.cluster_id}/approve`);
    showToast('Cluster approved.', 'ok');
    await loadClusters();
    if (state.photoFiltersLoaded) {
      await loadPhotoFilters();
    }
    await refreshStatus();
  } catch (error) {
    showToast(error.message, 'err');
  }
}

async function approveSuggestedCluster(personLabel) {
  const cluster = selectedCluster();
  if (!cluster || !personLabel) {
    return;
  }
  try {
    state.clusterLabelDrafts[cluster.cluster_id] = personLabel;
    await apiPost(`/clusters/${cluster.cluster_id}/accept-suggestion`, { person_label: personLabel });
    await apiPost(`/clusters/${cluster.cluster_id}/approve`);
    state.selectedClusterId = null;
    showToast(`Approved cluster as ${personLabel}.`, 'ok');
    await loadClusters();
    if (state.photoFiltersLoaded) {
      await loadPhotoFilters();
    }
    await refreshStatus();
  } catch (error) {
    showToast(error.message, 'err');
  }
}

async function untagCluster() {
  if (!state.selectedClusterId) {
    return;
  }
  if (!isPrototypeMode() && !isPersonMode() && state.selectedFaceIds.size > 0) {
    await removeSelectedFaces();
    return;
  }

  const confirmed = window.confirm(`Untag the entire cluster ${state.selectedClusterId}?`);
  if (!confirmed) {
    return;
  }
  try {
    await apiPost(`/clusters/${state.selectedClusterId}/untag`);
    showToast('Cluster untagged.', 'ok');
    await loadClusters();
    await refreshStatus();
  } catch (error) {
    showToast(error.message, 'err');
  }
}

async function markClusterNoise() {
  if (!state.selectedClusterId) {
    return;
  }
  const confirmed = window.confirm(`Mark cluster ${state.selectedClusterId} as noise?`);
  if (!confirmed) {
    return;
  }
  try {
    await apiPost(`/clusters/${state.selectedClusterId}/noise`);
    showToast('Cluster marked as noise.', 'ok');
    await loadClusters();
    await refreshStatus();
  } catch (error) {
    showToast(error.message, 'err');
  }
}

async function loadPhotoFilters() {
  try {
    const filters = await api('/photo-filters');
    renderPhotoFilters(filters);
    state.photoFiltersLoaded = true;
  } catch (error) {
    showToast(`Photo filters failed: ${error.message}`, 'err');
  }
}

async function loadPhotos(page = 1) {
  state.photoPage = page;
  el('photo-grid').innerHTML = '<div class="empty-state">Loading photos...</div>';
  try {
    const data = await api(`/photos?${buildPhotoParams(page)}`);
    state.photos = data.photos || [];
    state.photoTotal = data.total || 0;
    renderPhotos();
  } catch (error) {
    el('photo-grid').innerHTML = `<div class="empty-state">${escHtml(error.message)}</div>`;
    el('library-pagination').innerHTML = '';
  }
}

async function openPhotoModal(photoId) {
  state.photoModalId = photoId;
  el('photo-modal').classList.add('open');
  el('photo-modal-title').textContent = 'Loading photo...';
  el('photo-modal-info').innerHTML = '<div class="empty-state">Loading photo details...</div>';
  try {
    const photo = await api(`/photos/${photoId}`);
    el('photo-modal-title').textContent = photo.filename;
    el('photo-modal-image').src = photo.preview_url || '';
    el('photo-modal-info').innerHTML = buildPhotoInfo(photo);
  } catch (error) {
    el('photo-modal-info').innerHTML = `<div class="empty-state">${escHtml(error.message)}</div>`;
  }
}

function buildPhotoInfo(photo) {
  const people = (photo.faces || [])
    .filter(face => face.person_label)
    .map(face => face.person_label);
  const tags = (photo.tags || []).map(tag => `${tag.tag}${tag.source ? ` (${tag.source})` : ''}`);
  const faces = (photo.faces || []).map(face => (
    face.person_label
      ? `${face.person_label} (${face.cluster_approved ? 'approved' : 'pending'})`
      : `Cluster ${face.cluster_id || '?'} unlabeled`
  ));
  const detections = (photo.detections || []).map(detection => `
    <div class="info-action-row">
      <div>
        <strong>${escHtml(detection.tag)}</strong>
        <div class="info-line">${escHtml(detection.model || 'model')} | ${(detection.confidence || 0).toFixed(2)}</div>
      </div>
      <button
        class="btn btn-small ${detection.approved ? 'btn-danger' : ''}"
        data-detection-action="${detection.approved ? 'reject' : 'approve'}"
        data-detection-id="${detection.detection_id}"
        type="button"
      >
        ${detection.approved ? 'Reject' : 'Approve'}
      </button>
    </div>
  `).join('');

  return `
    ${infoSection('Metadata', [
      `Date: ${fmtDate(photo.exif_date)} (${photo.date_source || 'unknown'})`,
      `File: ${photo.filename}`,
      `Source: ${photo.source_path}`,
      photo.dest_path ? `Destination: ${photo.dest_path}` : '',
    ])}
    ${infoPillsSection('People', people)}
    ${infoPillsSection('Tags', tags)}
    ${infoPillsSection('Faces', faces)}
    <section class="info-section">
      <h4>Detections</h4>
      ${detections || '<div class="info-line">None</div>'}
    </section>
  `;
}

function infoSection(title, lines) {
  const body = lines.filter(Boolean).map(line => `<div class="info-line">${escHtml(line)}</div>`).join('');
  return `<section class="info-section"><h4>${escHtml(title)}</h4>${body || '<div class="info-line">None</div>'}</section>`;
}

function infoPillsSection(title, items) {
  const body = items.length
    ? `<div class="info-pills">${items.map(item => `<span class="mini-pill">${escHtml(item)}</span>`).join('')}</div>`
    : '<div class="info-line">None</div>';
  return `<section class="info-section"><h4>${escHtml(title)}</h4>${body}</section>`;
}

function closePhotoModal() {
  el('photo-modal').classList.remove('open');
  state.photoModalId = null;
}

async function approveDetection(detectionId) {
  if (state.modalActionBusy) {
    return;
  }
  state.modalActionBusy = true;
  try {
    await apiPost(`/objects/detections/${detectionId}/approve`);
    if (state.photoFiltersLoaded) {
      await loadPhotoFilters();
    }
    if (state.activeTab === 'objects') {
      await loadTagBrowser();
    }
    if (state.photoModalId != null) {
      await openPhotoModal(state.photoModalId);
    }
    showToast('Detection approved.', 'ok');
  } catch (error) {
    showToast(error.message, 'err');
  } finally {
    state.modalActionBusy = false;
  }
}

async function rejectDetection(detectionId) {
  if (state.modalActionBusy) {
    return;
  }
  state.modalActionBusy = true;
  try {
    await apiPost(`/objects/detections/${detectionId}/reject`);
    if (state.photoFiltersLoaded) {
      await loadPhotoFilters();
    }
    if (state.activeTab === 'objects') {
      await loadTagBrowser();
    }
    if (state.photoModalId != null) {
      await openPhotoModal(state.photoModalId);
    }
    showToast('Detection rejected.', 'ok');
  } catch (error) {
    showToast(error.message, 'err');
  } finally {
    state.modalActionBusy = false;
  }
}

async function loadSettings() {
  try {
    state.settings = await api('/settings');
    state.settingsLoaded = true;
    state.searchLayerEnabled = Boolean(state.settings.search_layer_enabled);
    renderSettings();
    renderScope();
    syncSearchLayerVisibility();
    if (state.searchLayerEnabled) {
      await refreshSavedSearches();
    }
  } catch (error) {
    showToast(`Settings failed: ${error.message}`, 'err');
  }
}

function getNumberField(id) {
  const raw = el(id).value.trim();
  return raw ? Number(raw) : null;
}

async function saveSettings() {
  const payload = {
    nas_source_dir: el('setting-nas-source-dir').value.trim() || null,
    local_base: el('setting-local-base').value.trim() || null,
    yolo_conf_threshold: getNumberField('setting-yolo-conf'),
    clip_tag_threshold: getNumberField('setting-clip-thresh'),
    max_inference_dim: getNumberField('setting-max-dim'),
    det_thresh: getNumberField('setting-det-thresh'),
    umap_n_neighbors: getNumberField('setting-umap-neighbors'),
    hdbscan_min_cluster_size: getNumberField('setting-hdbscan-min-cluster'),
    hdbscan_min_samples: getNumberField('setting-hdbscan-min-samples'),
  };

  try {
    const result = await apiPost('/settings', payload);
    showToast(result.note || 'Settings saved.', 'ok');
    await loadSettings();
  } catch (error) {
    showToast(error.message, 'err');
  }
}

async function clearDatabase() {
  const typed = window.prompt('Type CLEAR to reset the database.');
  if (typed !== 'CLEAR') {
    showToast('Clear DB cancelled.', 'err');
    return;
  }

  try {
    const result = await apiPost('/settings/clear-db', {});
    showToast(result.note || 'Database cleared.', 'ok');
    state.selectedClusterId = null;
    state.selectedPersonLabel = null;
    state.selectedPersonClusterId = null;
    state.reviewFaces = [];
    state.personFiles = [];
    state.clusterSuggestions = {};
    state.clusterLabelDrafts = {};
    state.prototypeGroups = [];
    state.selectedPrototypeLabel = null;
    state.personReviewData = null;
    state.lastSelectedFaceIndex = null;
    state.semanticSearchActive = false;
    state.lastSearchQuery = '';
    state.savedSearches = [];
    state.selectedTag = null;
    state.objectPage = 1;
    state.objectTagsLoaded = false;
    state.objectTagGroups = {};
    clearFaceSelection();
    await Promise.all([refreshStatus(), loadSettings()]);
    if (state.activeTab === 'review') {
      await loadReview();
    }
    if (state.activeTab === 'library') {
      await loadLibraryView();
    }
    if (state.activeTab === 'objects') {
      await loadObjectsView();
    }
  } catch (error) {
    showToast(error.message, 'err');
  }
}

async function loadLog() {
  try {
    const data = await api('/pipeline/log-tail?lines=80');
    el('settings-log-tail').textContent = (data.lines || []).join('\n') || 'No log output yet.';
  } catch (error) {
    el('settings-log-tail').textContent = error.message;
  }
}

function toggleLogExpanded() {
  state.logExpanded = !state.logExpanded;
  el('settings-log-tail').classList.toggle('expanded', state.logExpanded);
  el('btn-expand-log').textContent = state.logExpanded ? 'Collapse' : 'Expand';
}

function bindEvents() {
  document.querySelectorAll('.tab').forEach(node => {
    node.addEventListener('click', () => openTab(node.dataset.tab));
  });

  el('btn-run-intake').addEventListener('click', () => runWorkflow('intake'));
  el('btn-run-delivery').addEventListener('click', () => runWorkflow('delivery'));
  el('btn-run-documents').addEventListener('click', () => runWorkflow('documents'));
  el('btn-refresh-intake').addEventListener('click', () => refreshStatus());
  el('btn-stop-pipeline').addEventListener('click', () => stopPipeline());

  ['phase-grid-intake', 'phase-grid-delivery', 'phase-grid-documents'].forEach(containerId => {
    el(containerId).addEventListener('click', event => {
      const runButton = event.target.closest('[data-run-phase]');
      const resetButton = event.target.closest('[data-reset-phase]');
      if (runButton) {
        runPhase(runButton.dataset.runPhase);
      }
      if (resetButton) {
        resetPhase(resetButton.dataset.resetPhase);
      }
    });
  });

  el('btn-refresh-clusters').addEventListener('click', () => loadReview());
  el('btn-review-mode-cluster').addEventListener('click', () => showReviewQueues());
  el('btn-review-mode-prototype').addEventListener('click', () => setReviewMode('prototype'));
  el('cluster-list').addEventListener('click', event => {
    const sectionToggle = event.target.closest('[data-toggle-review-section]');
    if (sectionToggle) {
      toggleReviewSection(sectionToggle.dataset.toggleReviewSection);
      return;
    }
    const clusterButton = event.target.closest('[data-cluster-id]');
    const personButton = event.target.closest('[data-person-label]');
    const prototypeButton = event.target.closest('[data-prototype-label]');
    if (!clusterButton && !personButton && !prototypeButton) {
      return;
    }
    clearFaceSelection();
    if (clusterButton) {
      state.reviewMode = 'cluster';
      state.selectedClusterId = Number(clusterButton.dataset.clusterId);
      state.selectedPersonLabel = null;
      state.selectedPersonClusterId = null;
      state.personReviewData = null;
      renderClusters();
      loadClusterCrops(state.selectedClusterId);
      return;
    }
    if (personButton) {
      state.reviewMode = 'people';
      state.selectedPersonLabel = personButton.dataset.personLabel;
      state.selectedPersonClusterId = Number(personButton.dataset.personClusterId);
      state.selectedClusterId = state.selectedPersonClusterId;
      renderClusters();
      loadPersonReview(state.selectedPersonClusterId);
      return;
    }
    state.selectedPrototypeLabel = prototypeButton.dataset.prototypeLabel;
    renderClusters();
    loadPrototypeFaces(state.selectedPrototypeLabel);
  });
  el('btn-cluster-save-label').addEventListener('click', () => saveClusterLabel());
  el('btn-cluster-approve').addEventListener('click', () => approveCluster());
  el('btn-cluster-untag').addEventListener('click', () => untagCluster());
  el('btn-cluster-noise').addEventListener('click', () => markClusterNoise());
  el('cluster-label-input').addEventListener('input', event => {
    const cluster = selectedCluster();
    if (!cluster || isPrototypeMode() || isPersonMode()) {
      return;
    }
    state.clusterLabelDrafts[cluster.cluster_id] = event.target.value;
  });
  el('cluster-suggestions').addEventListener('click', event => {
    const button = event.target.closest('[data-approve-suggestion]');
    if (!button) {
      return;
    }
    approveSuggestedCluster(button.dataset.approveSuggestion || '');
  });
  el('btn-review-whole-file').addEventListener('click', () => toggleWholeFileReview());
  el('btn-review-move-selected').addEventListener('click', () => moveSelectedFaces());
  el('btn-review-select-all').addEventListener('click', () => selectAllReviewFaces());
  el('btn-review-clear-selection').addEventListener('click', () => clearFaceSelectionAndRender());
  el('btn-review-undo-move').addEventListener('click', () => undoLastMove());
  el('btn-review-merge-cluster').addEventListener('click', () => mergeCluster());
  el('btn-review-remove-selected').addEventListener('click', () => removeSelectedFaces());
  el('review-destination-input').addEventListener('input', () => {
    if (el('review-destination-input').value.trim()) {
      el('review-destination-cluster').value = '';
    }
    renderSelectedCluster();
  });
  el('review-destination-cluster').addEventListener('change', () => {
    if (el('review-destination-cluster').value) {
      el('review-destination-input').value = '';
    }
    renderSelectedCluster();
  });
  el('review-merge-target').addEventListener('change', () => renderSelectedCluster());
  el('cluster-crops').addEventListener('click', event => {
    const card = event.target.closest('[data-face-id]');
    if (!card) {
      return;
    }
    toggleFaceSelection(
      Number(card.dataset.faceId),
      Number(card.dataset.faceIndex),
      event.shiftKey
    );
  });

  el('btn-search').addEventListener('click', () => runSemanticSearch());
  el('btn-search-clear').addEventListener('click', () => clearSemanticSearch());
  el('btn-search-save').addEventListener('click', () => saveCurrentSearch());
  el('search-input').addEventListener('keydown', event => {
    if (event.key === 'Enter') {
      event.preventDefault();
      runSemanticSearch();
    }
  });
  el('search-saved').addEventListener('change', event => {
    const searchId = Number(event.target.value || 0);
    const saved = state.savedSearches.find(item => item.search_id === searchId);
    if (!saved?.query?.q) {
      return;
    }
    el('search-input').value = saved.query.q;
    runSemanticSearch();
    event.target.value = '';
  });
  el('btn-quick-search').addEventListener('click', () => {
    applyQuickQuery(el('quick-search').value);
    loadPhotos(1);
  });
  el('quick-search').addEventListener('keydown', event => {
    if (event.key === 'Enter') {
      event.preventDefault();
      applyQuickQuery(el('quick-search').value);
      loadPhotos(1);
    }
  });
  el('btn-apply-library-filters').addEventListener('click', () => loadPhotos(1));
  el('btn-reset-library-filters').addEventListener('click', () => {
    el('filter-person').value = '';
    el('filter-tag').value = '';
    el('filter-year').value = '';
    el('filter-month').value = '';
    el('filter-query').value = '';
    el('filter-undated').checked = false;
    updateLibraryFieldLocks();
    loadPhotos(1);
  });
  el('filter-undated').addEventListener('change', () => updateLibraryFieldLocks());
  el('photo-grid').addEventListener('click', event => {
    const card = event.target.closest('[data-photo-id]');
    if (!card) {
      return;
    }
    openPhotoModal(Number(card.dataset.photoId));
  });
  el('tag-list').addEventListener('click', event => {
    const button = event.target.closest('[data-object-tag]');
    if (!button) {
      return;
    }
    state.selectedTag = button.dataset.objectTag;
    state.objectPage = 1;
    renderTagBrowser(state.objectTagGroups || {});
    loadObjectPhotos(1);
  });
  el('obj-photo-grid').addEventListener('click', event => {
    const card = event.target.closest('[data-photo-id]');
    if (!card) {
      return;
    }
    openPhotoModal(Number(card.dataset.photoId));
  });
  el('btn-vocab-manager').addEventListener('click', async () => {
    toggleVocabPanel(true);
    await loadVocab();
  });
  el('btn-vocab-close').addEventListener('click', () => toggleVocabPanel(false));
  el('btn-add-vocab').addEventListener('click', () => addVocab());
  el('vocab-tbody').addEventListener('click', event => {
    const button = event.target.closest('[data-delete-vocab]');
    if (!button) {
      return;
    }
    deleteVocab(Number(button.dataset.deleteVocab));
  });

  el('btn-close-photo-modal').addEventListener('click', () => closePhotoModal());
  el('photo-modal').addEventListener('click', event => {
    if (event.target === el('photo-modal')) {
      closePhotoModal();
    }
  });
  el('photo-modal-info').addEventListener('click', event => {
    const button = event.target.closest('[data-detection-action]');
    if (!button) {
      return;
    }
    const detectionId = Number(button.dataset.detectionId);
    if (!Number.isFinite(detectionId)) {
      return;
    }
    if (button.dataset.detectionAction === 'approve') {
      approveDetection(detectionId);
    } else {
      rejectDetection(detectionId);
    }
  });

  el('btn-save-settings').addEventListener('click', () => saveSettings());
  el('btn-clear-db').addEventListener('click', () => clearDatabase());
  el('btn-refresh-log').addEventListener('click', () => loadLog());
  el('btn-expand-log').addEventListener('click', () => toggleLogExpanded());

  document.addEventListener('keydown', event => {
    if (state.activeTab !== 'review') {
      return;
    }
    if (event.key === 'Escape') {
      if (state.selectedFaceIds.size) {
        event.preventDefault();
        clearFaceSelectionAndRender();
      }
      return;
    }
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'a' && !isTextEntryTarget(event.target)) {
      event.preventDefault();
      selectAllReviewFaces();
      return;
    }
    if (isTextEntryTarget(event.target)) {
      return;
    }

    if (event.key === 'Enter' && !isPrototypeMode() && !isPersonMode()) {
      event.preventDefault();
      approveCluster();
      return;
    }
    if ((event.key === 'u' || event.key === 'U') && !isPrototypeMode() && !isPersonMode()) {
      event.preventDefault();
      untagCluster();
      return;
    }
    if ((event.key === 'n' || event.key === 'N') && !isPrototypeMode() && !isPersonMode()) {
      event.preventDefault();
      markClusterNoise();
      return;
    }
    if (event.key === 'Delete' && !isPrototypeMode() && state.selectedFaceIds.size) {
      event.preventDefault();
      removeSelectedFaces();
      return;
    }
    if (event.key === 'ArrowRight') {
      event.preventDefault();
      navigateReviewSelection(1);
      return;
    }
    if (event.key === 'ArrowLeft') {
      event.preventDefault();
      navigateReviewSelection(-1);
    }
  });
}

function startAutoRefresh() {
  clearInterval(state.autoRefreshTimer);
  state.autoRefreshTimer = setInterval(async () => {
    await refreshStatus();
    if (state.activeTab === 'settings') {
      await loadLog();
    }
  }, 5000);
}

async function init() {
  bindEvents();
  await refreshStatus();
  await loadSettings();
  renderScope();
  startAutoRefresh();
}

init();
