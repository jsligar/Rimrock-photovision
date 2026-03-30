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
  selectedClusterId: null,
  clusterCrops: null,
  photoFiltersLoaded: false,
  settingsLoaded: false,
  photos: [],
  photoPage: 1,
  photoPerPage: 48,
  photoTotal: 0,
  photoModalId: null,
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
    loadClusters();
  } else if (tabId === 'library') {
    if (!state.photoFiltersLoaded) {
      loadPhotoFilters().finally(() => loadPhotos(1));
    } else {
      loadPhotos(state.photoPage);
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

function renderClusters() {
  const container = el('cluster-list');
  if (!state.clusters.length) {
    container.innerHTML = '<div class="empty-state">No clusters are waiting for review.</div>';
    renderSelectedCluster();
    return;
  }

  container.innerHTML = state.clusters.map(cluster => `
    <button class="cluster-item ${state.selectedClusterId === cluster.cluster_id ? 'active' : ''}" data-cluster-id="${cluster.cluster_id}" type="button">
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
}

function renderSelectedCluster() {
  const cluster = state.clusters.find(item => item.cluster_id === state.selectedClusterId) || null;
  if (!cluster) {
    el('cluster-detail-title').textContent = 'Choose a cluster';
    el('cluster-detail-meta').textContent = '-';
    el('cluster-label-input').value = '';
    el('cluster-priority-strip').innerHTML = '';
    el('cluster-crops').innerHTML = '<div class="empty-state">Select a cluster to review.</div>';
    setClusterActionDisabled(true);
    return;
  }

  el('cluster-detail-title').textContent = clusterDisplayName(cluster);
  el('cluster-detail-meta').textContent = `${fmtNumber(cluster.face_count)} faces | ${cluster.review_state || 'pending'}`;
  el('cluster-label-input').value = cluster.person_label || '';
  el('cluster-priority-strip').innerHTML = `
    <span class="priority-pill ${escHtml(cluster.review_priority_bucket || 'low')}">Priority ${escHtml(cluster.review_priority_bucket || 'low')}</span>
    <span class="priority-pill">Rank ${fmtNumber(cluster.review_priority_rank || 0)}</span>
    <span class="priority-pill">Score ${cluster.review_priority_score ?? '-'}</span>
  `;
  setClusterActionDisabled(false);

  if (state.clusterCrops === null) {
    el('cluster-crops').innerHTML = '<div class="empty-state">Loading crops...</div>';
    return;
  }

  if (!state.clusterCrops.length) {
    el('cluster-crops').innerHTML = '<div class="empty-state">No crops were returned for this cluster.</div>';
    return;
  }

  el('cluster-crops').innerHTML = state.clusterCrops.map(crop => `
    <article class="crop-card">
      <img src="${escHtml(crop.crop_url || '')}" alt="${escHtml(crop.filename || 'cluster crop')}" />
      <footer>
        <strong>${escHtml(crop.filename || 'Crop')}</strong>
        <span class="phase-meta">Score ${(crop.detection_score || 0).toFixed(2)}</span>
      </footer>
    </article>
  `).join('');
}

function setClusterActionDisabled(disabled) {
  ['btn-cluster-approve', 'btn-cluster-save-label', 'btn-cluster-untag', 'btn-cluster-noise'].forEach(id => {
    el(id).disabled = disabled || isBusy();
  });
}

function renderPhotoFilters(filters) {
  const personSelect = el('filter-person');
  const tagSelect = el('filter-tag');

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
}

function buildPhotoParams(page) {
  const params = new URLSearchParams();
  params.set('page', String(page));
  params.set('per_page', String(state.photoPerPage));

  const person = el('filter-person').value;
  const tag = el('filter-tag').value;
  const year = el('filter-year').value.trim();
  const month = el('filter-month').value.trim();
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

function renderPhotos() {
  const container = el('photo-grid');
  if (!state.photos.length) {
    container.innerHTML = '<div class="empty-state">No photos matched the current filters.</div>';
  } else {
    container.innerHTML = state.photos.map(photo => {
      const src = photo.dest_path
        ? `/organized/${photo.dest_path.split(/\\|\//).map(encodeURIComponent).join('/')}`
        : `/originals/${photo.source_path.split(/\\|\//).map(encodeURIComponent).join('/')}`;
      return `
        <button class="photo-card" data-photo-id="${photo.photo_id}" type="button">
          <img src="${escHtml(src)}" alt="${escHtml(photo.filename)}" />
          <footer>
            <strong>${escHtml(photo.filename)}</strong>
            <span class="phase-meta">${escHtml(fmtDate(photo.exif_date))}</span>
          </footer>
        </button>
      `;
    }).join('');
  }

  el('library-result-count').textContent = `${fmtNumber(state.photoTotal)} photos`;
  renderPagination();
}

function renderPagination() {
  const container = el('library-pagination');
  const totalPages = Math.max(1, Math.ceil(state.photoTotal / state.photoPerPage));
  container.innerHTML = '';

  const prev = document.createElement('button');
  prev.className = 'btn btn-small';
  prev.textContent = 'Previous';
  prev.disabled = state.photoPage <= 1;
  prev.addEventListener('click', () => loadPhotos(state.photoPage - 1));
  container.appendChild(prev);

  const info = document.createElement('span');
  info.textContent = `Page ${state.photoPage} of ${totalPages}`;
  container.appendChild(info);

  const next = document.createElement('button');
  next.className = 'btn btn-small';
  next.textContent = 'Next';
  next.disabled = state.photoPage >= totalPages;
  next.addEventListener('click', () => loadPhotos(state.photoPage + 1));
  container.appendChild(next);
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
      setClusterActionDisabled(!state.selectedClusterId);
    }
  } catch (error) {
    showToast(`Status refresh failed: ${error.message}`, 'err');
  }
}

async function runWorkflow(name) {
  try {
    const result = await apiPost(`/pipeline/workflows/${name}`);
    showToast(`Started ${result.started}.`, 'ok');
    await refreshStatus();
  } catch (error) {
    showToast(error.message, 'err');
  }
}

async function runPhase(phaseId) {
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

async function loadClusters() {
  try {
    state.clusters = await api('/clusters?sort=review');
    if (state.selectedClusterId && !state.clusters.some(item => item.cluster_id === state.selectedClusterId)) {
      state.selectedClusterId = null;
    }
    if (!state.selectedClusterId && state.clusters.length) {
      state.selectedClusterId = state.clusters[0].cluster_id;
    }
    renderClusters();
    if (state.selectedClusterId) {
      await loadClusterCrops(state.selectedClusterId);
    } else {
      renderSelectedCluster();
    }
  } catch (error) {
    el('cluster-list').innerHTML = `<div class="empty-state">${escHtml(error.message)}</div>`;
    renderSelectedCluster();
  }
}

async function loadClusterCrops(clusterId) {
  state.clusterCrops = null;
  renderSelectedCluster();
  try {
    state.clusterCrops = await api(`/clusters/${clusterId}/crops`);
    renderSelectedCluster();
  } catch (error) {
    el('cluster-crops').innerHTML = `<div class="empty-state">${escHtml(error.message)}</div>`;
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
    await refreshStatus();
  } catch (error) {
    showToast(error.message, 'err');
  }
}

async function untagCluster() {
  if (!state.selectedClusterId) {
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
  const tags = (photo.tags || []).map(tag => tag.tag);
  const detections = (photo.detections || []).map(detection => `${detection.tag} (${(detection.confidence || 0).toFixed(2)})`);

  return `
    ${infoSection('Metadata', [
      `Date: ${fmtDate(photo.exif_date)} (${photo.date_source || 'unknown'})`,
      `File: ${photo.filename}`,
      `Source: ${photo.source_path}`,
      photo.dest_path ? `Destination: ${photo.dest_path}` : '',
    ])}
    ${infoPillsSection('People', people)}
    ${infoPillsSection('Tags', tags)}
    ${infoPillsSection('Detections', detections)}
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

async function loadSettings() {
  try {
    state.settings = await api('/settings');
    state.settingsLoaded = true;
    renderSettings();
    renderScope();
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
    state.clusterCrops = [];
    await Promise.all([refreshStatus(), loadSettings()]);
    if (state.activeTab === 'review') {
      await loadClusters();
    }
    if (state.activeTab === 'library') {
      await loadPhotos(1);
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

  el('btn-refresh-clusters').addEventListener('click', () => loadClusters());
  el('cluster-list').addEventListener('click', event => {
    const button = event.target.closest('[data-cluster-id]');
    if (!button) {
      return;
    }
    state.selectedClusterId = Number(button.dataset.clusterId);
    renderClusters();
    loadClusterCrops(state.selectedClusterId);
  });
  el('btn-cluster-save-label').addEventListener('click', () => saveClusterLabel());
  el('btn-cluster-approve').addEventListener('click', () => approveCluster());
  el('btn-cluster-untag').addEventListener('click', () => untagCluster());
  el('btn-cluster-noise').addEventListener('click', () => markClusterNoise());

  el('btn-apply-library-filters').addEventListener('click', () => loadPhotos(1));
  el('btn-reset-library-filters').addEventListener('click', () => {
    el('filter-person').value = '';
    el('filter-tag').value = '';
    el('filter-year').value = '';
    el('filter-month').value = '';
    el('filter-query').value = '';
    el('filter-undated').checked = false;
    loadPhotos(1);
  });
  el('photo-grid').addEventListener('click', event => {
    const card = event.target.closest('[data-photo-id]');
    if (!card) {
      return;
    }
    openPhotoModal(Number(card.dataset.photoId));
  });

  el('btn-close-photo-modal').addEventListener('click', () => closePhotoModal());
  el('photo-modal').addEventListener('click', event => {
    if (event.target === el('photo-modal')) {
      closePhotoModal();
    }
  });

  el('btn-save-settings').addEventListener('click', () => saveSettings());
  el('btn-clear-db').addEventListener('click', () => clearDatabase());
  el('btn-refresh-log').addEventListener('click', () => loadLog());
  el('btn-expand-log').addEventListener('click', () => toggleLogExpanded());
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
