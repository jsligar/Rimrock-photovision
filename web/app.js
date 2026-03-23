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

  switch (tabId) {
    case 'clusters': loadClusters(); break;
    case 'objects': loadTagBrowser(); break;
    case 'photos': loadPhotos(1); break;
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
const TAB_LABELS = {
  dashboard: 'Dashboard',
  clusters: 'Cluster Review',
  objects: 'Objects & Pets',
  photos: 'Photo Browser',
  settings: 'Settings',
};

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
    const canRun = prevOk && p.status !== 'running';
    const isPush = def.id === 'push';
    const isCluster = def.id === 'cluster';

    const pct = p.progress_total > 0
      ? Math.round((p.progress_current / p.progress_total) * 100)
      : (p.status === 'complete' ? 100 : 0);

    const card = document.createElement('div');
    card.className = 'phase-card';
    card.innerHTML = `
      <div class="phase-card-header">
        <div><span class="phase-num">[${def.num}]</span><span class="phase-name">${def.name}</span></div>
        <span class="badge badge-${p.status}">${p.status.toUpperCase()}</span>
      </div>
      <div class="phase-progress"><div class="phase-progress-bar" style="width:${pct}%"></div></div>
      <div class="phase-count">${_phaseCountLine(def.id, p, counts)}</div>
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
      btn.addEventListener('click', () => triggerPhase(btn.dataset.phase));
    }
  });
}

function _phaseCountLine(id, p, counts) {
  if (!counts) return `${fmt(p.progress_current)} / ${fmt(p.progress_total)}`;

  switch (id) {
    case 'pull':
      return `${fmt(counts.total_photos)} photos`;
    case 'process':
      return `${fmt(counts.total_photos)} photos | ${fmt(counts.total_faces)} faces`;
    case 'cluster':
      return `${fmt(counts.total_clusters)} clusters | ${fmt(counts.labeled_clusters)} labeled`;
    case 'organize':
      return `${fmt(counts.photos_organized)} organized`;
    case 'tag':
      return `${fmt(counts.total_detections)} detections`;
    default:
      return `${fmt(p.progress_current)} / ${fmt(p.progress_total)}`;
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

async function refreshStatus() {
  try {
    const data = await api('/status');
    statusData = data;

    renderPhaseGrid(data.phases, data.counts);
    updateHeaderStatus(data.phases);
    renderSidebarSnapshot(data.phases);
    updateLogTail();

    const anyRunning = data.phases.some(p => p.status === 'running');
    if (anyRunning) scheduleAutoRefresh();
    else clearAutoRefresh();
  } catch (e) {
    console.error('Status refresh failed:', e);
  }
}

function updateHeaderStatus(phases) {
  const running = phases.find(p => p.status === 'running');
  const errors = phases.filter(p => p.status === 'error');

  let txt = 'READY';
  if (running) txt = `${running.phase.toUpperCase()} RUNNING`;
  else if (errors.length) txt = `${errors.length} ERROR(S)`;

  const statusNode = el('header-status');
  if (statusNode) statusNode.textContent = txt;

  const sideStatus = el('sidebar-status');
  if (sideStatus) sideStatus.textContent = txt;
}

function renderSidebarSnapshot(phases) {
  const running = phases.find(p => p.status === 'running');
  const complete = phases.filter(p => p.status === 'complete').length;
  const errors = phases.filter(p => p.status === 'error').length;

  const runNode = el('sidebar-running-phase');
  if (runNode) runNode.textContent = running ? running.phase.toUpperCase() : '-';

  const completeNode = el('sidebar-complete-count');
  if (completeNode) completeNode.textContent = `${complete} / ${PHASE_DEFS.length}`;

  const errNode = el('sidebar-error-count');
  if (errNode) errNode.textContent = String(errors);
}

function updateSidebarActiveTab(tabId) {
  const node = el('sidebar-active-tab');
  if (node) node.textContent = TAB_LABELS[tabId] || tabId;
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

async function updateLogTail() {
  try {
    const data = await api('/pipeline/log-tail?lines=50');
    const box = el('log-tail');
    if (!box) return;

    if (data.lines && data.lines.length > 0) {
      box.textContent = data.lines.join('\n');
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

async function loadClusters() {
  setEmpty('cluster-list', 'Loading clusters...');
  setSkeleton('crop-grid', 'crop', 12);

  try {
    clusters = await api('/clusters');
    renderClusterList();

    if (clusters.length > 0) {
      await selectCluster(0);
    } else {
      setEmpty('crop-grid', 'No clusters yet. Run Process and Cluster phases.');
    }
  } catch (e) {
    setEmpty('cluster-list', `Error: ${e.message}`, true);
    setEmpty('crop-grid', 'Unable to load cluster crops.', true);
  }
}

function clusterDotClass(c) {
  if (c.is_noise) return 'dot-noise';
  if (c.approved) return 'dot-approved';
  if (c.person_label) return 'dot-labeled';
  return 'dot-unlabeled';
}

function renderClusterList() {
  const list = el('cluster-list');
  if (!list) return;
  list.innerHTML = '';

  const labeled = clusters.filter(c => c.person_label && !c.is_noise).length;
  const total = clusters.filter(c => !c.is_noise).length;
  const progressNode = el('cluster-progress');
  if (progressNode) progressNode.textContent = `${labeled} / ${total} labeled`;

  if (clusters.length === 0) {
    setEmpty('cluster-list', 'No clusters found.');
    return;
  }

  clusters.forEach((c, i) => {
    const div = document.createElement('div');
    div.className = 'sidebar-item' + (i === selectedClusterIdx ? ' active' : '');
    div.innerHTML = `
      <span class="status-dot ${clusterDotClass(c)}"></span>
      <span class="item-label">${escHtml(c.person_label || `Cluster ${c.cluster_id}`)}</span>
      <span class="item-count">${c.face_count}</span>
    `;
    div.addEventListener('click', () => selectCluster(i));
    list.appendChild(div);
  });

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
}

async function selectCluster(idx) {
  selectedClusterIdx = idx;
  const c = clusters[idx];
  if (!c) return;

  document.querySelectorAll('#cluster-list .sidebar-item').forEach((node, i) => {
    node.classList.toggle('active', i === idx);
  });

  const toolbar = el('cluster-toolbar');
  if (toolbar) toolbar.style.display = 'flex';

  const nameInput = el('cluster-name-input');
  if (nameInput) nameInput.value = c.person_label || '';

  setSkeleton('crop-grid', 'crop', 12);

  try {
    const crops = await api(`/clusters/${c.cluster_id}/crops`);
    const grid = el('crop-grid');
    if (!grid) return;

    grid.innerHTML = '';
    if (!crops.length) {
      setEmpty('crop-grid', 'No crops for this cluster.');
      return;
    }

    crops.forEach(crop => {
      const tile = document.createElement('div');
      tile.className = 'crop-tile';
      tile.innerHTML = `
        <img src="${crop.crop_url || ''}" alt="" loading="lazy" onerror="this.style.display='none'" />
        <div class="score">${(crop.detection_score || 0).toFixed(2)}</div>
      `;
      grid.appendChild(tile);
    });
  } catch (_) {
    setEmpty('crop-grid', 'Error loading crops.', true);
  }
}

el('btn-approve-cluster').addEventListener('click', async () => {
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
    if (selectedClusterIdx < clusters.length) await selectCluster(selectedClusterIdx);
    showToast('Cluster approved.', 'ok');
  } catch (e) {
    showToast('Failed to approve cluster.', 'err');
  } finally {
    setBusy(button, false);
  }
});

el('btn-noise-cluster').addEventListener('click', async () => {
  const c = clusters[selectedClusterIdx];
  if (!c) return;

  const button = el('btn-noise-cluster');
  setBusy(button, true);

  try {
    await apiPost(`/clusters/${c.cluster_id}/noise`);
    await loadClusters();
    if (selectedClusterIdx < clusters.length) await selectCluster(selectedClusterIdx);
    showToast('Cluster marked as noise.', 'ok');
  } catch (e) {
    showToast('Failed to update cluster.', 'err');
  } finally {
    setBusy(button, false);
  }
});

el('btn-merge-cluster').addEventListener('click', async () => {
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
    if (clusters.length) await selectCluster(0);
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
  if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;

  switch (e.key) {
    case 'Enter':
      el('btn-approve-cluster').click();
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

  try {
    const grouped = await api('/objects/tags');
    renderTagBrowser(grouped);
  } catch (e) {
    setEmpty('tag-list', `Error: ${e.message}`, true);
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
  } catch (e) {
    setEmpty('obj-photo-grid', `Error: ${e.message}`, true);
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
        <td><button class="btn btn-danger" onclick="deleteVocab(${v.vocab_id})">Delete</button></td>
      `;
      tbody.appendChild(tr);
    });
  } catch (e) {
    tbody.innerHTML = '<tr><td colspan="5" class="red">Failed to load vocabulary.</td></tr>';
  }
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
  } catch (e) {
    setEmpty('photo-grid', `Error: ${e.message}`, true);
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
  try {
    const cl = await api('/clusters');
    const personSel = el('filter-person');
    cl.filter(c => c.person_label && c.approved && !c.is_noise).forEach(c => {
      const opt = document.createElement('option');
      opt.value = c.person_label;
      opt.textContent = c.person_label;
      personSel.appendChild(opt);
    });
  } catch (_) {
    // optional
  }

  try {
    const grouped = await api('/objects/tags');
    const tagSel = el('filter-tag');

    Object.values(grouped).forEach(tags => {
      tags.forEach(t => {
        const opt = document.createElement('option');
        opt.value = t.tag;
        opt.textContent = t.tag;
        tagSel.appendChild(opt);
      });
    });
  } catch (_) {
    // optional
  }

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
              ? `<button class="btn btn-danger" onclick="rejectDetection(${d.detection_id})">Reject</button>`
              : `<button class="btn" onclick="approveDetection(${d.detection_id})">Approve</button>`}
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

async function loadSettings() {
  try {
    const s = await api('/settings');
    el('s-nas-dir').value = s.nas_source_dir || '';
    el('s-local-base').value = s.local_base || '';
    el('s-yolo-conf').value = s.yolo_conf_threshold || 0.45;
    el('s-clip-thresh').value = s.clip_tag_threshold || 0.26;
    el('s-max-dim').value = s.max_inference_dim || 1920;

    const stats = el('stats-rows');
    stats.innerHTML = `
      <div class="stat-row"><span>NVMe Free</span><span class="val">${s.nvme_free_gb} GB</span></div>
      <div class="stat-row"><span>NVMe Total</span><span class="val">${s.nvme_total_gb} GB</span></div>
      <div class="stat-row"><span>DB Size</span><span class="val">${s.db_size_mb} MB</span></div>
      <div class="stat-row"><span>Total Photos</span><span class="val">${fmt(s.total_photos)}</span></div>
      <div class="stat-row"><span>Total Faces</span><span class="val">${fmt(s.total_faces)}</span></div>
    `;
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

el('btn-command').addEventListener('click', () => {
  openCommandPalette();
});

const sidebarCmdBtn = el('sidebar-command-go');
const sidebarCmdInput = el('sidebar-command-input');
if (sidebarCmdBtn) {
  sidebarCmdBtn.addEventListener('click', () => {
    openCommandPalette((sidebarCmdInput?.value || '').trim());
  });
}
if (sidebarCmdInput) {
  sidebarCmdInput.addEventListener('keydown', e => {
    if (e.key === 'Enter') {
      e.preventDefault();
      openCommandPalette(sidebarCmdInput.value.trim());
    }
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

(async () => {
  updateSidebarActiveTab('dashboard');
  renderSidebarSnapshot(PHASE_DEFS.map(p => ({ phase: p.id, status: 'pending' })));
  await refreshStatus();
  await initPhotoFilters();
  scheduleAutoRefresh();
})();
