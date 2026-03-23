/* ── Rimrock Photo Tagger — Vanilla ES6 UI ─────────────────────────────────── */
'use strict';

const API = '';  // same origin

// ── Utility ───────────────────────────────────────────────────────────────────

async function api(path, opts = {}) {
  const r = await fetch(API + '/api' + path, opts);
  if (!r.ok) throw new Error(`API ${path} → ${r.status}`);
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
  if (!r.ok) throw new Error(`DELETE ${path} → ${r.status}`);
  return r.json();
}

function el(id) { return document.getElementById(id); }

function fmt(n) {
  if (n == null) return '–';
  return n.toLocaleString();
}

function fmtDate(s) {
  if (!s) return 'undated';
  return s.substring(0, 10);
}

// ── Tab navigation ────────────────────────────────────────────────────────────

const tabBtns = document.querySelectorAll('.tab-btn');
const tabPanes = document.querySelectorAll('.tab-pane');

tabBtns.forEach(btn => {
  btn.addEventListener('click', () => {
    tabBtns.forEach(b => b.classList.remove('active'));
    tabPanes.forEach(p => p.classList.remove('active'));
    btn.classList.add('active');
    el(`tab-${btn.dataset.tab}`).classList.add('active');

    switch (btn.dataset.tab) {
      case 'clusters': loadClusters(); break;
      case 'objects':  loadTagBrowser(); break;
      case 'photos':   loadPhotos(); break;
      case 'settings': loadSettings(); break;
    }
  });
});

// ── Dashboard ─────────────────────────────────────────────────────────────────

const PHASE_DEFS = [
  { id: 'preflight', num: 0, name: 'Preflight',  desc: 'Verify storage, memory, models' },
  { id: 'pull',      num: 1, name: 'Pull',       desc: 'rsync NAS → NVMe' },
  { id: 'process',   num: 2, name: 'Process',    desc: 'Face + semantic tagging' },
  { id: 'cluster',   num: 3, name: 'Cluster',    desc: 'UMAP + HDBSCAN clustering' },
  { id: 'organize',  num: 4, name: 'Organize',   desc: 'Copy to YYYY/YYYY-MM/' },
  { id: 'tag',       num: 5, name: 'Tag',        desc: 'Write XMP via exiftool' },
  { id: 'push',      num: 6, name: 'Push',       desc: 'rsync organized → NAS' },
  { id: 'verify',    num: 7, name: 'Verify',     desc: 'Checksum spot-check' },
];

let autoRefreshTimer = null;
let statusData = null;

function renderPhaseGrid(phases, counts) {
  const grid = el('phase-grid');
  grid.innerHTML = '';

  const phaseMap = {};
  phases.forEach(p => phaseMap[p.phase] = p);

  PHASE_DEFS.forEach((def, i) => {
    const p = phaseMap[def.id] || { status: 'pending', progress_current: 0, progress_total: 0 };
    const prevDef = i > 0 ? PHASE_DEFS[i - 1] : null;
    const prevPhase = prevDef ? phaseMap[prevDef.id] : null;

    // Enable run button only if previous phase is complete (or first phase)
    const prevOk = !prevPhase || prevPhase.status === 'complete';
    const canRun = prevOk && p.status !== 'running';
    const isPush = def.id === 'push';
    const isCluster = def.id === 'cluster';

    const pct = p.progress_total > 0
      ? Math.round((p.progress_current / p.progress_total) * 100)
      : (p.status === 'complete' ? 100 : 0);

    const countLine = _phaseCountLine(def.id, p, counts);

    const card = document.createElement('div');
    card.className = 'phase-card';
    card.innerHTML = `
      <div class="phase-card-header">
        <div><span class="phase-num">[${def.num}]</span><span class="phase-name">${def.name}</span></div>
        <span class="badge badge-${p.status}">${p.status.toUpperCase()}</span>
      </div>
      <div class="phase-progress"><div class="phase-progress-bar" style="width:${pct}%"></div></div>
      <div class="phase-count">${countLine}</div>
      ${p.error_message ? `<div class="phase-error-msg">${escHtml(p.error_message)}</div>` : ''}
      <div class="phase-actions">
        ${isPush ? `
          <label class="push-confirm">
            <input type="checkbox" id="push-confirm-cb" />
            I've reviewed output — ready to push
          </label>
        ` : ''}
        <button class="btn btn-accent run-btn"
          data-phase="${def.id}"
          ${canRun && !isPush ? '' : 'disabled'}
          ${isPush ? 'id="btn-run-push"' : ''}>
          Run
        </button>
        ${isCluster ? '<span class="note" style="font-size:10px;">Review clusters before Organize</span>' : ''}
      </div>
    `;
    grid.appendChild(card);

    if (isPush) {
      const cb = card.querySelector('#push-confirm-cb');
      const runBtn = card.querySelector('#btn-run-push');
      cb && cb.addEventListener('change', () => {
        runBtn.disabled = !(cb.checked && prevOk);
      });
    }
  });

  // Attach run button handlers
  grid.querySelectorAll('.run-btn').forEach(btn => {
    if (!btn.disabled) {
      btn.addEventListener('click', () => triggerPhase(btn.dataset.phase));
    }
  });
}

function _phaseCountLine(id, p, counts) {
  if (!counts) return `${fmt(p.progress_current)} / ${fmt(p.progress_total)}`;
  switch (id) {
    case 'pull':    return `${fmt(counts.total_photos)} photos`;
    case 'process': return `${fmt(counts.total_photos)} photos · ${fmt(counts.total_faces)} faces`;
    case 'cluster': return `${fmt(counts.total_clusters)} clusters · ${fmt(counts.labeled_clusters)} labeled`;
    case 'organize':return `${fmt(counts.photos_organized)} organized`;
    case 'tag':     return `${fmt(counts.total_detections)} detections`;
    default:        return `${fmt(p.progress_current)} / ${fmt(p.progress_total)}`;
  }
}

async function triggerPhase(phase) {
  try {
    await apiPost(`/pipeline/run/${phase}`);
    refreshStatus();
    scheduleAutoRefresh();
  } catch (e) {
    alert(`Failed to start ${phase}: ${e.message}`);
  }
}

async function refreshStatus() {
  try {
    const data = await api('/status');
    statusData = data;
    renderPhaseGrid(data.phases, data.counts);
    updateHeaderStatus(data.phases);
    updateLogTail();

    const anyRunning = data.phases.some(p => p.status === 'running');
    if (anyRunning) {
      scheduleAutoRefresh();
    } else {
      clearAutoRefresh();
    }
  } catch (e) {
    console.error('Status refresh failed:', e);
  }
}

function updateHeaderStatus(phases) {
  const running = phases.find(p => p.status === 'running');
  const errors  = phases.filter(p => p.status === 'error');
  let txt = 'READY';
  if (running) txt = `▶ ${running.phase.toUpperCase()} RUNNING`;
  else if (errors.length) txt = `⚠ ${errors.length} ERROR(S)`;
  el('header-status').textContent = txt;
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
    if (data.lines && data.lines.length > 0) {
      box.textContent = data.lines.join('\n');
      box.scrollTop = box.scrollHeight;
    } else {
      box.textContent = '[ No log output yet ]';
    }
  } catch (_) {
    // Log tail is a nice-to-have; skip silently if not available
  }
}

// ── Cluster Review ────────────────────────────────────────────────────────────

let clusters = [];
let selectedClusterIdx = 0;

async function loadClusters() {
  try {
    clusters = await api('/clusters');
    renderClusterList();
    if (clusters.length > 0) selectCluster(0);
  } catch (e) {
    el('cluster-list').innerHTML = `<div class="empty-state red">Error: ${e.message}</div>`;
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
  list.innerHTML = '';

  const labeled = clusters.filter(c => c.person_label && !c.is_noise).length;
  const total   = clusters.filter(c => !c.is_noise).length;
  el('cluster-progress').textContent = `${labeled} / ${total} labeled`;

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

  // Populate merge dropdown
  const sel = el('merge-target');
  sel.innerHTML = '<option value="">Merge into…</option>';
  clusters.forEach(c => {
    if (!c.is_noise) {
      const opt = document.createElement('option');
      opt.value = c.cluster_id;
      opt.textContent = c.person_label || `Cluster ${c.cluster_id}`;
      sel.appendChild(opt);
    }
  });
}

async function selectCluster(idx) {
  selectedClusterIdx = idx;
  const c = clusters[idx];
  if (!c) return;

  // Highlight selected
  document.querySelectorAll('#cluster-list .sidebar-item').forEach((el, i) => {
    el.classList.toggle('active', i === idx);
  });

  // Show toolbar
  const toolbar = el('cluster-toolbar');
  toolbar.style.display = 'flex';

  // Populate name input
  el('cluster-name-input').value = c.person_label || '';

  // Load crops
  const grid = el('crop-grid');
  grid.innerHTML = '<div class="empty-state dim">Loading...</div>';

  try {
    const crops = await api(`/clusters/${c.cluster_id}/crops`);
    grid.innerHTML = '';
    if (crops.length === 0) {
      grid.innerHTML = '<div class="empty-state">No crops for this cluster</div>';
      return;
    }
    crops.forEach(crop => {
      const tile = document.createElement('div');
      tile.className = 'crop-tile';
      tile.innerHTML = `
        <img src="${crop.crop_url || ''}" alt="" loading="lazy"
             onerror="this.style.display='none'" />
        <div class="score">${(crop.detection_score || 0).toFixed(2)}</div>
      `;
      grid.appendChild(tile);
    });
  } catch (e) {
    grid.innerHTML = `<div class="empty-state red">Error loading crops</div>`;
  }
}

// Cluster action buttons
el('btn-approve-cluster').addEventListener('click', async () => {
  const c = clusters[selectedClusterIdx];
  if (!c) return;
  const name = el('cluster-name-input').value.trim();
  if (name) {
    await apiPost(`/clusters/${c.cluster_id}/label`, { person_label: name });
  }
  await apiPost(`/clusters/${c.cluster_id}/approve`);
  await loadClusters();
  if (selectedClusterIdx < clusters.length) selectCluster(selectedClusterIdx);
});

el('btn-noise-cluster').addEventListener('click', async () => {
  const c = clusters[selectedClusterIdx];
  if (!c) return;
  await apiPost(`/clusters/${c.cluster_id}/noise`);
  await loadClusters();
  if (selectedClusterIdx < clusters.length) selectCluster(selectedClusterIdx);
});

el('btn-merge-cluster').addEventListener('click', async () => {
  const c = clusters[selectedClusterIdx];
  if (!c) return;
  const targetId = parseInt(el('merge-target').value);
  if (!targetId || targetId === c.cluster_id) {
    alert('Select a valid target cluster to merge into.');
    return;
  }
  await apiPost('/clusters/merge', {
    source_cluster_id: c.cluster_id,
    target_cluster_id: targetId,
  });
  await loadClusters();
  selectCluster(0);
});

// Keyboard shortcuts for cluster review
document.addEventListener('keydown', e => {
  if (!el('tab-clusters').classList.contains('active')) return;
  if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;

  switch (e.key) {
    case 'Enter':
      el('btn-approve-cluster').click();
      break;
    case 'n': case 'N':
      el('btn-noise-cluster').click();
      break;
    case 'ArrowRight':
      if (selectedClusterIdx < clusters.length - 1) selectCluster(selectedClusterIdx + 1);
      break;
    case 'ArrowLeft':
      if (selectedClusterIdx > 0) selectCluster(selectedClusterIdx - 1);
      break;
  }
});

// ── Objects & Pets ────────────────────────────────────────────────────────────

let selectedTag = null;
let objPage = 1;

async function loadTagBrowser() {
  try {
    const grouped = await api('/objects/tags');
    renderTagBrowser(grouped);
  } catch (e) {
    el('tag-list').innerHTML = `<div class="empty-state red">Error: ${e.message}</div>`;
  }
}

function renderTagBrowser(grouped) {
  const list = el('tag-list');
  list.innerHTML = '';

  for (const [group, tags] of Object.entries(grouped)) {
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
  }
}

async function loadObjPhotos() {
  if (!selectedTag) return;
  try {
    const data = await api(`/objects/tags/${encodeURIComponent(selectedTag)}?page=${objPage}&per_page=48`);
    el('obj-count').textContent = `${fmt(data.total)} photos`;
    renderPhotoGrid('obj-photo-grid', data.photos);
    renderPagination('obj-pagination', objPage, data.total, 48, p => { objPage = p; loadObjPhotos(); });
  } catch (e) {
    el('obj-photo-grid').innerHTML = `<div class="empty-state red">Error: ${e.message}</div>`;
  }
}

// Vocabulary manager
el('btn-vocab-manager').addEventListener('click', async () => {
  const panel = el('vocab-panel');
  panel.style.display = panel.style.display === 'none' ? 'block' : 'none';
  if (panel.style.display !== 'none') await loadVocab();
});

el('btn-vocab-close').addEventListener('click', () => {
  el('vocab-panel').style.display = 'none';
});

async function loadVocab() {
  const vocab = await api('/objects/vocabulary');
  const tbody = el('vocab-tbody');
  tbody.innerHTML = '';
  vocab.forEach(v => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td class="mono dim" style="font-size:11px;">${escHtml(v.tag_group)}</td>
      <td>${escHtml(v.tag_name)}</td>
      <td style="font-size:11px; color:var(--text-dim);">${escHtml(v.prompts.join(', '))}</td>
      <td><input type="checkbox" ${v.enabled ? 'checked' : ''} disabled /></td>
      <td><button class="btn btn-danger" style="font-size:11px; padding:3px 8px;"
          onclick="deleteVocab(${v.vocab_id})">✕</button></td>
    `;
    tbody.appendChild(tr);
  });
}

async function deleteVocab(id) {
  if (!confirm('Delete this vocabulary entry?')) return;
  await apiDelete(`/objects/vocabulary/${id}`);
  await loadVocab();
}

el('btn-add-vocab').addEventListener('click', async () => {
  const group   = el('new-vocab-group').value.trim();
  const name    = el('new-vocab-name').value.trim();
  const prompts = el('new-vocab-prompts').value.trim().split('\n').filter(x => x);
  const enabled = el('new-vocab-enabled').checked;

  if (!group || !name || prompts.length === 0) {
    alert('Fill in group, tag name, and at least one prompt.');
    return;
  }
  await apiPost('/objects/vocabulary', { tag_group: group, tag_name: name, prompts, enabled });
  el('new-vocab-group').value = '';
  el('new-vocab-name').value = '';
  el('new-vocab-prompts').value = '';
  await loadVocab();
});

// ── Photo Browser ──────────────────────────────────────────────────────────────

let photoPage = 1;

async function loadPhotos(page = 1) {
  photoPage = page;
  const params = buildPhotoParams();
  try {
    const data = await api(`/photos?${params}&page=${page}&per_page=60`);
    el('photo-count').textContent = `${fmt(data.total)} photos`;
    renderPhotoGrid('photo-grid', data.photos);
    renderPagination('photo-pagination', page, data.total, 60, p => loadPhotos(p));
  } catch (e) {
    el('photo-grid').innerHTML = `<div class="empty-state red">Error: ${e.message}</div>`;
  }
}

function buildPhotoParams() {
  const p = new URLSearchParams();
  const person  = el('filter-person').value;
  const tag     = el('filter-tag').value;
  const year    = el('filter-year').value;
  const month   = el('filter-month').value;
  const undated = el('filter-undated').checked;

  if (person)  p.set('person', person);
  if (tag)     p.set('tag', tag);
  if (year)    p.set('year', year);
  if (month)   p.set('month', month.padStart(2, '0'));
  if (undated) p.set('undated', 'true');
  return p.toString();
}

el('btn-apply-filters').addEventListener('click', () => loadPhotos(1));
el('filter-undated').addEventListener('change', () => {
  const undated = el('filter-undated').checked;
  ['filter-person','filter-tag','filter-year','filter-month'].forEach(id => {
    el(id).disabled = undated;
  });
});

async function initPhotoFilters() {
  // Populate person dropdown from approved clusters
  try {
    const clusters = await api('/clusters');
    const sel = el('filter-person');
    clusters.filter(c => c.person_label && c.approved && !c.is_noise).forEach(c => {
      const opt = document.createElement('option');
      opt.value = c.person_label;
      opt.textContent = c.person_label;
      sel.appendChild(opt);
    });
  } catch (_) {}

  // Populate tag dropdown
  try {
    const grouped = await api('/objects/tags');
    const sel = el('filter-tag');
    for (const tags of Object.values(grouped)) {
      tags.forEach(t => {
        const opt = document.createElement('option');
        opt.value = t.tag;
        opt.textContent = t.tag;
        sel.appendChild(opt);
      });
    }
  } catch (_) {}

  // Year/month dropdowns
  const yearSel = el('filter-year');
  const monthSel = el('filter-month');
  const currentYear = new Date().getFullYear();
  for (let y = currentYear; y >= 1970; y--) {
    const opt = document.createElement('option');
    opt.value = y;
    opt.textContent = y;
    yearSel.appendChild(opt);
  }
  for (let m = 1; m <= 12; m++) {
    const opt = document.createElement('option');
    opt.value = String(m).padStart(2, '0');
    opt.textContent = new Date(2000, m - 1, 1).toLocaleString('en', { month: 'long' });
    monthSel.appendChild(opt);
  }
}

// ── Shared: Photo Grid ────────────────────────────────────────────────────────

function renderPhotoGrid(gridId, photos) {
  const grid = el(gridId);
  grid.innerHTML = '';

  if (!photos || photos.length === 0) {
    grid.innerHTML = '<div class="empty-state">No photos found</div>';
    return;
  }

  photos.forEach(photo => {
    const thumb = document.createElement('div');
    thumb.className = 'photo-thumb';
    const src = photo.dest_path
      ? `/organized/${photo.dest_path}`
      : `/originals/${photo.source_path}`;
    thumb.innerHTML = `
      <img src="${src}" alt="${escHtml(photo.filename)}" loading="lazy"
           onerror="this.parentElement.style.background='#1a1e1c';" />
      <div class="date-overlay">${fmtDate(photo.exif_date)}</div>
    `;
    thumb.addEventListener('click', () => openPhotoModal(photo.photo_id));
    grid.appendChild(thumb);
  });
}

function renderPagination(containerId, page, total, perPage, onPage) {
  const container = el(containerId);
  const totalPages = Math.ceil(total / perPage);
  container.innerHTML = '';
  if (totalPages <= 1) return;

  const prev = document.createElement('button');
  prev.className = 'btn';
  prev.textContent = '← Prev';
  prev.disabled = page <= 1;
  prev.addEventListener('click', () => onPage(page - 1));
  container.appendChild(prev);

  const info = document.createElement('span');
  info.className = 'page-info';
  info.textContent = `Page ${page} of ${totalPages}`;
  container.appendChild(info);

  const next = document.createElement('button');
  next.className = 'btn';
  next.textContent = 'Next →';
  next.disabled = page >= totalPages;
  next.addEventListener('click', () => onPage(page + 1));
  container.appendChild(next);
}

// ── Photo Detail Modal ────────────────────────────────────────────────────────

el('modal-close').addEventListener('click', () => {
  el('photo-modal').classList.remove('open');
});
el('photo-modal').addEventListener('click', e => {
  if (e.target === el('photo-modal')) el('photo-modal').classList.remove('open');
});

async function openPhotoModal(photoId) {
  try {
    const photo = await api(`/photos/${photoId}`);
    el('modal-filename').textContent = photo.filename;
    el('photo-modal').classList.add('open');

    // Load image onto canvas with face bboxes
    const canvas = el('modal-canvas');
    const ctx = canvas.getContext('2d');
    const imgSrc = photo.preview_url;

    const img = new Image();
    img.crossOrigin = 'anonymous';
    img.onload = () => {
      const maxW = 560, maxH = 420;
      let w = img.naturalWidth, h = img.naturalHeight;
      const scale = Math.min(maxW / w, maxH / h, 1);
      canvas.width  = Math.round(w * scale);
      canvas.height = Math.round(h * scale);
      ctx.drawImage(img, 0, 0, canvas.width, canvas.height);

      // Draw face bounding boxes
      photo.faces && photo.faces.forEach(f => {
        if (!f.bbox) return;
        const [x1, y1, x2, y2] = f.bbox.map(v => v * scale);
        ctx.strokeStyle = f.cluster_approved ? '#4caf74' : '#c8a84b';
        ctx.lineWidth = 2;
        ctx.strokeRect(x1, y1, x2 - x1, y2 - y1);
        if (f.person_label) {
          ctx.fillStyle = 'rgba(0,0,0,0.6)';
          ctx.fillRect(x1, y1 - 16, (x2 - x1), 16);
          ctx.fillStyle = '#c8a84b';
          ctx.font = '11px monospace';
          ctx.fillText(f.person_label, x1 + 3, y1 - 4);
        }
      });

      // Draw YOLO bboxes
      photo.detections && photo.detections.forEach(d => {
        if (!d.bbox || !d.approved) return;
        const [x1, y1, x2, y2] = d.bbox.map(v => v * scale);
        ctx.strokeStyle = '#4b8fc8';
        ctx.lineWidth = 1;
        ctx.strokeRect(x1, y1, x2 - x1, y2 - y1);
        ctx.fillStyle = 'rgba(75,143,200,0.15)';
        ctx.fillRect(x1, y1, x2 - x1, y2 - y1);
      });
    };
    img.onerror = () => {
      ctx.fillStyle = '#1a1e1c';
      ctx.fillRect(0, 0, canvas.width, canvas.height);
      ctx.fillStyle = '#7a8078';
      ctx.font = '13px monospace';
      ctx.fillText('Preview unavailable', 20, canvas.height / 2);
    };
    img.src = imgSrc;

    // Render metadata panel
    renderPhotoInfo(photo);
  } catch (e) {
    console.error('Modal error:', e);
  }
}

function renderPhotoInfo(photo) {
  const info = el('modal-info');
  const faceTags   = (photo.faces || []).filter(f => f.person_label && f.cluster_approved).map(f => f.person_label);
  const objectTags = (photo.tags || []).filter(t => t.source !== 'face').map(t => t.tag);

  info.innerHTML = `
    <div class="info-section">
      <h4>Metadata</h4>
      <div class="info-row"><span class="label">Date: </span>${fmtDate(photo.exif_date)} <span class="dim">(${photo.date_source || ''})</span></div>
      <div class="info-row"><span class="label">File: </span><span class="mono" style="font-size:11px;">${escHtml(photo.filename)}</span></div>
      <div class="info-row"><span class="label">Source: </span><span class="mono dim" style="font-size:10px;">${escHtml(photo.source_path)}</span></div>
      ${photo.dest_path ? `<div class="info-row"><span class="label">Dest: </span><span class="mono dim" style="font-size:10px;">${escHtml(photo.dest_path)}</span></div>` : ''}
    </div>

    ${faceTags.length > 0 ? `
    <div class="info-section">
      <h4>People</h4>
      <div class="tag-pills">
        ${faceTags.map(t => `<span class="pill pill-face">${escHtml(t)}</span>`).join('')}
      </div>
    </div>` : ''}

    ${objectTags.length > 0 ? `
    <div class="info-section">
      <h4>Tags</h4>
      <div class="tag-pills">
        ${(photo.tags || []).map(t => {
          const cls = t.source === 'yolo' ? 'pill-yolo' : 'pill-clip';
          return `<span class="pill ${cls}">${escHtml(t.tag)}</span>`;
        }).join('')}
      </div>
    </div>` : ''}

    ${(photo.detections || []).length > 0 ? `
    <div class="info-section">
      <h4>Detections</h4>
      ${(photo.detections || []).map(d => `
        <div class="info-row" style="display:flex; justify-content:space-between; align-items:center;">
          <span>${escHtml(d.tag)} <span class="dim">(${d.model})</span></span>
          <span class="mono dim" style="font-size:10px;">${(d.confidence || 0).toFixed(2)}</span>
          ${d.approved
            ? `<button class="btn btn-danger" style="font-size:10px; padding:2px 6px;" onclick="rejectDetection(${d.detection_id})">✕</button>`
            : `<button class="btn" style="font-size:10px; padding:2px 6px;" onclick="approveDetection(${d.detection_id})">✓</button>`}
        </div>
      `).join('')}
    </div>` : ''}

    ${(photo.faces || []).length > 0 ? `
    <div class="info-section">
      <h4>Faces (${photo.faces.length})</h4>
      ${photo.faces.map(f => `
        <div class="info-row">
          ${f.person_label
            ? `<span class="green">${escHtml(f.person_label)}</span>${f.cluster_approved ? ' ✓' : ' (pending)'}`
            : `<span class="dim">Cluster ${f.cluster_id ?? '?'} — unlabeled</span>`}
          <span class="mono dim" style="font-size:10px; margin-left:6px;">${(f.detection_score || 0).toFixed(2)}</span>
        </div>
      `).join('')}
    </div>` : ''}
  `;
}

async function rejectDetection(id) {
  await apiPost(`/objects/detections/${id}/reject`);
  // Re-open modal to refresh
}

async function approveDetection(id) {
  await apiPost(`/objects/detections/${id}/approve`);
}

// ── Settings ───────────────────────────────────────────────────────────────────

async function loadSettings() {
  try {
    const s = await api('/settings');
    el('s-nas-dir').value    = s.nas_source_dir || '';
    el('s-local-base').value = s.local_base || '';
    el('s-yolo-conf').value  = s.yolo_conf_threshold || 0.45;
    el('s-clip-thresh').value = s.clip_tag_threshold || 0.26;
    el('s-max-dim').value    = s.max_inference_dim || 1920;

    const stats = el('stats-rows');
    stats.innerHTML = `
      <div class="stat-row"><span>NVMe Free</span><span class="val">${s.nvme_free_gb} GB</span></div>
      <div class="stat-row"><span>NVMe Total</span><span class="val">${s.nvme_total_gb} GB</span></div>
      <div class="stat-row"><span>DB Size</span><span class="val">${s.db_size_mb} MB</span></div>
      <div class="stat-row"><span>Total Photos</span><span class="val">${fmt(s.total_photos)}</span></div>
      <div class="stat-row"><span>Total Faces</span><span class="val">${fmt(s.total_faces)}</span></div>
    `;
  } catch (e) {
    console.error('Settings load error:', e);
  }
}

el('btn-save-settings').addEventListener('click', async () => {
  const body = {
    nas_source_dir:      el('s-nas-dir').value.trim() || null,
    local_base:          el('s-local-base').value.trim() || null,
    yolo_conf_threshold: parseFloat(el('s-yolo-conf').value) || null,
    clip_tag_threshold:  parseFloat(el('s-clip-thresh').value) || null,
    max_inference_dim:   parseInt(el('s-max-dim').value) || null,
  };
  try {
    const r = await apiPost('/settings', body);
    alert(r.note || 'Saved.');
  } catch (e) {
    alert(`Save failed: ${e.message}`);
  }
});

// ── Escape HTML ───────────────────────────────────────────────────────────────

function escHtml(str) {
  if (!str) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// ── Init ──────────────────────────────────────────────────────────────────────

(async () => {
  await refreshStatus();
  await initPhotoFilters();
  scheduleAutoRefresh();
})();
