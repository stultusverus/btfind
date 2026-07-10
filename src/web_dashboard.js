const SEARCH_LIMITS = new Set([25, 50, 100, 250]);
const SEARCH_SORTS = new Set(['rank', 'last_seen', 'first_seen', 'total_size', 'name']);
const FILE_LIMIT = 100;

const state = {
    results: [],
    expandedHash: null,
    details: new Map(),
    total: 0,
    statsTotal: null,
    limit: 50,
    offset: 0,
    hasNext: false,
    effectiveSort: 'last_seen',
    searchController: null,
    searchBusy: false,
    rankEligible: false,
};

const els = {
    stats: document.getElementById('stats'),
    form: document.getElementById('search-form'),
    query: document.getElementById('query'),
    sort: document.getElementById('sort'),
    limit: document.getElementById('limit'),
    minSize: document.getElementById('min-size'),
    maxSize: document.getElementById('max-size'),
    filterToggle: document.getElementById('filter-toggle'),
    sizeFilters: document.getElementById('size-filters'),
    clearFilters: document.getElementById('clear-filters'),
    filterChips: document.getElementById('filter-chips'),
    searchButton: document.getElementById('search-button'),
    status: document.getElementById('status'),
    pager: document.getElementById('pager'),
    pagerStatus: document.getElementById('pager-status'),
    prevPage: document.getElementById('prev-page'),
    nextPage: document.getElementById('next-page'),
    results: document.getElementById('results'),
    refresh: document.getElementById('refresh'),
    toast: document.getElementById('toast'),
};

let toastTimer = null;

async function loadStats() {
    els.stats.setAttribute('aria-busy', 'true');
    try {
        const resp = await fetch('/api/stats');
        if (!resp.ok) throw new Error(await errorMessage(resp));
        const data = await resp.json();
        state.statsTotal = data.total_torrents;
        els.stats.innerHTML = [
            statCard('Torrents', formatNumber(data.total_torrents)),
            statCard('With metadata', formatNumber(data.total_metadata_complete)),
            statCard('Files', formatNumber(data.total_files)),
            statCard('Total size', formatSize(data.total_size)),
        ].join('');
        if (state.total === 0 && state.results.length === 0 && !state.searchBusy) renderResults();
    } catch (error) {
        els.stats.innerHTML = statCard('Statistics', 'Unavailable');
        console.error(error);
    } finally {
        els.stats.setAttribute('aria-busy', 'false');
    }
}

function statCard(label, value) {
    return `<div class="stat-card"><div class="label">${escapeHtml(label)}</div><div class="value">${escapeHtml(value)}</div></div>`;
}

function loadControlsFromUrl() {
    const params = new URLSearchParams(window.location.search);
    const query = params.get('q') || '';
    const rankEligible = Array.from(query.trim()).length >= 3;
    const requestedSort = params.get('sort');
    const sort = SEARCH_SORTS.has(requestedSort) ? requestedSort : (rankEligible ? 'rank' : 'last_seen');
    const limit = Number(params.get('limit'));
    const offset = readNonNegativeInteger(params.get('offset'), 0);
    const minSize = readNonNegativeInteger(params.get('min_size'), null);
    const maxSize = readNonNegativeInteger(params.get('max_size'), null);

    els.query.value = query;
    els.sort.value = sort === 'rank' && !rankEligible ? 'last_seen' : sort;
    els.limit.value = String(SEARCH_LIMITS.has(limit) ? limit : 50);
    els.minSize.value = minSize === null ? '' : formatSize(minSize);
    els.maxSize.value = maxSize === null ? '' : formatSize(maxSize);
    state.offset = offset;
    state.rankEligible = rankEligible;
    updateSortAvailability(false);
    renderFilterChips();
    if (minSize !== null || maxSize !== null) setFiltersOpen(true);
}

function readNonNegativeInteger(value, fallback) {
    if (value === null || !/^\d+$/.test(value)) return fallback;
    const parsed = Number(value);
    return Number.isSafeInteger(parsed) ? parsed : fallback;
}

function updateSortAvailability(autoSelect) {
    const eligible = Array.from(els.query.value.trim()).length >= 3;
    const rankOption = els.sort.querySelector('option[value="rank"]');
    rankOption.disabled = !eligible;
    if (!eligible && els.sort.value === 'rank') els.sort.value = 'last_seen';
    if (autoSelect && eligible && !state.rankEligible) els.sort.value = 'rank';
    state.rankEligible = eligible;
}

function buildSearchRequest() {
    const query = els.query.value.trim();
    const minSize = parseSizeInput(els.minSize.value.trim(), 'minimum size');
    const maxSize = parseSizeInput(els.maxSize.value.trim(), 'maximum size');
    if (minSize !== null && maxSize !== null && maxSize < minSize) {
        throw new Error('Maximum size must be greater than or equal to minimum size.');
    }

    updateSortAvailability(false);
    const sort = els.sort.value;
    const limit = Number(els.limit.value);
    const api = new URLSearchParams();
    if (query) api.set('q', query);
    api.set('sort', sort);
    api.set('limit', String(limit));
    api.set('offset', String(state.offset));
    api.set('complete_only', 'true');
    if (minSize !== null) api.set('min_size', String(minSize));
    if (maxSize !== null) api.set('max_size', String(maxSize));
    return { api, query, sort, limit, minSize, maxSize };
}

function syncUrl(request, mode) {
    const params = new URLSearchParams();
    if (request.query) params.set('q', request.query);
    params.set('sort', request.sort);
    params.set('limit', String(request.limit));
    if (state.offset > 0) params.set('offset', String(state.offset));
    if (request.minSize !== null) params.set('min_size', String(request.minSize));
    if (request.maxSize !== null) params.set('max_size', String(request.maxSize));
    const url = `${window.location.pathname}?${params}`;
    window.history[mode === 'push' ? 'pushState' : 'replaceState']({}, '', url);
}

async function search(options = {}) {
    const { history = 'none', recoverPage = true } = options;
    let request;
    try {
        request = buildSearchRequest();
    } catch (error) {
        showError(error.message);
        renderFilterChips();
        return;
    }

    if (history !== 'none') syncUrl(request, history);
    renderFilterChips(request);
    if (state.searchController) state.searchController.abort();
    const controller = new AbortController();
    state.searchController = controller;
    setSearchBusy(true);
    clearStatus();
    if (state.results.length === 0) renderEmpty('Searching…');

    try {
        const resp = await fetch(`/api/search?${request.api}`, { signal: controller.signal });
        if (!resp.ok) throw new Error(await errorMessage(resp));
        const data = await resp.json();
        if (controller !== state.searchController) return;

        if (recoverPage && data.results.length === 0 && data.total > 0 && state.offset > 0) {
            state.offset = Math.floor((data.total - 1) / data.limit) * data.limit;
            return search({ history: 'replace', recoverPage: false });
        }

        state.results = data.results;
        state.total = data.total;
        state.limit = data.limit;
        state.offset = data.offset;
        state.hasNext = data.has_next;
        state.effectiveSort = data.effective_sort;
        if (!state.results.some(result => result.info_hash === state.expandedHash)) state.expandedHash = null;
        renderResults();
        renderPager();
        setStatus(resultStatus());
    } catch (error) {
        if (error.name === 'AbortError') return;
        showError(`Search failed: ${error.message}`);
        if (state.results.length === 0) renderEmpty('The search could not be loaded.');
    } finally {
        if (controller === state.searchController) setSearchBusy(false);
    }
}

function setSearchBusy(busy) {
    state.searchBusy = busy;
    els.results.setAttribute('aria-busy', String(busy));
    els.searchButton.disabled = busy;
    els.refresh.disabled = busy;
    renderPager();
}

function parseSizeInput(value, label) {
    if (!value) return null;
    const match = value.match(/^(\d+(?:\.\d+)?)\s*(b|kb|mb|gb|tb)?$/i);
    if (!match) throw new Error(`Use a number with an optional unit for ${label}, such as 700 MB or 4 GB.`);
    const amount = Number(match[1]);
    const unit = (match[2] || 'b').toLowerCase();
    const multipliers = { b: 1, kb: 1024, mb: 1024 ** 2, gb: 1024 ** 3, tb: 1024 ** 4 };
    const bytes = Math.round(amount * multipliers[unit]);
    if (!Number.isSafeInteger(bytes)) throw new Error(`${label} is too large.`);
    return bytes;
}

function renderFilterChips(request = null) {
    let minSize;
    let maxSize;
    try {
        minSize = request ? request.minSize : parseSizeInput(els.minSize.value.trim(), 'minimum size');
        maxSize = request ? request.maxSize : parseSizeInput(els.maxSize.value.trim(), 'maximum size');
    } catch (_) {
        els.filterChips.innerHTML = '';
        return;
    }
    const chips = [];
    if (minSize !== null) chips.push(filterChip('min', `At least ${formatSize(minSize)}`));
    if (maxSize !== null) chips.push(filterChip('max', `At most ${formatSize(maxSize)}`));
    els.filterChips.innerHTML = chips.join('');
}

function filterChip(name, label) {
    return `<button class="chip" type="button" data-clear-filter="${name}" aria-label="Remove ${escapeAttr(label)} filter">${escapeHtml(label)} ×</button>`;
}

function renderResults() {
    if (state.results.length === 0) {
        if (state.statsTotal === 0) renderEmpty('The database does not contain any torrents yet.');
        else if (state.total === 0) renderEmpty('No torrents match this search.');
        else renderEmpty('No results to display.');
        return;
    }
    els.results.innerHTML = state.results.map(resultCard).join('');
    updateRelativeTimes();
}

function resultCard(torrent) {
    const expanded = torrent.info_hash === state.expandedHash;
    const detailId = `detail-${torrent.info_hash}`;
    return `<article class="torrent-card${expanded ? ' expanded' : ''}" data-hash="${escapeAttr(torrent.info_hash)}">
        <div class="torrent-summary">
            <button class="torrent-title" type="button" data-expand="${escapeAttr(torrent.info_hash)}" aria-expanded="${expanded}" aria-controls="${detailId}">
                <span class="chevron" aria-hidden="true">›</span>
                <span class="title-text"><span class="name">${escapeHtml(torrent.name || '(unknown)')}</span><span class="hash">${escapeHtml(torrent.info_hash)}</span></span>
            </button>
            <div class="cell"><span class="mobile-label">Size</span>${escapeHtml(formatSize(torrent.total_size))}</div>
            <div class="cell"><span class="mobile-label">Files</span>${escapeHtml(formatNumber(torrent.file_count))}</div>
            <div class="cell"><span class="mobile-label">Last seen</span>${timeElement(torrent.last_seen)}</div>
            <div class="actions">
                <button class="mini primary-action" type="button" data-copy="${escapeAttr(torrent.magnet)}" data-copy-label="Magnet link">Copy magnet</button>
                <a class="mini" href="${escapeAttr(torrent.magnet)}">Open magnet</a>
            </div>
        </div>
        ${expanded ? detailPanel(torrent.info_hash, detailId) : ''}
    </article>`;
}

function detailPanel(infoHash, detailId) {
    const detail = state.details.get(infoHash);
    if (!detail || detail.loading) {
        return `<section class="detail" id="${detailId}" aria-label="Torrent details"><div class="status">Loading torrent details…</div></section>`;
    }
    if (detail.error) {
        return `<section class="detail" id="${detailId}" aria-label="Torrent details"><div class="status error">${escapeHtml(detail.error)}</div><button class="mini" type="button" data-detail-retry="${escapeAttr(infoHash)}">Retry</button></section>`;
    }
    const summary = detail.summary;
    return `<section class="detail" id="${detailId}" aria-label="Torrent details">
        <div class="detail-grid">
            <div>First seen<br>${timeElement(summary.first_seen)}</div>
            <div>Last seen<br>${timeElement(summary.last_seen)}</div>
            <div>Total size<br>${escapeHtml(formatSize(summary.total_size))}</div>
            <div>Files<br>${escapeHtml(formatNumber(summary.file_count))}</div>
        </div>
        <div class="actions detail-actions">
            <button class="mini" type="button" data-copy="${escapeAttr(summary.info_hash)}" data-copy-label="Info hash">Copy hash</button>
        </div>
        ${filePanel(infoHash, detail)}
    </section>`;
}

function filePanel(infoHash, detail) {
    const files = detail.files || [];
    const list = files.length > 0
        ? files.map(fileRow).join('')
        : `<div class="empty">${detail.filesLoading ? 'Loading files…' : (detail.fileQuery ? 'No file paths match this filter.' : 'No files indexed.')}</div>`;
    const error = detail.filesError
        ? `<div class="status error">${escapeHtml(detail.filesError)}</div><button class="mini" type="button" data-files-retry="${escapeAttr(infoHash)}">Retry</button>`
        : '';
    const shown = Math.min(files.length, detail.fileTotal || 0);
    return `<form class="file-search" data-file-search="${escapeAttr(infoHash)}">
            <label class="visually-hidden" for="file-query-${escapeAttr(infoHash)}">Filter file paths</label>
            <div class="file-toolbar">
                <input id="file-query-${escapeAttr(infoHash)}" type="search" value="${escapeAttr(detail.fileQuery || '')}" placeholder="Filter file paths" autocomplete="off">
                <button class="mini" type="submit">Filter files</button>
            </div>
        </form>
        ${error}
        <div class="file-list" aria-busy="${detail.filesLoading}">${list}</div>
        <div class="file-footer">
            <span>Showing ${escapeHtml(formatNumber(shown))} of ${escapeHtml(formatNumber(detail.fileTotal || 0))} files</span>
            ${detail.hasMoreFiles ? `<button class="mini" type="button" data-files-more="${escapeAttr(infoHash)}"${detail.filesLoading ? ' disabled' : ''}>Show more</button>` : ''}
        </div>`;
}

function fileRow(file) {
    return `<div class="file-row"><div class="file-path">${escapeHtml(file.path)}</div><div class="cell">${escapeHtml(formatSize(file.size))}</div></div>`;
}

async function loadDetail(infoHash, force = false) {
    const existing = state.details.get(infoHash);
    if (existing && !force && (existing.loading || existing.summary)) return;
    if (existing && existing.fileController) existing.fileController.abort();
    const detail = { loading: true, error: null, summary: null, files: [], fileTotal: 0, fileOffset: 0, fileQuery: '', filesLoading: false, filesError: null, hasMoreFiles: false, fileController: null };
    state.details.set(infoHash, detail);
    renderResults();
    try {
        const resp = await fetch(`/api/torrents/${encodeURIComponent(infoHash)}`);
        if (!resp.ok) throw new Error(await errorMessage(resp));
        detail.summary = await resp.json();
        detail.loading = false;
        if (state.expandedHash === infoHash) renderResults();
        await loadFiles(infoHash, true);
    } catch (error) {
        detail.loading = false;
        detail.error = `Could not load torrent details: ${error.message}`;
        if (state.expandedHash === infoHash) renderResults();
    }
}

async function loadFiles(infoHash, reset) {
    const detail = state.details.get(infoHash);
    if (!detail || !detail.summary) return;
    if (detail.fileController) detail.fileController.abort();
    const controller = new AbortController();
    detail.fileController = controller;
    if (reset) {
        detail.files = [];
        detail.fileOffset = 0;
        detail.fileTotal = 0;
        detail.hasMoreFiles = false;
    }
    detail.filesLoading = true;
    detail.filesError = null;
    if (state.expandedHash === infoHash) renderResults();

    const params = new URLSearchParams({ limit: String(FILE_LIMIT), offset: String(detail.fileOffset) });
    if (detail.fileQuery) params.set('q', detail.fileQuery);
    try {
        const resp = await fetch(`/api/torrents/${encodeURIComponent(infoHash)}/files?${params}`, { signal: controller.signal });
        if (!resp.ok) throw new Error(await errorMessage(resp));
        const data = await resp.json();
        if (controller !== detail.fileController) return;
        detail.files = reset ? data.files : detail.files.concat(data.files);
        detail.fileTotal = data.total;
        detail.fileOffset = data.offset + data.files.length;
        detail.hasMoreFiles = data.has_next;
    } catch (error) {
        if (error.name === 'AbortError') return;
        detail.filesError = `Could not load files: ${error.message}`;
    } finally {
        if (controller === detail.fileController) {
            detail.filesLoading = false;
            if (state.expandedHash === infoHash) renderResults();
        }
    }
}

function renderPager() {
    if (state.total === 0) {
        els.pager.hidden = true;
        return;
    }
    const start = state.offset + 1;
    const end = state.offset + state.results.length;
    els.pager.hidden = false;
    els.pagerStatus.textContent = `Showing ${formatNumber(start)}–${formatNumber(end)} of ${formatNumber(state.total)}`;
    els.prevPage.disabled = state.searchBusy || state.offset === 0;
    els.nextPage.disabled = state.searchBusy || !state.hasNext;
}

function resultStatus() {
    if (state.total === 0) return 'No complete torrents found.';
    const currentPage = Math.floor(state.offset / state.limit) + 1;
    const pageCount = Math.max(1, Math.ceil(state.total / state.limit));
    const labels = { rank: 'relevance', last_seen: 'recently seen', first_seen: 'recently added', total_size: 'largest', name: 'name' };
    return `Page ${formatNumber(currentPage)} of ${formatNumber(pageCount)}, sorted by ${labels[state.effectiveSort] || state.effectiveSort}.`;
}

function renderEmpty(message) {
    els.results.innerHTML = `<div class="empty">${escapeHtml(message)}</div>`;
}

async function errorMessage(resp) {
    try {
        const data = await resp.json();
        return data.error || resp.statusText;
    } catch (_) {
        return resp.statusText;
    }
}

function setStatus(message) {
    els.status.className = 'status';
    els.status.textContent = message;
}

function showError(message) {
    els.status.className = 'status error';
    els.status.textContent = message;
}

function clearStatus() {
    els.status.className = 'status';
    els.status.textContent = '';
}

async function copyText(value, label) {
    try {
        await navigator.clipboard.writeText(value);
        showToast(`${label} copied.`);
    } catch (error) {
        showError(`Copy failed: ${error.message}`);
    }
}

function showToast(message) {
    if (toastTimer) window.clearTimeout(toastTimer);
    els.toast.textContent = message;
    els.toast.hidden = false;
    toastTimer = window.setTimeout(() => {
        els.toast.hidden = true;
    }, 1800);
}

function setFiltersOpen(open) {
    els.filterToggle.setAttribute('aria-expanded', String(open));
    els.sizeFilters.classList.toggle('open', open);
}

function formatSize(bytes) {
    if (!Number.isFinite(bytes) || bytes <= 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let size = bytes;
    let unit = 0;
    while (size >= 1024 && unit < units.length - 1) {
        size /= 1024;
        unit += 1;
    }
    return unit === 0 ? `${bytes} B` : `${Number(size.toFixed(1))} ${units[unit]}`;
}

function timeElement(seconds) {
    if (!seconds) return 'Unknown';
    const date = new Date(seconds * 1000);
    return `<time datetime="${date.toISOString()}" title="${escapeAttr(date.toLocaleString('en-US'))}" data-relative-time="${seconds}">${escapeHtml(formatRelativeTime(seconds))}</time>`;
}

function formatRelativeTime(seconds) {
    if (!seconds) return 'Unknown';
    const delta = seconds - Math.floor(Date.now() / 1000);
    const absolute = Math.abs(delta);
    let divisor = 1;
    let unit = 'second';
    if (absolute >= 86400) {
        divisor = 86400;
        unit = 'day';
    } else if (absolute >= 3600) {
        divisor = 3600;
        unit = 'hour';
    } else if (absolute >= 60) {
        divisor = 60;
        unit = 'minute';
    }
    return new Intl.RelativeTimeFormat('en', { numeric: 'auto' }).format(Math.round(delta / divisor), unit);
}

function updateRelativeTimes() {
    document.querySelectorAll('[data-relative-time]').forEach(element => {
        element.textContent = formatRelativeTime(Number(element.dataset.relativeTime));
    });
}

function formatNumber(value) {
    return new Intl.NumberFormat('en-US').format(value || 0);
}

function escapeHtml(value) {
    return String(value)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function escapeAttr(value) {
    return escapeHtml(value);
}

els.form.addEventListener('submit', event => {
    event.preventDefault();
    state.offset = 0;
    state.expandedHash = null;
    search({ history: 'push' });
});

els.query.addEventListener('input', () => updateSortAvailability(true));

els.refresh.addEventListener('click', () => {
    state.details.forEach(detail => {
        if (detail.fileController) detail.fileController.abort();
    });
    state.details.clear();
    state.expandedHash = null;
    loadStats();
    search({ history: 'replace' });
});

els.clearFilters.addEventListener('click', () => {
    els.query.value = '';
    els.minSize.value = '';
    els.maxSize.value = '';
    els.sort.value = 'last_seen';
    state.rankEligible = false;
    state.offset = 0;
    state.expandedHash = null;
    updateSortAvailability(false);
    setFiltersOpen(false);
    search({ history: 'push' });
});

els.filterToggle.addEventListener('click', () => {
    setFiltersOpen(els.filterToggle.getAttribute('aria-expanded') !== 'true');
});

els.filterChips.addEventListener('click', event => {
    const button = event.target.closest('[data-clear-filter]');
    if (!button) return;
    if (button.dataset.clearFilter === 'min') els.minSize.value = '';
    if (button.dataset.clearFilter === 'max') els.maxSize.value = '';
    state.offset = 0;
    search({ history: 'push' });
});

els.prevPage.addEventListener('click', () => {
    state.offset = Math.max(0, state.offset - state.limit);
    state.expandedHash = null;
    search({ history: 'push' });
});

els.nextPage.addEventListener('click', () => {
    if (!state.hasNext) return;
    state.offset += state.limit;
    state.expandedHash = null;
    search({ history: 'push' });
});

els.results.addEventListener('click', event => {
    const copyButton = event.target.closest('[data-copy]');
    if (copyButton) {
        copyText(copyButton.dataset.copy, copyButton.dataset.copyLabel || 'Value');
        return;
    }
    const expandButton = event.target.closest('[data-expand]');
    if (expandButton) {
        const infoHash = expandButton.dataset.expand;
        state.expandedHash = state.expandedHash === infoHash ? null : infoHash;
        renderResults();
        if (state.expandedHash) loadDetail(infoHash);
        return;
    }
    const detailRetry = event.target.closest('[data-detail-retry]');
    if (detailRetry) {
        loadDetail(detailRetry.dataset.detailRetry, true);
        return;
    }
    const filesRetry = event.target.closest('[data-files-retry]');
    if (filesRetry) {
        loadFiles(filesRetry.dataset.filesRetry, false);
        return;
    }
    const filesMore = event.target.closest('[data-files-more]');
    if (filesMore) loadFiles(filesMore.dataset.filesMore, false);
});

els.results.addEventListener('submit', event => {
    const form = event.target.closest('[data-file-search]');
    if (!form) return;
    event.preventDefault();
    const infoHash = form.dataset.fileSearch;
    const detail = state.details.get(infoHash);
    if (!detail) return;
    detail.fileQuery = form.querySelector('input').value.trim();
    loadFiles(infoHash, true);
});

window.addEventListener('popstate', () => {
    if (state.searchController) state.searchController.abort();
    state.expandedHash = null;
    loadControlsFromUrl();
    search();
});

loadControlsFromUrl();
loadStats();
search({ history: 'replace' });
window.setInterval(updateRelativeTimes, 60_000);
