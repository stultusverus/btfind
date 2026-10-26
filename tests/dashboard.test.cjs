const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const { test } = require('node:test');
const vm = require('node:vm');

function dashboard() {
    const elements = new Map();
    function element() {
        return { value: '', textContent: '', innerHTML: '', hidden: false, disabled: false,
            setAttribute() {}, getAttribute() { return 'false'; }, addEventListener() {},
            querySelector() { return element(); }, querySelectorAll() { return []; },
            classList: {add() {}, remove() {}, toggle() {}},
        };
    }
    const context = vm.createContext({
        document: { getElementById(id) {
            if (!elements.has(id)) elements.set(id, element());
            return elements.get(id);
        }, querySelectorAll() { return []; } },
        window: { location: {search: '', pathname: '/'}, addEventListener() {}, setInterval() {} },
        navigator: {}, URLSearchParams, AbortController, console, setTimeout, clearTimeout,
        fetch: async () => { throw new Error('unexpected fetch'); },
    });
    const source = readFileSync('src/web_dashboard.js', 'utf8');
    vm.runInContext(source.slice(0, source.lastIndexOf('\nloadControlsFromUrl();')), context);
    vm.runInContext('loadControlsFromUrl()', context);
    return { context, elements, run: code => vm.runInContext(code, context) };
}

test('search renders returned results without an exact total', async () => {
    const d = dashboard();
    d.context.fetch = async () => ({ok: true, json: async () => ({
        results: [{ info_hash: 'a'.repeat(40), name: '東京 中文', total_size: 1,
            file_count: 1, first_seen: 1, last_seen: 1, magnet: 'magnet:test' }],
        limit: 50, offset: 0, has_next: true, effective_sort: 'rank',
    })});
    await d.run('search()');
    assert.equal(d.run('state.results.length'), 1);
    assert.match(d.elements.get('results').innerHTML, /東京 中文/);
    assert.equal(d.elements.get('pager-status').textContent, 'Showing 1–1');
    assert.equal(d.elements.get('next-page').disabled, false);
    assert.doesNotMatch(d.elements.get('status').textContent, /NaN|undefined/);
});

test('empty later pages still allow going back', () => {
    const d = dashboard();
    d.run('state.offset=50; state.results=[]; state.hasNext=false; renderPager()');
    assert.equal(d.elements.get('pager').hidden, false);
    assert.equal(d.elements.get('prev-page').disabled, false);
    assert.equal(d.elements.get('next-page').disabled, true);
});

test('two-character CJK queries enable relevance sorting', () => {
    const d = dashboard();
    d.elements.get('query').value = '東京';
    d.run('updateSortAvailability(true)');
    assert.equal(d.elements.get('sort').value, 'rank');
});
