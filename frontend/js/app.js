/* InstaBot — app logic */
'use strict';

// ═══════════════════════════════════════════════════════
//  API helpers
// ═══════════════════════════════════════════════════════
const api = async (method, path, body) => {
  const opts = { method, headers: { 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const r = await fetch('/api' + path, opts);
  if (!r.ok) {
    const err = await r.json().catch(() => ({ detail: r.statusText }));
    throw new Error(err.detail || r.statusText);
  }
  return r.json();
};

// ═══════════════════════════════════════════════════════
//  State
// ═══════════════════════════════════════════════════════
let accounts = [];
let posts    = [];
let running  = false;
let _accHash = '';   // для пропуска лишних перерисовок

// ═══════════════════════════════════════════════════════
//  Accounts
// ═══════════════════════════════════════════════════════
async function loadAccounts() {
  accounts = await api('GET', '/accounts');
  renderAccounts();
  updateSummary();
}

async function addAccount() {
  const name = qs('#accName').value.trim();
  const file = qs('#accFile').value.trim();
  if (!name || !file) return toast('Введи имя и путь к кукам', 'warn');
  try {
    await api('POST', '/accounts', { name, cookies_file: file });
    qs('#accName').value = '';
    qs('#accFile').value = '';
    await loadAccounts();
    appendLog('system', `Аккаунт добавлен: ${name}`, 'info');
  } catch (e) { toast(e.message, 'error'); }
}

async function removeAccount(name) {
  await api('DELETE', `/accounts/${encodeURIComponent(name)}`);
  await loadAccounts();
  appendLog('system', `Аккаунт удалён: ${name}`, 'warn');
}

async function toggleAccount(name) {
  await api('PATCH', `/accounts/${encodeURIComponent(name)}/toggle`);
  await loadAccounts();
}

function renderAccounts(force) {
  const hash = JSON.stringify(accounts);
  if (!force && hash === _accHash) return;  // данные не изменились — пропускаем
  _accHash = hash;

  const el = qs('#accList');
  qs('#accBadge').textContent = accounts.length;
  if (!accounts.length) {
    el.innerHTML = '<div class="empty"><div class="ei">👤</div>Нет аккаунтов</div>';
    return;
  }
  el.innerHTML = accounts.map(a => `
    <div class="acc-row ${a.enabled ? '' : 'disabled'}" style="opacity:${a.enabled?1:.5}">
      <div class="acc-info">
        <div class="acc-name">${esc(a.name)}</div>
        <div class="acc-file">${esc(a.cookies_file)}</div>
      </div>
      <span class="status-tag tag-${a.status||'idle'}">${statusLabel(a.status)}</span>
      <button class="toggle ${a.enabled?'on':''}" onclick="toggleAccount('${esc(a.name)}')"></button>
      <button class="btn-del" onclick="removeAccount('${esc(a.name)}')">✕</button>
    </div>`).join('');
}

// ═══════════════════════════════════════════════════════
//  Posts
// ═══════════════════════════════════════════════════════
async function loadPosts() {
  posts = await api('GET', '/posts');
  renderPosts();
  updateSummary();
}

async function addPost() {
  const url = qs('#postUrl').value.trim();
  if (!url) return toast('Введи ссылку', 'warn');
  try {
    await api('POST', '/posts', { url });
    qs('#postUrl').value = '';
    await loadPosts();
    const sc = url.match(/\/p\/([A-Za-z0-9_-]+)/)?.[1] || url;
    appendLog('system', `Пост добавлен: /p/${sc}`, 'info');
  } catch (e) { toast(e.message, 'error'); }
}

async function removePost(idx) {
  await api('DELETE', `/posts/${idx}`);
  await loadPosts();
}

function renderPosts() {
  const el = qs('#postList');
  qs('#postBadge').textContent = posts.length;
  if (!posts.length) {
    el.innerHTML = '<div class="empty"><div class="ei">🔗</div>Нет постов</div>';
    return;
  }
  el.innerHTML = posts.map((p, i) => `
    <div class="post-row">
      <span class="post-sc">/p/${esc(p.shortcode)}</span>
      <span class="post-url">${esc(p.url)}</span>
      <button class="btn-del" onclick="removePost(${i})">✕</button>
    </div>`).join('');
}

// ═══════════════════════════════════════════════════════
//  Run / Stop
// ═══════════════════════════════════════════════════════
async function toggleRun() {
  if (running) {
    await api('POST', '/stop');
    return;
  }
  try {
    await api('POST', '/run', {
      model:            qs('#sModel').value,
      max_parallel:    +qs('#sParallel').value,
      ollama_parallel: +qs('#sOllamaParallel').value,
      delay_min:       +qs('#sDelayMin').value,
      delay_max:   +qs('#sDelayMax').value,
      retries:     +qs('#sRetries').value,
      retry_delay: +qs('#sRetryDelay').value,
      mode:         qs('#sMode').value,
    });
    setRunning(true);
  } catch (e) { toast(e.message, 'error'); }
}

function setRunning(v) {
  running = v;
  const btn = qs('#runBtn');
  const dot = qs('#sdot');
  if (v) {
    btn.textContent = '⏹ Остановить';
    btn.classList.add('running');
    dot.classList.add('active'); dot.classList.remove('error');
    qs('#stext').textContent = 'Работает';
  } else {
    btn.textContent = '▶ Запустить';
    btn.classList.remove('running');
    dot.classList.remove('active');
    qs('#stext').textContent = 'Готово';
  }
}

// ═══════════════════════════════════════════════════════
//  Progress polling
// ═══════════════════════════════════════════════════════
let _pollTimer = null;

function startPolling() {
  _pollTimer = setInterval(async () => {
    try {
      const s = await api('GET', '/state');
      // Update account statuses
      accounts = s.accounts;
      renderAccounts();
      // Progress
      const done  = s.progress_done;
      const total = s.progress_total || 1;
      const pct   = Math.round(done / total * 100);
      qs('#progFill').style.width = pct + '%';
      qs('#progLbl').textContent  = total > 0 ? `${done} / ${total}` : '';
      // Detect finish
      if (!s.running && running) {
        setRunning(false);
        stopPolling();
        let totalOk = 0, totalErr = 0;
        Object.values(s.results).forEach(r => { totalOk += r.ok; totalErr += r.err; });
        notifyDone(totalOk, totalErr);
        showReport(s.results);
      }
    } catch {}
  }, 800);
}

function stopPolling() {
  if (_pollTimer) { clearInterval(_pollTimer); _pollTimer = null; }
}

// ═══════════════════════════════════════════════════════
//  SSE log stream
// ═══════════════════════════════════════════════════════
let _sse = null;

function connectSSE() {
  if (_sse) _sse.close();
  _sse = new EventSource('/api/logs/stream');
  _sse.onmessage = (e) => {
    try {
      const entry = JSON.parse(e.data);
      appendLog(entry.acc, entry.msg, entry.level);
      // If bot just started running — start polling
      if (!running && entry.acc === 'system' && entry.level === 'info') {
        setRunning(true);
        startPolling();
      }
    } catch {}
  };
  _sse.onerror = () => {
    // Reconnect after 3s
    setTimeout(connectSSE, 3000);
  };
}

// ═══════════════════════════════════════════════════════
//  Log
// ═══════════════════════════════════════════════════════
function appendLog(acc, msg, level = '') {
  const term = qs('#logTerm');
  const empty = term.querySelector('.log-empty');
  if (empty) empty.remove();
  const t = new Date().toLocaleTimeString('ru', {hour:'2-digit',minute:'2-digit',second:'2-digit'});
  const line = document.createElement('div');
  line.className = 'log-line';
  line.innerHTML = `<span class="lt">${t}</span><span class="la">${esc(acc)}</span><span class="lm ${level}">${esc(msg)}</span>`;
  term.appendChild(line);
  term.scrollTop = term.scrollHeight;
}

function clearLog() {
  qs('#logTerm').innerHTML = '<div class="log-empty">Лог очищен</div>';
}

async function downloadLogs() {
  const lines = [...qs('#logTerm').querySelectorAll('.log-line')].map(el => {
    const t = el.querySelector('.lt')?.textContent || '';
    const a = el.querySelector('.la')?.textContent || '';
    const m = el.querySelector('.lm')?.textContent || '';
    return `${t} [${a}] ${m}`;
  });
  if (!lines.length) return toast('Лог пуст', 'warn');
  const content  = lines.join('\n');
  const ts       = new Date().toISOString().slice(0, 19).replace(/:/g, '-');
  const filename = `instabot_${ts}.txt`;

  if (window.showSaveFilePicker) {
    try {
      const handle   = await window.showSaveFilePicker({
        suggestedName: filename,
        types: [{ description: 'Text file', accept: { 'text/plain': ['.txt', '.log'] } }],
      });
      const writable = await handle.createWritable();
      await writable.write(content);
      await writable.close();
      toast('Лог сохранён', 'ok');
      return;
    } catch (e) {
      if (e.name === 'AbortError') return; // пользователь отменил
    }
  }
  // Fallback для браузеров без showSaveFilePicker
  const blob = new Blob([content], { type: 'text/plain;charset=utf-8' });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a');
  a.href = url; a.download = filename; a.click();
  URL.revokeObjectURL(url);
}

// ═══════════════════════════════════════════════════════
//  Report
// ═══════════════════════════════════════════════════════
function showReport(results) {
  let totalOk = 0, totalErr = 0, totalAll = 0;
  const rows = Object.entries(results).map(([name, r]) => {
    totalOk  += r.ok;  totalErr += r.err; totalAll += r.total;
    return `<tr>
      <td>${esc(name)}</td>
      <td>${r.total}</td>
      <td class="c-ok">${r.ok}</td>
      <td class="c-err ${r.err ? '' : ''}">${r.err}</td>
    </tr>`;
  }).join('');
  qs('#reportBody').innerHTML = rows + `
    <tr class="total-row">
      <td>ИТОГО</td><td>${totalAll}</td>
      <td class="c-ok">${totalOk}</td>
      <td class="c-err">${totalErr}</td>
    </tr>`;
  qs('#overlay').classList.add('show');
}

function closeReport() {
  qs('#overlay').classList.remove('show');
}

// ═══════════════════════════════════════════════════════
//  Helpers
// ═══════════════════════════════════════════════════════
function updateSummary() {
  const en    = accounts.filter(a => a.enabled).length;
  const ps    = posts.length;
  const mode  = qs('#sMode').value;
  const tasks = mode === 'all' ? en * ps : ps;
  qs('#summary').innerHTML =
    `<strong>${en}</strong> акк. · <strong>${ps}</strong> постов · <strong>${tasks}</strong> задач`;
}

function statusLabel(s) {
  return {idle:'Ожидание', ok:'Готово', error:'Ошибка', running:'Работает'}[s] || s;
}

function esc(s) {
  return String(s)
    .replace(/&/g,'&amp;').replace(/</g,'&lt;')
    .replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function qs(sel) { return document.querySelector(sel); }

// ── Browse file (работает только в pywebview через window.pywebview_bridge) ──
function browseFile() {
  // В pywebview можно вызвать Python через js_api.
  // Если не в pywebview — просто подсвечиваем поле.
  if (window.pywebview) {
    window.pywebview.api.pick_file().then(p => {
      if (p) qs('#accFile').value = p;
    }).catch(() => {});
  } else {
    qs('#accFile').focus();
    toast('В браузере — введи путь вручную', 'warn');
  }
}

// ── Simple toast ─────────────────────────────────────────────────────
function toast(msg, level = 'info') {
  appendLog('UI', msg, level);
}

// ═══════════════════════════════════════════════════════
//  Enter key support
// ═══════════════════════════════════════════════════════
document.addEventListener('keydown', e => {
  if (e.key === 'Enter') {
    if (document.activeElement.id === 'postUrl')   addPost();
    if (document.activeElement.id === 'accFile')   addAccount();
    if (document.activeElement.id === 'accName')   qs('#accFile').focus();
  }
});

// ═══════════════════════════════════════════════════════
//  Settings persistence
// ═══════════════════════════════════════════════════════
async function saveSettings() {
  try {
    await api('PUT', '/settings', {
      model:           qs('#sModel').value,
      mode:            qs('#sMode').value,
      max_parallel:   +qs('#sParallel').value,
      ollama_parallel:+qs('#sOllamaParallel').value,
      delay_min:      +qs('#sDelayMin').value,
      delay_max:      +qs('#sDelayMax').value,
      retries:        +qs('#sRetries').value,
      retry_delay:    +qs('#sRetryDelay').value,
    });
    toast('Настройки сохранены', 'ok');
  } catch (e) { toast(e.message, 'error'); }
}

async function loadSettings() {
  try {
    const s = await api('GET', '/settings');
    if (!s || !Object.keys(s).length) return;
    if (s.model)        qs('#sModel').value      = s.model;
    if (s.mode)             qs('#sMode').value           = s.mode;
    if (s.max_parallel)     qs('#sParallel').value       = s.max_parallel;
    if (s.ollama_parallel)  qs('#sOllamaParallel').value = s.ollama_parallel;
    if (s.delay_min)    qs('#sDelayMin').value    = s.delay_min;
    if (s.delay_max)    qs('#sDelayMax').value    = s.delay_max;
    if (s.retries)      qs('#sRetries').value     = s.retries;
    if (s.retry_delay)  qs('#sRetryDelay').value  = s.retry_delay;
  } catch {}
}

// ═══════════════════════════════════════════════════════
//  Notification
// ═══════════════════════════════════════════════════════
function notifyDone(totalOk, totalErr) {
  // Browser notification
  if ('Notification' in window && Notification.permission === 'granted') {
    new Notification('InstaBot завершил работу', {
      body: `✓ ${totalOk} успешно · ✗ ${totalErr} ошибок`,
      icon: 'assets/icon.ico',
    });
  }
  // Audio beep
  try {
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.connect(gain); gain.connect(ctx.destination);
    osc.frequency.value = 800; gain.gain.value = 0.3;
    osc.start(); osc.stop(ctx.currentTime + 0.3);
  } catch {}
}

// ═══════════════════════════════════════════════════════
//  Stress test
// ═══════════════════════════════════════════════════════
async function stressTest() {
  const numAcc = prompt('Количество тестовых аккаунтов:', '10');
  if (!numAcc) return;
  const numPosts = prompt('Количество тестовых постов:', '5');
  if (!numPosts) return;
  try {
    await api('POST', '/stress-test', {
      num_accounts: +numAcc,
      num_posts:    +numPosts,
      max_parallel: +qs('#sParallel').value || 5,
    });
    setRunning(true);
    startPolling();
    await loadAccounts();
  } catch (e) { toast(e.message, 'error'); }
}

// ═══════════════════════════════════════════════════════
//  Init
// ═══════════════════════════════════════════════════════
(async () => {
  // Request notification permission
  if ('Notification' in window && Notification.permission === 'default') {
    Notification.requestPermission();
  }
  await loadAccounts();
  await loadPosts();
  await loadSettings();
  updateSummary();
  connectSSE();
  appendLog('system', 'InstaBot готов к работе', 'info');
  appendLog('system', 'Добавьте аккаунты, посты и нажмите Запустить', '');
})();
