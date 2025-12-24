const state = {
  theme: localStorage.getItem('newsai_theme') || 'light',
  lang: localStorage.getItem('newsai_lang') || 'en',
  token: localStorage.getItem('newsai_jwt') || '',
  preview: [],
  feedbackEvents: [],
};

function setTheme(theme) {
  document.body.setAttribute('data-theme', theme);
  localStorage.setItem('newsai_theme', theme);
  state.theme = theme;
}

function setLang(lang) {
  localStorage.setItem('newsai_lang', lang);
  state.lang = lang;
  // Re-render feed to apply language filter immediately
  try {
    if (Array.isArray(state.preview) && state.preview.length) {
      renderFeed(state.preview);
      status(`Language set to ${lang.toUpperCase()} • feed updated`);
    }
  } catch (e) {}
}

function setToken(token) {
  localStorage.setItem('newsai_jwt', token);
  state.token = token;
}

function updateStages(stage, status) {
  const nodes = document.querySelectorAll('.stage');
  nodes.forEach((n) => {
    const s = n.getAttribute('data-stage');
    n.classList.remove('active');
    if (status === 'active' && s === stage) n.classList.add('active');
    if (status === 'done' && s === stage) n.classList.add('done');
    if (stage === 'reset') n.classList.remove('done');
  });
}

function status(msg) {
  document.getElementById('pipelineStatus').textContent = msg;
}

async function apiPost(path, body, timeoutMs = 20000) {
  if (!state.token) {
    status('Missing JWT: paste token at top and click Save Token.');
    throw new Error('missing_jwt');
  }
  const controller = new AbortController();
  const to = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(path, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(state.token ? { Authorization: `Bearer ${state.token}` } : {}),
      },
      body: JSON.stringify(body || {}),
      signal: controller.signal,
    });
    clearTimeout(to);
    const json = await res.json().catch(() => ({ error: 'bad_json' }));
    if (!res.ok) throw new Error(json?.detail || json?.error || res.statusText);
    return json;
  } catch (err) {
    clearTimeout(to);
    throw err;
  }
}

function renderFeed(items) {
  const root = document.getElementById('feedList');
  root.innerHTML = '';
  const itemsToShow = (items || []).filter((it) => {
    const il = (it.lang || '').toLowerCase();
    const sl = (state.lang || 'en').toLowerCase();
    return !il || il === sl;
  });
  itemsToShow.forEach((it) => {
    const card = document.createElement('div');
    card.className = 'card';
    const title = document.createElement('div');
    title.className = 'title';
    title.textContent = it.title || '(Untitled)';
    const badgebar = document.createElement('div');
    badgebar.className = 'badgebar';
    const bLang = document.createElement('span'); bLang.className = 'badge'; bLang.textContent = it.lang || 'n/a';
    const bAud = document.createElement('span'); bAud.className = 'badge'; bAud.textContent = it.audience || 'general';
    const bTone = document.createElement('span'); bTone.className = 'badge'; bTone.textContent = it.tone || 'neutral';
    badgebar.append(bLang, bAud, bTone);
    const actions = document.createElement('div');
    actions.className = 'actions-row';
    const likeBtn = mkBtn('Like', () => sendFeedback(it, 'like'));
    const dislikeBtn = mkBtn('Dislike', () => sendFeedback(it, 'dislike'));
    const approveBtn = mkBtn('Approve', () => sendFeedback(it, 'save'));
    const flagBtn = mkBtn('Flag', () => sendFeedback(it, 'skip', { flag: true })); // map flag->skip with context
    actions.append(likeBtn, dislikeBtn, approveBtn, flagBtn);
    card.append(title, badgebar, actions);
    root.append(card);
  });
}

function mkBtn(label, onClick) {
  const b = document.createElement('button');
  b.className = 'btn';
  b.textContent = label;
  b.onclick = onClick;
  return b;
}

async function sendFeedback(item, action, extraContext) {
  try {
    const userId = parseJwtUser(state.token);
    if (!userId) throw new Error('Missing or invalid JWT');
    const payload = {
      user_id: userId,
      article_id: item.id || hashText(item.title || ''),
      action,
      timestamp: new Date().toISOString(),
      context: {
        lang: item.lang,
        tone: item.tone,
        audience: item.audience,
        reason: extraContext?.flag ? 'flag' : undefined,
        device: isMobile() ? 'mobile' : 'desktop',
        session_id: sessionId(),
      },
    };
    const res = await apiPost('/feedback', payload);
    pushEvent({ kind: 'feedback', payload, res });
  } catch (err) {
    pushEvent({ kind: 'error', message: 'Feedback failed', detail: String(err?.message || err) });
  }
}

function pushEvent(ev) {
  state.feedbackEvents.unshift(ev);
  state.feedbackEvents = state.feedbackEvents.slice(0, 20);
  const ul = document.getElementById('feedbackEvents');
  ul.innerHTML = '';
  state.feedbackEvents.forEach((e) => {
    const li = document.createElement('li');
    li.className = 'event';
    li.textContent = e.kind === 'feedback' ? `${e.payload.action} • ${e.payload.article_id}` : `${e.message}`;
    ul.append(li);
  });
}

function parseJwtUser(token) {
  try {
    const p = token.split('.')[1];
    if (!p) return null;
    const raw = atob(p.replace(/-/g, '+').replace(/_/g, '/'));
    const obj = JSON.parse(raw);
    return obj.user_id || obj.sub || null;
  } catch {
    return null;
  }
}

function isMobile() {
  return window.matchMedia('(max-width: 600px)').matches;
}

function sessionId() {
  let s = sessionStorage.getItem('newsai_session');
  if (!s) {
    s = Math.random().toString(36).slice(2);
    sessionStorage.setItem('newsai_session', s);
  }
  return s;
}

function hashText(t) {
  let h = 0;
  for (let i = 0; i < t.length; i++) {
    h = (h << 5) - h + t.charCodeAt(i);
    h |= 0;
  }
  return String(h);
}

async function runFetch() {
  updateStages('reset');
  updateStages('filter', 'active');
  status('Fetching…');
  try {
    const json = await apiPost('/fetch', { registry: 'single', category: 'general', limit_preview: 12 });
    state.preview = json.preview || [];
    renderFeed(state.preview);
    updateStages('filter', 'done');
    updateStages('verify', 'done'); // lightweight verify as fetch succeeded
    status(`Fetched ${json.count} items, showing preview`);
  } catch (err) {
    status(`Fetch failed: ${String(err.message || err)}`);
  }
}

async function runProcess() {
  updateStages('script', 'active');
  status('Processing…');
  try {
    const json = await apiPost('/process', { registry: 'single', category: state.lang === 'en' ? 'general' : 'general', limit_preview: 12 });
    // Prefer scripts preview for richer display
    state.preview = json.preview || [];
    renderFeed(state.preview);
    updateStages('script', 'done');
    status(`Processed • filtered=${json.counts?.filtered} • scripts=${json.counts?.scripts}`);
  } catch (err) {
    status(`Process failed: ${String(err.message || err)}`);
  }
}

async function runVoice() {
  updateStages('voice', 'active');
  status('Voicing…');
  try {
    // Voice can take longer when many scripts are present; allow more time
    const json = await apiPost('/voice', { registry: 'single', category: 'general', voice: 'en-US-Neural-1', limit: 12 }, 30000);
    updateStages('voice', 'done');
    status(`Voice items: ${json.count}`);
  } catch (err) {
    status(`Voice failed: ${String(err.message || err)}`);
  }
}

async function runAvatar() {
  updateStages('avatar', 'active');
  status('Rendering avatar…');
  try {
    const json = await apiPost('/api/agents/avatar/render', { registry: 'single', category: 'general', style: 'news-anchor' }, 30000);
    updateStages('avatar', 'done');
    const outFile = json?.meta?.output_file || json?.output_file || 'single_pipeline/output/single_avatar.json';
    status(`Avatar rendered • items=${json?.meta?.count || json?.count || 'n/a'} • file=${outFile}`);
    pushEvent({ kind: 'info', message: `Avatar JSON written: ${outFile}` });
  } catch (err) {
    status(`Avatar render failed: ${String(err.message || err)}`);
  }
}

function init() {
  setTheme(state.theme);
  document.getElementById('themeToggle').checked = state.theme === 'dark';
  document.getElementById('themeToggle').addEventListener('change', (e) => setTheme(e.target.checked ? 'dark' : 'light'));
  document.getElementById('langSelect').value = state.lang;
  document.getElementById('langSelect').addEventListener('change', (e) => setLang(e.target.value));
  document.getElementById('jwtInput').value = state.token;
  document.getElementById('saveJwtBtn').addEventListener('click', () => {
    const tok = document.getElementById('jwtInput').value.trim();
    setToken(tok);
    const uid = parseJwtUser(tok);
    if (uid) status(`Token saved for user: ${uid}`); else status('Token saved. Unable to parse user_id from JWT.');
  });
  document.getElementById('runFetchBtn').addEventListener('click', runFetch);
  document.getElementById('runProcessBtn').addEventListener('click', runProcess);
  document.getElementById('runVoiceBtn').addEventListener('click', runVoice);
  document.getElementById('runAvatarBtn').addEventListener('click', runAvatar);
}

document.addEventListener('DOMContentLoaded', init);