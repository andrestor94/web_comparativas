(function () {
  'use strict';

  const VIEW_RE = /\/mercado-publico\/lectura-pliegos\/\d+\/vista(?:-ampliada)?\/?$/;
  const CACHE_PREFIX = 'lp:view:';
  const SCROLL_PREFIX = 'lp:scroll:';
  const TTL_MS = 4 * 60 * 1000;
  const memoryCache = new Map();

  function normalizedUrl(rawUrl) {
    try {
      const url = new URL(rawUrl, window.location.origin);
      if (url.origin !== window.location.origin) return null;
      return url;
    } catch (_) {
      return null;
    }
  }

  function cacheKey(url) {
    return CACHE_PREFIX + url.pathname;
  }

  function isViewUrl(url) {
    return url && VIEW_RE.test(url.pathname);
  }

  function getCached(url) {
    const key = cacheKey(url);
    const mem = memoryCache.get(key);
    if (mem && Date.now() - mem.ts < TTL_MS) return mem.html;

    try {
      const raw = sessionStorage.getItem(key);
      if (!raw) return null;
      const parsed = JSON.parse(raw);
      if (!parsed || Date.now() - parsed.ts > TTL_MS || !parsed.html) {
        sessionStorage.removeItem(key);
        return null;
      }
      memoryCache.set(key, parsed);
      return parsed.html;
    } catch (_) {
      return null;
    }
  }

  function setCached(url, html) {
    if (!html || html.indexOf('<html') === -1) return;
    const entry = { ts: Date.now(), html };
    memoryCache.set(cacheKey(url), entry);
    try {
      sessionStorage.setItem(cacheKey(url), JSON.stringify(entry));
    } catch (_) {
      // Large cases can exceed sessionStorage. Memory cache still covers this visit.
    }
  }

  function ensureProgress() {
    let bar = document.getElementById('lp-nav-bar');
    if (!bar) {
      bar = document.createElement('div');
      bar.id = 'lp-nav-bar';
      document.body.appendChild(bar);
    }
    return bar;
  }

  function setProgress(width, opacity) {
    const bar = ensureProgress();
    bar.style.opacity = String(opacity);
    bar.style.width = width;
  }

  function finishProgress() {
    setProgress('100%', 1);
    window.setTimeout(() => {
      const bar = document.getElementById('lp-nav-bar');
      if (!bar) return;
      bar.style.opacity = '0';
      bar.style.width = '0';
    }, 180);
  }

  function prefetch(rawUrl) {
    const url = normalizedUrl(rawUrl);
    if (!isViewUrl(url) || url.pathname === window.location.pathname || getCached(url)) {
      return Promise.resolve(null);
    }

    return fetch(url.href, {
      credentials: 'same-origin',
      cache: 'force-cache',
      headers: { 'X-LP-Prefetch': '1' },
    })
      .then((response) => {
        const contentType = response.headers.get('content-type') || '';
        if (!response.ok || contentType.indexOf('text/html') === -1) {
          return null;
        }
        return response.text();
      })
      .then((html) => {
        if (html) setCached(url, html);
        return html;
      })
      .catch(() => null);
  }

  function writeDocument(url, html) {
    try {
      sessionStorage.setItem(SCROLL_PREFIX + window.location.pathname, String(window.scrollY || 0));
    } catch (_) {}

    history.pushState({ lpInstant: true }, '', url.href);
    document.open();
    document.write(html);
    document.close();
  }

  function bindLinks() {
    const anchors = Array.from(document.querySelectorAll('a[href]'))
      .map((link) => ({ link, url: normalizedUrl(link.getAttribute('href')) }))
      .filter((item) => isViewUrl(item.url));

    const uniqueTargets = new Set();
    anchors.forEach(({ link, url }) => {
      if (url.pathname !== window.location.pathname) uniqueTargets.add(url.href);

      link.addEventListener('pointerenter', () => prefetch(url.href), { passive: true });
      link.addEventListener('focus', () => prefetch(url.href), { passive: true });

      link.addEventListener('click', (event) => {
        if (event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
        if (link.target || link.hasAttribute('download')) return;
        if (url.pathname === window.location.pathname && url.hash) return;

        event.preventDefault();
        setProgress('42%', 1);

        const cached = getCached(url);
        if (cached) {
          setProgress('86%', 1);
          writeDocument(url, cached);
          return;
        }

        prefetch(url.href).then((html) => {
          if (html) {
            setProgress('86%', 1);
            writeDocument(url, html);
          } else {
            window.location.href = url.href;
          }
        });
      });
    });

    const runPrefetch = () => uniqueTargets.forEach((href) => prefetch(href));
    if ('requestIdleCallback' in window) {
      window.requestIdleCallback(runPrefetch, { timeout: 900 });
    } else {
      window.setTimeout(runPrefetch, 350);
    }
  }

  function restoreScroll() {
    try {
      const key = SCROLL_PREFIX + window.location.pathname;
      const saved = sessionStorage.getItem(key);
      if (!saved) return;
      sessionStorage.removeItem(key);
      window.scrollTo(0, Math.max(0, Number(saved) || 0));
    } catch (_) {}
  }

  document.addEventListener('DOMContentLoaded', () => {
    bindLinks();
    restoreScroll();
    finishProgress();
  });
})();
