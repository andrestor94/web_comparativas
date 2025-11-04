// static/js/comments_inbox_enter.js
(function () {
  // Enter = enviar, Shift+Enter = salto. Cuida IME (e.isComposing).
  function isReplyForm(form) {
    if (!form) return false;
    // Cubre distintos nombres/estructuras sin tocar el HTML.
    const action = (form.getAttribute('action') || '');
    return form.matches('[data-reply-form], .js-reply-form, #reply-form')
        || action.includes('/api/comments');
  }

  document.addEventListener('keydown', function (e) {
    if (e.key !== 'Enter' || e.shiftKey || e.ctrlKey || e.altKey || e.metaKey || e.isComposing) return;
    const ta = e.target;
    if (!(ta instanceof HTMLTextAreaElement)) return;

    const form = ta.closest('form');
    if (!isReplyForm(form)) return;

    // Evita enviar en blanco
    if (!ta.value || !ta.value.trim()) return;

    e.preventDefault();
    if (form.requestSubmit) form.requestSubmit();
    else form.submit();
  });
})();
