/**
 * theme-toggle.js
 * Handles light/dark mode switching and persistence.
 */

(function () {
    const STORAGE_KEY = 'wc_theme';
    const ATTRIBUTE = 'data-theme';

    // 1. Detect preference
    function getPreferredTheme() {
        const stored = localStorage.getItem(STORAGE_KEY);
        if (stored) return stored;
        return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
    }

    // 2. Apply theme
    function setTheme(theme) {
        if (theme === 'dark') {
            document.documentElement.setAttribute(ATTRIBUTE, 'dark');
            document.documentElement.setAttribute('data-bs-theme', 'dark');
        } else {
            document.documentElement.removeAttribute(ATTRIBUTE);
            document.documentElement.removeAttribute('data-bs-theme');
        }
        localStorage.setItem(STORAGE_KEY, theme);
        updateUI(theme);
    }

    // 3. Update Toggle UI (Icon)
    function updateUI(theme) {
        const btn = document.getElementById('themeToggleBtn');
        if (!btn) return;

        const sun = btn.querySelector('.theme-icon-sun');
        const moon = btn.querySelector('.theme-icon-moon');

        if (sun && moon) {
            if (theme === 'dark') {
                sun.classList.add('d-none');
                moon.classList.remove('d-none');
                // Optional: Update button style for active state
                btn.classList.add('active');
            } else {
                sun.classList.remove('d-none');
                moon.classList.add('d-none');
                btn.classList.remove('active');
            }
        }
    }

    // 4. Init
    const currentTheme = getPreferredTheme();
    setTheme(currentTheme);

    // 5. Expose toggle function globally or bind click
    window.toggleTheme = function () {
        const current = document.documentElement.getAttribute(ATTRIBUTE) === 'dark' ? 'dark' : 'light';
        const next = current === 'dark' ? 'light' : 'dark';
        setTheme(next);
    };

    // 6. Bind to button when DOM is ready
    document.addEventListener('DOMContentLoaded', () => {
        const btn = document.getElementById('themeToggleBtn');
        if (btn) {
            btn.addEventListener('click', window.toggleTheme);
            // Ensure UI is correct on load
            const current = document.documentElement.getAttribute(ATTRIBUTE) === 'dark' ? 'dark' : 'light';
            updateUI(current);
        }
    });

})();
