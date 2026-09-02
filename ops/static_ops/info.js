// The landing page's behaviour.
//
// Plan 138 Stage 3c lifted this out of four <script> blocks in
// ops/templates/info.html so the public page can run under a same-origin CSP
// with no 'unsafe-inline'. The blocks are unchanged apart from the null guards
// noted below.
//
// Each block used to sit immediately after the markup it reads, so the elements
// were guaranteed to exist by the time it ran. In one deferred file they all run
// after parsing instead, which is equivalent -- except that a throw in one block
// would now abort the ones after it. Each block therefore checks the elements it
// needs before touching them; that is the only behavioural change here.
//
// The JSON-LD block stays inline in the template. A <script> whose type is not a
// JavaScript MIME type is a data block: it never executes, so script-src does
// not govern it.
//
// This file is authored code and lives on the image side of Stage 7's mount
// seam, so a change to it ships with an image build and not with `git pull`.

// ── Live stats: render the analytics boundary in the reader's locale ──
(function () {
    var el = document.getElementById('analytics-data-through');
    if (!el) return;
    var d = new Date(el.getAttribute('datetime'));
    if (isNaN(d)) return;
    el.textContent = d.toLocaleString(undefined, {
        month: 'short', day: 'numeric',
        hour: 'numeric', minute: '2-digit'
    });
})();

// ── Services grid: expand one card at a time into a shared panel ──
(function () {
    var grid = document.getElementById('services-grid');
    var panel = document.getElementById('service-detail-panel');
    var content = document.getElementById('service-detail-content');
    var activeCard = null;
    if (!grid || !panel || !content) return;

    function lastCardInRow(card) {
        var cards = Array.from(grid.querySelectorAll('.service-card'));
        var rowTop = card.offsetTop;
        var last = card;
        cards.forEach(function (c) {
            if (Math.abs(c.offsetTop - rowTop) < 5) last = c;
        });
        return last;
    }

    document.querySelectorAll('.service-card').forEach(function (card) {
        card.addEventListener('click', function () {
            if (activeCard === card) {
                card.classList.remove('active');
                panel.hidden = true;
                activeCard = null;
            } else {
                if (activeCard) activeCard.classList.remove('active');
                card.classList.add('active');
                content.innerHTML = card.querySelector('.card-detail').innerHTML;
                lastCardInRow(card).after(panel);
                panel.hidden = false;
                activeCard = card;
            }
        });
    });
})();

// ── Recent and planned work ──
// Stage 1d: render /static_ops/generated/project-updates.json into the two lists
// above. Every string goes in through textContent — the source is a
// build-time artifact rather than user input, but this is a public page
// and the summaries are lifted verbatim out of Markdown, so the rule is
// "no markup path" rather than "no markup we expect".
(function () {
    var SOURCE = '/static_ops/generated/project-updates.json';
    var SCHEMA_VERSION = 1;
    // The generator emits only blob links into this repository. Anything
    // else in an href is a payload, not a link, so it is not rendered.
    var HREF_PREFIX = 'https://github.com/whitewalls86/new_car_tracker/blob/';

    function isNonEmptyString(value) {
        return typeof value === 'string' && value.trim() !== '';
    }

    function isRenderable(item) {
        return item
            && isNonEmptyString(item.title)
            && isNonEmptyString(item.summary)
            && isNonEmptyString(item.plan)
            && isNonEmptyString(item.href)
            && item.href.indexOf(HREF_PREFIX) === 0;
    }

    function meta(item) {
        // Planned rows carry priority and effort; completed rows carry a
        // date and neither, because the archive has no such columns.
        if (item.state === 'completed') {
            return isNonEmptyString(item.date) ? 'Completed ' + item.date : '';
        }
        var parts = [];
        if (typeof item.priority === 'number') parts.push('Priority ' + item.priority);
        if (isNonEmptyString(item.effort)) parts.push('Effort ' + item.effort);
        return parts.join(' · ');
    }

    function listItem(item) {
        var li = document.createElement('li');

        var title = document.createElement('span');
        title.className = 'work-title';
        title.textContent = item.title;
        li.appendChild(title);

        var summary = document.createElement('span');
        summary.className = 'work-summary';
        summary.textContent = item.summary;
        li.appendChild(summary);

        var metaSpan = document.createElement('span');
        metaSpan.className = 'work-meta';
        var text = meta(item);
        if (text) metaSpan.appendChild(document.createTextNode(text + ' · '));

        var link = document.createElement('a');
        link.href = item.href;
        link.target = '_blank';
        link.rel = 'noopener';
        link.textContent = 'Plan ' + item.plan + ' →';
        metaSpan.appendChild(link);
        li.appendChild(metaSpan);

        return li;
    }

    function render(list, items) {
        // Built off-document and swapped in one go, so a throw partway
        // through leaves the authored fallback standing rather than half
        // a list.
        var fragment = document.createDocumentFragment();
        items.forEach(function (item) { fragment.appendChild(listItem(item)); });
        list.textContent = '';
        list.appendChild(fragment);
    }

    fetch(SOURCE, { credentials: 'omit' })
        .then(function (response) {
            if (!response.ok) throw new Error('status ' + response.status);
            return response.json();
        })
        .then(function (data) {
            if (!data || data.schema_version !== SCHEMA_VERSION) return;

            ['planned', 'completed'].forEach(function (key) {
                var list = document.getElementById('work-' + key);
                var items = data[key];
                if (!list || !Array.isArray(items) || items.length === 0) return;
                if (!items.every(isRenderable)) return;
                render(list, items);
            });
        })
        .catch(function () {
            // No artifact, a non-200, or unparseable JSON. The authored
            // pointers below the headings are the fallback and are already
            // on the page, so there is nothing to do and nothing to log.
        });
})();

// ── Engineering highlights: the same expander over a different grid ──
(function () {
    var grid = document.querySelector('.highlights-grid');
    var panel = document.getElementById('highlight-detail-panel');
    var content = document.getElementById('highlight-detail-content');
    var activeCard = null;
    if (!grid || !panel || !content) return;

    function lastCardInRow(card) {
        var cards = Array.from(grid.querySelectorAll('.highlight-card'));
        var rowTop = card.offsetTop;
        var last = card;
        cards.forEach(function (c) {
            if (Math.abs(c.offsetTop - rowTop) < 5) last = c;
        });
        return last;
    }

    grid.querySelectorAll('.highlight-card.expandable').forEach(function (card) {
        card.addEventListener('click', function () {
            if (activeCard === card) {
                card.classList.remove('active');
                panel.hidden = true;
                activeCard = null;
            } else {
                if (activeCard) activeCard.classList.remove('active');
                card.classList.add('active');
                content.innerHTML = card.querySelector('.card-detail').innerHTML;
                lastCardInRow(card).after(panel);
                panel.hidden = false;
                activeCard = card;
            }
        });
    });
})();
