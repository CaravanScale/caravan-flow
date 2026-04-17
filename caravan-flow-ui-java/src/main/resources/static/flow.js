// Graph bootstrap for /flow — reads the server-embedded JSON,
// renders a cytoscape.js DAG with dagre layout, and wires node
// clicks to the HTMX-driven slide-in drawer.
//
// Positions are never persisted: dagre lays the graph out fresh on
// every page load. Drag is enabled for session-local nudging but
// the config on disk never sees pixel coordinates.

(function () {
    const dataEl = document.getElementById("flow-data");
    if (!dataEl) return;

    // cytoscape-dagre is a UMD bundle that ONLY self-exposes as
    // window.cytoscapeDagre — it does not auto-register as a
    // cytoscape extension. Without this the dagre layout silently
    // no-ops and every node stacks at (0,0).
    if (typeof cytoscape !== "undefined" && typeof cytoscapeDagre !== "undefined") {
        cytoscape.use(cytoscapeDagre);
    }

    let raw;
    try { raw = JSON.parse(dataEl.textContent); }
    catch (e) { console.error("flow-data parse failed", e); return; }

    if (raw.error) { document.getElementById("flow-error").textContent = raw.error; return; }

    const stateClass = (s) => "state-" + String(s || "unknown").toLowerCase();

    const elements = [];
    raw.processors.forEach(p => {
        elements.push({
            group: "nodes",
            data: {
                id: p.name,
                label: p.name,
                type: p.type,
                state: p.state,
                stats: p.stats || {},
            },
            classes: stateClass(p.state),
        });
    });
    raw.edges.forEach(e => {
        elements.push({
            group: "edges",
            data: { id: e.id, source: e.source, target: e.target, label: e.label || "" },
        });
    });

    const cy = cytoscape({
        container: document.getElementById("flow-graph"),
        elements: elements,
        layout: { name: "dagre", rankDir: "LR", nodeSep: 60, rankSep: 120, edgeSep: 20 },
        minZoom: 0.2,
        maxZoom: 2.0,
        style: [
            {
                selector: "node",
                style: {
                    "shape": "round-rectangle",
                    "background-color": "#fff",
                    "border-width": 1.5,
                    "border-color": "#d0d8e3",
                    "label": "data(label)",
                    "text-valign": "center",
                    "text-halign": "center",
                    "color": "#223",
                    "font-size": 13,
                    "font-weight": 600,
                    "width": "label",
                    "height": 46,
                    "padding": "14px",
                    "text-wrap": "wrap",
                    "text-max-width": 200,
                },
            },
            {
                selector: "node.state-enabled",
                style: { "border-color": "#2b8a47", "border-width": 2 },
            },
            {
                selector: "node.state-disabled",
                style: { "border-color": "#8b1c1c", "opacity": 0.65 },
            },
            {
                selector: "node:selected",
                style: { "border-color": "#1f3d70", "border-width": 3, "background-color": "#eef4fb" },
            },
            {
                selector: "edge",
                style: {
                    "curve-style": "bezier",
                    "target-arrow-shape": "triangle",
                    "width": 2,
                    "line-color": "#9aa8b8",
                    "target-arrow-color": "#9aa8b8",
                    "label": "data(label)",
                    "font-size": 10,
                    "color": "#667",
                    "text-background-color": "#fafbfc",
                    "text-background-opacity": 0.9,
                    "text-background-padding": 2,
                },
            },
        ],
    });

    cy.fit(null, 30);

    async function openDrawer(name) {
        const drawer = document.getElementById("flow-drawer");
        const body = document.getElementById("flow-drawer-body");
        drawer.classList.add("open");
        drawer.setAttribute("aria-hidden", "false");
        // Drawer shrinks #flow-graph via flex. Cytoscape caches canvas
        // dimensions at init — without resize() its canvases keep their
        // old width and overflow on top of the drawer, hiding it.
        setTimeout(() => cy.resize().fit(null, 30), 220);
        body.innerHTML = '<p class="drawer-loading">Loading…</p>';
        try {
            const resp = await fetch("/flow/panel/" + encodeURIComponent(name), { cache: "no-store" });
            if (!resp.ok) throw new Error("HTTP " + resp.status);
            body.innerHTML = await resp.text();
            // The loaded panel carries hx-trigger="every 2s" on its
            // root — we need to tell htmx to scan the new DOM so the
            // auto-refresh kicks in.
            if (typeof htmx !== "undefined" && htmx.process) htmx.process(body);
        } catch (e) {
            console.error("[flow] panel load failed", e);
            body.innerHTML = '<p class="drawer-error">Panel load failed: ' + String(e) + '</p>';
        }
    }

    function closeDrawer() {
        const drawer = document.getElementById("flow-drawer");
        drawer.classList.remove("open");
        drawer.setAttribute("aria-hidden", "true");
        document.getElementById("flow-drawer-body").innerHTML = "";
        setTimeout(() => cy.resize().fit(null, 30), 220);
    }

    cy.on("tap", "node", (evt) => {
        const name = evt.target.id();
        console.log("[flow] node tapped:", name);
        openDrawer(name);
    });

    cy.on("tap", (evt) => {
        if (evt.target === cy) closeDrawer();
    });

    // Live stats refresh. Pulls just state + stats every 2s, patches
    // the node data in place — cytoscape repaints colors/labels
    // without re-running layout, so nodes don't jitter.
    async function refreshStats() {
        try {
            const resp = await fetch("/flow/stats.json", { cache: "no-store" });
            if (!resp.ok) return;
            const body = await resp.json();
            (body.processors || []).forEach(p => {
                const n = cy.getElementById(p.name);
                if (!n || n.empty()) return;
                const oldState = n.data("state");
                n.data("state", p.state);
                n.data("stats", p.stats || {});
                if (oldState !== p.state) {
                    n.removeClass("state-enabled state-disabled state-unknown");
                    n.addClass(stateClass(p.state));
                }
            });
        } catch (e) { /* transient — next tick will try again */ }
    }
    setInterval(refreshStats, 2000);

    // Close drawer on Escape.
    document.addEventListener("keydown", (e) => {
        if (e.key === "Escape") closeDrawer();
    });
})();
