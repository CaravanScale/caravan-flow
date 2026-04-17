// caravan-flow — dagre + SVG graph renderer
//
// Exposes a single `caravanGraph` global used by Graph.razor via
// IJSRuntime. Three entry points:
//   - render(containerId, flow, dotnetRef) — full SVG rebuild
//   - updateStats(statsMap) — in-place label patch, no reflow
//   - hashTopology(flow) — stable fingerprint for "did topology change?"
//
// Clicks on a node invoke `dotnetRef.invokeMethodAsync('OnNodeClick',
// name)`; clicks on an edge invoke `OnEdgeClick(from, rel, to)`.
// Background clicks invoke `OnCanvasClick()` so the drawer can close.
//
// Port of CaravanFlow/dashboard.html lines 92–324. Presentation state
// (positions, zoom, pan) is intentionally NOT persisted; dagre relays
// out on every render.

(function () {
  const NODE_W = 200;
  const NODE_H = 68;
  const SVG_NS = 'http://www.w3.org/2000/svg';

  function formatNum(n) {
    if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M';
    if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K';
    return String(n);
  }

  function isSink(proc) {
    const c = proc.connections;
    if (!c) return true;
    return Object.values(c).every(targets => !targets || targets.length === 0);
  }

  // Stable topology fingerprint. Structural only — stats are excluded
  // so a stats-only tick doesn't force a re-layout.
  function hashTopology(flow) {
    if (!flow || !Array.isArray(flow.processors)) return '';
    const procs = flow.processors
      .map(p => {
        const conns = p.connections || {};
        const connsKey = Object.keys(conns).sort()
          .map(rel => `${rel}:${(conns[rel] || []).join(',')}`).join('|');
        return `${p.name}|${p.type}|${p.state}|${connsKey}`;
      })
      .sort()
      .join(';');
    const entries = (flow.entryPoints || []).slice().sort().join(',');
    return `${procs}||${entries}`;
  }

  function buildMarkers(defs) {
    defs.innerHTML =
      '<marker id="arrow-success" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse">' +
      '<path d="M 0 0 L 10 5 L 0 10 z" fill="#4ecca3"/></marker>' +
      '<marker id="arrow-failure" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse">' +
      '<path d="M 0 0 L 10 5 L 0 10 z" fill="#e74c3c"/></marker>' +
      '<marker id="arrow-custom" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse">' +
      '<path d="M 0 0 L 10 5 L 0 10 z" fill="#f0a500"/></marker>';
  }

  function wireDotnetClick(el, method, args, dotnetRef) {
    if (!dotnetRef) return;
    el.addEventListener('click', ev => {
      ev.stopPropagation();
      dotnetRef.invokeMethodAsync(method, ...args);
    });
  }

  function render(containerId, flow, dotnetRef) {
    const container = document.getElementById(containerId);
    if (!container) return;
    if (!window.dagre) {
      container.innerHTML = '<p class="error">dagre.js failed to load</p>';
      return;
    }
    if (!flow || !Array.isArray(flow.processors) || flow.processors.length === 0) {
      container.innerHTML = '<p class="muted">no processors loaded — check the worker\'s config.yaml</p>';
      return;
    }

    // Build SVG skeleton (replace anything previously rendered).
    container.innerHTML = '<svg class="graph-svg"><defs class="graph-defs"></defs></svg>';
    const svg = container.firstChild;
    const defs = svg.firstChild;
    buildMarkers(defs);

    // Background click → close drawer
    svg.addEventListener('click', () => {
      if (dotnetRef) dotnetRef.invokeMethodAsync('OnCanvasClick');
    });

    // Dagre layout
    const g = new dagre.graphlib.Graph();
    g.setGraph({ rankdir: 'TB', ranksep: 60, nodesep: 40, marginx: 30, marginy: 30 });
    g.setDefaultEdgeLabel(() => ({}));

    const entrySet = new Set(flow.entryPoints || []);
    flow.processors.forEach(p => {
      g.setNode(p.name, { width: NODE_W, height: NODE_H, processor: p, isEntry: entrySet.has(p.name) });
    });
    const edges = [];
    flow.processors.forEach(p => {
      if (!p.connections) return;
      Object.entries(p.connections).forEach(([rel, targets]) => {
        (targets || []).forEach(t => {
          g.setEdge(p.name, t, { relationship: rel });
          edges.push({ from: p.name, to: t, rel });
        });
      });
    });
    dagre.layout(g);

    const info = g.graph();
    svg.setAttribute('width', info.width + 60);
    svg.setAttribute('height', info.height + 60);

    // Edges first (behind nodes)
    edges.forEach(({ from, to, rel }) => {
      const edge = g.edge(from, to);
      if (!edge || !edge.points) return;
      const edgeClass = rel === 'failure' ? 'failure' : (rel === 'success' ? 'success' : 'custom');
      const markerId = `arrow-${edgeClass}`;

      let d = `M ${edge.points[0].x} ${edge.points[0].y}`;
      for (let i = 1; i < edge.points.length; i++) {
        d += ` L ${edge.points[i].x} ${edge.points[i].y}`;
      }

      const group = document.createElementNS(SVG_NS, 'g');
      group.setAttribute('class', `edge ${edgeClass}`);
      group.style.cursor = 'pointer';

      // Invisible wider hit path on top for easy clicking
      const hit = document.createElementNS(SVG_NS, 'path');
      hit.setAttribute('d', d);
      hit.setAttribute('stroke', 'transparent');
      hit.setAttribute('stroke-width', '14');
      hit.setAttribute('fill', 'none');
      group.appendChild(hit);

      const path = document.createElementNS(SVG_NS, 'path');
      path.setAttribute('d', d);
      path.setAttribute('marker-end', `url(#${markerId})`);
      group.appendChild(path);

      if (rel !== 'success') {
        const mid = edge.points[Math.floor(edge.points.length / 2)];
        const label = document.createElementNS(SVG_NS, 'text');
        label.setAttribute('x', mid.x + 8);
        label.setAttribute('y', mid.y - 6);
        label.setAttribute('class', 'edge-label');
        label.textContent = rel;
        group.appendChild(label);
      }

      wireDotnetClick(group, 'OnEdgeClick', [from, rel, to], dotnetRef);
      svg.appendChild(group);
    });

    // Nodes
    g.nodes().forEach(name => {
      const node = g.node(name);
      const p = node.processor;
      const x = node.x - NODE_W / 2;
      const y = node.y - NODE_H / 2;
      const processed = (p.stats && p.stats.processed) || 0;
      const errors = (p.stats && p.stats.errors) || 0;
      const isEntry = entrySet.has(name);
      const sink = isSink(p);

      const group = document.createElementNS(SVG_NS, 'g');
      group.setAttribute('class', 'node');
      group.setAttribute('data-name', name);
      group.setAttribute('transform', `translate(${x}, ${y})`);

      const rect = document.createElementNS(SVG_NS, 'rect');
      rect.setAttribute('id', `rect-${name}`);
      rect.setAttribute('width', NODE_W);
      rect.setAttribute('height', NODE_H);
      rect.setAttribute('rx', 8);
      rect.setAttribute('ry', 8);
      rect.setAttribute('fill', errors > 0 ? '#2a1a1a' : p.state === 'ENABLED' ? '#16213e' : '#1a1a2e');
      rect.setAttribute('stroke', errors > 0 ? '#e74c3c' : isEntry ? '#00d2ff' : p.state === 'ENABLED' ? '#0f3460' : '#333');
      rect.setAttribute('stroke-width', 2);
      group.appendChild(rect);

      if (isEntry) {
        const badge = document.createElementNS(SVG_NS, 'text');
        badge.setAttribute('x', NODE_W - 8);
        badge.setAttribute('y', 14);
        badge.setAttribute('text-anchor', 'end');
        badge.setAttribute('class', 'node-entry-badge');
        badge.textContent = 'ENTRY';
        group.appendChild(badge);
      } else if (sink) {
        const badge = document.createElementNS(SVG_NS, 'text');
        badge.setAttribute('x', NODE_W - 8);
        badge.setAttribute('y', 14);
        badge.setAttribute('text-anchor', 'end');
        badge.setAttribute('class', 'node-entry-badge');
        badge.setAttribute('fill', '#f0a500');
        badge.textContent = 'SINK';
        group.appendChild(badge);
      }

      const nameText = document.createElementNS(SVG_NS, 'text');
      nameText.setAttribute('x', 12);
      nameText.setAttribute('y', 20);
      nameText.setAttribute('class', 'node-name');
      nameText.textContent = name.length > 22 ? name.substring(0, 20) + '..' : name;
      group.appendChild(nameText);

      const typeText = document.createElementNS(SVG_NS, 'text');
      typeText.setAttribute('x', 12);
      typeText.setAttribute('y', 36);
      typeText.setAttribute('class', 'node-type');
      const typeStr = p.type || 'unknown';
      typeText.textContent = typeStr.length > 28 ? typeStr.substring(0, 26) + '..' : typeStr;
      group.appendChild(typeText);

      const statsText = document.createElementNS(SVG_NS, 'text');
      statsText.setAttribute('x', 12);
      statsText.setAttribute('y', 54);
      statsText.setAttribute('class', 'node-stats');

      const procSpan = document.createElementNS(SVG_NS, 'tspan');
      procSpan.setAttribute('id', `stat-proc-${name}`);
      procSpan.setAttribute('class', 'node-processed');
      procSpan.textContent = formatNum(processed);
      statsText.appendChild(procSpan);

      const procLabel = document.createElementNS(SVG_NS, 'tspan');
      procLabel.setAttribute('fill', '#555');
      procLabel.textContent = ' processed';
      statsText.appendChild(procLabel);

      const errGap = document.createElementNS(SVG_NS, 'tspan');
      errGap.setAttribute('id', `stat-err-gap-${name}`);
      errGap.setAttribute('fill', '#555');
      errGap.textContent = errors > 0 ? '  ' : '';
      statsText.appendChild(errGap);

      const errSpan = document.createElementNS(SVG_NS, 'tspan');
      errSpan.setAttribute('id', `stat-err-${name}`);
      errSpan.setAttribute('class', 'node-errors');
      errSpan.textContent = errors > 0 ? formatNum(errors) : '';
      statsText.appendChild(errSpan);

      const errLabel = document.createElementNS(SVG_NS, 'tspan');
      errLabel.setAttribute('id', `stat-err-label-${name}`);
      errLabel.setAttribute('fill', '#e74c3c');
      errLabel.textContent = errors > 0 ? ' errors' : '';
      statsText.appendChild(errLabel);

      group.appendChild(statsText);

      wireDotnetClick(group, 'OnNodeClick', [name], dotnetRef);
      svg.appendChild(group);
    });
  }

  // Patch per-node stats in place. Does NOT re-layout; safe to call
  // every 2 s without disturbing open drawers or input focus.
  function updateStats(statsMap) {
    if (!statsMap) return;
    Object.entries(statsMap).forEach(([name, s]) => {
      const processed = (s && s.processed) || 0;
      const errors = (s && s.errors) || 0;
      const procEl = document.getElementById(`stat-proc-${name}`);
      if (procEl) procEl.textContent = formatNum(processed);
      const errGap = document.getElementById(`stat-err-gap-${name}`);
      const errEl = document.getElementById(`stat-err-${name}`);
      const errLabel = document.getElementById(`stat-err-label-${name}`);
      if (errGap) errGap.textContent = errors > 0 ? '  ' : '';
      if (errEl) errEl.textContent = errors > 0 ? formatNum(errors) : '';
      if (errLabel) errLabel.textContent = errors > 0 ? ' errors' : '';
      const rect = document.getElementById(`rect-${name}`);
      if (rect && errors > 0) {
        rect.setAttribute('fill', '#2a1a1a');
        rect.setAttribute('stroke', '#e74c3c');
      }
    });
  }

  // Toggle the `selected` class on the named node (and clear any
  // previous selection). Pass `null` to clear. Called from Graph.razor
  // whenever the drawer opens or closes so the SVG reflects the
  // current drawer target.
  function setSelected(name) {
    document.querySelectorAll('.graph-svg .node.selected').forEach(el => el.classList.remove('selected'));
    if (!name) return;
    const el = document.querySelector(`.graph-svg .node[data-name="${CSS.escape(name)}"]`);
    if (el) el.classList.add('selected');
  }

  // Visibility-change wiring so the Razor timers can pause when the
  // tab is hidden. Razor subscribes by calling
  // caravanGraph.onVisibilityChange(dotnetRef) once; callback receives
  // the visibility string ('visible' or 'hidden').
  function onVisibilityChange(dotnetRef) {
    if (!dotnetRef) return;
    document.addEventListener('visibilitychange', () => {
      dotnetRef.invokeMethodAsync('OnVisibilityChange', document.visibilityState);
    });
  }

  window.caravanGraph = { render, updateStats, hashTopology, setSelected, onVisibilityChange };
})();
