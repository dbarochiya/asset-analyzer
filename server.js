#!/usr/bin/env node
/**
 * server.js — lightweight static file server + POST /api/save endpoint
 * Usage: node server.js
 * Serves on http://localhost:8787
 */

const http = require('http');
const fs   = require('fs');
const path = require('path');

const PORT    = 8787;
const ROOT    = __dirname;
const DATA_FILE = path.join(ROOT, 'data.json');

const MIME = {
  '.html': 'text/html',
  '.js':   'application/javascript',
  '.css':  'text/css',
  '.json': 'application/json',
  '.png':  'image/png',
  '.ico':  'image/x-icon',
};

const server = http.createServer((req, res) => {
  // ── CORS headers (allow browser fetch) ─────────────────────────────────
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  // ── POST /api/save ──────────────────────────────────────────────────────
  if (req.method === 'POST' && req.url === '/api/save') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      try {
        const parsed = JSON.parse(body);        // validate JSON
        fs.writeFileSync(DATA_FILE, JSON.stringify(parsed, null, 2), 'utf8');
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true }));
        console.log(`[${new Date().toISOString()}] data.json saved (${body.length} bytes)`);
      } catch (e) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: e.message }));
      }
    });
    return;
  }

  // ── Static file serving ─────────────────────────────────────────────────
  let urlPath = req.url.split('?')[0];
  if (urlPath === '/') urlPath = '/index.html';

  const filePath = path.join(ROOT, urlPath);

  // Prevent directory traversal
  if (!filePath.startsWith(ROOT)) {
    res.writeHead(403); res.end('Forbidden'); return;
  }

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('Not found: ' + urlPath);
      return;
    }
    const ext  = path.extname(filePath);
    const mime = MIME[ext] || 'application/octet-stream';
    res.writeHead(200, { 'Content-Type': mime });
    res.end(data);
  });
});

server.listen(PORT, () => {
  console.log(`Portfolio Analyzer server running at http://localhost:${PORT}`);
  console.log(`Serving files from: ${ROOT}`);
  console.log('Press Ctrl+C to stop.');
});
