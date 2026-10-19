'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const { classifyJobs } = require('./nightly-stability-jobs');

test('classifies malformed failed jobs without throwing', () => {
  const failures = classifyJobs([
    {
      id: 1,
      name: 'Race Detector Sweep',
      conclusion: 'failure',
      steps: {},
    },
  ]);

  assert.equal(failures.length, 1);
  assert.match(failures[0].classificationError, /steps must be an array/);
});

test('classifies malformed jobs alongside recognized failures', () => {
  const failures = classifyJobs([
    {
      id: 1,
      name: 'Race Detector Sweep',
      conclusion: 'failure',
      steps: [],
    },
    {
      id: 2,
      name: 'MinIO Soak',
      steps: [],
    },
  ]);

  assert.equal(failures.length, 2);
  assert.equal(failures[0].name, 'Race Detector Sweep');
  assert.equal(failures[1].name, 'MinIO Soak');
  assert.match(failures[1].classificationError, /conclusion must be a non-empty string/);
});

test('ignores successful and notifier jobs', () => {
  const failures = classifyJobs([
    {
      id: 1,
      name: 'Race Detector Sweep',
      conclusion: 'success',
      steps: [],
    },
    {
      id: 2,
      name: 'Notify on Failure',
      conclusion: '',
      steps: [],
    },
  ]);

  assert.deepEqual(failures, []);
});

test('creates a tracking issue for malformed job objects', async () => {
  const jobs = [
    {
      id: 1,
      name: 'Race Detector Sweep',
      conclusion: 'failure',
      steps: {},
    },
    {
      id: 2,
      name: 'MinIO Soak',
      steps: [],
    },
  ];
  const createdIssues = [];
  const listForRepo = () => {};
  const github = {
    paginate: async route => (typeof route === 'string' ? jobs : []),
    rest: {
      issues: {
        listForRepo,
        create: async input => {
          createdIssues.push(input);
          return {
            data: {
              number: 123,
              html_url: 'https://github.com/benbjohnson/litestream/issues/123',
            },
          };
        },
      },
    },
  };
  const context = {
    repo: { owner: 'benbjohnson', repo: 'litestream' },
    runId: 456,
    serverUrl: 'https://github.com',
    ref: 'refs/heads/test',
    sha: '1234567890abcdef',
  };

  const originalWorkspace = process.env.GITHUB_WORKSPACE;
  process.env.GITHUB_WORKSPACE = process.cwd();
  try {
    await runNotifierScript({
      context,
      github,
      core: {},
      fetch: async () => {
        throw new Error('malformed records must not fetch job logs');
      },
    });
  } finally {
    if (originalWorkspace === undefined) {
      delete process.env.GITHUB_WORKSPACE;
    } else {
      process.env.GITHUB_WORKSPACE = originalWorkspace;
    }
  }

  assert.equal(createdIssues.length, 1);
  assert.match(createdIssues[0].body, /Race Detector Sweep/);
  assert.match(createdIssues[0].body, /steps must be an array/);
  assert.match(createdIssues[0].body, /MinIO Soak/);
  assert.match(createdIssues[0].body, /conclusion must be a non-empty string/);
});

async function runNotifierScript({ context, github, core, fetch }) {
  const workflowPath = path.join(process.cwd(), '.github/workflows/nightly-stability.yml');
  const lines = fs.readFileSync(workflowPath, 'utf8').split('\n');
  const markerIndex = lines.findIndex(line => line.trim() === 'script: |');
  assert.notEqual(markerIndex, -1);

  const markerIndent = lines[markerIndex].length - lines[markerIndex].trimStart().length;
  const scriptLines = [];
  for (const line of lines.slice(markerIndex + 1)) {
    const indent = line.length - line.trimStart().length;
    if (line.trim() !== '' && indent <= markerIndent) {
      break;
    }
    scriptLines.push(line.slice(markerIndent + 2));
  }

  const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
  const execute = new AsyncFunction('require', 'context', 'github', 'core', 'fetch', scriptLines.join('\n'));
  await execute(require, context, github, core, fetch);
}
