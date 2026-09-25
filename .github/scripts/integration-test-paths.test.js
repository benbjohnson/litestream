'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const test = require('node:test');
const { hasRelevantIntegrationTestChanges } = require('./integration-test-paths');

const workflow = fs.readFileSync(
  path.join(__dirname, '..', 'workflows', 'integration-tests.yml'),
  'utf8'
);
const commitWorkflow = fs.readFileSync(
  path.join(__dirname, '..', 'workflows', 'commit.yml'),
  'utf8'
);

test('runs the integration workflow for every pull request', () => {
  const pullRequestTrigger = workflow.match(/  pull_request:\n([\s\S]*?)  workflow_dispatch:/);

  assert.ok(pullRequestTrigger);
  assert.doesNotMatch(pullRequestTrigger[1], /^    paths:/m);
  assert.match(workflow, /  workflow_dispatch:\n    inputs:\n      test_type:/);
});

test('detects relevant pull request changes with least privilege', () => {
  const changes = jobBlock('changes', 'quick-tests');
  const workflowPermissions = workflow.match(/^permissions:\n([\s\S]*?)(?=^jobs:)/m);

  assert.ok(workflowPermissions);
  assert.match(workflowPermissions[1], /^  contents: read$/m);
  assert.doesNotMatch(workflowPermissions[1], /^  pull-requests:/m);
  assert.match(
    changes,
    /^    permissions:\n      contents: read\n      pull-requests: read$/m
  );
  assert.match(changes, /^    if: github\.event_name == 'pull_request'$/m);
  assert.match(changes, /^      relevant: \$\{\{ steps\.paths\.outputs\.relevant \}\}$/m);
  assert.match(changes, /uses: actions\/checkout@v4/);
  assert.match(changes, /uses: actions\/github-script@v7/);
  assert.match(changes, /github\.paginate\(github\.rest\.pulls\.listFiles/);
  assert.match(changes, /hasRelevantIntegrationTestChanges/);
  assert.match(changes, /context\.payload\.pull_request\.changed_files/);
  assert.match(changes, /String\(relevant\)/);
});

test('does not load a pull-request-modified matcher', async () => {
  const changesScript = extractRunScript(
    jobBlock('changes', 'quick-tests'),
    'Check changed paths',
    'script: |'
  );
  const matcherGuardIndex = changesScript.indexOf('const matcherPath');
  const requireIndex = changesScript.indexOf('require(');

  assert.notEqual(matcherGuardIndex, -1);
  assert.ok(matcherGuardIndex < requireIndex);

  const matcherPath = '.github/scripts/integration-test-paths.js';
  const scenarios = [
    {
      name: 'incomplete response',
      files: [{ filename: 'README.md' }],
      totalChangedFiles: 2,
    },
    {
      name: 'malformed declared count',
      files: [{ filename: 'README.md' }],
      totalChangedFiles: '1',
    },
    {
      name: 'matcher current filename',
      files: [{ filename: matcherPath }],
      totalChangedFiles: 1,
    },
    {
      name: 'matcher previous filename',
      files: [
        {
          filename: 'docs/integration-test-paths.js',
          previous_filename: matcherPath,
        },
      ],
      totalChangedFiles: 1,
    },
  ];

  for (const scenario of scenarios) {
    const result = await runChangesScript({
      files: scenario.files,
      totalChangedFiles: scenario.totalChangedFiles,
      requireImpl: () => assert.fail(`${scenario.name} loaded the matcher`),
    });

    assert.deepEqual(result.outputs, [['relevant', 'true']], scenario.name);
    assert.equal(result.requireCalls, 0, scenario.name);
  }
});

test('gates quick tests on successful relevant changes or manual selection', () => {
  const quickTests = jobBlock('quick-tests', 'scenario-tests');

  assert.match(quickTests, /^    needs: changes$/m);
  assert.match(quickTests, /^    if: always\(\)/m);
  assert.match(quickTests, /needs\.changes\.result == 'success'/);
  assert.match(quickTests, /needs\.changes\.outputs\.relevant == 'true'/);
  assert.match(quickTests, /github\.event_name == 'workflow_dispatch'/);
  assert.match(quickTests, /inputs\.test_type == 'quick'/);
  assert.match(quickTests, /inputs\.test_type == 'all'/);
});

test('excludes manual long runs from quick tests and the summary', () => {
  const quickTests = jobBlock('quick-tests', 'scenario-tests');
  const summary = finalJobBlock('summary');
  const quickCondition = quickTests.match(/^    if: (.*)$/m);
  const summaryCondition = summary.match(/^    if: (.*)$/m);

  assert.ok(quickCondition);
  assert.ok(summaryCondition);
  assert.doesNotMatch(quickCondition[1], /inputs\.test_type == 'long'/);
  assert.doesNotMatch(summaryCondition[1], /inputs\.test_type == 'long'/);
});

test('runs the summary after change detection and quick tests', () => {
  const summary = finalJobBlock('summary');

  assert.match(summary, /^    needs: \[changes, quick-tests\]$/m);
  assert.match(summary, /^    if: always\(\)/m);
  assert.match(summary, /github\.event_name == 'pull_request'/);
  assert.match(summary, /inputs\.test_type == 'quick'/);
  assert.match(summary, /inputs\.test_type == 'all'/);
  assert.match(summary, /CHANGES_RESULT: \$\{\{ needs\.changes\.result \}\}/);
  assert.match(summary, /RELEVANT_CHANGES: \$\{\{ needs\.changes\.outputs\.relevant \}\}/);
  assert.match(summary, /QUICK_TESTS_RESULT: \$\{\{ needs\.quick-tests\.result \}\}/);
});

test('fails the summary closed for required detection and test results', () => {
  const scenarios = [
    {
      name: 'irrelevant pull request',
      env: pullRequestResults('success', 'false', 'skipped'),
      status: 0,
    },
    {
      name: 'successful relevant pull request',
      env: pullRequestResults('success', 'true', 'success'),
      status: 0,
    },
    {
      name: 'failed change detection',
      env: pullRequestResults('failure', '', 'skipped'),
      status: 1,
    },
    {
      name: 'cancelled change detection',
      env: pullRequestResults('cancelled', '', 'skipped'),
      status: 1,
    },
    {
      name: 'unexpected skipped change detection',
      env: pullRequestResults('skipped', '', 'skipped'),
      status: 1,
    },
    {
      name: 'malformed change detection output',
      env: pullRequestResults('success', '', 'skipped'),
      status: 1,
    },
    {
      name: 'failed required quick tests',
      env: pullRequestResults('success', 'true', 'failure'),
      status: 1,
    },
    {
      name: 'cancelled required quick tests',
      env: pullRequestResults('success', 'true', 'cancelled'),
      status: 1,
    },
    {
      name: 'unexpected skipped required quick tests',
      env: pullRequestResults('success', 'true', 'skipped'),
      status: 1,
    },
    {
      name: 'successful manual quick tests',
      env: manualResults('quick', 'success'),
      status: 0,
    },
    {
      name: 'successful manual all tests',
      env: manualResults('all', 'success'),
      status: 0,
    },
    {
      name: 'failed manual quick tests',
      env: manualResults('quick', 'failure'),
      status: 1,
    },
    {
      name: 'cancelled manual quick tests',
      env: manualResults('quick', 'cancelled'),
      status: 1,
    },
    {
      name: 'unexpected skipped manual quick tests',
      env: manualResults('quick', 'skipped'),
      status: 1,
    },
  ];

  for (const scenario of scenarios) {
    const result = runSummary(scenario.env);
    assert.equal(
      result.status,
      scenario.status,
      `${scenario.name}\nstdout:\n${result.stdout}\nstderr:\n${result.stderr}`
    );
  }
});

test('runs every script test from the commit workflow', () => {
  assert.match(
    commitWorkflow,
    /^\s+run: node --test \.github\/scripts\/\*\.test\.js$/m
  );
});

test('includes paths that affect quick integration tests', () => {
  const relevantPaths = [
    'main.go',
    'internal/db/replica.go',
    'go.mod',
    'go.sum',
    'tests/integration/restore_testdata.sql',
    '.github/workflows/integration-tests.yml',
    '.github/scripts/integration-test-paths.js',
  ];

  for (const filename of relevantPaths) {
    assert.equal(hasRelevantIntegrationTestChanges([{ filename }], 1), true, filename);
  }
});

test('excludes paths that do not affect quick integration tests', () => {
  const irrelevantPaths = [
    'README.md',
    'docs/config.md',
    'internal/db/replica.js',
    'tools/go.mod',
    'tests/unit/database_testdata.sql',
    '.github/workflows/commit.yml',
    '.github/workflows/integration-tests.yml.disabled',
  ];

  for (const filename of irrelevantPaths) {
    assert.equal(hasRelevantIntegrationTestChanges([{ filename }], 1), false, filename);
  }
});

test('includes relevant previous paths for renamed files', () => {
  assert.equal(
    hasRelevantIntegrationTestChanges([
      { filename: 'docs/removed-code.md', previous_filename: 'db.go' },
    ], 1),
    true
  );
  assert.equal(
    hasRelevantIntegrationTestChanges([
      {
        filename: 'tests/archive/restore_testdata.sql',
        previous_filename: 'tests/integration/restore_testdata.sql',
      },
    ], 1),
    true
  );
});

test('excludes an empty file list', () => {
  assert.equal(hasRelevantIntegrationTestChanges([], 0), false);
});

test('includes changes when the returned file list is incomplete', () => {
  assert.equal(
    hasRelevantIntegrationTestChanges([{ filename: 'README.md' }], 2),
    true
  );
});

test('rejects malformed file lists and changed-file counts', () => {
  const malformedInputs = [
    [null, 0],
    [{}, 0],
    [[null], 1],
    [[{}], 1],
    [[{ filename: '' }], 1],
    [[{ filename: 42 }], 1],
    [[{ filename: 'README.md', previous_filename: '' }], 1],
    [[{ filename: 'README.md', previous_filename: 42 }], 1],
    [[], undefined],
    [[], null],
    [[], -1],
    [[], 1.5],
    [[], Number.MAX_SAFE_INTEGER + 1],
    [[], '0'],
    [[{ filename: 'README.md' }], 0],
  ];

  for (const [files, totalChangedFiles] of malformedInputs) {
    assert.throws(
      () => hasRelevantIntegrationTestChanges(files, totalChangedFiles),
      TypeError
    );
  }
});

function jobBlock(name, nextName) {
  const block = workflow.match(
    new RegExp(`^  ${name}:\\n([\\s\\S]*?)(?=^  ${nextName}:)`, 'm')
  );

  assert.ok(block, `${name} job must precede ${nextName}`);
  return block[1];
}

function finalJobBlock(name) {
  const block = workflow.match(new RegExp(`^  ${name}:\\n([\\s\\S]*)$`, 'm'));

  assert.ok(block, `${name} job must exist`);
  return block[1];
}

function pullRequestResults(changesResult, relevantChanges, quickTestsResult) {
  return {
    EVENT_NAME: 'pull_request',
    TEST_TYPE: '',
    CHANGES_RESULT: changesResult,
    RELEVANT_CHANGES: relevantChanges,
    QUICK_TESTS_RESULT: quickTestsResult,
  };
}

function manualResults(testType, quickTestsResult) {
  return {
    EVENT_NAME: 'workflow_dispatch',
    TEST_TYPE: testType,
    CHANGES_RESULT: 'skipped',
    RELEVANT_CHANGES: '',
    QUICK_TESTS_RESULT: quickTestsResult,
  };
}

function runSummary(env) {
  const summaryScript = extractRunScript(finalJobBlock('summary'), 'Generate summary');

  return spawnSync('/bin/bash', ['-c', summaryScript], {
    encoding: 'utf8',
    env: {
      ...process.env,
      ...env,
      GITHUB_STEP_SUMMARY: '/dev/null',
    },
  });
}

async function runChangesScript({ files, totalChangedFiles, requireImpl }) {
  const changesScript = extractRunScript(
    jobBlock('changes', 'quick-tests'),
    'Check changed paths',
    'script: |'
  );
  const outputs = [];
  let requireCalls = 0;
  const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;

  await new AsyncFunction('require', 'context', 'github', 'core', changesScript)(
    modulePath => {
      requireCalls += 1;
      return requireImpl(modulePath);
    },
    {
      repo: { owner: 'benbjohnson', repo: 'litestream' },
      payload: {
        pull_request: {
          number: 1473,
          changed_files: totalChangedFiles,
        },
      },
    },
    {
      paginate: async () => files,
      rest: { pulls: { listFiles: () => {} } },
    },
    {
      setOutput: (name, value) => outputs.push([name, value]),
    }
  );

  return { outputs, requireCalls };
}

function extractRunScript(job, stepName, marker = 'run: |') {
  const lines = job.split('\n');
  const stepIndex = lines.findIndex(line => line.trim() === `- name: ${stepName}`);
  assert.notEqual(stepIndex, -1, `${stepName} step must exist`);

  const markerIndex = lines.findIndex(
    (line, index) => index > stepIndex && line.trim() === marker
  );
  assert.notEqual(markerIndex, -1, `${stepName} step must have ${marker}`);

  const markerIndent = lines[markerIndex].length - lines[markerIndex].trimStart().length;
  const scriptLines = [];
  for (const line of lines.slice(markerIndex + 1)) {
    const indent = line.length - line.trimStart().length;
    if (line.trim() !== '' && indent <= markerIndent) {
      break;
    }
    scriptLines.push(line.slice(markerIndent + 2));
  }

  return scriptLines.join('\n');
}
