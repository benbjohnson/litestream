'use strict';

const failedConclusions = new Set([
  'failure',
  'neutral',
  'timed_out',
  'action_required',
  'cancelled',
  'stale',
  'startup_failure',
]);
const successfulConclusions = new Set(['success', 'skipped']);

function classifyJobs(jobs) {
  if (!Array.isArray(jobs)) {
    return [malformedFailure(0, null, ['jobs response must be an array'])];
  }

  const failures = [];
  let monitoredJobCount = 0;

  jobs.forEach((job, index) => {
    if (!isRecord(job)) {
      failures.push(malformedFailure(index, job, ['job must be an object']));
      return;
    }
    if (job.name === 'Notify on Failure') {
      return;
    }

    monitoredJobCount += 1;
    const problems = validateJob(job);
    if (problems.length > 0) {
      failures.push(malformedFailure(index, job, problems));
      return;
    }

    if (successfulConclusions.has(job.conclusion)) {
      return;
    }
    if (!failedConclusions.has(job.conclusion)) {
      failures.push(malformedFailure(index, job, [`unsupported conclusion ${JSON.stringify(job.conclusion)}`]));
      return;
    }

    failures.push({
      id: job.id,
      name: job.name,
      url: job.html_url || job.url || '',
      failedSteps: job.steps
        .filter(step => step.conclusion === 'failure')
        .map(step => step.name),
      classificationError: '',
    });
  });

  if (monitoredJobCount === 0 && failures.length === 0) {
    failures.push(malformedFailure(0, null, ['no monitored job records']));
  }

  return failures;
}

function isRecord(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function validateJob(job) {
  const problems = [];

  if (!Number.isSafeInteger(job.id) || job.id <= 0) {
    problems.push('id must be a positive integer');
  }
  if (typeof job.name !== 'string' || job.name.trim() === '') {
    problems.push('name must be a non-empty string');
  }
  if (typeof job.conclusion !== 'string' || job.conclusion.trim() === '') {
    problems.push('conclusion must be a non-empty string');
  }
  if (!Array.isArray(job.steps)) {
    problems.push('steps must be an array');
  } else {
    job.steps.forEach((step, index) => {
      if (!isRecord(step)) {
        problems.push(`steps[${index}] must be an object`);
        return;
      }
      if (typeof step.name !== 'string' || step.name.trim() === '') {
        problems.push(`steps[${index}].name must be a non-empty string`);
      }
      if (typeof step.conclusion !== 'string' || step.conclusion.trim() === '') {
        problems.push(`steps[${index}].conclusion must be a non-empty string`);
      }
    });
  }

  return problems;
}

function malformedFailure(index, job, problems) {
  const validName = isRecord(job) && typeof job.name === 'string' && job.name.trim() !== '';
  const validID = isRecord(job) && Number.isSafeInteger(job.id) && job.id > 0;
  const validURL = isRecord(job) && typeof (job.html_url || job.url) === 'string';

  return {
    id: validID ? job.id : null,
    name: validName ? job.name : `Malformed job record #${index + 1}`,
    url: validURL ? job.html_url || job.url : '',
    failedSteps: [],
    classificationError: `Malformed job record #${index + 1}: ${problems.join('; ')}`,
  };
}

module.exports = { classifyJobs };
