'use strict';

function hasRelevantIntegrationTestChanges(files, totalChangedFiles) {
  if (!Array.isArray(files)) {
    throw new TypeError('files must be an array');
  }
  if (!Number.isSafeInteger(totalChangedFiles) || totalChangedFiles < 0) {
    throw new TypeError('totalChangedFiles must be a nonnegative safe integer');
  }

  const filenames = [];
  files.forEach((file, index) => {
    if (file === null || typeof file !== 'object' || Array.isArray(file)) {
      throw new TypeError(`files[${index}] must be an object`);
    }
    if (typeof file.filename !== 'string' || file.filename.trim() === '') {
      throw new TypeError(`files[${index}].filename must be a non-empty string`);
    }

    filenames.push(file.filename);
    if (file.previous_filename !== undefined) {
      if (
        typeof file.previous_filename !== 'string' ||
        file.previous_filename.trim() === ''
      ) {
        throw new TypeError(
          `files[${index}].previous_filename must be a non-empty string`
        );
      }
      filenames.push(file.previous_filename);
    }
  });

  if (totalChangedFiles < files.length) {
    throw new TypeError('totalChangedFiles must not be less than files.length');
  }
  if (totalChangedFiles > files.length) {
    return true;
  }

  return filenames.some(isRelevantIntegrationTestPath);
}

function isRelevantIntegrationTestPath(filename) {
  return (
    filename.endsWith('.go') ||
    filename === 'go.mod' ||
    filename === 'go.sum' ||
    filename.startsWith('tests/integration/') ||
    filename === '.github/scripts/integration-test-paths.js' ||
    filename === '.github/workflows/integration-tests.yml'
  );
}

module.exports = { hasRelevantIntegrationTestChanges };
