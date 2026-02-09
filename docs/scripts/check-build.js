/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Build Validation Script
 *
 * Validates the production build output:
 * - Build directory exists
 * - Expected files are present
 * - HTML files are valid
 * - Search index is generated
 * - Static assets are present
 */

const fs = require('fs');
const path = require('path');

const buildDir = path.join(__dirname, '../build');

// ANSI color codes
const colors = {
  reset: '\x1b[0m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  gray: '\x1b[90m'
};

// Results tracking
const results = {
  checks: [],
  warnings: [],
  errors: []
};

/**
 * Add a check result
 */
function check(name, passed, message = '') {
  results.checks.push({ name, passed, message });
  if (!passed) {
    results.errors.push({ name, message });
  }
}

/**
 * Add a warning
 */
function warn(name, message) {
  results.warnings.push({ name, message });
}

/**
 * Check if build directory exists
 */
function checkBuildDir() {
  const exists = fs.existsSync(buildDir) && fs.statSync(buildDir).isDirectory();
  check('Build directory exists', exists, exists ? '' : 'Run npm run build first');
  return exists;
}

/**
 * Check if index.html exists
 */
function checkIndexHtml() {
  const indexPath = path.join(buildDir, 'index.html');
  const exists = fs.existsSync(indexPath);
  check('index.html exists', exists);
  return exists;
}

/**
 * Count HTML files in build
 */
function countHtmlFiles(dir) {
  let count = 0;

  function traverse(currentDir) {
    if (!fs.existsSync(currentDir)) return;

    const files = fs.readdirSync(currentDir);
    files.forEach(file => {
      const filePath = path.join(currentDir, file);
      const stat = fs.statSync(filePath);

      if (stat.isDirectory()) {
        traverse(filePath);
      } else if (file.endsWith('.html')) {
        count++;
      }
    });
  }

  traverse(dir);
  return count;
}

/**
 * Check HTML file generation
 */
function checkHtmlFiles() {
  const count = countHtmlFiles(buildDir);
  const passed = count > 0;
  check('HTML files generated', passed, passed ? `Found ${count} HTML files` : 'No HTML files found');
  return passed;
}

/**
 * Check search index generation
 */
function checkSearchIndex() {
  const searchIndexPath = path.join(buildDir, 'search-index.json');
  const exists = fs.existsSync(searchIndexPath);

  if (exists) {
    try {
      const content = fs.readFileSync(searchIndexPath, 'utf8');
      const data = JSON.parse(content);
      const hasEntries = Array.isArray(data) && data.length > 0;
      check('Search index generated', hasEntries, hasEntries ? `${data.length} entries` : 'Index is empty');
      return hasEntries;
    } catch (e) {
      check('Search index valid', false, `Invalid JSON: ${e.message}`);
      return false;
    }
  } else {
    check('Search index generated', false, 'search-index.json not found');
    return false;
  }
}

/**
 * Check assets directory
 */
function checkAssets() {
  const assetsDir = path.join(buildDir, 'assets');
  const exists = fs.existsSync(assetsDir) && fs.statSync(assetsDir).isDirectory();

  if (exists) {
    const files = fs.readdirSync(assetsDir);
    const jsFiles = files.filter(f => f.endsWith('.js'));
    const cssFiles = files.filter(f => f.endsWith('.css'));

    check('Assets directory exists', true, `${jsFiles.length} JS, ${cssFiles.length} CSS files`);

    if (jsFiles.length === 0) {
      warn('No JavaScript files', 'Expected JS bundles in assets/');
    }
    if (cssFiles.length === 0) {
      warn('No CSS files', 'Expected CSS files in assets/');
    }

    return true;
  } else {
    check('Assets directory exists', false);
    return false;
  }
}

/**
 * Check static files
 */
function checkStaticFiles() {
  const imgDir = path.join(buildDir, 'img');
  const exists = fs.existsSync(imgDir);

  if (exists) {
    const files = fs.readdirSync(imgDir);
    check('Static files copied', true, `${files.length} files in img/`);
  } else {
    warn('Static files', 'img/ directory not found');
  }

  // Check for favicon
  const faviconPath = path.join(buildDir, 'img', 'favicon.ico');
  if (fs.existsSync(faviconPath)) {
    check('Favicon present', true);
  } else {
    warn('Favicon', 'favicon.ico not found in img/');
  }
}

/**
 * Check version directories
 */
function checkVersions() {
  const version310Dir = path.join(buildDir, '3.1.0');
  const version300Dir = path.join(buildDir, '3.0.0');

  const v310Exists = fs.existsSync(version310Dir) && fs.statSync(version310Dir).isDirectory();
  const v300Exists = fs.existsSync(version300Dir) && fs.statSync(version300Dir).isDirectory();

  check('Version 3.1.0 directory', v310Exists);
  check('Version 3.0.0 directory', v300Exists);

  if (v310Exists) {
    const indexPath = path.join(version310Dir, 'index.html');
    check('Version 3.1.0 index.html', fs.existsSync(indexPath));
  }

  if (v300Exists) {
    const indexPath = path.join(version300Dir, 'index.html');
    check('Version 3.0.0 index.html', fs.existsSync(indexPath));
  }
}

/**
 * Calculate build size
 */
function calculateBuildSize(dir) {
  let totalSize = 0;

  function traverse(currentDir) {
    if (!fs.existsSync(currentDir)) return;

    const files = fs.readdirSync(currentDir);
    files.forEach(file => {
      const filePath = path.join(currentDir, file);
      const stat = fs.statSync(filePath);

      if (stat.isDirectory()) {
        traverse(filePath);
      } else {
        totalSize += stat.size;
      }
    });
  }

  traverse(dir);
  return totalSize;
}

/**
 * Format bytes to human-readable size
 */
function formatBytes(bytes) {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i];
}

/**
 * Print results summary
 */
function printResults() {
  console.log('\n' + colors.blue + '=== Build Validation Results ===' + colors.reset);

  // Print checks
  console.log('\n' + colors.gray + '--- Checks ---' + colors.reset);
  results.checks.forEach(c => {
    const icon = c.passed ? colors.green + '✓' : colors.red + '✗';
    const status = c.passed ? colors.green : colors.red;
    console.log(`${icon} ${status}${c.name}${colors.reset}${c.message ? colors.gray + ' (' + c.message + ')' + colors.reset : ''}`);
  });

  // Print warnings
  if (results.warnings.length > 0) {
    console.log('\n' + colors.yellow + `--- Warnings (${results.warnings.length}) ---` + colors.reset);
    results.warnings.forEach(w => {
      console.log(colors.yellow + `  ${w.name}` + colors.reset);
      console.log(colors.gray + `    ${w.message}` + colors.reset);
    });
  }

  // Build size
  const buildSize = calculateBuildSize(buildDir);
  console.log('\n' + colors.gray + `Build size: ${formatBytes(buildSize)}` + colors.reset);

  // Final result
  if (results.errors.length > 0) {
    console.log('\n' + colors.red + `!!! Build validation FAILED (${results.errors.length} errors)` + colors.reset);
    process.exit(1);
  } else if (results.warnings.length > 0) {
    console.log('\n' + colors.yellow + '<<< Build validation PASSED with warnings' + colors.reset);
  } else {
    console.log('\n' + colors.green + '<<< Build validation PASSED' + colors.reset);
  }
}

// Main execution
console.log(colors.blue + '>>> Validating production build' + colors.reset);

if (checkBuildDir()) {
  checkIndexHtml();
  checkHtmlFiles();
  checkSearchIndex();
  checkAssets();
  checkStaticFiles();
  checkVersions();
}

printResults();
