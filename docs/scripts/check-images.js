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
 * Image Validation Script
 *
 * Scans all Markdown/MDX files in the docs directory and validates:
 * - Image references (![alt](path))
 * - Image file existence in static/img directory
 * - Alt text presence (accessibility requirement)
 *
 * Does not validate:
 * - External image URLs
 * - Images referenced in React components
 */

const fs = require('fs');
const path = require('path');

const docsDir = path.join(__dirname, '../docs');
const versionedDocsDir = path.join(__dirname, '../versioned_docs');
const staticDir = path.join(__dirname, '../static');

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
  totalFiles: 0,
  totalImages: 0,
  brokenImages: [],
  missingAlt: [],
  warnings: []
};

/**
 * Find all Markdown/MDX files recursively
 */
function findMarkdownFiles(dir, fileList = []) {
  if (!fs.existsSync(dir)) {
    return fileList;
  }

  const files = fs.readdirSync(dir);

  files.forEach(file => {
    const filePath = path.join(dir, file);
    const stat = fs.statSync(filePath);

    if (stat.isDirectory()) {
      findMarkdownFiles(filePath, fileList);
    } else if (file.match(/\.(md|mdx)$/)) {
      fileList.push(filePath);
    }
  });

  return fileList;
}

/**
 * Extract image references from markdown content
 * Matches ![alt](path) and <img src="path" alt="alt" /> patterns
 */
function extractImages(content) {
  const images = [];

  // Markdown image syntax: ![alt](path)
  const mdImageRegex = /!\[([^\]]*)\]\(([^)]+)\)/g;
  let match;

  while ((match = mdImageRegex.exec(content)) !== null) {
    images.push({
      alt: match[1],
      src: match[2].split(/\s+/)[0], // Remove title if present
      line: content.substring(0, match.index).split('\n').length,
      type: 'markdown'
    });
  }

  // HTML img tag syntax: <img src="path" alt="alt" />
  const htmlImageRegex = /<img[^>]+src=["']([^"']+)["'][^>]*(?:alt=["']([^"']*)["'])?[^>]*>/gi;

  while ((match = htmlImageRegex.exec(content)) !== null) {
    images.push({
      alt: match[2] || '',
      src: match[1],
      line: content.substring(0, match.index).split('\n').length,
      type: 'html'
    });
  }

  return images;
}

/**
 * Check if image source is external URL
 */
function isExternalImage(src) {
  return src.match(/^(https?:\/\/|data:)/);
}

/**
 * Resolve image path
 * Docusaurus images should be in static/img and referenced as /img/...
 * or relative paths from the markdown file
 */
function resolveImagePath(sourcePath, imageSrc) {
  // External images
  if (isExternalImage(imageSrc)) {
    return null;
  }

  // Absolute path from site root (/img/...)
  if (imageSrc.startsWith('/')) {
    return path.join(staticDir, imageSrc);
  }

  // Relative path from source file
  const sourceDir = path.dirname(sourcePath);
  return path.resolve(sourceDir, imageSrc);
}

/**
 * Validate images in a single file
 */
function validateFile(filePath) {
  results.totalFiles++;

  const content = fs.readFileSync(filePath, 'utf8');
  const images = extractImages(content);

  images.forEach(image => {
    results.totalImages++;

    // Check for missing alt text
    if (!image.alt || image.alt.trim() === '') {
      results.missingAlt.push({
        file: filePath,
        line: image.line,
        src: image.src,
        type: image.type
      });
    }

    // Skip external images
    if (isExternalImage(image.src)) {
      return;
    }

    // Check if image file exists
    const resolvedPath = resolveImagePath(filePath, image.src);

    if (!resolvedPath) {
      return;
    }

    if (!fs.existsSync(resolvedPath)) {
      results.brokenImages.push({
        file: filePath,
        line: image.line,
        src: image.src,
        resolvedPath: resolvedPath,
        type: image.type
      });
    }
  });
}

/**
 * Print results summary
 */
function printResults() {
  console.log('\n' + colors.blue + '=== Image Validation Results ===' + colors.reset);
  console.log(colors.gray + `Scanned ${results.totalFiles} files` + colors.reset);
  console.log(colors.gray + `Found ${results.totalImages} images` + colors.reset);

  let hasErrors = false;

  if (results.missingAlt.length > 0) {
    console.log('\n' + colors.yellow + `--- Missing Alt Text (${results.missingAlt.length}) ---` + colors.reset);
    results.missingAlt.forEach(img => {
      console.log(colors.yellow + `  ${path.relative(process.cwd(), img.file)}:${img.line}` + colors.reset);
      console.log(colors.gray + `    Image: ${img.src}` + colors.reset);
      console.log(colors.gray + `    Type: ${img.type}` + colors.reset);
    });
  }

  if (results.brokenImages.length > 0) {
    hasErrors = true;
    console.log('\n' + colors.red + `!!! Broken Image References (${results.brokenImages.length}) !!!` + colors.reset);
    results.brokenImages.forEach(broken => {
      console.log(colors.red + `  ${path.relative(process.cwd(), broken.file)}:${broken.line}` + colors.reset);
      console.log(colors.gray + `    Source: ${broken.src}` + colors.reset);
      console.log(colors.gray + `    Resolved: ${broken.resolvedPath}` + colors.reset);
      console.log(colors.gray + `    Type: ${broken.type}` + colors.reset);
    });
  }

  if (hasErrors) {
    console.log('\n' + colors.red + 'Image validation FAILED' + colors.reset);
    process.exit(1);
  } else if (results.missingAlt.length > 0) {
    console.log('\n' + colors.yellow + '<<< Image validation PASSED with warnings (missing alt text)' + colors.reset);
  } else {
    console.log('\n' + colors.green + '<<< All image references valid' + colors.reset);
  }
}

// Main execution
console.log(colors.blue + '>>> Validating image references in documentation' + colors.reset);

// Find all markdown files
const files = [
  ...findMarkdownFiles(docsDir),
  ...findMarkdownFiles(versionedDocsDir)
];

console.log(colors.gray + `Found ${files.length} markdown files to scan` + colors.reset);

// Validate each file
files.forEach(validateFile);

// Print results
printResults();
