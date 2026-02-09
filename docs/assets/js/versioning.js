// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

function initVersioning() {
    const dropdown = document.getElementById("version-selector");
    if (!dropdown) return;

    const currentPath = document.location.pathname;
    const isIgnite3 = currentPath.indexOf("/docs/ignite3/") !== -1;
    const versionsFile = isIgnite3
      ? '/docs/ignite3/available-versions.txt'
      : '/docs/available-versions.txt';

    const parts = currentPath.split('/');
    let currentDocVersion = "";
    if (isIgnite3) {
        currentDocVersion = parts[3];
    } else {
        currentDocVersion = parts[2];
    }

    function replaceVersion(newVersion) {
        const p = currentPath.split('/');
        if (isIgnite3) {
            p[3] = newVersion;
        } else {
            p[2] = newVersion;
        }
        return p.join('/');
    }

    fetch(versionsFile)
      .then(function(response) {
          if (response.status !== 200) {
              console.warn("Problem fetching versions: " + response.status);
              return;
          }
          return response.text();
      })
      .then(function(data) {
          if (typeof data !== 'string') return;
          const lines = data.split('\n');
          dropdown.innerHTML = "";

          lines.forEach(function(version) {
              version = version.trim();
              if (version.length > 0) {
                  const option = document.createElement('option');
                  option.text = version;
                  option.value = replaceVersion(version);
                  if (version === currentDocVersion) {
                      option.selected = true;
                  }
                  dropdown.add(option);
              }
          });

          dropdown.addEventListener('change', function() {
              window.location.href = this.value;
          });
      })
      .catch(function(err) {
          console.error("Error fetching versions", err);
      });
}

function initProductSelector() {
    const productSelector = document.getElementById("product-selector");
    if (!productSelector) return;

    productSelector.addEventListener('change', function() {
        window.location.href = this.value;
    });
}

window.addEventListener('load', initVersioning);
window.addEventListener('load', initProductSelector);