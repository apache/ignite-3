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

import rr, * as rrClass from "./railroad.js";
Object.assign(window, rr);
window.rrOptions = rrClass.Options;

let elements = document.querySelectorAll('.diagram-container p');
[].forEach.call(elements, function(el) {
    let result = eval(el.innerHTML).format();
    let diagramContainer = el.closest('.diagram-container');
    diagramContainer.innerHTML = '';
    result.addTo(diagramContainer);
})
