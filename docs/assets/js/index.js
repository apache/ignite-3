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

import './code-tabs.js?1'
import {hideLeftNav, scrollLeftNavToActive} from './docs-menu.js?20201005'
//import {hideTopNav} from './top-navigation.js'
import './page-nav.js'
import './search.js'




document.addEventListener('topNavigationShow', hideLeftNav)
// document.addEventListener('leftNavigationShow', hideTopNav)
window.addEventListener('load', scrollLeftNavToActive);


