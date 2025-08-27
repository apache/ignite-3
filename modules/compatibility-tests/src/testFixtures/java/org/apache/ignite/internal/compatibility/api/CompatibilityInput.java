/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.compatibility.api;

class CompatibilityInput {
    private final String module;
    private final ApiCompatibilityTest annotation;

    CompatibilityInput(String module, ApiCompatibilityTest annotation) {
        this.module = module;
        this.annotation = annotation;
    }

    String module() {
        return module;
    }

    String oldVersion() {
        return annotation.oldVersion();
    }

    String newVersion() {
        return annotation.newVersion();
    }

    String oldVersionNotation() {
        return "org.apache.ignite:" + module + ":" + annotation.oldVersion();
    }

    String newVersionNotation() {
        return "org.apache.ignite:" + module + ":" + annotation.newVersion();
    }

    String exclude() {
        return annotation.exclude();
    }

    boolean errorOnIncompatibility() {
        return annotation.errorOnIncompatibility();
    }
}
