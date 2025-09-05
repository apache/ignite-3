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
    private final String oldVersion;
    private final String newVersion;
    private final String exclude;
    private final boolean errorOnIncompatibility;

    CompatibilityInput(String module, String oldVersion, ApiCompatibilityTest annotation) {
        this.module = module;
        this.oldVersion = oldVersion;
        this.newVersion = annotation.newVersion();
        this.exclude = annotation.exclude();
        this.errorOnIncompatibility = annotation.errorOnIncompatibility();
    }

    String module() {
        return module;
    }

    String oldVersion() {
        return oldVersion;
    }

    String newVersion() {
        return newVersion;
    }

    String oldVersionNotation() {
        return "org.apache.ignite:" + module + ":" + oldVersion;
    }

    String newVersionNotation() {
        return "org.apache.ignite:" + module + ":" + newVersion;
    }

    String exclude() {
        return exclude;
    }

    boolean errorOnIncompatibility() {
        return errorOnIncompatibility;
    }
}
