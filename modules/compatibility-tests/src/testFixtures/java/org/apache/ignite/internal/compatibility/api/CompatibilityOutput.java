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

import japicmp.cmp.JarArchiveComparator;
import japicmp.config.Options;
import japicmp.model.JApiClass;
import java.util.List;

/**
 * Japicmp comparison output. Can be handy for custom {@link japicmp.output.OutputGenerator}
 */
public class CompatibilityOutput {
    private final Options options;
    private final List<JApiClass> javaApiClasses;
    private final JarArchiveComparator jarArchiveComparator;

    CompatibilityOutput(Options options, List<JApiClass> javaApiClasses, JarArchiveComparator jarArchiveComparator) {
        this.options = options;
        this.javaApiClasses = javaApiClasses;
        this.jarArchiveComparator = jarArchiveComparator;
    }

    Options options() {
        return options;
    }

    List<JApiClass> javaApiClasses() {
        return javaApiClasses;
    }

    JarArchiveComparator jarArchiveComparator() {
        return jarArchiveComparator;
    }
}
