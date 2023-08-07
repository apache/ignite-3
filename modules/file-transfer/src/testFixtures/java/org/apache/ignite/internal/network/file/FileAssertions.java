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

package org.apache.ignite.internal.network.file;

import static org.apache.ignite.internal.testframework.matchers.FileContentMatcher.hasContent;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * File assertions.
 */
public class FileAssertions {
    /**
     * Asserts that the content of two sets of files is the same.
     *
     * @param files1 First set of files.
     * @param files2 Second set of files.
     */
    public static void assertContentEquals(List<File> files1, List<File> files2) {
        assertThat(files1.size(), is(files2.size()));

        Map<String, File> map = files1.stream()
                .collect(Collectors.toMap(File::getName, file -> file));

        for (File file : files2) {
            assertThat(file, hasContent(map.get(file.getName())));
        }
    }
}
