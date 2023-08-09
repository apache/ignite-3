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

import static org.hamcrest.Matchers.containsInAnyOrder;

import java.nio.file.Path;
import java.util.List;
import org.apache.ignite.internal.testframework.matchers.PathMatcher;
import org.hamcrest.Matcher;

/**
 * Path assertions.
 */
public class PathAssertions {

    /**
     * Asserts that the given list of paths contains the same files as the expected list.
     *
     * @param expectedFiles The expected list of files.
     * @return A matcher that will match if the given list of paths contains the same files as the expected list.
     */
    public static Matcher<Iterable<? extends Path>> namesAndContentEquals(List<Path> expectedFiles) {
        Matcher<Path>[] matchers = expectedFiles.stream()
                .map(PathMatcher::hasSameContentAndName)
                .toArray(Matcher[]::new);
        return containsInAnyOrder(matchers);
    }
}
