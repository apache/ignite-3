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

package org.apache.ignite.internal.testframework.matchers;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

/**
 * Matchers for {@link Path}.
 */
public final class PathMatcher {

    private PathMatcher() {
    }

    /**
     * Creates a matcher for matching file content and name.
     *
     * @param expectedFile Expected file.
     * @return Matcher that checks if file has the same content and name as expected file.
     */
    public static Matcher<Path> hasSameContentAndName(Path expectedFile) {
        return both(hasSameContent(expectedFile)).and(hasSameName(expectedFile));
    }

    /**
     * Creates a matcher for matching file content.
     *
     * @param expectedFile Expected file.
     * @return Matcher for matching file content.
     */
    public static Matcher<Path> hasSameContent(Path expectedFile) {
        return hasContent(equalTo(readFile(expectedFile)));
    }

    /**
     * Creates a matcher for matching file content.
     *
     * @param contentMatcher Matcher for matching file content.
     * @return Matcher for matching file content.
     */
    public static Matcher<Path> hasContent(Matcher<byte[]> contentMatcher) {
        return new FeatureMatcher<>(contentMatcher, "A file with content", "content") {
            @Override
            protected byte[] featureValueOf(Path actual) {
                return readFile(actual);
            }
        };
    }

    /**
     * Creates a matcher for matching file name.
     *
     * @param expectedFile Expected file.
     * @return Matcher for matching file name.
     */
    public static Matcher<Path> hasSameName(Path expectedFile) {
        return hasName(equalTo(expectedFile.getFileName().toString()));
    }

    /**
     * Creates a matcher for matching file name.
     *
     * @param nameMatcher Matcher for matching file name.
     * @return Matcher for matching file name.
     */
    public static Matcher<Path> hasName(Matcher<String> nameMatcher) {
        return new FeatureMatcher<>(nameMatcher, "A file with name", "name") {
            @Override
            protected String featureValueOf(Path actual) {
                return actual.getFileName().toString();
            }
        };
    }

    private static byte[] readFile(Path path) {
        try {
            return Files.readAllBytes(path);
        } catch (IOException e) {
            throw new RuntimeException("Could not read file content", e);
        }
    }

}
