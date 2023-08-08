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
import static org.hamcrest.io.FileMatchers.aFileNamed;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

/**
 * File matchers.
 */
public final class FileMatchers {

    private FileMatchers() {
    }

    /**
     * Matcher that checks if file has the same content and name as expected file.
     *
     * @param expectedFile Expected file.
     * @return Matcher that checks if file has the same content and name as expected file.
     */
    public static Matcher<File> hasSameContentAndName(File expectedFile) {
        return both(hasSameContent(expectedFile)).and(aFileNamed(equalTo(expectedFile.getName())));
    }

    /**
     * Matcher that checks if file has the same content as expected file.
     *
     * @param expectedFile Expected file.
     * @return Matcher that checks if file has the same content as expected file.
     */
    public static Matcher<File> hasSameContent(File expectedFile) {
        return hasContent(equalTo(readFile(expectedFile)));
    }

    /**
     * Creates a matcher for matching file content.
     *
     * @param contentMatcher Matcher for matching file content.
     * @return Matcher for matching file content.
     */
    public static Matcher<File> hasContent(Matcher<byte[]> contentMatcher) {
        return new FeatureMatcher<>(contentMatcher, "A file with content", "content") {
            @Override
            protected byte[] featureValueOf(File actual) {
                return readFile(actual);
            }
        };
    }

    private static byte[] readFile(File file) {
        try {
            return Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            throw new RuntimeException("Could not read file content", e);
        }
    }

}
