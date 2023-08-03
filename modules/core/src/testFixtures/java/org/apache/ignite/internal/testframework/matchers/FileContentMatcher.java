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

import static org.hamcrest.CoreMatchers.is;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * {@link TypeSafeMatcher} for matching file content.
 */
public class FileContentMatcher extends TypeSafeMatcher<File> {

    /** Matcher to forward the result of the completable future. */
    private final Matcher<byte[]> matcher;

    private FileContentMatcher(Matcher<byte[]> matcher) {
        this.matcher = matcher;
    }

    /**
     * Creates a matcher for matching file content.
     *
     * @param file File to match.
     * @return Matcher for matching file content.
     */
    public static FileContentMatcher hasContent(File file) {
        try {
            return new FileContentMatcher(is(Files.readAllBytes(file.toPath())));
        } catch (IOException e) {
            throw new RuntimeException("Could not read file content", e);
        }
    }

    /**
     * Creates a matcher for matching file content.
     *
     * @param matcher Matcher for matching file content.
     * @return Matcher for matching file content.
     */
    public static FileContentMatcher hasContent(Matcher<byte[]> matcher) {
        return new FileContentMatcher(matcher);
    }

    @Override
    protected boolean matchesSafely(File file) {
        try {
            return matcher.matches(Files.readAllBytes(file.toPath()));
        } catch (IOException e) {
            throw new RuntimeException("Could not read file content", e);
        }
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("matches content: ").appendDescriptionOf(matcher);
    }

    @Override
    protected void describeMismatchSafely(File item, Description mismatchDescription) {
        try {
            mismatchDescription.appendText("was ").appendValue(Files.readAllBytes(item.toPath()));
        } catch (IOException e) {
            throw new RuntimeException("Could not read file content", e);
        }
    }
}

