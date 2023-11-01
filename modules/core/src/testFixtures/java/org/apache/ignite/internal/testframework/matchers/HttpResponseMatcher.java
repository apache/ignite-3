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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.net.http.HttpResponse;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher for {@link HttpResponse}.
 */
public class HttpResponseMatcher<T> extends TypeSafeMatcher<HttpResponse<T>> {
    private final Matcher<Integer> statusCodeMatcher;

    private final Matcher<T> bodyMatcher;

    private HttpResponseMatcher(Matcher<Integer> statusCodeMatcher, Matcher<T> bodyMatcher) {
        this.statusCodeMatcher = statusCodeMatcher;
        this.bodyMatcher = bodyMatcher;
    }

    public static <T> HttpResponseMatcher<T> hasStatusCode(int statusCode) {
        return new HttpResponseMatcher<>(is(statusCode), null);
    }

    public static <T> HttpResponseMatcher<T> hasStatusCode(Matcher<Integer> statusCodeMatcher) {
        return new HttpResponseMatcher<>(statusCodeMatcher, null);
    }

    public static <T> HttpResponseMatcher<T> hasStatusCodeAndBody(int statusCode, T body) {
        return new HttpResponseMatcher<>(is(statusCode), equalTo(body));
    }

    public static <T> HttpResponseMatcher<T> hasStatusCodeAndBody(int statusCode, Matcher<T> bodyMatcher) {
        return new HttpResponseMatcher<>(is(statusCode), bodyMatcher);
    }

    @Override
    protected boolean matchesSafely(HttpResponse<T> httpResponse) {
        if (!statusCodeMatcher.matches(httpResponse.statusCode())) {
            return false;
        }

        if (bodyMatcher != null && !bodyMatcher.matches(httpResponse.body())) {
            return false;
        }

        return true;
    }

    @Override
    public void describeTo(Description description) {
        if (statusCodeMatcher != null) {
            description.appendText("an HttpResponse with status code matching ").appendDescriptionOf(statusCodeMatcher);
        }

        if (bodyMatcher != null) {
            description.appendText(" and body ").appendDescriptionOf(bodyMatcher);
        }
    }

    @Override
    protected void describeMismatchSafely(HttpResponse<T> item, Description mismatchDescription) {
        mismatchDescription.appendText("status code was ")
                .appendValue(item.statusCode())
                .appendText(" and ")
                .appendText("body was ")
                .appendValue(item.body());
    }
}
