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

package org.apache.ignite.internal.deployment;

import static org.hamcrest.Matchers.is;

import java.util.Objects;
import java.util.function.Function;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.deployunit.DeploymentStatus;
import org.apache.ignite.internal.deployunit.UnitStatus;
import org.apache.ignite.internal.deployunit.UnitStatuses;
import org.apache.ignite.internal.deployunit.UnitVersionStatus;
import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matchers for {@link UnitStatus} and {@link UnitStatuses}.
 */
public class UnitStatusMatchers {
    /**
     * Matches replication info with a specified fst progress.
     */
    public static Matcher<UnitStatus> versionIs(Version version) {
        return unitStatusFeatureMatcher(UnitStatus::version, version, "version");
    }

    public static Matcher<UnitStatus> deploymentStatusIs(DeploymentStatus status) {
        return unitStatusFeatureMatcher(UnitStatus::status, status, "status");
    }

    /**
     * Creates a matcher for {@link UnitVersionStatus} with specified version and status.
     */
    public static Matcher<UnitVersionStatus> unitVersionStatusIs(Version version, DeploymentStatus status) {
        return new TypeSafeMatcher<>() {
            @Override
            public void describeTo(Description description) {
                description.appendText("unitVersionStatus is ").appendValue(version)
                        .appendText(", status is ").appendValue(status);
            }

            @Override
            protected boolean matchesSafely(UnitVersionStatus item) {
                return item.getStatus() == status && Objects.equals(item.getVersion(), version);
            }
        };
    }

    /**
     * Creates a matcher for {@code versionStatuses} of the {@link UnitStatuses}.
     *
     * @param matcher A matcher to match the list of {@link UnitVersionStatus}.
     * @return Created matcher.
     */
    public static Matcher<UnitStatuses> versionStatuses(Matcher<Iterable<? extends UnitVersionStatus>> matcher) {
        return new FeatureMatcher<>(matcher, "unit statuses with versions and statuses list", "versions and statuses") {
            @Override
            protected Iterable<? extends UnitVersionStatus> featureValueOf(UnitStatuses actual) {
                return actual.versionStatuses();
            }
        };
    }

    private static <T> Matcher<UnitStatus> unitStatusFeatureMatcher(Function<UnitStatus, T> extractor, T actual, String featureName) {
        return new FeatureMatcher<>(is(actual), "unit status with " + featureName, featureName) {
            @Override
            protected T featureValueOf(UnitStatus actual) {
                return extractor.apply(actual);
            }
        };
    }
}
