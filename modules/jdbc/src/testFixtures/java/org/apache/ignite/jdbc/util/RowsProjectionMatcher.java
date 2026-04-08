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

package org.apache.ignite.jdbc.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.hamcrest.Description;
import org.hamcrest.SelfDescribing;
import org.hamcrest.TypeSafeMatcher;
import org.jetbrains.annotations.Nullable;

/** Matcher for result set rows column projection. */
public abstract class RowsProjectionMatcher<T> extends TypeSafeMatcher<List<T>> {

    private @Nullable MismatchDescriber mismatch;

    @Override
    protected final boolean matchesSafely(List<T> projection) {
        try {
            doMatch(projection);
            return true;

        } catch (MismatchException e) {
            mismatch = e.describer();
            return false;
        }
    }

    /**
     * Performs the actual matching.
     *
     * @throws MismatchException on first mismatch.
     */
    abstract void doMatch(List<T> projection) throws MismatchException;

    @Override
    protected void describeMismatchSafely(List<T> projection, Description description) {
        assert mismatch != null;
        description.appendText("Actual projection " + projection + " ");
        mismatch.describeTo(description);
    }

    /** Matcher which ensures that checked projection contains all values from provided list. */
    @SafeVarargs
    public static <T> RowsProjectionMatcher<T> hasValuesInAnyOrder(T... expectedVals) {
        return hasValues("Rows projection with following values in any order ", (projection, expectedProjection) -> {

            Map<T, Integer> remaining = new HashMap<>();
            for (T value : expectedProjection) {
                remaining.merge(value, 1, Integer::sum);
            }

            int expectedSize = expectedProjection.size();
            int actualSize = projection.size();

            for (int index = 0; index < actualSize; index++) {

                if (index >= expectedSize) {
                    throw MismatchException.tooManyRecords(actualSize, expectedSize,
                            projection.subList(index, actualSize)
                    );
                }

                T actual = projection.get(index);
                Integer count = remaining.get(actual);

                if (count == null) {
                    throw MismatchException.unexpectedValue(actual, index,
                            remaining.entrySet().stream()
                                    .flatMap(e -> Collections.nCopies(e.getValue(), e.getKey()).stream())
                                    .collect(Collectors.toList()));
                }

                if (count == 1) {
                    remaining.remove(actual);
                } else {
                    remaining.put(actual, count - 1);
                }
            }

            if (!remaining.isEmpty()) {
                throw MismatchException.notEnoughRecords(actualSize, expectedSize,
                        remaining.entrySet().stream()
                                .flatMap(e -> Collections.nCopies(e.getValue(), e.getKey()).stream())
                                .collect(Collectors.toList()));
            }

        }, expectedVals);
    }

    /** Matcher which ensures that checked projection contains all values from provided list in same order. */
    @SafeVarargs
    public static <T> RowsProjectionMatcher<T> hasValuesOrder(T... expectedVals) {
        return hasValues("Rows projection with following values order ", (projection, expectedProjection) -> {

            int expectedSize = expectedProjection.size();
            int actualSize = projection.size();

            int index;

            for (index = 0; index < actualSize; index++) {

                if (index >= expectedSize) {
                    throw MismatchException.tooManyRecords(actualSize, expectedSize,
                            projection.subList(index, actualSize)
                    );
                }

                T actual = projection.get(index);
                T expected = expectedProjection.get(index);

                if (!Objects.equals(actual, expected)) {
                    throw MismatchException.unexpectedValue(actual, index, expected);
                }
            }

            if (index < expectedSize) {
                throw MismatchException.notEnoughRecords(actualSize, expectedSize,
                        expectedProjection.subList(index, expectedSize));
            }

        }, expectedVals);
    }

    @SafeVarargs
    private static <T> RowsProjectionMatcher<T> hasValues(
            String baseDescription,
            ProjectionCheck<T> projectionCheck,
            T... expectedVals
    ) {
        List<T> expectedValues = Arrays.asList(expectedVals);

        return new RowsProjectionMatcher<>() {

            @Override
            protected void doMatch(List<T> projection) throws MismatchException {
                int expectedSize = expectedValues.size();
                int actualSize = projection.size();

                if (expectedSize == 0 && actualSize > 0) {
                    throw MismatchException.shouldHaveBeenEmpty();
                }

                projectionCheck.check(projection, expectedValues);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(expectedValues.isEmpty()
                        ? "Empty projection"
                        : (baseDescription + expectedValues));
            }
        };
    }


    /** Encapsulates a mismatch description for {@link RowsProjectionMatcher}. */
    @FunctionalInterface
    private interface MismatchDescriber extends SelfDescribing {
        String INDENTATION = "          ";
    }

    /** Utility interface that encapsulates part of projection check. */
    @FunctionalInterface
    private interface ProjectionCheck<T> {
        void check(List<T> projection, List<T> expectedProjection) throws MismatchException;
    }

    @SuppressWarnings("CheckedExceptionClass")
    private static class MismatchException extends Exception {

        private static final long serialVersionUID = 351697710366897072L;

        private final MismatchDescriber describer;

        private MismatchException(MismatchDescriber describer) {
            this.describer = describer;
        }

        MismatchDescriber describer() {
            return describer;
        }

        /**
         * An unexpected value was encountered while matching in any order.
         *
         * @param actual The value that was actually extracted.
         * @param rowIndex Zero-based row index at which the mismatch occurred.
         * @param unmatched Remaining expected values that have not been matched yet.
         */
        static <T> MismatchException unexpectedValue(T actual, int rowIndex, List<T> unmatched) {
            return new MismatchException(description -> description
                    .appendText("has unexpected value ")
                    .appendValue(actual)
                    .appendText(" at row #" + rowIndex + ";\n")
                    .appendText(MismatchDescriber.INDENTATION + "Expected values not matched yet: " + unmatched)
            );
        }

        /**
         * A value at a specific position did not match the expected value (strict-order check).
         *
         * @param actual The value that was actually extracted.
         * @param rowIndex Zero-based row index at which the mismatch occurred.
         * @param expected The value that was expected at this position.
         */
        static <T> MismatchException unexpectedValue(T actual, int rowIndex, T expected) {
            return new MismatchException(description -> description
                    .appendText("at row #" + rowIndex + " has ")
                    .appendValue(actual)
                    .appendText(" while ")
                    .appendValue(expected)
                    .appendText(" was expected")
            );
        }

        /**
         * Projection contains more rows than expected.
         *
         * @param actualSize Number of rows actually included by projection.
         * @param expectedSize Total number of rows that were expected.
         * @param redundant The tail of the projection list that wasn't expected.
         */
        static <T> MismatchException tooManyRecords(int actualSize, int expectedSize, List<T> redundant) {
            return new MismatchException(description -> description
                    .appendText("has more rows than expected, got " + actualSize + " but " + expectedSize + " was expected;\n")
                    .appendText(MismatchDescriber.INDENTATION + "Redundant values: " + redundant)
            );
        }

        /**
         * Projection contains supposed to be empty.
         */
        static <T> MismatchException shouldHaveBeenEmpty() {
            return new MismatchException(description -> description
                    .appendText("has values")
            );
        }

        /**
         * The projection ended before all expected values were seen.
         *
         * @param actualSize Number of rows actually included by projection.
         * @param expectedSize Total number of rows that were expected.
         * @param missing The tail of the expected list that was never reached.
         */
        static <T> MismatchException notEnoughRecords(int actualSize, int expectedSize, List<T> missing) {
            return new MismatchException(description -> description
                    .appendText("has fewer rows than expected, got " + actualSize + " but " + expectedSize + " was expected;\n")
                    .appendText(MismatchDescriber.INDENTATION + "Missing values: " + missing)
            );
        }
    }
}
