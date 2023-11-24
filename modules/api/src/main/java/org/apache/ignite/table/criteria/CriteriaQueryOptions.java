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

package org.apache.ignite.table.criteria;

/**
 * Criteria query options.
 */
public class CriteriaQueryOptions {
    /** Default options. */
    public static final CriteriaQueryOptions DEFAULT = builder().build();

    private final int pageSize;

    /**
     * Constructor.
     *
     * @param pageSize Page size.
     */
    private CriteriaQueryOptions(int pageSize) {
        this.pageSize = pageSize;
    }

    /**
     * Creates a new builder.
     *
     * @return Builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Gets a page size - the maximum number of result rows that can be fetched at a time.
     *
     * @return Batch size.
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * Builder.
     */
    public static class Builder {
        private int pageSize = 1000;

        /**
         * Sets the page size (the number of entries that will be sent to the cluster in one network call).
         *
         * @param pageSize Page size.
         * @return This builder instance.
         */
        public Builder pageSize(int pageSize) {
            if (pageSize <= 0) {
                throw new IllegalArgumentException("Page size must be positive: " + pageSize);
            }

            this.pageSize = pageSize;

            return this;
        }

        /**
         * Builds the options.
         *
         * @return Criteria query options.
         */
        public CriteriaQueryOptions build() {
            return new CriteriaQueryOptions(pageSize);
        }
    }
}
