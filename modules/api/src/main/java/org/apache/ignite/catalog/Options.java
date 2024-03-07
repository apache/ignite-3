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

package org.apache.ignite.catalog;

/**
 * SQL query generation options.
 */
public class Options {
    /**
     * Default options object - prints queries without line breaks, doesn't quote identifiers.
     */
    public static final Options DEFAULT = new Options(false, false, 2);

    private final boolean prettyPrint;

    private final boolean quoteIdentifiers;

    private final int indentWidth;

    private Options(boolean prettyPrint, boolean quoteIdentifiers, int indentWidth) {
        this.prettyPrint = prettyPrint;
        this.quoteIdentifiers = quoteIdentifiers;
        this.indentWidth = indentWidth;
    }

    /**
     * Creates new builder for options.
     *
     * @return New builder instance.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns pretty printing status.
     *
     * @return {@code true} if pretty printing is enabled.
     */
    public boolean isPrettyPrint() {
        return prettyPrint;
    }

    /**
     * Returns quoting identifiers status.
     *
     * @return {@code true} if identifiers are quoted.
     */
    public boolean isQuoteIdentifiers() {
        return quoteIdentifiers;
    }

    /**
     * Returns indentation width.
     *
     * @return Indentation width.
     */
    public int indentWidth() {
        return indentWidth;
    }

    /**
     * Builder class for {@link Options}.
     */
    public static class Builder {
        private boolean prettyPrint = false;

        private boolean quoteIdentifiers = false;

        private int indentWidth = 2;

        /**
         * Enables pretty printing.
         *
         * @return Builder object.
         */
        public Builder prettyPrint() {
            return prettyPrint(true);
        }

        /**
         * Sets pretty printing.
         *
         * @param b If {@code true}, pretty printing is enabled.
         * @return Builder object.
         */
        public Builder prettyPrint(boolean b) {
            this.prettyPrint = b;
            return this;
        }

        /**
         * Enables quoting identifiers.
         *
         * @return Builder object.
         */
        public Builder quoteIdentifiers() {
            return quoteIdentifiers(true);
        }

        /**
         * Sets quoting identifiers.
         *
         * @param b If {@code true}, quotes identifiers.
         * @return Builder object.
         */
        public Builder quoteIdentifiers(boolean b) {
            this.quoteIdentifiers = b;
            return this;
        }

        /**
         * Sets indentation width.
         *
         * @param width Indentation width.
         * @return Builder object.
         */
        public Builder indentWidth(int width) {
            this.indentWidth = width;
            return this;
        }

        /**
         * Builds options object.
         *
         * @return Constructed options object.
         */
        public Options build() {
            return new Options(prettyPrint, quoteIdentifiers, indentWidth);
        }
    }
}
