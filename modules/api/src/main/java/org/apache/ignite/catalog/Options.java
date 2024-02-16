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
    private boolean prettyPrint = false;

    private boolean quoteIdentifiers = false;

    private int indentWidth = 2;

    /**
     * Constructs an options object with default values - prints queries without line breaks, doesn't quote identifiers.
     *
     * @return Options object.
     */
    public static Options defaultOptions() {
        return new Options();
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
     * Enables pretty printing.
     *
     * @return Options object.
     */
    public Options prettyPrint() {
        return prettyPrint(true);
    }

    /**
     * Sets pretty printing.
     *
     * @param b If {@code true}, pretty printing is enabled.
     * @return Options object.
     */
    public Options prettyPrint(boolean b) {
        this.prettyPrint = b;
        return this;
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
     * Enables quoting identifiers.
     *
     * @return Options object.
     */
    public Options quoteIdentifiers() {
        return quoteIdentifiers(true);
    }

    /**
     * Sets quoting identifiers.
     *
     * @param b If {@code true}, quotes identifiers.
     * @return Options object.
     */
    public Options quoteIdentifiers(boolean b) {
        this.quoteIdentifiers = b;
        return this;
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
     * Sets indentation width.
     *
     * @param width Indentation width.
     * @return Options object.
     */
    public Options indentWidth(int width) {
        this.indentWidth = width;
        return this;
    }
}
