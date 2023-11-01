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

package org.apache.ignite.internal.sql.engine.sql;

/**
 * A service whose sole purpose is to take a query string and convert it into a syntax tree according to the rules of grammar.
 */
@SuppressWarnings("InterfaceMayBeAnnotatedFunctional")
public interface ParserService<T> {
    /**
     * Takes a query string and convert it into a syntax tree according to the rules of grammar.
     *
     * @param query A query to convert.
     * @return Result of the parsing.
     */
    T parse(String query);
}
