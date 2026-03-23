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

package org.apache.ignite.internal.sql.engine.api.expressions;

/**
 * Exception thrown when a given expression is semantically invalid (e.g., type mismatches, undefined columns).
 *
 * <p>Some examples are:<ul>
 *     <li>Expression is expected to return value of type T1, but given expression returns value of type T2, and no implicit cast from T1
 *     to T2 exists.</li>
 *     <li>There is reference to undeclared column or row alias.</li>
 *     <li>Operator or function argument's type mismatch.</li>
 * </ul>
 */
public class ExpressionValidationException extends Exception {
    private static final long serialVersionUID = 2040204247361408378L;

    /** Constructs the exception. */
    public ExpressionValidationException(String message) {
        super(message, null, true, false);
    }
}
