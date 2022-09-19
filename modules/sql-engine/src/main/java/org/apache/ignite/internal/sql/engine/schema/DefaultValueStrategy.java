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

package org.apache.ignite.internal.sql.engine.schema;

/**
 * The strategy of default value computation for a particular column.
 */
public enum DefaultValueStrategy {
    /** Default value is not specified, thus {@code null} will be used instead. */
    DEFAULT_NULL,

    /**
     * Default value is specified as a constant, thus may be inlined into the final plan.
     *
     * <p>Note: the value may still be {@code null} if someone specify this explicitly.
     */
    DEFAULT_CONSTANT,

    /** Default value is specified as an expression and will be evaluated at a runtime. */
    DEFAULT_COMPUTED
}
