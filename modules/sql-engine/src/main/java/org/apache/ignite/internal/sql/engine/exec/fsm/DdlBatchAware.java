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

package org.apache.ignite.internal.sql.engine.exec.fsm;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Annotation that defines how multi-statement handler executes DDL statement.
 *
 * <p>DDL statements of the same {@link DdlBatchGroup} can be executed in together within the same batch except {@link DdlBatchGroup#OTHER}.
 * Statements marked as {@link DdlBatchGroup#OTHER} can be executed only separately, this is the default behavior.
 */
@Target({TYPE})
@Retention(RUNTIME)
public @interface DdlBatchAware {
    /** Returns DDL batch group. */
    DdlBatchGroup group() default DdlBatchGroup.OTHER;
}
