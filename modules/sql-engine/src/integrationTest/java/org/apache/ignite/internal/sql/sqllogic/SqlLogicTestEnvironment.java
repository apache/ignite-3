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

package org.apache.ignite.internal.sql.sqllogic;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Scripts run environment.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface SqlLogicTestEnvironment {
    /**
     * Test scripts root directory.
     *
     * @return Test scripts root directory.
     */
    String scriptsRoot();

    /**
     * The Regular expression may be used by debug / development runs of the test scripts
     * to specify only necessary tests to run.
     *
     * @return Regular expression to filter test path to execute.
     */
    String regex() default "";

    /**
     * Ignite nodes count.
     *
     * @return Ignite nodes count.
     */
    int nodes() default 2;

    /**
     * Test timeout (3 min by default).
     *
     * @return Test timeout in millis.
     */
    long timeout() default 10 * 60 * 1000L;

    /**
     * Cluster restart mode.
     *
     * @return Cluster restart mode.
     */
    RestartMode restart() default RestartMode.NONE;

    /**
     * Cluster restart mode.
     */
    enum RestartMode {
        /** Cluster isn't restarted. */
        NONE,

        /** Restart cluster for each tests' folder. */
        FOLDER,

        /** Restart cluster for each test script. */
        TEST
    }
}
