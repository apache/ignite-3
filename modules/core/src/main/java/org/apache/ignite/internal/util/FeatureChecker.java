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

package org.apache.ignite.internal.util;

/**
 * Class extracted for fields from GridUnsafe to be absolutely independent with current and future static block initialization effects.
 */
public class FeatureChecker {
    /** Java specific startup parameters warning to be added in case access failed. */
    public static final String JAVA_STARTUP_PARAMS_WARN = System.lineSeparator()
            + "Please check the required parameters to JVM startup settings and restart the application."
            + " Parameters: " + System.lineSeparator()
            + "\t--add-opens java.base/java.lang=ALL-UNNAMED" + System.lineSeparator()
            + "\t--add-opens java.base/java.lang.invoke=ALL-UNNAMED" + System.lineSeparator()
            + "\t--add-opens java.base/java.lang.reflect=ALL-UNNAMED" + System.lineSeparator()
            + "\t--add-opens java.base/java.io=ALL-UNNAMED" + System.lineSeparator()
            + "\t--add-opens java.base/java.nio=ALL-UNNAMED" + System.lineSeparator()
            + "\t--add-opens java.base/java.math=ALL-UNNAMED" + System.lineSeparator()
            + "\t--add-opens java.base/java.util=ALL-UNNAMED" + System.lineSeparator()
            + "\t--add-opens java.base/java.time=ALL-UNNAMED" + System.lineSeparator()
            + "\t--add-opens java.base/jdk.internal.misc=ALL-UNNAMED" + System.lineSeparator()
            + "\t--add-opens java.base/jdk.internal.access=ALL-UNNAMED" + System.lineSeparator()
            + "\t--add-opens java.base/sun.nio.ch=ALL-UNNAMED" + System.lineSeparator()
            + "\t-Dio.netty.tryReflectionSetAccessible=true" + System.lineSeparator()
            + " See https://ignite.apache.org/docs/3.0.0-beta/quick-start/getting-started-guide for more information.";
}
