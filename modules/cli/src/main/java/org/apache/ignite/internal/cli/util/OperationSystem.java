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

package org.apache.ignite.internal.cli.util;

import java.util.Locale;

/** Utility class for detection OS. */
public enum OperationSystem {

    /**
     * IBM AIX operating system.
     */
    AIX,

    /**
     * FreeBSD operating system.
     */
    FREEBSD,

    /**
     * Linux-based operating system.
     */
    LINUX,

    /**
     * Apple Macintosh operating system (e.g., macOS).
     */
    MAC,

    /**
     * OpenBSD operating system.
     */
    OPENBSD,

    /**
     * Oracle Solaris operating system.
     */
    SOLARIS,

    /**
     * Microsoft Windows operating system.
     */
    WINDOWS,

    /**
     * An operating system other than {@link #AIX}, {@link #FREEBSD}, {@link #LINUX}, {@link #MAC}, {@link #OPENBSD}, {@link #SOLARIS}, or
     * {@link #WINDOWS}.
     */
    OTHER;

    private static final OperationSystem CURRENT_OS = determineCurrentOs();

    public static OperationSystem current() {
        return CURRENT_OS;
    }

    private static OperationSystem determineCurrentOs() {
        return parse(System.getProperty("os.name"));
    }

    private static OperationSystem parse(String osName) {

        osName = osName.toLowerCase(Locale.ENGLISH);

        if (osName.contains("aix")) {
            return AIX;
        }
        if (osName.contains("freebsd")) {
            return FREEBSD;
        }
        if (osName.contains("linux")) {
            return LINUX;
        }
        if (osName.contains("mac")) {
            return MAC;
        }
        if (osName.contains("openbsd")) {
            return OPENBSD;
        }
        if (osName.contains("sunos") || osName.contains("solaris")) {
            return SOLARIS;
        }
        if (osName.contains("win")) {
            return WINDOWS;
        }
        return OTHER;
    }
}
