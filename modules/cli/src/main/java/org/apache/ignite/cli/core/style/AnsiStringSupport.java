/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cli.core.style;

import picocli.CommandLine.Help.Ansi;

/**
 * Utility class with ANSI string support.
 */
public final class AnsiStringSupport {
    private AnsiStringSupport() {}

    public static String ansi(String markupText, Object... args) {
        return Ansi.AUTO.string(String.format(markupText, args));
    }

    public static Fg fg(Color color) {
        return new Fg(color);
    }

    /**
     * Dsl for ansi fg: takes color and marks string like: mytext -> @|fg(10) mytext|@ where 10 is the ansi color.
     */
    public static class Fg {
        private final Color color;

        private Fg(Color color) {
            this.color = color;
        }

        public String mark(String textToMark) {
            return String.format("@|fg(%d) %s|@", color.code, textToMark);
        }
    }

    /**
     * Represents ansi colors that are used in CLI.
     */
    public enum Color {
        RED(1), GREEN(2), YELLOW(3);

        Color(int code) {
            this.code = code;
        }

        private final int code;
    }
}
