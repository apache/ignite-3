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

package org.apache.ignite.internal.cli.core.style.element;

import static org.apache.ignite.internal.cli.core.style.AnsiStringSupport.fg;

import org.apache.ignite.internal.cli.core.style.AnsiStringSupport.Color;
import org.apache.ignite.internal.cli.core.style.AnsiStringSupport.Style;

/** Defines all UI Elements that are used in the CLI. */
public class UiElements {
    /** URL UI element. */
    public static UiElement url(String content) {
        return new MarkedUiElement(content, Style.UNDERLINE);
    }

    /** Command UI element. */
    public static UiElement command(String content) {
        return new MarkedUiElement(content, Style.BOLD);
    }

    /** Deployment unit UI element. */
    public static UiElement unit(String id, String version) {
        if (version == null) {
            return new MarkedUiElement(id, Style.BOLD);
        } else {
            return new MarkedUiElement(id + ":" + version, Style.BOLD);
        }
    }

    /** Option UI element. */
    public static UiElement option(String content) {
        return new MarkedUiElement(content, fg(Color.YELLOW));
    }

    /** Done UI element. */
    public static UiElement done() {
        return new MarkedUiElement("Done", fg(Color.GREEN).with(Style.BOLD));
    }

    /** UI element for the [Y/n] option. */
    public static UiElement yesNo() {
        return new MarkedUiElement("[Y/n]", fg(Color.GRAY));
    }

    /** Username UI element. */
    public static UiElement username(String content) {
        return () -> content;
    }
}
