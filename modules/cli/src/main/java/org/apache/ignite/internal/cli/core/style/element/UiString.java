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

import java.util.Arrays;

/** Can format the string with UI Elements. */
public class UiString {
    /** Accepts the String template with UI Elements. */
    public static String format(String template, UiElement... elements) {
        if (elements == null || elements.length == 0) {
            return template;
        }

        Object[] elementsAsObjects = Arrays.stream(elements).map(UiElement::represent).toArray();

        return String.format(template, elementsAsObjects);
    }
}
