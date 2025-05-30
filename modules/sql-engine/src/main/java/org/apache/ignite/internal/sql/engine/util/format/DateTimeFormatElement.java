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

package org.apache.ignite.internal.sql.engine.util.format;

/**
 * Format element - a delimiter or a template field.
 */
final class DateTimeFormatElement {
    enum ElementKind {
        DELIMITER,
        FIELD
    }

    final ElementKind kind;
    final String text;
    final DateTimeTemplateField template;

    DateTimeFormatElement(char c) {
        this.kind = ElementKind.DELIMITER;
        this.text = Character.toString(c);
        this.template = null;
    }

    DateTimeFormatElement(DateTimeTemplateField template) {
        this.kind = ElementKind.FIELD;
        this.text = null;
        this.template = template;
    }

    @Override
    public String toString() {
        switch (kind) {
            case DELIMITER:
                return String.format("<%s>", text);
            case FIELD:
                return template.name();
            default:
                throw new IllegalStateException();
        }
    }
}
