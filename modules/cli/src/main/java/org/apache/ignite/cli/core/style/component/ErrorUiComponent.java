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

package org.apache.ignite.cli.core.style.component;

import static org.apache.ignite.cli.core.style.AnsiStringSupport.ansi;
import static org.apache.ignite.cli.core.style.AnsiStringSupport.fg;

import java.util.UUID;
import org.apache.ignite.cli.core.style.AnsiStringSupport.Color;
import org.apache.ignite.cli.core.style.AnsiStringSupport.Style;
import org.apache.ignite.cli.core.style.element.UiElement;
import org.apache.ignite.cli.core.style.element.UiString;

/**
 * UI component that represent any error message.
 */
public class ErrorUiComponent implements UiComponent {
    private final String header;

    private final UiElement[] headerUiElements;

    private final String details;

    private final UiElement[] detailsUiElements;

    private final UUID traceId;

    private final String errorCode;

    private ErrorUiComponent(
            String header, UiElement[] headerUiElements,
            String details, UiElement[] detailsUiElements,
            UUID traceId,
            String errorCode) {
        this.header = header;
        this.headerUiElements = headerUiElements;
        this.details = details;
        this.detailsUiElements = detailsUiElements;
        this.traceId = traceId;
        this.errorCode = errorCode;
    }

    /** Creates ErrorComponent from given header. */
    public static ErrorUiComponent fromHeader(String header) {
        return builder().header(header).build();
    }

    /** Builder. */
    public static ErrorComponentBuilder builder() {
        return new ErrorComponentBuilder();
    }

    public String header() {
        return header;
    }

    public String details() {
        return details;
    }

    @Override
    public String render() {
        return ansi(
                (errorCode == null ? "" : fg(Color.GRAY).mark(errorCode))
                        + traceDetails()
                        + fg(Color.RED).with(Style.BOLD).mark(ansi(UiString.format(header, headerUiElements)))
                        + (details == null ? "" : System.lineSeparator() + UiString.format(details, detailsUiElements))
        );
    }

    private String traceDetails() {
        return traceId == null ? "" : fg(Color.GRAY).mark(" Trace ID: " + traceId + System.lineSeparator());
    }

    /** Builder. */
    public static class ErrorComponentBuilder {
        private String header;

        private UiElement[] headerUiElements;

        private String details;

        private UiElement[] detailsUiElement;

        private UUID traceId;

        private String errorCode;

        /** Sets header. */
        public ErrorComponentBuilder header(String header, UiElement... uiElements) {
            this.header = header;
            this.headerUiElements = uiElements;
            return this;
        }

        /** Sets details. */
        public ErrorComponentBuilder details(String details, UiElement... uiElements) {
            this.details = details;
            this.detailsUiElement = uiElements;
            return this;
        }

        public ErrorComponentBuilder traceId(UUID traceId) {
            this.traceId = traceId;
            return this;
        }

        public ErrorComponentBuilder errorCode(String errorCode) {
            this.errorCode = errorCode;
            return this;
        }

        public ErrorUiComponent build() {
            return new ErrorUiComponent(header, headerUiElements, details, detailsUiElement, traceId, errorCode);
        }
    }
}
