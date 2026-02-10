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

package org.apache.ignite.internal.cli.core.style.component;

import static org.apache.ignite.internal.cli.core.style.AnsiStringSupport.ansi;
import static org.apache.ignite.internal.cli.core.style.AnsiStringSupport.fg;

import java.util.UUID;
import org.apache.ignite.internal.cli.core.style.AnsiStringSupport.Color;
import org.apache.ignite.internal.cli.core.style.AnsiStringSupport.Style;
import org.apache.ignite.internal.cli.core.style.element.UiElement;
import org.apache.ignite.internal.cli.core.style.element.UiString;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.jetbrains.annotations.Nullable;

/**
 * UI component that represent any error message.
 */
public class ErrorUiComponent implements UiComponent {
    private final String header;

    private final UiElement[] headerUiElements;

    private final String details;

    private final UiElement[] detailsUiElements;

    private final String verbose;

    private final UiElement[] verboseUiElements;

    private final UUID traceId;

    private final String errorCode;

    private ErrorUiComponent(
            String header, UiElement[] headerUiElements,
            @Nullable String details, UiElement[] detailsUiElements,
            @Nullable String verbose, UiElement[] verboseUiElements,
            UUID traceId,
            String errorCode
    ) {
        this.header = header;
        this.headerUiElements = headerUiElements;
        this.details = details;
        this.detailsUiElements = detailsUiElements;
        this.verbose = verbose;
        this.verboseUiElements = verboseUiElements;
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

    @Override
    public String render() {
        return ansi(renderErrorCode() + renderTrace() + renderHeader() + renderDetails() + renderVerbose());
    }

    private String renderErrorCode() {
        return errorCode == null ? "" : fg(Color.GRAY).mark(errorCode);
    }

    private String renderTrace() {
        return traceId == null ? "" : fg(Color.GRAY).mark(" Trace ID: " + traceId + System.lineSeparator());
    }

    private String renderHeader() {
        return header == null ? "" : fg(Color.RED).with(Style.BOLD).mark(ansi(UiString.format(header, headerUiElements)));
    }

    private String renderDetails() {
        return details == null ? "" : System.lineSeparator() + UiString.format(details, detailsUiElements);
    }

    private String renderVerbose() {
        return verbose == null || !CliLoggers.isVerbose() ? "" : System.lineSeparator() + UiString.format(verbose, verboseUiElements);
    }

    /** Builder. */
    public static class ErrorComponentBuilder {
        private String header;

        private UiElement[] headerUiElements;

        @Nullable
        private String details;

        private UiElement[] detailsUiElements;

        @Nullable
        private String verbose;

        private UiElement[] verboseUiElements;

        private UUID traceId;

        private String errorCode;

        /** Sets header. */
        public ErrorComponentBuilder header(String header, UiElement... uiElements) {
            this.header = header;
            this.headerUiElements = uiElements;
            return this;
        }

        /** Sets details. */
        public ErrorComponentBuilder details(@Nullable String details, UiElement... uiElements) {
            this.details = details;
            this.detailsUiElements = uiElements;
            return this;
        }

        /** Sets details. */
        public ErrorComponentBuilder details(UiElement... uiElements) {
            return details("%s", uiElements);
        }

        /** Sets verbose. */
        public ErrorComponentBuilder verbose(@Nullable String verbose, UiElement... uiElements) {
            this.verbose = verbose;
            this.verboseUiElements = uiElements;
            return this;
        }

        /** Sets trace id. */
        public ErrorComponentBuilder traceId(UUID traceId) {
            this.traceId = traceId;
            return this;
        }

        /** Sets error code. */
        public ErrorComponentBuilder errorCode(String errorCode) {
            this.errorCode = errorCode;
            return this;
        }

        /** Builds the component. */
        public ErrorUiComponent build() {
            return new ErrorUiComponent(
                    header, headerUiElements,
                    details, detailsUiElements,
                    verbose, verboseUiElements,
                    traceId,
                    errorCode
            );
        }
    }
}
