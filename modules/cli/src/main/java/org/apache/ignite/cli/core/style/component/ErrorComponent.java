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

package org.apache.ignite.cli.core.style.component;

import static org.apache.ignite.cli.core.style.AnsiStringSupport.ansi;
import static org.apache.ignite.cli.core.style.AnsiStringSupport.fg;

import java.util.UUID;
import org.apache.ignite.cli.core.style.AnsiStringSupport.Color;
import org.apache.ignite.cli.core.style.AnsiStringSupport.Style;

/**
 * UI component that represent any error message.
 */
public class ErrorComponent implements Component {

    private final String header;

    private final String details;

    private final UUID traceId;

    private final String errorCode;

    private final String example;

    private ErrorComponent(String header, String details, UUID traceId, String errorCode, String example) {
        this.header = header;
        this.details = details;
        this.traceId = traceId;
        this.errorCode = errorCode;
        this.example = example;
    }

    /** Creates ErrorComponent from given header. */
    public static ErrorComponent fromHeader(String header) {
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

    public String example() {
        return example;
    }

    @Override
    public String render() {
        return ansi(
                (errorCode == null ? "" : fg(Color.GRAY).mark(errorCode))
                        + traceDetails()
                        + fg(Color.RED).with(Style.BOLD).mark(header)
                        + (details == null ? "" : System.lineSeparator() + details)
        );
    }

    private String traceDetails() {
        return traceId == null ? "" : fg(Color.GRAY).mark(" Trace ID: " + traceId + System.lineSeparator());
    }

    /** Builder. */
    public static class ErrorComponentBuilder {
        private String header;

        private String details;

        private String example;

        private UUID traceId;

        private String errorCode;

        public void setHeader(String header) {
            this.header = header;
        }

        public ErrorComponentBuilder header(String header) {
            this.header = header;
            return this;
        }

        public ErrorComponentBuilder details(String details) {
            this.details = details;
            return this;
        }

        public ErrorComponentBuilder example(String example) {
            this.example = example;
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

        public ErrorComponent build() {
            return new ErrorComponent(header, details, traceId, errorCode, example);
        }
    }
}
