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

package org.apache.ignite.cli.core.flow.question;

import org.apache.ignite.cli.core.flow.FlowInterruptException;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.MaskingCallback;
import org.jline.reader.UserInterruptException;
import org.jline.widget.TailTipWidgets;

/**
 * Implementation of {@link QuestionWriterReader} based on {@link LineReader}.
 */
public class JlineQuestionWriterReader implements QuestionWriterReader {
    private final LineReader reader;
    private final TailTipWidgets widgets;

    public JlineQuestionWriterReader(LineReader reader, TailTipWidgets widgets) {
        this.reader = reader;
        this.widgets = widgets;
    }

    public JlineQuestionWriterReader(LineReader reader) {
        this(reader, null);
    }

    /** {@inheritDoc} */
    @Override
    public String readAnswer(String question) {
        if (widgets != null) {
            widgets.disable();
        }
        reader.setVariable(LineReader.DISABLE_HISTORY, true);
        String s = readLine(reader, question);
        reader.setVariable(LineReader.DISABLE_HISTORY, false);
        if (widgets != null) {
            widgets.enable();
        }
        return s;
    }

    private String readLine(LineReader reader, String question) {
        try {
            return reader.readLine(question, null, (MaskingCallback) null, null);
        } catch (UserInterruptException /* Ctrl-C pressed */ | EndOfFileException /* Ctrl-D pressed */ ignored) {
            throw new FlowInterruptException();
        }
    }
}
