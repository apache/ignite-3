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

/**
 * Implementation of {@link QuestionWriterReader} based on {@link LineReader}.
 */
public class JlineQuestionWriterReader implements QuestionWriterReader {
    private final LineReader reader;

    public JlineQuestionWriterReader(LineReader reader) {
        this.reader = reader;
    }

    @Override
    public void write(String question) {
        System.out.print(question);
    }

    @Override
    public String readAnswer() {
        LineReader.SuggestionType prev = reader.getAutosuggestion();
        reader.setAutosuggestion(LineReader.SuggestionType.NONE);
        String s = readLine(reader);
        reader.setAutosuggestion(prev);
        return s;
    }

    private String readLine(LineReader reader) {
        try {
            return reader.readLine("", null, (MaskingCallback) null, null);
        } catch (UserInterruptException /* Ctrl-C pressed */ | EndOfFileException /* Ctrl-D pressed */ ignored) {
            throw new FlowInterruptException();
        }
    }
}
