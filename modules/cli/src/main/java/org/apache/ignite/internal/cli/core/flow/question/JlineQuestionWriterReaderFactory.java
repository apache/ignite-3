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

package org.apache.ignite.internal.cli.core.flow.question;

import org.jline.builtins.Completers.FileNameCompleter;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.Parser;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;

/** Creates {@link QuestionWriterReader} using custom {@link LineReader}. **/
public class JlineQuestionWriterReaderFactory implements QuestionWriterReaderFactory {
    private final Terminal terminal;

    public JlineQuestionWriterReaderFactory(Terminal terminal) {
        this.terminal = terminal;
    }

    @Override
    public QuestionWriterReader createWriterReader(boolean completeFilePaths) {
        Parser parser = new DefaultParser().escapeChars(null);
        LineReaderBuilder builder = LineReaderBuilder.builder()
                .terminal(terminal)
                .parser(parser);
        if (completeFilePaths) {
            builder.completer(new FileNameCompleter());
        }
        LineReader reader = builder.build();
        return new JlineQuestionWriterReader(reader);
    }
}
