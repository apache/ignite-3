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

package org.apache.ignite.cli.core.flow.builder;

import java.io.PrintWriter;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.ignite.cli.core.exception.ExceptionHandler;
import org.apache.ignite.cli.core.flow.Flow;
import org.apache.ignite.cli.core.flow.question.QuestionAnswer;
import org.apache.ignite.cli.core.repl.context.CommandLineContext;

/**
 * Builder of {@link Flow}.
 *
 * @param <I> input type.
 * @param <O> output type.
 */
public interface FlowBuilder<I, O>  {

    <OT> FlowBuilder<I, OT> then(Flow<O, OT> flow);

    default <OT> FlowBuilder<I, OT> map(Function<O, OT> mapper) {
        return then(Flows.mono(mapper));
    }

    <OT> FlowBuilder<I, O> ifThen(Predicate<O> tester, Flow<O, OT> flow);

    <QT> FlowBuilder<I, QT> question(String questionText, List<QuestionAnswer<O, QT>> answers);

    <QT> FlowBuilder<I, QT> question(Function<O, String> questionText, List<QuestionAnswer<O, QT>> answers);

    FlowBuilder<I, O> exceptionHandler(ExceptionHandler<?> exceptionHandler);

    FlowBuilder<I, O> toOutput(PrintWriter output, PrintWriter errorOutput);

    default FlowBuilder<I, O> toOutput(CommandLineContext context) {
        return toOutput(context.out(), context.err());
    }

    Flow<I, O> build();
}
