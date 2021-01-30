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

package org.apache.ignite.cli.ui;

import java.io.PrintWriter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import picocli.CommandLine.Help.Ansi;

/**
 * Basic implementation of a progress bar.
 */
public class IgniteProgressBar implements AutoCloseable {
    /** Out to output the progress bar UI.. */
    private final PrintWriter out;

    /** Current progress. */
    private int curr;

    /** Maximum progress bar value. */
    private int max;

    /** Current width of terminal. */
    private int terminalWidth;

    /** Execute. */
    private ScheduledExecutorService exec;

    /**
     * Creates a new progress bar.
     *
     * @param initMax Initial maximum number of steps.
     */
    public IgniteProgressBar(PrintWriter out, int initMax, int terminalWidth) {
        this.out = out;

        assert initMax > 0;

        max = initMax;

        this.terminalWidth = terminalWidth;
    }

    public IgniteProgressBar(PrintWriter out, int initMax) {
        this(out, initMax, 80);
    }

    /**
     * Updates maximum number of steps.
     *
     * @param newMax New maximum.
     */
    public void setMax(int newMax) {
        assert newMax > 0;

        max = newMax;
    }

    /**
     * Performs a single step.
     */
    public void step() {
        if (curr < max)
            curr++;

        out.print('\r' + render());
        out.flush();
    }

    /**
     * Performs a single step every N milliseconds.
     *
     * @param interval Interval in milliseconds.
     */
    public void stepPeriodically(long interval) {
        if (exec == null) {
            exec = Executors.newSingleThreadScheduledExecutor();

            exec.scheduleAtFixedRate(this::step, interval, interval, TimeUnit.MILLISECONDS);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        while (curr < max) {
            try {
                Thread.sleep(10);
            }
            catch (InterruptedException ignored) {
                break;
            }

            step();
        }

        out.println();
    }

    /**
     * Renders current progress bar state to Ansi string.
     *
     * @return Ansi string with progress bar.
     */
    private String render() {
        assert curr <= max;

        var completedPart = ((double)curr / (double)max);

        // Space reserved for '||Done!'
        var reservedSpace = 7;
        var numOfCompletedSymbols = (int) (completedPart * (terminalWidth - reservedSpace));

        StringBuilder sb = new StringBuilder("|");

        // 1 symbol will be used buy '>' of the progress bar
        sb.append("=".repeat(Math.max(numOfCompletedSymbols - 1, 0)));

        String percentage;
        int percentageLen;
        if (completedPart < 1) {
            sb.append('>').append(" ".repeat(terminalWidth - reservedSpace - numOfCompletedSymbols));

            percentage = (int) (completedPart * 100) + "%";
            percentageLen = percentage.length();
            sb.append("|").append(" ".repeat(4 - percentageLen)).append(percentage);
        }
        else
            sb.append("=|@|green,bold Done!|@");

        return Ansi.AUTO.string(sb.toString());
    }
}
