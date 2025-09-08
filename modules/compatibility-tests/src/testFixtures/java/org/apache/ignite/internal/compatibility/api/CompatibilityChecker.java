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

package org.apache.ignite.internal.compatibility.api;

import japicmp.cli.CliParser;
import japicmp.cli.JApiCli;
import japicmp.cmp.JarArchiveComparator;
import japicmp.cmp.JarArchiveComparatorOptions;
import japicmp.config.Options;
import japicmp.exception.JApiCmpException;
import japicmp.model.JApiClass;
import japicmp.output.html.HtmlOutput;
import japicmp.output.html.HtmlOutputGenerator;
import japicmp.output.html.HtmlOutputGeneratorOptions;
import japicmp.output.incompatible.IncompatibleErrorOutput;
import japicmp.output.markdown.MarkdownOutputGenerator;
import japicmp.output.semver.SemverOut;
import japicmp.output.stdout.StdoutOutputGenerator;
import japicmp.output.xml.XmlOutput;
import japicmp.output.xml.XmlOutputGenerator;
import japicmp.output.xml.XmlOutputGeneratorOptions;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.apache.ignite.internal.Dependencies;
import org.apache.ignite.internal.util.ArrayUtils;

class CompatibilityChecker {

    /**
     * Runs japicmp with given input and returns the output.
     *
     * @see <a href="https://siom79.github.io/japicmp/CliTool.html">japicmp options</a>
     */
    static CompatibilityOutput check(CompatibilityInput input) {
        String[] args = {
                "--old", Dependencies.path(input.oldVersionNotation(), false),
                "--new", Dependencies.path(input.newVersionNotation(), false),
                "--exclude", input.exclude(),
                "--markdown",
                "--only-incompatible",
                "--ignore-missing-classes",
                "--html-file", "build/reports/" + input.module() + "-japicmp.html",
                "--xml-file", "build/reports/" + input.module() + "-japicmp.xml",
        };

        if (input.errorOnIncompatibility()) {
            args = ArrayUtils.concat(args,
                    "--error-on-source-incompatibility"
            );
        }

        Options options = new CliParser().parse(args);
        JarArchiveComparator jarArchiveComparator = new JarArchiveComparator(JarArchiveComparatorOptions.of(options));
        List<JApiClass> javaApiClasses = jarArchiveComparator.compare(options.getOldArchives(), options.getNewArchives());
        return new CompatibilityOutput(options, javaApiClasses, jarArchiveComparator);
    }

    static void generateOutput(CompatibilityOutput output) {
        generateOutput(output.options(), output.javaApiClasses(), output.jarArchiveComparator());
        // use custom output generator to throw exceptions and list of incompatibilities
    }

    /**
     * Origin method is private.
     *
     * @see JApiCli#generateOutput(Options, List, JarArchiveComparator)
     */
    private static void generateOutput(Options options, List<JApiClass> javaApiClasses, JarArchiveComparator jarArchiveComparator) {
        if (options.isSemanticVersioning()) {
            SemverOut semverOut = new SemverOut(options, javaApiClasses);
            String output = semverOut.generate();
            System.out.println(output);
            return;
        }
        SemverOut semverOut = new SemverOut(options, javaApiClasses);
        if (options.getXmlOutputFile().isPresent()) {
            XmlOutputGeneratorOptions xmlOutputGeneratorOptions = new XmlOutputGeneratorOptions();
            xmlOutputGeneratorOptions.setCreateSchemaFile(true);
            xmlOutputGeneratorOptions.setSemanticVersioningInformation(semverOut.generate());
            XmlOutputGenerator xmlGenerator = new XmlOutputGenerator(javaApiClasses, options, xmlOutputGeneratorOptions);
            try (XmlOutput xmlOutput = xmlGenerator.generate()) {
                XmlOutputGenerator.writeToFiles(options, xmlOutput);
            } catch (Exception e) {
                throw new JApiCmpException(JApiCmpException.Reason.IoException, "Could not write XML file: " + e.getMessage(), e);
            }
        }
        if (options.getHtmlOutputFile().isPresent()) {
            HtmlOutputGeneratorOptions htmlOutputGeneratorOptions = new HtmlOutputGeneratorOptions();
            htmlOutputGeneratorOptions.setSemanticVersioningInformation(semverOut.generate());
            HtmlOutputGenerator outputGenerator = new HtmlOutputGenerator(javaApiClasses, options, htmlOutputGeneratorOptions);
            HtmlOutput htmlOutput = outputGenerator.generate();
            try {
                Files.write(Paths.get(options.getHtmlOutputFile().get()), htmlOutput.getHtml().getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new JApiCmpException(JApiCmpException.Reason.IoException, "Could not write HTML file: " + e.getMessage(), e);
            }
        }
        if (options.isMarkdown()) {
            MarkdownOutputGenerator markdownOutputGenerator = new MarkdownOutputGenerator(options, javaApiClasses);
            String output = markdownOutputGenerator.generate();
            System.out.println(output);
        } else {
            StdoutOutputGenerator stdoutOutputGenerator = new StdoutOutputGenerator(options, javaApiClasses);
            String output = stdoutOutputGenerator.generate();
            System.out.println(output);
        }
        if (options.isErrorOnBinaryIncompatibility()
                || options.isErrorOnSourceIncompatibility()
                || options.isErrorOnExclusionIncompatibility()
                || options.isErrorOnModifications()
                || options.isErrorOnSemanticIncompatibility()) {
            IncompatibleErrorOutput errorOutput = new IncompatibleErrorOutput(options, javaApiClasses, jarArchiveComparator);
            errorOutput.generate();
        }
    }

}
