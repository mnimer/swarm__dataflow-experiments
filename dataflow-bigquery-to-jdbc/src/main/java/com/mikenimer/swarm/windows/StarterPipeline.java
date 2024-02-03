/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mikenimer.swarm.windows;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.mikenimer.swarm.windows.transforms.JdbcWriter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * Test different window configurations against pubsub
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 * --project=<YOUR_PROJECT_ID>
 * --runner=DirectRunner
 * --topic=dataflow-window-experiments
 */
public class StarterPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

    public static void main(String[] args) {
        JobOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(JobOptions.class);

        build(options);
    }

    private static void build(JobOptions options) {
        Pipeline p = Pipeline.create(options);

        String[] parts = options.getBqTable().split("\\.");

        TableReference tableRef = new TableReference().setProjectId(parts[0]).setDatasetId(parts[1]).setTableId(parts[2]);
        p.apply("Read Table Data", BigQueryIO.readTableRowsWithSchema().from(tableRef).withSelectedFields(Arrays.asList(options.getBqColumnList())))
        .apply("log", ParDo.of(new DoFn<TableRow, TableRow>() {
            @ProcessElement
            public void process(ProcessContext c){
                c.output(c.element());
            }
            }))
        .apply("Write to db", new JdbcWriter(options));

        p.run();
    }



}
