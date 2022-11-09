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
package com.mikenimer.swarm.globalsequence;

import com.google.cloud.bigquery.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test different window configurations against pubsub
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 * --project=<YOUR_PROJECT_ID>
 * --runner=DirectRunner
 * --topic=dataflow-window-experiments
 */
public class CreateTablesBug {
    private static final Logger log = LoggerFactory.getLogger(CreateTablesBug.class);

    public interface JobOptions extends GcpOptions {


        String getDataset();
        void setDataset(String value);

        @Default.Integer(20)
        Integer getTablesPerSecond();
        void setTablesPerSecond(Integer value);

    }


    public static void main(String[] args) {
        JobOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(JobOptions.class);

        buildAndRun(options);
    }

    private static void buildAndRun(JobOptions options) {
        Pipeline p = Pipeline.create(options);

        p.apply("create sequence", GenerateSequence.from(0).withRate(options.getTablesPerSecond(), Duration.standardSeconds(1)))
                .apply("Create Table", ParDo.of(new DoFn<Long, String>() {

                    private BigQuery bigQuery;

                    @Setup
                    public void setup(PipelineOptions opt) {
                        this.bigQuery = BigQueryOptions
                                .newBuilder()
                                .setProjectId(opt.as(JobOptions.class).getProject())
                                .build()
                                .getService();
                    }

                    @ProcessElement
                    public void process(ProcessContext c){
                        long start = System.currentTimeMillis();

                        try {
                            Schema _schema = Schema.of(Schema.of(
                                    Field.newBuilder("col1", StandardSQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
                                    Field.newBuilder("col2", StandardSQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
                                    Field.newBuilder("col3", StandardSQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
                            ).getFields());

                            String table = "test-table-" + c.element();

                            //create
                            TableId tableId = TableId.of(c.getPipelineOptions().as(JobOptions.class).getDataset(), table);
                            TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
                                    .setSchema(_schema)
                                    .build();
                            TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
                            Table t = this.bigQuery.create(tableInfo);

                            long end = System.currentTimeMillis();
                            log.info(String.format("[%s] #%s | name=%s | thread=%s", (end - start), c.element(), t.getGeneratedId(), Thread.currentThread().getId()));
                            c.output(t.getGeneratedId());
                        }catch (Exception ex){
                            ex.printStackTrace();
                            throw new RuntimeException(ex);
                        }
                    }
                }));


        p.run();
    }


}
