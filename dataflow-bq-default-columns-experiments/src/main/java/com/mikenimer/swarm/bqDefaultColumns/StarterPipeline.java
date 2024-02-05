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
package com.mikenimer.swarm.bqDefaultColumns;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;


/**
 *
 * CREATE TABLE mock_data.default_tests (
 *  id STRING,
 *  name STRING,
 *  df_ts TIMESTAMP
 *  date_created TIMESTAMP DEFAULT current_timestamp()
 * );
 *
 */
public class StarterPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

    static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");

    public static void main(String[] args) {


        Pipeline p = Pipeline.create(
                PipelineOptionsFactory.fromArgs(args).withValidation().create());

                p.apply("Sample Records", Create.of(Arrays.asList("Stephen", "Kamal", "Paul")))
                        .apply("Write to BigQuery", BigQueryIO
                                .<String>write()
                                .to("mikenimer-playground.mock_data.default_tests")
                                .withSchema(new TableSchema()
                                        .setFields(ImmutableList.of(
                                                        new TableFieldSchema().setName("id").setType("STRING"),
                                                        new TableFieldSchema().setName("name").setType("STRING"),
                                                        new TableFieldSchema().setName("df_ts").setType("TIMESTAMP")
                                                )
                                        ))
                                .withAvroFormatFunction((AvroWriteRequest<String> e)  -> {
                                    GenericRecord rec = new GenericData.Record(e.getSchema());
                                    rec.put("id", UUID.randomUUID().toString());
                                    rec.put("name", e.getElement());
                                    rec.put("df_ts", new Date().getTime());
                                    return rec;
                                })
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                                .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
                        );

                p.run();
    }


    public static void main1(String[] args) {


        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("df_ts").setType("TIMESTAMP"));
        //fields.add(new TableFieldSchema().setName("date_created").setType("TIMESTAMP").setDefaultValueExpression("CURRENT_TIMESTAMP"));
        TableSchema schema = new TableSchema().setFields(fields);



        Pipeline p = Pipeline.create(
                PipelineOptionsFactory.fromArgs(args).withValidation().create());

                p.apply("Test Data", Create.of(Arrays.asList("John", "Andrew", "Steve")))
                .apply("Write to BigQuery", BigQueryIO
                        .<String>write()
                        .withFormatFunction((String name) -> new TableRow().set("id", UUID.randomUUID().toString()).set("name", name))
                        .to("mikenimer-playground.mock_data.default_tests")
                        .withJsonSchema(BigQueryHelpers.toJsonString(schema))
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                        .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
                );

        p.run();
    }

    public static void main2(String[] args) {

        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("df_ts").setType("TIMESTAMP"));
        //fields.add(new TableFieldSchema().setName("date_created").setType("TIMESTAMP").setDefaultValueExpression("CURRENT_TIMESTAMP"));
        TableSchema schema = new TableSchema().setFields(fields);


        Pipeline p = Pipeline.create(
                PipelineOptionsFactory.fromArgs(args).withValidation().create());

        PCollection<TableRow> rows = p.apply("",
                        GenerateSequence.from(1).withRate(3, Duration.standardSeconds(10)))
                .apply("", ParDo.of(new DoFn<Long, TableRow>() {
                    @ProcessElement
                    public void process(ProcessContext c) {
                        TableRow row = new TableRow();
                        row.put("id", UUID.randomUUID().toString());
                        row.put("name", "val = " + c.element());
                        row.put("df_ts", df.format(new Date()));
                        //row.put("date_created", null);
                        c.output(row);
                    }
                }));

        WriteResult result = rows.apply("", BigQueryIO.writeTableRows()
                .to("mikenimer-playground.mock_data.default_tests")
                .withSchema(schema)
                .withAutoSchemaUpdate(false)
                .ignoreInsertIds()
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)

                .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
                .withTriggeringFrequency(Duration.standardMinutes(1))

                //.withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)

                //.withAutoSharding()
                //.withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                //.withTriggeringFrequency(Duration.standardSeconds(30))
                );

        p.run();
    }
}
