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
package com.mikenimer.swarm.bqToJdbc;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.mikenimer.swarm.bqToJdbc.transforms.JdbcWriter;

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
        Pipeline p = Pipeline.create(options);
        
        for (String table : options.getBqTable()) {
            buildTableToTablePipeline(table, options, p);    
        }
        

        p.run();
    }

    private static void buildTableToTablePipeline(String table, JobOptions options, Pipeline p) {
        
        String[] parts = table.split("\\.");
        List<String> bqColumns = getColumnsForTable(table);

        TableReference tableRef = new TableReference().setProjectId(parts[0]).setDatasetId(parts[1]).setTableId(parts[2]);

        p.apply(String.format("Read Table Data | %s",table), BigQueryIO
                        .readTableRowsWithSchema()
                        .from(tableRef)
                        .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ)
                        .withSelectedFields(bqColumns)
        )
        .apply(String.format("log | %s",table), ParDo.of(new DoFn<TableRow, TableRow>() {
            @ProcessElement
            public void process(ProcessContext c){
                LOG.info("[" +table +"] Row:" +c.element().toString());
                c.output(c.element());
            }
            }));
        //.apply(String.format("Write to db | %s",table), new JdbcWriter(table, options.getDestDatabase(), bqColumns ));
    }


    /**
     * Return a hard coded list of columns for the incoming table.
     * 
     * TODO: right now this is hard coded for 3 test tables. You could easily query the BQ information schema (or call api) to get all columns in a table. 
     * 
     * @param options
     * @return
     */
    public static List<String> getColumnsForTable(String table) {
        if( table.equals("bigquery-public-data.baseball.schedules")){
            return Arrays.asList("gameId,gameNumber,seasonId,year".split(","));
        }else if( table.equals("bigquery-public-data.baseball.games_post_wide")){
            return Arrays.asList("gameId,seasonId,year,startTime,gameStatus,attendance,dayNight,duration".split(","));
        }else{
            throw new RuntimeException("Not implemented");
        }

    }
}
