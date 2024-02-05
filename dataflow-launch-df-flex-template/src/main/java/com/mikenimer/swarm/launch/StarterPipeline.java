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
package com.mikenimer.swarm.launch;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.dataflow.v1beta3.ContainerSpec;
import com.google.dataflow.v1beta3.FlexTemplatesServiceClient;
import com.google.dataflow.v1beta3.LaunchFlexTemplateParameter;
import com.google.dataflow.v1beta3.LaunchFlexTemplateRequest;
import com.google.dataflow.v1beta3.LaunchFlexTemplateResponse;

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
        
        // TODO: replace Create.of with PubSubIO
        //String topic = "projects/" +options.getProject() +"/subscriptions/" +options.getSubscription();

        p.apply("Read Messages", Create.of(KV.of("table","bigquery-public-data.baseball.schedules"), KV.of("table","bigquery-public-data.baseball.games_post_wide")))
        //todo: Slow this day, every few minutes should be good.
        .apply("Window", Window.<KV<String, String>>into(FixedWindows.of(Duration.standardMinutes(1))))
        .apply(GroupIntoBatches.ofSize(25))
        .apply("launch", ParDo.of(new DoFn<KV<String, Iterable<String>>,String>() {
            @ProcessElement
            public void processElement(ProcessContext c) throws IOException, GeneralSecurityException {

                // Launch a flex template for every set of ~25 tables

                JobOptions _options = c.getPipelineOptions().as(JobOptions.class);
                ArrayList<String> tableList = new ArrayList<String>();
                for (String table : c.element().getValue()) {
                    tableList.add(table);
                }


                //GoogleCredentials workerCredentials = GoogleCredentials.getApplicationDefault();
                //Dataflow dataflow = new Dataflow.Builder(GoogleNetHttpTransport.newTrustedTransport(),
                //    new GsonFactory(), null)
                //    .setApplicationName("Dataflow Worker")
                //    .build();

                    LaunchFlexTemplateParameter launchParameter = LaunchFlexTemplateParameter.newBuilder()
                    .setJobName("launch-flex-instance--" +System.currentTimeMillis())
                    .setContainerSpecGcsPath(_options.getTemplate())
                    .putAllParameters(Map.of(
                        "bqTable", String.join(",", tableList),
                        "destDatabase", "postgres",
                        "driver" , "org.postgresql.Driver",
                        "url", "jdbc:postgresql://localhost:5432/postgres",
                        "username", "postgres",
                        "password", ""
                    ))
                    .build();


                    LaunchFlexTemplateRequest flexTemplateRequest = LaunchFlexTemplateRequest.newBuilder()
                    .setProjectId(_options.getProject())
                    .setLaunchParameter(launchParameter)
                    .build();

                    try (FlexTemplatesServiceClient flexTemplatesServiceClient = FlexTemplatesServiceClient.create()) {
                       LaunchFlexTemplateResponse response = flexTemplatesServiceClient.launchFlexTemplate(flexTemplateRequest);
                       System.out.println(response.getJob().toString());
                    }

                   catch (Exception e) {
                    LOG.error("Failed to start the Dataflow job", e);
                    throw new RuntimeException("Failed to start the Dataflow job");
                  }


                System.out.println(c.element());
                c.output("launched");
            }
        }));

        //.apply("Debug Logging", ParDo.of(new DebugWindowFn()));

        p.run();
    }



}
