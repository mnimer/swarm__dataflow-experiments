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
package com.mikenimer.swarm.csvparser;


import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.mikenimer.swarm.csvparser.formatters.DestinationFormatterFn;
import com.mikenimer.swarm.csvparser.transforms.TextIOKVTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * A starter example for writing Beam programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 * --project=<YOUR_PROJECT_ID>
 * --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 * --runner=DataflowRunner
 */
public class TextIOExamplePipeline {
    private static final Logger LOG = LoggerFactory.getLogger(TextIOExamplePipeline.class);

    public static TupleTag mainTag = new TupleTag<String>() {
    };
    public static TupleTag traceTag = new TupleTag<String>() {
    };
    public static TupleTag errorTag = new TupleTag<String>() {
    };

    public static void main(String[] args) {
        Pipeline p = Pipeline.create(
                PipelineOptionsFactory.fromArgs(args).withValidation().create());

        //TODO: replace with PubSubIO
        PCollection<PubsubMessage> messages = p.apply(Create.of(
                getMockMessage("mock_csv_experiments/mock-10k.csv"),
                getMockMessage("mock_csv_experiments/mock_100k.csv"),
                getMockMessage("mock_csv_experiments/mock_1m.csv")
//                getMockMessage("mock_csv_experiments/mock_100k.csv"),
//                getMockMessage("mock_csv_experiments/mock_100k.csv"),
//                getMockMessage("mock_csv_experiments/mock_100k.csv"),
//                getMockMessage("mock_csv_experiments/mock_1m.csv"),
//                getMockMessage("mock_csv_experiments/mock_1m.csv"),
//                getMockMessage("mock_csv_experiments/mock_1m.csv"),
//                getMockMessage("mock_csv_experiments/mock_1m.csv"),
//                getMockMessage("mock_csv_experiments/mock_1m.csv"),
//                getMockMessage("mock_csv_experiments/mock_1m.csv"),
//                getMockMessage("mock_csv_experiments/mock_1m.csv"),
//                getMockMessage("mock_csv_experiments/mock_1m.csv"),
//                getMockMessage("mock_csv_experiments/mock_1m.csv"),
//                getMockMessage("mock_csv_experiments/mock_1m.csv"),
//                getMockMessage("mock_csv_experiments/mock_1m.csv"),
//                getMockMessage("mock_csv_experiments/mock_1m.csv"),
//                getMockMessage("mock_csv_experiments/mock_1m.csv"),
//                getMockMessage("mock_csv_experiments/mock_1m.csv"),
//                getMockMessage("mock_csv_experiments/mock_1m.csv"),
//                getMockMessage("mock_csv_experiments/mock_1m.csv"),
//                getMockMessage("mock_csv_experiments/mock_1m.csv"),
//                getMockMessage("mock_csv_experiments/mock_1m.csv"),
//                getMockMessage("mock_csv_experiments/mock_1m.csv"),
//                getMockMessage("mock_csv_experiments/mock_1m.csv"),
//                getMockMessage("mock_csv_experiments/mock_1m.csv"),
//                getMockMessage("mock_csv_experiments/mock_1m.csv"),
//                getMockMessage("mock_csv_experiments/mock_1m.csv"),
//                getMockMessage("mock_csv_experiments/mock_1m.csv")
        ));


        //Read and Parse CSV files from GCS
        messages
                .apply("transform", ParDo.of(new DoFn<PubsubMessage, String>() {
                    @ProcessElement
                    public void process(ProcessContext c){
                        c.output("gs://" +c.element().getAttributesOrThrow("bucketId") + "/" + c.element().getAttributesOrThrow("objectId"));
                    }
                }))
                .apply("Match all files", FileIO.matchAll())
                .apply("Read all files", FileIO.readMatches())
                .apply("Read lines", TextIO.readFiles());

                /** todo: Works with DirectRunner but not with DataflowRunner
                .apply("read lines", new TextIOKVTransform())

                //Send CSV rows to Writer
                .apply("debug", ParDo.of(new DoFn<KV<String,String>, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        LOG.info("mainTag: " + c.element().toString());
                        //TODO: replace with IO Writer
                        c.output(c.element().getValue());
                    }
                }));
                 */

        p.run();
    }

    public static PubsubMessage getMockMessage(String file) {
        Map<String, String> attribs = ImmutableMap.of(
                "eventType", "OBJECT_FINALIZE",
                "bucketId", "sample-databases",
                "objectId", file);

        return PubsubMessage.newBuilder()
                .setData(ByteString.EMPTY)
                .putAllAttributes(attribs)
                .build();

    }
}
