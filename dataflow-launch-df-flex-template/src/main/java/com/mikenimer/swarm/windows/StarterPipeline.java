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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
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
public class StarterPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

    public interface JobOptions extends GcpOptions {

        String getTopic();
        void setTopic(String value);
    }


    public static void main(String[] args) {
        JobOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(JobOptions.class);

        build(options);
    }

    private static void build(JobOptions options) {
        Pipeline p = Pipeline.create(options);
        String topic = "projects/" +options.getProject() +"/topics/" +options.getTopic();

        p.apply("Read Messages", PubsubIO.readMessagesWithAttributes().fromTopic(topic))
        .apply("assign key", ParDo.of(new DoFn<PubsubMessage, KV<String, PubsubMessage>>() {
            @ProcessElement
            public void process(ProcessContext c){
                c.output( KV.of(c.element().getAttribute("groupKey"), c.element()) );
            }
        }))

        .apply("window", Window.<KV<String, PubsubMessage>>into(FixedWindows.of(Duration.standardSeconds(5))))
        .apply("group", GroupByKey.create());

        //.apply("Debug Logging", ParDo.of(new DebugWindowFn()));

        p.run();
    }



}
