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

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.TopicName;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

        buildDataFlowDag(options);
    }

    private static void buildDataFlowDag(JobOptions options) {
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
        .apply("group", GroupByKey.create())

        .apply("Debug Log", ParDo.of(new DoFn<KV<String, Iterable<PubsubMessage>>, String>() {
            @ProcessElement
            public void process(ProcessContext c, BoundedWindow window) throws Exception {
                PaneInfo p = c.pane();
                KV<String, Iterable<PubsubMessage>> elem = c.element();
                SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss.SSS");
                Long minDate = null;
                Long maxDate = null;
                long count = 0;
                String groupKey = null;
                List<String> seq = new ArrayList<>();

                for (PubsubMessage pubsubMessage : elem.getValue()) {
                    count++;
                    seq.add(pubsubMessage.getAttribute("sequence"));
                    Long dt = Long.parseLong(pubsubMessage.getAttribute("publishTs"));
                    if( minDate == null || minDate > dt){
                        minDate = dt;
                    }
                    if( maxDate == null || maxDate < dt){
                        maxDate = dt;
                    }
                    if( groupKey == null ){
                        groupKey = pubsubMessage.getAttribute("groupKey");
                    }
                    if( !pubsubMessage.getAttribute("groupKey").equals(groupKey) ){
                        System.out.println("ERROR - groupKeys are mixed together");
                        throw new RuntimeException("ERROR - groupKeys are mixed together");
                    }
                }
                System.out.println(String.format("[%s] Thread=%s | groupKey=%s | # of items=%s | data range=%s - %s | window range=%s - %s | sequence=%s ",
                        p.getTiming(),
                        String.valueOf(Thread.currentThread().getId()),
                        groupKey,
                        count,
                        df.format(minDate),
                        df.format(maxDate),
                        df.format(((IntervalWindow) window).start().toDate()),
                        df.format(((IntervalWindow) window).end().toDate()),
                        seq.stream().map((val)->Long.valueOf(val)).sorted().map((val)->val.toString()).collect(Collectors.joining(",")))); //sort as longs before outputting string
                Thread.sleep(500);
            }
        }));

        p.run();
    }


}
