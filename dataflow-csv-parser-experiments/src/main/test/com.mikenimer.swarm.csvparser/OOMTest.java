package com.mikenimer.swarm.csvparser;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.mikenimer.swarm.csvparser.orig.ApacheCvsBytesFn;
import com.mikenimer.swarm.csvparser.orig.ReadFullyFn;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Map;

import static com.mikenimer.swarm.csvparser.ApacheExamplePipeline.*;

@RunWith(org.junit.runners.JUnit4.class)
public class OOMTest implements Serializable {

    @Rule
    public final transient TestPipeline p = TestPipeline.create();



    @Test
    public void test12kCsv(){
        Map<String, String> attribs = ImmutableMap.of(
                "eventType", "OBJECT_FINALIZE",
                "bucketId", "sample-databases",
                "objectId", "disney_csv/disney_100k.csv");

        PubsubMessage msg = PubsubMessage.newBuilder()
                .setData(ByteString.EMPTY)
                .putAllAttributes(attribs)
                .build();


        PCollectionTuple bytesTuple = (PCollectionTuple)p.apply(Create.of(msg))
                .apply("read file", ParDo.of(new ReadFullyFn()).withOutputTags(mainTag, TupleTagList.of(traceTag).and(errorTag)));

        bytesTuple.get(mainTag).apply("transform", ParDo.of(new ApacheCvsBytesFn())
                .withOutputTags(mainTag, TupleTagList.of(ApacheExamplePipeline.traceTag).and(ApacheExamplePipeline.errorTag)));

        p.run();
    }


    @Test
    public void test100kCsv(){
        Map<String, String> attribs = ImmutableMap.of(
                "eventType", "OBJECT_FINALIZE",
                "bucketId", "sample-databases",
                "objectId", "disney_csv/disney_100k.csv");

        PubsubMessage msg = PubsubMessage.newBuilder()
                .setData(ByteString.EMPTY)
                .putAllAttributes(attribs)
                .build();


        PCollectionTuple bytesTuple = (PCollectionTuple)p.apply(Create.of(msg))
                .apply("read file", ParDo.of(new ReadFullyFn()).withOutputTags(mainTag, TupleTagList.of(traceTag).and(errorTag)));

        PCollectionTuple csvTuple = (PCollectionTuple)bytesTuple.get(mainTag).apply("transform", ParDo.of(new ApacheCvsBytesFn())
                .withOutputTags(mainTag, TupleTagList.of(ApacheExamplePipeline.traceTag).and(ApacheExamplePipeline.errorTag)));

        csvTuple.get(mainTag).apply("debug", ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                c.output(null);
            }
        }));

        p.run();
    }


    @Test
    public void test1mCsv(){
        Map<String, String> attribs = ImmutableMap.of(
                "eventType", "OBJECT_FINALIZE",
                "bucketId", "sample-databases",
                "objectId", "disney_csv/disney_1m.csv");

        PubsubMessage msg = PubsubMessage.newBuilder()
                .setData(ByteString.EMPTY)
                .putAllAttributes(attribs)
                .build();


        PCollectionTuple bytesTuple = (PCollectionTuple)p.apply(Create.of(msg))
                .apply("read file", ParDo.of(new ReadFullyFn()).withOutputTags(mainTag, TupleTagList.of(traceTag).and(errorTag)));

        bytesTuple.get(mainTag).apply("transform", ParDo.of(new ApacheCvsBytesFn())
                .withOutputTags(mainTag, TupleTagList.of(ApacheExamplePipeline.traceTag).and(ApacheExamplePipeline.errorTag)));

        p.run();
    }
}