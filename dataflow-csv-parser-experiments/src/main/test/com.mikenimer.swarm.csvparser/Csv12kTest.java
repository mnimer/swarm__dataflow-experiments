package com.mikenimer.swarm.csvparser;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.common.collect.ImmutableMap;
import com.mikenimer.swarm.csvparser.parsers.ApacheCsvParserFn;
import com.mikenimer.swarm.csvparser.parsers.BufferedReaderParserFn;
import com.mikenimer.swarm.csvparser.parsers.FastCsvParserFn;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Map;


@RunWith(org.junit.runners.JUnit4.class)
public class Csv12kTest implements Serializable {

    @Rule
    public final transient TestPipeline p = TestPipeline.create();

    @Test
    public void testApacheCsv(){
        Map<String, String> attribs = ImmutableMap.of(
                "eventType", "OBJECT_FINALIZE",
                "bucketId", "sample-databases",
                "objectId", "mock_csv_experiments/mock-10k.csv");

        PubsubMessage msg = PubsubMessage.newBuilder()
                .setData(ByteString.EMPTY)
                .putAllAttributes(attribs)
                .build();


        p.apply(Create.of(msg))
        .apply("transform", ParDo.of(new ApacheCsvParserFn())
                .withOutputTags(ApacheExamplePipeline.mainTag, TupleTagList.of(ApacheExamplePipeline.traceTag).and(ApacheExamplePipeline.errorTag)));

        p.run();
    }

    @Test
    public void testBufferedReader(){
        Map<String, String> attribs = ImmutableMap.of(
                "eventType", "OBJECT_FINALIZE",
                "bucketId", "sample-databases",
                "objectId", "mock_csv_experiments/mock-10k.csv");

        PubsubMessage msg = PubsubMessage.newBuilder()
                .setData(ByteString.EMPTY)
                .putAllAttributes(attribs)
                .build();


        p.apply(Create.of(msg))
        .apply("transform", ParDo.of(new BufferedReaderParserFn()));

        p.run();
    }

    @Test
    public void testFastCsvParser(){
        Map<String, String> attribs = ImmutableMap.of(
                "eventType", "OBJECT_FINALIZE",
                "bucketId", "sample-databases",
                "objectId", "mock_csv_experiments/mock-10k.csv");

        PubsubMessage msg = PubsubMessage.newBuilder()
                .setData(ByteString.EMPTY)
                .putAllAttributes(attribs)
                .build();


        p.apply(Create.of(msg))
        .apply("transform", ParDo.of(new FastCsvParserFn()));

        p.run();
    }
}