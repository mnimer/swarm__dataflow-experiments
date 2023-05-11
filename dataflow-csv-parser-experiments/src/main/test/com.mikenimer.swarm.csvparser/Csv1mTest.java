package com.mikenimer.swarm.csvparser;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.mikenimer.swarm.csvparser.parsers.ApacheCsvParserFn;
import com.mikenimer.swarm.csvparser.parsers.BufferedReaderParserFn;
import com.mikenimer.swarm.csvparser.parsers.FastCsvParserFn;
import com.mikenimer.swarm.csvparser.parsers.SimpleFlatMapperParserFn;
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

import static com.mikenimer.swarm.csvparser.ApacheExamplePipeline.mainTag;


@RunWith(org.junit.runners.JUnit4.class)
public class Csv1mTest implements Serializable {

    @Rule
    public final transient TestPipeline p = TestPipeline.create();


    @Test
    public void testBufferedReader(){
        Map<String, String> attribs = ImmutableMap.of(
                "eventType", "OBJECT_FINALIZE",
                "bucketId", "sample-databases",
                "objectId", "mock_csv_experiments/mock_1m.csv");

        PubsubMessage msg = PubsubMessage.newBuilder()
                .setData(ByteString.EMPTY)
                .putAllAttributes(attribs)
                .build();


        p.apply(Create.of(msg))
                .apply("transform", ParDo.of(new BufferedReaderParserFn()));

        p.run();
    }


    @Test
    public void testApacheCsv(){
        Map<String, String> attribs = ImmutableMap.of(
                "eventType", "OBJECT_FINALIZE",
                "bucketId", "sample-databases",
                "objectId", "mock_csv_experiments/mock_1m.csv");

        PubsubMessage msg = PubsubMessage.newBuilder()
                .setData(ByteString.EMPTY)
                .putAllAttributes(attribs)
                .build();


        PCollectionTuple csvTuple = (PCollectionTuple) p.apply(Create.of(msg))
        .apply("transform", ParDo.of(new ApacheCsvParserFn())
                .withOutputTags(ApacheExamplePipeline.mainTag, TupleTagList.of(ApacheExamplePipeline.traceTag).and(ApacheExamplePipeline.errorTag)));

        csvTuple.get(mainTag).apply("debug", ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                c.output(null);
            }
        }));

        p.run();
    }

    @Test
    public void testFastCsvParser(){
        Map<String, String> attribs = ImmutableMap.of(
                "eventType", "OBJECT_FINALIZE",
                "bucketId", "sample-databases",
                "objectId", "mock_csv_experiments/mock_1m.csv");

        PubsubMessage msg = PubsubMessage.newBuilder()
                .setData(ByteString.EMPTY)
                .putAllAttributes(attribs)
                .build();


        p.apply(Create.of(msg))
        .apply("transform", ParDo.of(new FastCsvParserFn()));

        p.run();
    }

    @Test
    public void testSimpleFlatMapperParser(){
        Map<String, String> attribs = ImmutableMap.of(
                "eventType", "OBJECT_FINALIZE",
                "bucketId", "sample-databases",
                "objectId", "mock_csv_experiments/mock_1m.csv");

        PubsubMessage msg = PubsubMessage.newBuilder()
                .setData(ByteString.EMPTY)
                .putAllAttributes(attribs)
                .build();


        p.apply(Create.of(msg))
        .apply("transform", ParDo.of(new SimpleFlatMapperParserFn()));

        p.run();
    }
}