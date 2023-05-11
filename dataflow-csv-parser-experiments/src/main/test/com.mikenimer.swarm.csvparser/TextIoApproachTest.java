package com.mikenimer.swarm.csvparser;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.mikenimer.swarm.csvparser.transforms.TextIOKVTransform;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Map;

@RunWith(org.junit.runners.JUnit4.class)
public class TextIoApproachTest implements Serializable {

    @Rule
    public final transient TestPipeline p = TestPipeline.create();



    @Test
    public void test1File(){

        Map<String, String> attribs = ImmutableMap.of(
                "eventType", "OBJECT_FINALIZE",
                "bucketId", "sample-databases",
                "objectId", "mock_csv_experiments/mock-10.csv");

        PubsubMessage msg = PubsubMessage.newBuilder()
                .setData(ByteString.EMPTY)
                .putAllAttributes(attribs)
                .build();


        PCollection<String> linesInFiles = p.apply(Create.of(msg))
                .apply("transform", ParDo.of(new DoFn<PubsubMessage, String>() {
                    @ProcessElement
                    public void process(ProcessContext c){
                        c.output("gs://" +c.element().getAttributesOrThrow("bucketId") + "/" + c.element().getAttributesOrThrow("objectId"));
                    }
                }))
                .apply("Match all files", FileIO.matchAll())
                .apply("Read all files", FileIO.readMatches())
                .apply("Read lines", TextIO.readFiles());

        linesInFiles.apply("log lines", ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void process(ProcessContext c){
                System.out.println(c.element());
            }
        }));

        p.run();
    }

    @Test
    public void testMultipleFileWithKV(){

        Map<String, String> attribs = ImmutableMap.of(
                "eventType", "OBJECT_FINALIZE",
                "bucketId", "sample-databases",
                "objectId", "mock_csv_experiments/mock-10.csv");

        PubsubMessage msg = PubsubMessage.newBuilder()
                .setData(ByteString.EMPTY)
                .putAllAttributes(attribs)
                .build();


        PCollection<KV<String, String>> fileLines = p.apply(Create.of(msg))
                .apply("transform", ParDo.of(new DoFn<PubsubMessage, String>() {
                    @ProcessElement
                    public void process(ProcessContext c){
                        c.output("gs://" +c.element().getAttributesOrThrow("bucketId") + "/" + c.element().getAttributesOrThrow("objectId"));
                    }
                }))
                .apply("Match all files", FileIO.matchAll())
                .apply("Read all files", FileIO.readMatches())
                //.apply("Read lines", TextIO.readFiles());
                .apply("read lines", new TextIOKVTransform());

        fileLines.apply("log lines", ParDo.of(new DoFn<KV<String, String>, String>() {
            @ProcessElement
            public void process(ProcessContext c){
                System.out.println(c.element());
            }
        }));

        p.run();
    }

    @Test
    public void test1mFileWithKV(){

        Map<String, String> attribs = ImmutableMap.of(
                "eventType", "OBJECT_FINALIZE",
                "bucketId", "sample-databases",
                "objectId", "mock_csv_experiments/mock_1m.csv");

        PubsubMessage msg = PubsubMessage.newBuilder()
                .setData(ByteString.EMPTY)
                .putAllAttributes(attribs)
                .build();


        PCollection<KV<String, String>> fileLines = p.apply(Create.of(msg))
                .apply("transform", ParDo.of(new DoFn<PubsubMessage, String>() {
                    @ProcessElement
                    public void process(ProcessContext c){
                        c.output("gs://" +c.element().getAttributesOrThrow("bucketId") + "/" + c.element().getAttributesOrThrow("objectId"));
                    }
                }))
                .apply("Match all files", FileIO.matchAll())
                .apply("Read all files", FileIO.readMatches())
                //.apply("Read lines", TextIO.readFiles());
                .apply("read lines", new TextIOKVTransform());

        fileLines.apply("log lines", ParDo.of(new DoFn<KV<String, String>, String>() {
            @ProcessElement
            public void process(ProcessContext c){
                //System.out.println(c.element());
                c.output("");
            }
        }));

        p.run();
    }

    @Test
    public void test30_1mFilesWithKV(){

        Map<String, String> attribs = ImmutableMap.of(
                "eventType", "OBJECT_FINALIZE",
                "bucketId", "sample-databases",
                "objectId", "mock_csv_experiments/mock_1m.csv");

        PubsubMessage msg = PubsubMessage.newBuilder()
                .setData(ByteString.EMPTY)
                .putAllAttributes(attribs)
                .build();


        PCollection<KV<String, String>> fileLines = p.apply(Create.of(msg,msg,msg,msg,msg,msg,msg,msg,msg,msg,msg,msg,msg,msg,msg,msg,msg,msg,msg,msg,msg,msg,msg,msg,msg,msg,msg,msg,msg))
                .apply("transform", ParDo.of(new DoFn<PubsubMessage, String>() {
                    @ProcessElement
                    public void process(ProcessContext c){
                        c.output("gs://" +c.element().getAttributesOrThrow("bucketId") + "/" + c.element().getAttributesOrThrow("objectId"));
                    }
                }))
                .apply("Match all files", FileIO.matchAll())
                .apply("Read all files", FileIO.readMatches())
                //.apply("Read lines", TextIO.readFiles());
                .apply("read lines", new TextIOKVTransform());

        fileLines.apply("log lines", ParDo.of(new DoFn<KV<String, String>, String>() {
            @ProcessElement
            public void process(ProcessContext c){
                //System.out.println(c.element());
                c.output("");
            }
        }));

        p.run();
    }



}
