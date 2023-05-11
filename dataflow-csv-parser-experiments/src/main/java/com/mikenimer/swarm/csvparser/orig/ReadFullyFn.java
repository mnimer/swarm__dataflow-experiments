package com.mikenimer.swarm.csvparser.orig;

import com.google.pubsub.v1.PubsubMessage;
import com.mikenimer.swarm.csvparser.ApacheExamplePipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.List;

public class ReadFullyFn extends DoFn<PubsubMessage, String> {

    private static final Logger log = LoggerFactory.getLogger(ReadFullyFn.class);
    @ProcessElement
    public void process(ProcessContext c){
        try {
            long start = System.currentTimeMillis();
            PubsubMessage msg = c.element();
            //build gs://... path
            String path = "gs://" + msg.getAttributesOrThrow("bucketId") + "/" + msg.getAttributesOrThrow("objectId");
            c.output(ApacheExamplePipeline.traceTag, "Start parsing | file=" + path);

            //get metadata from gcs
            //we can use the java FileSystems library because it has support for gs:// paths in Beam
            List<MatchResult.Metadata> md = FileSystems.match(path).metadata();
            //open gcs file as a java.io channel
            SeekableByteChannel channel = (SeekableByteChannel)FileSystems.open(md.get(0).resourceId());


            int fileSize = (int) channel.size();
            ByteBuffer byteBuffer = ByteBuffer.allocate(fileSize);
            channel.read(byteBuffer);
            byteBuffer.flip();
            byte[] bytes = byteBuffer.array();
            byteBuffer.clear();
            channel.close();


            log.info("[" +System.currentTimeMillis() +"]  file downloaded | ms="  + (System.currentTimeMillis() - start) +" | file=" + path);
            c.output(ApacheExamplePipeline.mainTag, new String(bytes));

        }catch (Exception ex){
            c.output(ApacheExamplePipeline.errorTag, ex.getMessage());
        }

    }

}