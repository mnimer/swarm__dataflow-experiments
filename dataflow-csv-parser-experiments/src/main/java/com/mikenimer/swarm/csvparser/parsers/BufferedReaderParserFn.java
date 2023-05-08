package com.mikenimer.swarm.csvparser.parsers;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.sql.Array;
import java.util.*;

public class BufferedReaderParserFn extends DoFn<PubsubMessage, String>  implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(BufferedReaderParserFn.class);

    private ObjectMapper mapper = new ObjectMapper();

    @ProcessElement
    public void process(ProcessContext c) throws IOException {
        long start = System.currentTimeMillis();
        long rowCount = 0;

        PubsubMessage msg = c.element();
        //build gs://... path
        String path = "gs://" +msg.getAttributesOrThrow("bucketId") + "/" + msg.getAttributesOrThrow("objectId");

        //get metadata from gcs
        //we can use the java FileSystems library because it has support for gs:// paths in Beam
        List<MatchResult.Metadata> md = FileSystems.match(path).metadata();
        //open gcs file as a java.io channel
        ReadableByteChannel channel = FileSystems.open(md.get(0).resourceId());
        BufferedReader br = new BufferedReader(Channels.newReader(channel, "UTF-8"));

        log.info("[" +(System.currentTimeMillis()-start) +"ms] file downloaded | file=" +path);
        long start2 = System.currentTimeMillis();


        int rowIndx = 0;
        String line;
        List<String> headers = new ArrayList<>();
        while ((line = br.readLine()) != null) {
            if( rowIndx++ == 0 ){
                headers = Arrays.asList(line.split(","));
            } else {
                String[] values = line.split(",");
                Map mFields = new HashMap();
                for (int i = 0; i < headers.size(); i++) {
                    mFields.put(headers.get(i), values[i]);
                }
                c.output(mapper.writeValueAsString(mFields));
            }
        }


        //log timing results
        log.info("[" +(System.currentTimeMillis()-start2) +"ms] file parse completed | rows=" +rowIndx);
    }
}
