package com.mikenimer.swarm.csvparser.parsers;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.pubsub.v1.PubsubMessage;
import com.mikenimer.swarm.csvparser.StarterPipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.List;

public class ApacheCsvParserFn  extends DoFn<PubsubMessage, String>  implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(ApacheCsvParserFn.class);

    private ObjectMapper mapper = new ObjectMapper();

    @ProcessElement
    public void process(ProcessContext c) throws IOException {
        try {
            long start = System.currentTimeMillis();
            PubsubMessage msg = c.element();
            //build gs://... path
            String path = "gs://" +msg.getAttributesOrThrow("bucketId") + "/" + msg.getAttributesOrThrow("objectId");
            c.output(StarterPipeline.traceTag, "Start parsing | file=" +path);

            //get metadata from gcs
            //we can use the java FileSystems library because it has support for gs:// paths in Beam
            List<MatchResult.Metadata> md = FileSystems.match(path).metadata();
            //open gcs file as a java.io channel
            ReadableByteChannel channel = FileSystems.open(md.get(0).resourceId());
            BufferedReader br = new BufferedReader(Channels.newReader(channel, "UTF-8"));

            log.info("[" +(System.currentTimeMillis()-start) +"ms] file downloaded | file=" +path);
            long start2 = System.currentTimeMillis();

            //parse and loop over the file with Apache Commons CSV parser
            Iterable<CSVRecord> records = CSVFormat.DEFAULT
                    .withHeader()
                    .parse(br);
            long rowCount = 0;
            for (CSVRecord record : records) {
                rowCount++;
                String row = mapper.writeValueAsString(record.toMap());
                c.output(row);
            }

            //log timing results
            String traceMessage = "[" +(System.currentTimeMillis()-start2) +"ms] file parse completed | rows=" +rowCount +" | file=" +path;
            log.info(traceMessage);
            c.output(StarterPipeline.traceTag, traceMessage);
        } catch (Exception ex){
            c.output(StarterPipeline.errorTag, ex.getMessage());
        }
    }
}
