package com.mikenimer.swarm.csvparser.orig;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.mikenimer.swarm.csvparser.ApacheExamplePipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class ApacheCvsBytesFn extends DoFn<String, String>  implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(ApacheCvsBytesFn.class);

    private ObjectMapper mapper = new ObjectMapper();

    @ProcessElement
    public void process(ProcessContext c) throws IOException {
        try {
            long start = System.currentTimeMillis();
            String msg = c.element();
            //build gs://... path
            log.info("[" +System.currentTimeMillis() +"] Start parsing bytes | len=" +msg.length());
            c.output(ApacheExamplePipeline.traceTag, "[" +System.currentTimeMillis() +"] Start parsing bytes | len=" +msg.length());

            Reader reader = new InputStreamReader(new ByteArrayInputStream(msg.getBytes()));

            log.info("[" +(System.currentTimeMillis()-start) +"ms] bytes loaded | bytes=" +msg.length());
            long start2 = System.currentTimeMillis();

            //parse and loop over the file with Apache Commons CSV parser
            Iterable<CSVRecord> records = CSVFormat.DEFAULT
                    .withHeader()
                    .parse(reader);
            long rowCount = 0;
            for (CSVRecord record : records) {
                rowCount++;
                String row = mapper.writeValueAsString(record.toMap());
                c.output(row);
            }

            //log timing results
            String traceMessage = "[" +System.currentTimeMillis() +"] file parse completed | ms=" +(System.currentTimeMillis()-start2)  +" | rows=" +rowCount ;
            log.info(traceMessage);
            c.output(ApacheExamplePipeline.traceTag, traceMessage);
        } catch (Exception ex){
            c.output(ApacheExamplePipeline.errorTag, ex.getMessage());
        }
    }
}
