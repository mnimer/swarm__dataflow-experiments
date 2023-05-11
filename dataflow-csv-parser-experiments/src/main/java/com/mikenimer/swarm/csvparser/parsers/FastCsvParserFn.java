package com.mikenimer.swarm.csvparser.parsers;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.pubsub.v1.PubsubMessage;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRow;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class FastCsvParserFn extends DoFn<PubsubMessage, String>  implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(FastCsvParserFn.class);

    private ObjectMapper mapper = new ObjectMapper();

    //local cache for headers
    public Map<String, List<String>> headers = new HashMap<>();
    @ProcessElement
    public void process(ProcessContext c) throws IOException {
        long start = System.currentTimeMillis();
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

        try {
            CsvReader reader = CsvReader.builder()
                    .fieldSeparator(',')
                    .build(br);

            //parse and loop over the file with Apache Commons CSV parser
            long rowCount = reader.stream().map((Function<CsvRow, Integer>) csvRow -> {
                if (csvRow.getOriginalLineNumber() == 1) {
                    //save header keys
                    headers.put(path, csvRow.getFields());
                    return 0;
                } else {
                    try {
                        List<String> h = headers.get(path);
                        Map mFields = new HashMap();
                        for (int i = 0; i < h.size(); i++) {
                            mFields.put(h.get(i), csvRow.getField(i));
                        }

                        c.output(mapper.writeValueAsString(mFields));
                    } catch (Exception e) {
                        log.error("error parsing record", csvRow.toString());
                    }
                    return 1;
                }

            }).count();


            //log timing results
            log.info("[" +(System.currentTimeMillis()-start2) +"ms] file parse completed | rows=" +rowCount);
        }finally {
            br.close();
            channel.close();
        }


    }
}
