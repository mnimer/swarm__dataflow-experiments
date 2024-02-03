package com.mikenimer.swarm.csvparser.transforms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mikenimer.swarm.csvparser.parsers.ApacheCsvParserFn;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.ReadAllViaFileBasedSource;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TextIOKVTransform extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<KV<String, String>>> {

    private static final Logger log = LoggerFactory.getLogger(ApacheCsvParserFn.class);

    @Override
    public PCollection<KV<String, String>> expand(PCollection<FileIO.ReadableFile> input) {

        PCollectionView<List<String>> fileView = getFileName(input);
        PCollectionView<List<String>> headerRow = getHeaderRow(input);

        return input
                .apply("read lines", TextIO.read().from()) //67108864
                .apply("convert to KV", ParDo.of(new DoFn<String, KV<String, String>>() {
                    @ProcessElement
                    public void process(ProcessContext c){
                        try {

                            String name = c.sideInput(fileView).get(0);
                            String header = c.sideInput(headerRow).get(0);
                            Map<String, String> row = new HashMap<>();
                            ObjectMapper mapper = new ObjectMapper();

                            //skip first row
                            if (c.element().equals(header)) {
                                return;
                            }

                            //merge row with headers to create map
                            String[] columns = c.element().split(",");
                            String[] headers = header.split(",");
                            for (int i = 0; i < headers.length; i++) {
                                row.put(headers[i], columns[i]);
                            }
                            //return row with file name
                            c.output(KV.of(name, mapper.writeValueAsString(row)));
                        }catch (JsonProcessingException ex){
                            log.error(ex.getMessage() +" | file=" +name +" | row=" +c.element());
                        }
                    }
                })
                .withSideInput("name", fileView)
                .withSideInput("header", headerRow));
    }

    /**
     * Pull out the full path to the file, as the name, to use as a key
     * @param input
     * @return
     */
    private static PCollectionView<List<String>> getFileName(PCollection<FileIO.ReadableFile> input) {
        return input
                .apply("get name", ParDo.of(new DoFn<FileIO.ReadableFile, String>() {
            @ProcessElement
            public void process(ProcessContext c) {
                    c.output(c.element().getMetadata().resourceId().toString());
                    }
                }))
                .apply("window name", Window.into(new GlobalWindows()))
                .apply("name", View.asList());
    }

    /**
     * Pull out the full path to the file, as the name, to use as a key
     * @param input
     * @return
     */
    private static PCollectionView<List<String>> getHeaderRow(PCollection<FileIO.ReadableFile> input) {
        return input
                .apply("get header", ParDo.of(new DoFn<FileIO.ReadableFile, String>() {
                    @ProcessElement
                    public void process(ProcessContext c) {
                            c.output("TRANSACTION_NUMBER,STORE_NUMBER,STORE_DESCRIPTION,BUSINESS_DATE,TRANSACTION_DATE_TIME,TRANSACTION_SALES_TYPE,GRC_VPN,ITEM_CODE,ITEM_DESCRIPTION,STYLE_CODE,STYLE_DESCRIPTION,LOCAL_CURRENCY,SALES_UNITS,SALES_PRICE_LC,SALES_PRICE_US,SALES_RETAIL_LC,SALES_RETAIL_US,COST_LC,COST_US,CURRENT_RETAIL_PRICE_US,LINE");
                        }
                    }))
                .apply("window header", Window.into(new GlobalWindows()))
                .apply("header", View.asList());
    }
}
