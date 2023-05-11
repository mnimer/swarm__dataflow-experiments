package com.mikenimer.swarm.csvparser.formatters;

import com.mikenimer.swarm.csvparser.ApacheExamplePipeline;
import org.apache.beam.sdk.transforms.DoFn;

public class DestinationFormatterFn extends DoFn<String, String> {

    @ProcessElement
    public void process(ProcessContext c) {
        try {
            //c.output(StarterPipeline.traceTag, "Start formatter");

            String row = c.element();
            c.output(row);
        }catch (Exception ex){
            c.output(ApacheExamplePipeline.errorTag, ex.getMessage());
        }
    }
}
