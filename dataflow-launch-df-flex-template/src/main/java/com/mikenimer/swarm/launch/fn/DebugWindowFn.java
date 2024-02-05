package com.mikenimer.swarm.launch.fn;

import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class DebugWindowFn extends DoFn<KV<String, Iterable<PubsubMessage>>, String> {

    @ProcessElement
    public void process(ProcessContext c, BoundedWindow window ) throws Exception {
        PaneInfo p = c.pane();
        KV<String, Iterable<PubsubMessage>> elem = c.element();
        SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss.SSS");
        Long minDate = null;
        Long maxDate = null;
        long count = 0;
        String groupKey = null;
        List<String> seq = new ArrayList<>();

        for (PubsubMessage pubsubMessage : elem.getValue()) {
            count++;
            seq.add(pubsubMessage.getAttribute("sequence"));
            Long dt = Long.parseLong(pubsubMessage.getAttribute("publishTs"));
            if( minDate == null || minDate > dt){
                minDate = dt;
            }
            if( maxDate == null || maxDate < dt){
                maxDate = dt;
            }
            if( groupKey == null ){
                groupKey = pubsubMessage.getAttribute("groupKey");
            }
            if( !pubsubMessage.getAttribute("groupKey").equals(groupKey) ){
                System.out.println("ERROR - groupKeys are mixed together");
                throw new RuntimeException("ERROR - groupKeys are mixed together");
            }
        }


        System.out.println(String.format("[%s] Thread=%s | groupKey=%s | # of items=%s | data range=%s - %s | window range=%s - %s | sequence=%s  ",
                p.getTiming(),
                String.valueOf(Thread.currentThread().getId()),
                groupKey,
                count,
                df.format(minDate),
                df.format(maxDate),
                df.format(((IntervalWindow) window).start().toDate()),
                df.format(((IntervalWindow) window).end().toDate()),
                seq.stream().map((val)->Long.valueOf(val)).sorted().map((val)->val.toString()).collect(Collectors.joining(",")))); //sort as longs before outputting string
        Thread.sleep(500);
    }

}
