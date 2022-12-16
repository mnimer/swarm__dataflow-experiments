package com.mikenimer.swarm.globalsequence.fn;

import com.mikenimer.swarm.globalsequence.StarterPipeline;
import com.mikenimer.swarm.globalsequence.models.CalculatedBook;
import com.mikenimer.swarm.globalsequence.models.PendingTrade;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


public class DebugGlobalWindowFn2 extends DoFn<KV<String, PubsubMessage>, CalculatedBook> {

    private static final Logger log = LoggerFactory.getLogger(DebugGlobalWindowFn2.class);

    @StateId("lastWin")
    private final StateSpec<ValueState<CalculatedBook>> indexSpec = StateSpecs.value(SerializableCoder.of(CalculatedBook.class));


    @ProcessElement
    public void process(ProcessContext c, BoundedWindow window, @StateId("lastWin") ValueState<CalculatedBook> cache ) throws Exception {
        //SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss.SSS");
        //StarterPipeline.JobOptions options = c.getPipelineOptions().as(StarterPipeline.JobOptions.class);

        //extract data from message
        KV<String, PubsubMessage> elem = c.element();
        String channel = elem.getKey();
        PubsubMessage pubsubMessage = elem.getValue();
        Long seq = Long.valueOf(pubsubMessage.getAttribute("sequence"));
        Long tradeDate = Long.parseLong(pubsubMessage.getAttribute("publishTs"));
        BigDecimal price = new BigDecimal(0.0); // todo: get from payload, we'll cheat now and hard code a value

        //log.info(String.format("Channel=%s Seq=%s", channel, seq));
        //pull from cache
        CalculatedBook lastBook = cache.read();
        if( lastBook == null ){
            //initialize cache if missing
            lastBook = new CalculatedBook(channel, seq, tradeDate, price);
            cache.write(lastBook);
        }else {
            //add trade to pending queue
            lastBook.addTrade(new PendingTrade(channel, seq, tradeDate, price, pubsubMessage.getPayload()));
        }

        //sort all pending trades in book
        CalculatedBook workingBook = sortPendingTrades(lastBook);

        //If we have any pending trades that are in order, calculate a new book, save in cache and return
        BigDecimal newPrice = lastBook.getPrice();
        if( workingBook.getInOrderTrades().size() > 0){

            log.info(String.format("Channel=%s Seq=%s | InOrderTrades=%s | pendingTrades=%s | range=%s-%s | thread=%s | ts=%s",
                    channel, seq,
                    workingBook.getInOrderTrades().size(),
                    workingBook.getPendingTrades().size(),
                    workingBook.getSequence(), workingBook.getInOrderTrades().get(workingBook.getInOrderTrades().size()-1).getSequence(),
                    Thread.currentThread().getId(), System.currentTimeMillis()));

            //loop over and calc a new book

            for (PendingTrade inOrderTrade : workingBook.getInOrderTrades()) {
                workingBook.setSequence( inOrderTrade.getSequence() );
                newPrice.add( new BigDecimal(0.01) ); //inOrderTrade.getPayload().getPrice()
            }
            //save new price for book
            workingBook.setPrice(newPrice);


            //clear temp list before saving to cache
            workingBook.getInOrderTrades().clear();

            cache.write(workingBook);
            c.output(workingBook);

        }

    }

    private CalculatedBook sortPendingTrades(CalculatedBook lastBook) {
        Long lastSeq = lastBook.getSequence();
        List<PendingTrade> pendingTrades = lastBook.getPendingTrades().stream().sorted().collect(Collectors.toList());

        CalculatedBook clonedBook = lastBook.clone();
        List<PendingTrade> trades = new ArrayList<>();
        List<PendingTrade> outOfOrderTrades = new ArrayList<>();
        for (PendingTrade pt : pendingTrades) {
            if( pt.getSequence().equals(lastSeq) ) {
                //do nothing
            }else if( pt.getSequence().equals(lastSeq+1) ) { //is next item in order
                clonedBook.getInOrderTrades().add(pt);
                lastSeq = pt.getSequence();
            }else if( pt.getSequence() > lastSeq ){
                clonedBook.getPendingTrades().add(pt);
            }
        }

        return clonedBook;
    }

}
