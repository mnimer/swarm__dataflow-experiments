package com.mikenimer.swarm.globalsequence.fn;

import com.mikenimer.swarm.globalsequence.StarterPipeline;
import com.mikenimer.swarm.globalsequence.models.CalculatedBook;
import com.mikenimer.swarm.globalsequence.models.PendingTrade;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
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
import java.util.*;
import java.util.stream.Collectors;


public class DebugGlobalWindowFn extends DoFn<KV<String, PubsubMessage>, CalculatedBook> {

    private static final Logger log = LoggerFactory.getLogger(DebugGlobalWindowFn.class);

    @StateId("lastWin")
    private final StateSpec<ValueState<CalculatedBook>> indexSpec = StateSpecs.value(SerializableCoder.of(CalculatedBook.class));

    @ProcessElement
    public void process(ProcessContext c, BoundedWindow window, @StateId("lastWin") ValueState<CalculatedBook> cache ) throws Exception {
        SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss.SSS");
        StarterPipeline.JobOptions options = c.getPipelineOptions().as(StarterPipeline.JobOptions.class);

        //extract data from key
        KV<String, PubsubMessage> elem = c.element();
        String channel = elem.getKey();
        PubsubMessage pubsubMessage = elem.getValue();
        Long seq = Long.valueOf(pubsubMessage.getAttribute("sequence"));
        Long tradeDate = Long.parseLong(pubsubMessage.getAttribute("publishTs"));
        BigDecimal price = new BigDecimal(0.0); // todo: get from payload
        // log.info(String.format("Channel=%s Seq=%s | thread=%s ts=%s", channel, seq, Thread.currentThread().getId(), String.valueOf(System.currentTimeMillis())));



        //GlobalWindowCache globalWindowCache = new GlobalWindowCache(channel, tradeDate, seq, price);

        Long lastValidSeq = null;
        CalculatedBook lastBook = cache.read();

        CalculatedBook currentBook = null;
        //first entry
        if( lastBook == null ){
            log.info(String.format("Add channel '%s' to cache ", channel));
            currentBook = new CalculatedBook(channel, seq, tradeDate, price);
            cache.write(currentBook);
        }else {
            if( seq >= lastBook.getSequence() ) { //If a late arriving message arrives after we calculate the book, skip it.
                //if we are the next book, great - calculated the next book value
                if (lastBook.getSequence() + 1 == seq && lastBook.getPendingTrades().size() == 0) {
                    //add price
                    log.info(String.format("calculate single book | channel '%s' seq '%s' | thread=%s ts=%s", channel, seq, Thread.currentThread().getId(), System.currentTimeMillis()));

                    currentBook = getCalculatedBook(channel, seq, tradeDate, price, lastBook);
                    cache.write(currentBook);
                } else {
                    List<PendingTrade> pendingTrades = lastBook.getPendingTrades();
                    boolean isInSeq = false;

                    //compare all sequence numbers in pending trades list with this
                    List<Long> pendingSeq = pendingTrades.stream().map((v) -> v.getSequence()).collect(Collectors.toList());
                    pendingSeq.add(lastBook.getSequence());
                    isInSeq = checkSequence(pendingSeq, seq);


                    if (isInSeq) {
                        log.info(String.format("calculate pending book | channel '%s' seq '%s' | pending=%s | thread=%s ts=%s", channel, seq, pendingSeq.stream().sorted().collect(Collectors.toList()), Thread.currentThread().getId(), System.currentTimeMillis()));

                        //todo: add calculate price logic
                        currentBook = getCalculatedBook(channel, seq, tradeDate, price, lastBook);
                        cache.write(currentBook);
                    } else if (lastBook.getPendingTrades().size() > 1000) {
                        log.warn(String.format("Queue size has reached 1000, pulling data from cassandra | channel=%s | %s", channel, pendingSeq.stream().sorted().collect(Collectors.toList())));
                        //TODO: our pending trades queue is getting to large, the missing sequence might be lost. Time to go back to cassandra
                        //todo: calculate last book, after save to cache, with the data from cassandra
                        Thread.sleep(100); //simulate call to query cassandra and run calc
                        currentBook = getCalculatedBook(channel, seq, tradeDate, price, lastBook);
                        cache.write(currentBook);
                    } else {
                        //log.info(String.format("Out of sequence | last=%s | putting channel '%s' seq '%s' in cache", lastBook.getSequence(), channel, seq));
                        // Out of sequence, add trade to the pending trades cache and wait.
                        lastBook.addTrade(new PendingTrade(channel, seq, tradeDate, price, pubsubMessage.getPayload()));
                        cache.write(lastBook);
                    }
                }
            }
        }

        //cache.write(_cache);

        //Thread.sleep(500);
    }

    private static CalculatedBook getCalculatedBook(String channel, Long seq, Long tradeDate, BigDecimal price, CalculatedBook lastBook) {
        CalculatedBook calculatedBook;
        calculatedBook = new CalculatedBook(channel, seq, tradeDate, lastBook.getPrice().add(price));
        return calculatedBook;
    }

    private boolean checkSequence(List<Long> pendingSeq, Long currentSeq) {
        Long last = null;
        Long lastSeq = null;
        List<Long> sorted = pendingSeq.stream().sorted().collect(Collectors.toList());
        for (Long s : sorted) {
            lastSeq = s;
            if( last == null || last+1 == s){
                last = s;
            }else{
                //log.info(String.format("missing %s | %s", String.valueOf(last+1), pendingSeq.toString()));
                return false;
            }
        }
        if( lastSeq+1 == currentSeq) {
            return true;
        }
        return false;
    }

}
