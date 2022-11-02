package com.mikenimer.swarm.globalsequence.fn;

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

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;


public class DebugGlobalWindowFn extends DoFn<KV<String, PubsubMessage>, CalculatedBook> {

    private static final Logger log = LoggerFactory.getLogger(DebugGlobalWindowFn.class);

    @StateId("lastWin")
    private final StateSpec<ValueState<Map<String, CalculatedBook>>> indexSpec = StateSpecs.value(MapCoder.of(StringUtf8Coder.of(), SerializableCoder.of(CalculatedBook.class)));

    @ProcessElement
    public void process(ProcessContext c, BoundedWindow window, @StateId("lastWin") ValueState<Map<String, CalculatedBook>> cache ) throws Exception {
        SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss.SSS");

        //extract data from key
        KV<String, PubsubMessage> elem = c.element();
        String channel = elem.getKey();
        PubsubMessage pubsubMessage = elem.getValue();
        Long seq = Long.valueOf(pubsubMessage.getAttribute("sequence"));
        Long tradeDate = Long.parseLong(pubsubMessage.getAttribute("publishTs"));
        Double price = 0.0; // todo: get from payload
        // System.out.println(String.format("Channel=%s Seq=%s | thread=%s ts=%s", channel, seq, Thread.currentThread().getId(), String.valueOf(System.currentTimeMillis())));



        //GlobalWindowCache globalWindowCache = new GlobalWindowCache(channel, tradeDate, seq, price);

        Long lastValidSeq = null;
        Map<String, CalculatedBook> _cache = cache.read();
        if( _cache == null){
            //init cache
            _cache = new HashMap<>();
        }

        CalculatedBook calculatedBook;
        //first entry
        if(!_cache.containsKey(channel)){
            System.out.println(String.format("Add channel '%s' to cache ", channel));
            calculatedBook = new CalculatedBook(channel, seq, tradeDate, price);
            _cache.put(channel, calculatedBook);
        }else{
            CalculatedBook lastBook = _cache.get(channel);
            //if we are the next book, great - calculated the next book value
            if( lastBook.getSequence()+1 == seq && lastBook.getPendingTrades().size() == 0) {
                //add price
                System.out.println(String.format("calculate single book | channel '%s' seq '%s' | thread=%s ts=%s", channel, seq, Thread.currentThread().getId(), System.currentTimeMillis()));

                calculatedBook = getCalculatedBook(channel, seq, tradeDate, price, lastBook);
                _cache.put(channel, calculatedBook);
            }else{
                List<PendingTrade> pendingTrades = lastBook.getPendingTrades();
                boolean isInSeq = false;

                //compare all sequence numbers in pending trades list with this
                List<Long> pendingSeq = pendingTrades.stream().map((v)->v.getSequence()).collect(Collectors.toList());
                pendingSeq.add(lastBook.getSequence());
                isInSeq = checkSequence(pendingSeq, seq);


                if(isInSeq) {
                    System.out.println(String.format("calculate pending book | channel '%s' seq '%s' | pending=%s | thread=%s ts=%s", channel, seq, pendingSeq.stream().sorted().collect(Collectors.toList()), Thread.currentThread().getId(), System.currentTimeMillis()));

                    //todo: add calculate price logic
                    calculatedBook = getCalculatedBook(channel, seq, tradeDate, price, lastBook);
                    _cache.put(channel, calculatedBook);
                }else{
                    //System.out.println(String.format("Out of sequence | last=%s | putting channel '%s' seq '%s' in cache", lastBook.getSequence(), channel, seq));
                    // Not in sequence, add trade to the pending trades cache and wait.
                    lastBook.addTrade(new PendingTrade(channel, seq, tradeDate, price, pubsubMessage.getPayload()));
                    _cache.put(channel, lastBook);
                }
            }
        }

        cache.write(_cache);

        //Thread.sleep(500);
    }

    private static CalculatedBook getCalculatedBook(String channel, Long seq, Long tradeDate, Double price, CalculatedBook lastBook) {
        CalculatedBook calculatedBook;
        calculatedBook = new CalculatedBook(channel, seq, tradeDate, lastBook.getPrice() + price);
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
                //System.out.println(String.format("missing %s | %s", String.valueOf(last+1), pendingSeq.toString()));
                return false;
            }
        }
        if( lastSeq+1 == currentSeq) {
            return true;
        }
        return false;
    }

}
