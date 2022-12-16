package com.mikenimer.swarm.globalsequence.fn;

import com.google.common.collect.Iterables;
import com.mikenimer.swarm.globalsequence.StarterPipeline;
import com.mikenimer.swarm.globalsequence.models.CalculatedBook;
import com.mikenimer.swarm.globalsequence.models.PendingTrade;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;


public class DebugStateTimeWindowFn extends DoFn<KV<String, PubsubMessage>, CalculatedBook> {

    private static final Logger log = LoggerFactory.getLogger(DebugStateTimeWindowFn.class);

    @StateId("lastBook")
    private final StateSpec<ValueState<CalculatedBook>> lastBook = StateSpecs.value(SerializableCoder.of(CalculatedBook.class));

    @StateId("pendingTrades")
    private final StateSpec<BagState<PendingTrade>> pendingTrades = StateSpecs.bag(SerializableCoder.of(PendingTrade.class));

    @TimerId("calcTimer")
    private final TimerSpec calcTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @ProcessElement
    public void process(ProcessContext c, BoundedWindow window,
                        @StateId("lastBook") ValueState<CalculatedBook> book,
                        @StateId("pendingTrades") BagState<PendingTrade> trades,
                        @TimerId("calcTimer") Timer calcTimer
    ) throws Exception {
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

        // Initialized cache, if missing
        // Add Pending Trade to cache
        CalculatedBook lastBook = book.read();
        if (lastBook == null) {
            //if nothing in cache, calculate book off of first message
            lastBook = new CalculatedBook(channel, seq, tradeDate, price);
            book.write(lastBook);
            //start timer
            calcTimer.offset(Duration.standardSeconds(c.getPipelineOptions().as(StarterPipeline.JobOptions.class).getWindow())).setRelative();
        } else if(seq > lastBook.getSequence()) {
            PendingTrade trade = new PendingTrade(channel, seq, tradeDate, price, pubsubMessage.getPayload());
            trades.add(trade);
        } else {
            //received a message with a sequence number before that last calculated book
            //todo: decide how to handle this.
        }
    }

    @OnTimer("calcTimer")
    public void onTimer(
            OnTimerContext c,
            @StateId("lastBook") ValueState<CalculatedBook> book,
            @StateId("pendingTrades") BagState<PendingTrade> trades,
            @TimerId("calcTimer") Timer calcTimer) {

        Long start = System.currentTimeMillis();
        int tradeSize = Iterables.size(trades.read());

        Set<PendingTrade> tree = new TreeSet<>(Comparator.comparingLong(PendingTrade::getSequence));
        for (PendingTrade trade : trades.read()) {
            tree.add(trade);
        }


        // Find the trades in order, stop when we find a missing sequence
        List<PendingTrade> inOrderTrades = new ArrayList<>();
        List<PendingTrade> outOfOrderTrades = new ArrayList<>();
        Long firstSeq = book.read().getSequence();
        Long lastSeq = firstSeq;
        if( !tree.isEmpty() ) {
            //start after last calculated book sequence
            for (Iterator<PendingTrade> it = tree.iterator(); it.hasNext(); ) {
                PendingTrade t = it.next();
                if( lastSeq+1 == t.getSequence()){
                    lastSeq = t.getSequence();
                    inOrderTrades.add(t);
                }else{
                    //found missing seq, break out and check in next loop
                    outOfOrderTrades.add(t);
                }
            }
        }


        //With the In Order Trades, calculate a new book
        CalculatedBook workingBook = book.read().clone();
        for (PendingTrade inOrderTrade : inOrderTrades) {
            workingBook.setPrice(workingBook.getPrice().add( new BigDecimal(0.01) ) ); // add a penny to every trade
            workingBook.setSequence(inOrderTrade.getSequence());
        }
        //save new book
        book.write(workingBook);



        //Catch ALL. If the last calculated time has exceeded some time limit or the number of  outOfOrder trades is to large
        // we trigger fall back plan and go back to Cassandra to calculate the whole range
        if( inOrderTrades.size() == 0  && tradeSize > 0 ){
            // if 5 minutes since last book was calculated. Go back cassandra
            int ttl = 360;
            if( tradeSize > 10000 || (Instant.now().getEpochSecond() - book.read().getCalcTs().getEpochSecond()) > ttl  ){
                Long first = book.read().getSequence()+1;
                Long last = tree.stream().skip(tradeSize-1).findFirst().get().getSequence();

                // todo": Query cassandra for range of messages between first & last
                // ...

                //todo: calculate new book
                //workingBook = ....

                //todo: save new book
                //book.write(workingBook);

                // clear tree, don't need to save what's in cache.
                tree.clear();
            }
        }

        //With the Out of order trades, put them back in the cache.
        trades.clear();
        for (Iterator<PendingTrade> it = tree.iterator(); it.hasNext(); ) {
            PendingTrade remainingTrades = it.next();
            if( remainingTrades.getSequence() > workingBook.getSequence()) {
                trades.add(remainingTrades);
            }
        }


        //set trigger for next time
        Long end = System.currentTimeMillis();
        log.info("[{}ms] Timer @ {} fired | channel={} | pending={} | calc={} | remaining={} |  calc={}-{} ",
                (end-start), c.timestamp(), book.read().getChannel(), tradeSize, inOrderTrades.size(), outOfOrderTrades.size(), firstSeq, lastSeq);
        calcTimer.offset(Duration.standardSeconds(c.getPipelineOptions().as(StarterPipeline.JobOptions.class).getWindow())).setRelative();
    }


}
