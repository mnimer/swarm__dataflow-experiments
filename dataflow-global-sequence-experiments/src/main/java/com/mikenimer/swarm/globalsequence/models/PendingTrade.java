package com.mikenimer.swarm.globalsequence.models;

import java.io.Serializable;
import java.math.BigDecimal;

public class PendingTrade implements Serializable, Comparable {
    String channel;
    Long sequence;
    Long tradeDate;
    BigDecimal price;
    byte[] payload;

    public PendingTrade(String channel, Long sequence, Long tradeDate, BigDecimal price, byte[] payload) {
        this.channel = channel;
        this.sequence = sequence;
        this.tradeDate = tradeDate;
        this.price = price;
        this.payload = payload;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public Long getSequence() {
        return sequence;
    }

    public void setSequence(Long sequence) {
        this.sequence = sequence;
    }

    public Long getTradeDate() {
        return tradeDate;
    }

    public void setTradeDate(Long tradeDate) {
        this.tradeDate = tradeDate;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }


    @Override
    public int compareTo(Object o) {
        return this.sequence.compareTo(((PendingTrade)o).getSequence());
    }
}
