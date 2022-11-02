package com.mikenimer.swarm.globalsequence.models;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class CalculatedBook implements Serializable {

    private String channel;
    private Long sequence;
    private Long tradeDate;
    private Double price;

    List<PendingTrade> pendingTrades = new ArrayList<>();


    public CalculatedBook(String channel, Long sequence, Long tradeDate, Double price) {
        this.channel = channel;
        this.sequence = sequence;
        this.tradeDate = tradeDate;
        this.price = price;
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

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }


    public void addTrade(PendingTrade trade){
        this.pendingTrades.add(trade);
    }

    public List<PendingTrade> getPendingTrades() {
        return pendingTrades;
    }
}
