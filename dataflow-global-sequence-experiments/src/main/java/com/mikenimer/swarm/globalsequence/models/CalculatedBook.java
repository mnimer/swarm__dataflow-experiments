package com.mikenimer.swarm.globalsequence.models;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class CalculatedBook implements Serializable {

    private String channel;
    private Long sequence;
    private Long tradeDate;
    private BigDecimal price;
    private Instant calcTs = Instant.now();

    List<PendingTrade> inOrderTrades = new ArrayList<>();
    List<PendingTrade> pendingTrades = new ArrayList<>();


    public CalculatedBook(String channel, Long sequence, Long tradeDate, BigDecimal price) {
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

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public Instant getCalcTs() {
        return calcTs;
    }

    public void setCalcTs(Instant calcTs) {
        this.calcTs = calcTs;
    }

    public void addTrade(PendingTrade trade){
        this.pendingTrades.add(trade);
    }

    public List<PendingTrade> getPendingTrades() {
        return pendingTrades;
    }


    public void addInOrderTrade(PendingTrade trade){
        this.getInOrderTrades().add(trade);
    }

    public List<PendingTrade> getInOrderTrades() {
        return inOrderTrades;
    }


    public CalculatedBook clone(){
        return new CalculatedBook(this.channel, this.sequence, this.tradeDate, this.price);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CalculatedBook that = (CalculatedBook) o;
        return Objects.equals(channel, that.channel) && Objects.equals(sequence, that.sequence) && Objects.equals(tradeDate, that.tradeDate) && Objects.equals(price, that.price);
    }

    @Override
    public int hashCode() {
        return Objects.hash(channel, sequence, tradeDate, price);
    }

    @Override
    public String toString() {
        return "CalculatedBook{" +
                "channel='" + channel + '\'' +
                ", sequence=" + sequence +
                ", tradeDate=" + tradeDate +
                ", price=" + price +
                ", ts=" + calcTs +
                '}';
    }
}
