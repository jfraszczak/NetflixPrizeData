package com.example.bigdata;

import java.io.Serializable;

public class MovieAggregate implements Serializable {

    private int rateCount = 0;
    private int rateSum = 0;
    private int uniqueRatesCount = 0;

    public MovieAggregate() {

    }

    public MovieAggregate(int rateCount, int rateSum, int uniqueRatesCount) {
        this.rateCount = rateCount;
        this.rateSum = rateSum;
        this.uniqueRatesCount = uniqueRatesCount;
    }

    public void update(MovieRateRecord record) {
        rateCount++;
        rateSum += record.getRate();
    }

    @Override
    public String toString() {
        return String.format("%d;%d;%d", rateCount, rateSum, uniqueRatesCount);
    }

    public static MovieAggregate fromString(String aggregateString) {
        String[] splitted = aggregateString.split(";");
        return new MovieAggregate(Integer.parseInt(splitted[0]), Integer.parseInt(splitted[1]), Integer.parseInt(splitted[2]));
    }

    public int getRateCount() {
        return rateCount;
    }

    public void setRateCount(int rateCount) {
        this.rateCount = rateCount;
    }

    public int getRateSum() {
        return rateSum;
    }

    public void setRateSum(int rateSum) {
        this.rateSum = rateSum;
    }

    public int getUniqueRatesCount() {
        return uniqueRatesCount;
    }

    public void setUniqueRatesCount(int uniqueRatesCount) {
        this.uniqueRatesCount = uniqueRatesCount;
    }
}
