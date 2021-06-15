package com.example.bigdata;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MovieRateRecord implements Serializable {

    private static final String LINE_PATTERN_STRING = "^\\d{4}-\\d{2}-\\d{2},(\\d+),(\\d+),(\\d+)$";
    private static final Pattern LINE_PATTERN = Pattern.compile(LINE_PATTERN_STRING);

    private LocalDate date;
    private String filmId;
    private String userId;
    private int rate;
    private String title;

    public MovieRateRecord(LocalDate date, String film_id, String user_id, int rate) {
        this.date = date;
        this.filmId = film_id;
        this.userId = user_id;
        this.rate = rate;
        this.title = "";
    }

    public static boolean lineIsCorrect(String line) {
        Matcher m = LINE_PATTERN.matcher(line);
        return m.find();
    }

    public static MovieRateRecord parseFromLine(String line) {
        String[] splittedLine = line.split(",");

        LocalDate date = LocalDate.parse(splittedLine[0]);
        String filmId = splittedLine[1];
        String userId = splittedLine[2];
        int rate = Integer.parseInt(splittedLine[3]);

        return new MovieRateRecord(date, filmId, userId, rate);
    }

    public long getTimestampInMillis() {
        //milliseconds from 1960-01-01
        return (date.toEpochDay() + 3653) * 86400000;
    }

    @Override
    public String toString() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String dateString = date.format(formatter);

        return String.format("%s;%s;%s;%d;", dateString, filmId, userId, rate);
    }

    public static MovieRateRecord fromString(String line) {
        String[] splittedLine = line.split(";");

        LocalDate date = LocalDate.parse(splittedLine[0]);
        String filmId = splittedLine[1];
        String userId = splittedLine[2];
        int rate = Integer.parseInt(splittedLine[3]);

        return new MovieRateRecord(date, filmId, userId, rate);
    }

    public String getMonth() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM");
        return date.format(formatter);
    }

    public LocalDate getDate() {
        return date;
    }

    public void setDate(LocalDate date) {
        this.date = date;
    }

    public String getFilmId() {
        return filmId;
    }

    public void setFilmId(String filmId) {
        this.filmId = filmId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public int getRate() {
        return rate;
    }

    public void setRate(int rate) {
        this.rate = rate;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}
