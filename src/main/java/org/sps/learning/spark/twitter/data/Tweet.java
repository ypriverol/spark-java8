package org.sps.learning.spark.twitter.data;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;
import twitter4j.GeoLocation;
import twitter4j.Place;

public class Tweet {

    private String user;
    private String text;
    private Date createdAt;
    private Place place;
    private GeoLocation geoLocation;
    private String language;
    private String sentiment;

    public Tweet(String user, String text, Date createdAt, Place place, GeoLocation geoLocation, String language, String sent) {
        super();
        this.user = user;
        this.text = text;
        this.createdAt = createdAt;
        this.place = place;
        this.geoLocation = geoLocation;
        this.language = language;
        this.sentiment = sent;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss", timezone = "UTC")
    public Date getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }

    public String getLanguage() { return language; }

    public void setLanguage(String language) {
        this.language = language;
    }

    public Place getPlace() { return place; }

    public void setPlace(Place place) { this.place = place; }

    public GeoLocation getGeoLocation() { return geoLocation; }

    public void setGeoLocation(GeoLocation geoLocation) { this.geoLocation = geoLocation; }

    public int getSentiment(String text) {
        text = text.toLowerCase();
        return -500;
    }

}