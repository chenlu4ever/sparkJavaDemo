package com.model;

import java.io.Serializable;

public class RatingModel implements Serializable {

    private int userId;
    private int movieId;
    private double rating;
    private int timestamp;

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public int getMovieId() {
        return movieId;
    }

    public void setMovieId(int movieId) {
        this.movieId = movieId;
    }

    public double getRating() {
        return rating;
    }

    public void setRating(double rating) {
        this.rating = rating;
    }

    public int getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }

    public RatingModel(int userId, int movieId, double rating){
        this.userId=userId;
        this.movieId=movieId;
        this.rating=rating;
    }
}
