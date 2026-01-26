package com.lab.mapreduce.utils;

public class Record {
    public Float tempmax ;
    public Float  tempmin;
    public Float temp ;
    public Float feelslikemax ;
    public Float feelslikemin ;
    public Float feelslike ;
    public Float dew ;
    public Float humidity ;
    public Float precip ;
    public Float precipprob ;
    public Float precipcover ;
    public String preciptype ;
    public Float windspeed ;
    public Float winddir ;
    public Float sealevelpressure ;
    public Float cloudcover ;
    public Float  visibility ;
    public String conditions ;


    public Record(Float tempmax, Float tempmin, Float temp, Float feelslikemax, Float feelslikemin, Float feelslike, Float dew, Float humidity, Float precip, Float precipprob, Float precipcover, String preciptype, Float windspeed, Float winddir, Float sealevelpressure, Float cloudcover, Float visibility, String conditions) {
        this.tempmax = tempmax;
        this.tempmin = tempmin;
        this.temp = temp;
        this.feelslikemax = feelslikemax;
        this.feelslikemin = feelslikemin;
        this.feelslike = feelslike;
        this.dew = dew;
        this.humidity = humidity;
        this.precip = precip;
        this.precipprob = precipprob;
        this.precipcover = precipcover;
        this.preciptype = preciptype;
        this.windspeed = windspeed;
        this.winddir = winddir;
        this.sealevelpressure = sealevelpressure;
        this.cloudcover = cloudcover;
        this.visibility = visibility;
        this.conditions = conditions;
    }
}
