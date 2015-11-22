package de.dimajix.training.hadoop.weather;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


class WeatherMapper extends Mapper<LongWritable,Text,Text,FloatWritable> {
    private Text country = new Text();
    private FloatWritable temperature = new FloatWritable();

    @Override
    public void setup(Context context) {
    }

    @Override
    public void cleanup(Context context) {
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String row = value.toString();

        String station = row.substring(4,15);
        Character airTemperatureQuality = row.charAt(92);
        String airTemperature = row.substring(87,92);

        // Only emit if quality is okay
        if (airTemperatureQuality.charValue() == '1') {
            // country = countries[station]
            country.set(station);
            temperature.set(Float.valueOf(airTemperature));
            context.write(country, temperature);
        }
    }
}