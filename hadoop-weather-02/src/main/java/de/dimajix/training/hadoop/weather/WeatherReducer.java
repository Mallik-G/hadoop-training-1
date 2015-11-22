package de.dimajix.training.hadoop.weather;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


class WeatherReducer extends Reducer<Text,WeatherData,Text,WeatherData> {
    private WeatherData result = new WeatherData();

    @Override
    public void setup(Context context) {
    }

    @Override
    public void cleanup(Context context) {
    }

    @Override
    public void reduce(Text key, Iterable<WeatherData> values, Context context)
            throws IOException, InterruptedException {
        // Initialize variables such that the first real temperature will overwrite them
        float minTemp = 9999;
        float maxTemp = -9999;

        // Process all temperature values
        for (WeatherData val : values) {
            if (val.minTemperature < minTemp)
                minTemp = val.minTemperature;
            if (val.maxTemperature > maxTemp)
                maxTemp = val.maxTemperature;
        }

        result.maxTemperature = maxTemp;
        result.minTemperature = minTemp;
        context.write(key, result);
    }
}
