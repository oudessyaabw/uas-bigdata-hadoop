package org.example;
 /*
    Import library dari Java Package
  */

import java.io.IOException;
/*
    Import library dari Hadoop Package untuk menjalankan fungsi Pembacaan, Penulisan File ke dalam HDFS dan
  menjalankan MapReduce
  */

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;


public class MarketSalesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text t_key, Iterable<IntWritable> values, Context context) throws IOException,
            InterruptedException {

        Text key = t_key;

        int frequencyForMarket = 0;

        for (IntWritable val: values) {
            // frequencyForMarket = frequencyForMarket + val.get()
            frequencyForMarket += val.get();
        }
        context.write(key, new IntWritable(frequencyForMarket));
    }
}
