package mapreducejob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

public class MapReduceJob {

    public static class Map extends Mapper<LongWritable, Text, Text, WordCountMapWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] wordList = line.split(" ");
            HashMap<Text, WordCountMapWritable> record = new HashMap<>();
            for (int i = 0; i < wordList.length - 2; i++) {
                Text ab = new Text(wordList[i] + " " + wordList[i + 1]);
                Text c = new Text(wordList[i + 2]);
                if (!record.containsKey(ab)) record.put(ab, new WordCountMapWritable());
                record.get(ab).addCount(c, 1);
            }
            for(Text recordKey : record.keySet()){
                context.write(recordKey, record.get(recordKey));
            }
        }
    }

    public static class Reduce extends Reducer<Text, WordCountMapWritable, Text, WordCountMapWritable> {
        public void reduce(Text key, Iterable<WordCountMapWritable> values, Context context) throws IOException, InterruptedException {
            WordCountMapWritable mergeResultMap = new WordCountMapWritable();
            int sum = 0;
            for (WordCountMapWritable countMap : values) {
                for (java.util.Map.Entry<Writable, Writable> entry : countMap.entrySet()) {
                    int entryVal = ((IntWritable) entry.getValue()).get();
                    mergeResultMap.addCount((Text) entry.getKey(), entryVal);
                    sum += entryVal;
                }
            }
            for (java.util.Map.Entry<Writable, Writable> entry : mergeResultMap.entrySet()) {
                double rawData = (double) ((IntWritable) entry.getValue()).get();
                mergeResultMap.put(entry.getKey(), new DoubleWritable(rawData / sum));
            }
            context.write(key, mergeResultMap);
        }
    }

    public static class Combiner extends Reducer<Text, WordCountMapWritable, Text, WordCountMapWritable> {
        public void reduce(Text key, Iterable<WordCountMapWritable> values, Context context) throws IOException, InterruptedException {
            WordCountMapWritable mergeResultMap = new WordCountMapWritable();
            for (WordCountMapWritable countMap : values) {
                for (java.util.Map.Entry<Writable, Writable> entry : countMap.entrySet()) {
                    int entryVal = ((IntWritable) entry.getValue()).get();
                    mergeResultMap.addCount((Text) entry.getKey(), entryVal);
                }
            }
            context.write(key, mergeResultMap);
        }
    }

    public static void run(String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "tri-gram");
        job.setJarByClass(MapReduceJob.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(WordCountMapWritable.class);

        job.setMapperClass(MapReduceJob.Map.class);
        job.setReducerClass(MapReduceJob.Reduce.class);
        job.setCombinerClass(MapReduceJob.Combiner.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }
}
