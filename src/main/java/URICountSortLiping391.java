import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Liping on 4/28/16.
 */
public class URICountSortLiping391 {
    public static class SplitMapperLiping391 extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            word.set(value.toString().split("\"")[1].split(" ")[1]);
            context.write(word, one);
        }
    }

    public static class IntSumReducerLiping391 extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class CountMapperLiping391 extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            int count = Integer.parseInt(splits[splits.length - 1]);
            context.write(new IntWritable(count), value);
        }
    }

    public static class SortReducerLiping391 extends Reducer<IntWritable, Text, NullWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(NullWritable.get(), value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // Count Job
        Configuration countJobConf = new Configuration();
        Job countJob = Job.getInstance(countJobConf, "URI Hit Count");
        countJob.setJarByClass(URICountSortLiping391.class);
        countJob.setMapperClass(SplitMapperLiping391.class);
        countJob.setCombinerClass(IntSumReducerLiping391.class);
        countJob.setReducerClass(IntSumReducerLiping391.class);
        countJob.setOutputKeyClass(Text.class);
        countJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(countJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(countJob, new Path(args[1]));

        // Sort Job
        Configuration sortJobConf = new Configuration();
        Job sortJob = Job.getInstance(sortJobConf, "Count Sorting");
        sortJob.setJarByClass(URICountSortLiping391.class);
        sortJob.setMapperClass(CountMapperLiping391.class);
        sortJob.setReducerClass(SortReducerLiping391.class);
        //sortJob.setNumReduceTasks(2);
        sortJob.setMapOutputKeyClass(IntWritable.class);
        sortJob.setMapOutputValueClass(Text.class);
        sortJob.setOutputKeyClass(Text.class);
        sortJob.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(sortJob, new Path(args[1]));
        FileOutputFormat.setOutputPath(sortJob, new Path(args[2]));

        if (countJob.waitForCompletion(true)) {
            System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
        }
    }
}
