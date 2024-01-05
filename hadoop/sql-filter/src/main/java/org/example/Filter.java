package org.example;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Filter {

    public static class FilterMapper extends Mapper<Object, Text, Text, Text>{

        private final Text searchKey = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            String regex = conf.get("regex");
            searchKey.set(regex);

            Pattern pattern = Pattern.compile(regex);

            String line = value.toString();
            Matcher matcher = pattern.matcher(line);

            if (matcher.find()) {
                context.write(searchKey, new Text(line));
            }
        }
    }

    public static class FilterReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text val : values) {
                context.write(key, val);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("regex", args[0]);
        Job job = Job.getInstance(conf, "SQL filter");
        job.setJarByClass(Filter.class);
        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(FilterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}