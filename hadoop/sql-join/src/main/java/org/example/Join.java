package org.example;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Join {

    public static String removeColumn(String[] csvCols, int colIndex) {
        String[] newColumns = new String[csvCols.length - 1];

        for (int i = 0, j = 0; i < csvCols.length; i++) {
            if (i != colIndex) {
                newColumns[j++] = csvCols[i];
            }
        }

        return String.join(",", newColumns);
    }

    public static class Table1Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            String joinColId1String = conf.get("datasetColId1");
            int joinColId1 = Integer.parseInt(joinColId1String);

            String[] columns = value.toString().split(",");
            String joinKey = columns[joinColId1];

            removeColumn(columns, joinColId1);

            outputKey.set(joinKey);
            outputValue.set("table1," + removeColumn(columns, joinColId1));

            context.write(outputKey, outputValue);
        }
    }

    public static class Table2Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            String joinColId2String = conf.get("datasetColId2");
            int joinColId2 = Integer.parseInt(joinColId2String);

            String[] columns = value.toString().split(",");
            String joinKey = columns[joinColId2];

            outputKey.set(joinKey);
            outputValue.set("table2," + removeColumn(columns, joinColId2));

            context.write(outputKey, outputValue);
        }
    }

    public static class TableReducer extends Reducer<Text, Text, Text, Text> {

        private final Text outputValue = new Text();

        private final ArrayList<String> buffer = new ArrayList<>();

        private String currTag = null;

        public void merge(Text key, String row1, Context context) throws IOException, InterruptedException {
            for (String row2 : buffer) {
                outputValue.set(key + "," + row1 + "," + row2);
                context.write(key, outputValue);
            }
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text value : values) {

                // Expected format of "value": "[pathToDataSet],the,rest,is,normal,csv,row,data,..."
                String[] taggedValue = value.toString().split(",", 2);
                String tag = taggedValue[0];
                String row = removeColumn(taggedValue, 0);

                if (currTag == null) {
                    currTag = tag;
                }

                if (currTag.equals(tag)) {
                    // Buffer first table
                    buffer.add(row);
                } else {
                    currTag = tag;
                    // Join rows from second table against first
                    merge(key, row, context);
                }
            }

            // Garbage collection should take care of this
            buffer.clear();
        }
    }

    /**
     * 1. Make Input Directory
     *
     *      hdfs dfs -mkdir -p /simple_join/input
     *
     * 2. Insert into HDFS:
     *
     *      hdfs dfs -put pokegen.txt /simple_join/input
     *      hdfs dfs -put pokestats.txt /simple_join/input
     *
     *      hdfs dfs -ls /simple_join/input
     *
     * 3. Run Queries
     *
     *      hadoop jar sql-join-1.0-SNAPSHOT.jar /simple_join/input/pokegen.txt 0 /simple_join/input/pokestats.txt 0 /simple_join/output
     *
     *  4. Get results
     *
     *      hdfs dfs -ls /simple_join/output
     *
     *      hdfs dfs -get /simple_join/output simple-join-10-report
     *
     *  5. Clear results for future runs
     *
     *      hdfs dfs -rm -r /simple_join/output
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("dataset1Path", args[0]);
        conf.set("datasetColId1", args[1]);
        conf.set("dataset2Path", args[2]);
        conf.set("datasetColId2", args[3]);
        Job job = Job.getInstance(conf, "SQL Join");
        job.setJarByClass(Join.class);
        job.setReducerClass(TableReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Table1Mapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, Table2Mapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[4]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}