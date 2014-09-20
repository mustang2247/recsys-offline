package com.ganqiang.recsys.cf.old;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

//格式化用户的评分数据   key：userid   value:  Item Vector
//输出格式：1  {101:5.0,103:2.5,102:3.0}
//                     2   {101:2.0,103:5.0,104:2.0,102:2.5}
public class Step1 {

    public static final String INPUT_PATH = "hdfs://localhost:9000/input/first";
    public static final String OUTPUT_PATH = "hdfs://localhost:9000/output/first";

    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();

        Job job1 = new Job(conf1, "step1");
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.setNumReduceTasks(1);
        job1.setJarByClass(Step1.class);
        job1.setMapperClass(WikiMapper1.class);
        job1.setMapOutputKeyClass(VarLongWritable.class);
        job1.setMapOutputValueClass(LongAndFloat.class);
        job1.setReducerClass(WiKiReducer1.class);
        job1.setOutputKeyClass(VarLongWritable.class);
        job1.setOutputValueClass(VectorWritable.class);

        FileInputFormat.addInputPath(job1, new Path( INPUT_PATH ) );
        SequenceFileOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH ));
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }
    }

    public static class WikiMapper1 extends Mapper<LongWritable, Text, VarLongWritable, LongAndFloat> {
 
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            VarLongWritable userID = new VarLongWritable();
            LongWritable itemID = new LongWritable();
            FloatWritable itemValue = new FloatWritable();System.err.println("key:"+key+"    value:"+value+"   ");
            String line = value.toString();
            String[] info = line.split(",");
            if (info.length != 3) {
                return;
            }
            userID.set(Long.parseLong(info[0]));
            itemID.set(Long.parseLong(info[1]));
            itemValue.set(Float.parseFloat(info[2]));
            context.write(userID, new LongAndFloat(itemID, itemValue));
        }
    }

    public static class WiKiReducer1 extends  Reducer<VarLongWritable, LongAndFloat, VarLongWritable, VectorWritable> {

        public void reduce(VarLongWritable userID,   Iterable<LongAndFloat> itemPrefs, Context context)   throws IOException, InterruptedException {
            Vector userVector = new RandomAccessSparseVector(Integer.MAX_VALUE, 10);
            for (LongAndFloat itemPref : itemPrefs) {
                userVector.set( Integer.parseInt(itemPref.getFirst().toString()),  Float.parseFloat(itemPref.getSecond().toString()));
            }
            context.write(userID, new VectorWritable(userVector));
        }

    }

}
