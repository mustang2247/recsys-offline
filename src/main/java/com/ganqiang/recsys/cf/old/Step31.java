package com.ganqiang.recsys.cf.old;

import java.io.IOException;  

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;  
import org.apache.hadoop.util.GenericOptionsParser;  
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;  
import org.apache.mahout.math.VectorWritable;  
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable; 

//直接转换成VectorOrPrefWritable对象
//输出格式：101,    {106:2.0,104:4.0,103:4.0,105:2.0,101:5.0,107:1.0,102:3.0}
//                      102,    {106:1.0,104:2.0,103:3.0,105:1.0,101:3.0,102:3.0}
public class Step31 {
    
    public static final String INPUT_PATH = "hdfs://localhost:9000/output/second";
    public static final String OUTPUT_PATH = "hdfs://localhost:9000/output/thrid1";
    
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {  
        Configuration conf1 = new Configuration();  

        Job job1 = new Job(conf1, "wiki  job three1");  
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);  
        job1.setInputFormatClass(SequenceFileInputFormat.class);  
        job1.setNumReduceTasks(1);  
        job1.setJarByClass(Step31.class);  
        job1.setMapperClass(WikiMapper31.class);  
        job1.setMapOutputKeyClass(IntWritable.class);  
        job1.setMapOutputValueClass(VectorOrPrefWritable.class);  
          
        // set a reducer only to use SequenceFileOutputFormat  
        job1.setReducerClass(WiKiReducer31.class);  
        job1.setOutputKeyClass(IntWritable.class);  
        job1.setOutputValueClass(VectorOrPrefWritable.class);  
          
        // this MR's input is the MR2's output  
        SequenceFileInputFormat.addInputPath(job1, new Path(INPUT_PATH));  
        SequenceFileOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH));     
        if(!job1.waitForCompletion(true)){  
            System.exit(1); // run error then exit  
        }  
    }  
    
    
    
    public static class WikiMapper31 extends Mapper<IntWritable ,VectorWritable,IntWritable,VectorOrPrefWritable>{  
        
        public void map(IntWritable key,VectorWritable value,Context context) throws IOException, InterruptedException{  
            System.out.println(key.toString()+",    "+value.get());  
                    context.write(key, new VectorOrPrefWritable(value.get()));  
                    
                }  
        }  
    
    public static class WiKiReducer31 extends Reducer<IntWritable ,VectorOrPrefWritable,IntWritable,VectorOrPrefWritable> {  
        public void reduce(IntWritable key,Iterable<VectorOrPrefWritable> values ,Context context ) throws IOException, InterruptedException{  
              
            for(VectorOrPrefWritable va:values){  
                context.write(key, va);  
                System.err.println("key"+key.toString()+",vlaue"+va);  
            }  
        }  
      
    }  

}
