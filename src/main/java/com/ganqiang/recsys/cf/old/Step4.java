package com.ganqiang.recsys.cf.old;

import java.io.IOException;  

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;  
import org.apache.hadoop.util.GenericOptionsParser;  
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable;  
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;  
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable; 

import java.util.ArrayList;
import java.util.List;  

import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable;  
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;  
import org.apache.mahout.math.Vector;  


//将step31和step32的输出结果进行合并
//输出结果： 101, {106:2.0,104:4.0,103:4.0,105:2.0,101:5.0,107:1.0,102:3.0},[4, 5, 2, 3, 1],[5.0, 4.0, 2.0, 2.5, 5.0]
//                       102, {106:1.0,104:2.0,103:3.0,105:1.0,101:3.0,102:3.0},[5, 2, 1],[3.0, 2.5, 3.0]
public class Step4 {   
    
    public static final String INPUT1_PATH = "hdfs://localhost:9000/output/thrid1";
    public static final String INPUT2_PATH = "hdfs://localhost:9000/output/thrid2";
    public static final String OUTPUT_PATH = "hdfs://localhost:9000/output/four";
    
    
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {  
        // TODO Auto-generated method stub  
        Configuration conf1 = new Configuration();  
        String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();        
        Job job1 = new Job(conf1, "wiki  job four");  
        job1.setNumReduceTasks(1);  
        job1.setJarByClass(Step4.class);  
        job1.setInputFormatClass(SequenceFileInputFormat.class);  
        job1.setMapperClass(WikiMapper4.class);  
        job1.setMapOutputKeyClass(IntWritable.class);  
        job1.setMapOutputValueClass(VectorOrPrefWritable.class);      
        job1.setReducerClass(WiKiReducer4.class);  
        job1.setOutputKeyClass(IntWritable.class);  
       job1.setOutputValueClass(VectorAndPrefsWritable.class);  
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);  
        SequenceFileInputFormat.addInputPath(job1, new Path(INPUT1_PATH));  
        SequenceFileInputFormat.addInputPath(job1, new Path(INPUT2_PATH));  
        SequenceFileOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH));     
        if(!job1.waitForCompletion(true)){  
            System.exit(1); // run error then exit  
        }  
    }  
    
    //map不做任何事情
    public static class WikiMapper4 extends Mapper<IntWritable ,VectorOrPrefWritable,IntWritable,VectorOrPrefWritable> {  
        
        public void map(IntWritable key,VectorOrPrefWritable value,Context context) throws IOException, InterruptedException{  
            context.write(key, value);  
        }  
    }  
    
    //reduce输出就是把MR(31)和MR(32)的相同的itemID整合一下
    public static class WiKiReducer4 extends Reducer<IntWritable,VectorOrPrefWritable,IntWritable,VectorAndPrefsWritable> {  
        public void reduce(IntWritable key, Iterable<VectorOrPrefWritable> values,Context context) throws IOException, InterruptedException{  
            List<Long> userfs=new ArrayList<Long>();  
            List<Float> prefs=new ArrayList<Float>();  
            Vector v=null;  
            for(VectorOrPrefWritable value:values){  
                if(value.getVector()!=null){  
                    v=value.getVector();  
                }else{  
                    userfs.add(value.getUserID());  
                    prefs.add(value.getValue());  
                 }  
            }  
            context.write(key, new VectorAndPrefsWritable(v,userfs,prefs));  
          System.out.println("key ,itemid:  "+key.toString()+",    information:"+v+","+userfs+","+prefs);  
        }   
    }  
    
}
