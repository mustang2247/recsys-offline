package com.ganqiang.recsys.cf.old;

import java.io.IOException;
import java.io.IOException;  
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;  
import org.apache.hadoop.util.GenericOptionsParser;  
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;  
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;  
import org.apache.mahout.math.VarLongWritable;  
import org.apache.mahout.math.Vector;  
import org.apache.mahout.math.VectorWritable;  
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;

//将Step1的输出结果进行转换
//输出格式：101, 3:2.5
//                      101, 1:5.0
//                      102, 5:3.0
public class Step32 {

    public static final String INPUT_PATH = "hdfs://localhost:9000/output/first";//第一个MR的输出
    public static final String OUTPUT_PATH = "hdfs://localhost:9000/output/thrid2";
    
    
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {  
        // TODO Auto-generated method stub  
        Configuration conf1 = new Configuration();  

        Job job1 = new Job(conf1, "wiki  job one");  
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);  
        job1.setInputFormatClass(SequenceFileInputFormat.class);  
        job1.setNumReduceTasks(1);  
        job1.setJarByClass(Step32.class);  
        job1.setMapperClass(WikiMapper32.class);  
        job1.setMapOutputKeyClass(IntWritable.class);  
        job1.setMapOutputValueClass(VectorOrPrefWritable.class);  
  
        job1.setReducerClass(WiKiReducer32.class);  
        job1.setOutputKeyClass(IntWritable.class);  
        job1.setOutputValueClass(VectorOrPrefWritable.class);  
          
        // the WiKiDriver's out put is this one's input  
        SequenceFileInputFormat.addInputPath(job1, new Path(INPUT_PATH));  
        SequenceFileOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH));     
        if(!job1.waitForCompletion(true)){  
            System.exit(1); // run error then exit  
        }  
    }  
    
    
    
    public static class WikiMapper32 extends Mapper<VarLongWritable ,VectorWritable,IntWritable,VectorOrPrefWritable>{  
        
        public void map(VarLongWritable key,VectorWritable value,Context context) throws IOException, InterruptedException{  

                long userID=key.get();  
                Vector userVector=value.get();  
                Iterator<Vector.Element> it=userVector.nonZeroes().iterator();  
                IntWritable itemi=new IntWritable();  
                while(it.hasNext()){  
                    Vector.Element e=it.next();  
                    int itemIndex=e.index();  
                    float preferenceValue=(float)e.get();  
                    itemi.set(itemIndex);  
                    context.write(itemi, new VectorOrPrefWritable(userID,preferenceValue));  
                   System.out.println("item :"+itemi+",userand val:"+userID+","+preferenceValue);  
                } 
              
        }  
    }  
    
    
    
    public static  class WiKiReducer32 extends Reducer<IntWritable ,VectorOrPrefWritable,IntWritable,VectorOrPrefWritable> {  
        public void reduce(IntWritable key,Iterable<VectorOrPrefWritable> values ,Context context ) throws IOException, InterruptedException{  
              
            for(VectorOrPrefWritable va:values){  
                context.write(key, va);  
                System.err.println("key"+key.toString()+",vlaue"+va);  
            }  
        }  
      
    }  
}
