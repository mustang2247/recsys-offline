package com.ganqiang.recsys.cf.old;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.VectorWritable;
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.mahout.math.Vector;  


//构建共现矩阵：先抛开用户id，直接计算Item出现的次数
//输出是一个对称矩阵 key是itemid，value是出现的次数
//101,{107:1.0,106:2.0,105:2.0,104:4.0,103:4.0,102:3.0,101:5.0}  
//102,{106:1.0,105:1.0,104:2.0,103:3.0,102:3.0,101:3.0}
public class Step2 {
    
    public static final String INPUT_PATH = "hdfs://localhost:9000/output/first";
    public static final String OUTPUT_PATH = "hdfs://localhost:9000/output/second";
    
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {  

        Configuration conf1 = new Configuration();  

        Job job1 = new Job(conf1, "wiki  job two");  
        job1.setNumReduceTasks(1);  
        job1.setJarByClass(Step2.class);  
        job1.setInputFormatClass(SequenceFileInputFormat.class);  
        job1.setMapperClass(WikiMapper2.class);  
        job1.setMapOutputKeyClass(IntWritable.class);  
        job1.setMapOutputValueClass(IntWritable.class);  
        job1.setReducerClass(WiKiReducer2.class);  
        job1.setOutputKeyClass(IntWritable.class);  
        job1.setOutputValueClass(VectorWritable.class);  
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);  
        SequenceFileInputFormat.addInputPath(job1, new Path(INPUT_PATH));  
        SequenceFileOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH));     
        if(!job1.waitForCompletion(true)){  
            System.exit(1); // run error then exit  
        }  
    }
    
    
    public static class WikiMapper2 extends Mapper<VarLongWritable ,VectorWritable,IntWritable,IntWritable>{  
        
        public void map(VarLongWritable userID,VectorWritable userVector,Context context) throws IOException, InterruptedException{  
            Iterator<Vector.Element> it=userVector.get().nonZeroes().iterator();  
            while(it.hasNext()){ 
                int index1=it.next().index();
               System.err.println("index1:"+index1);  
                Iterator<Vector.Element> it2=userVector.get().nonZeroes().iterator();  
                while(it2.hasNext()){  
                    int index2=it2.next().index();  

                    context.write(new IntWritable(index1), new IntWritable(index2));  
                }  
            }  
        }  
    }  
    
    
    public static class WiKiReducer2 extends Reducer<IntWritable,IntWritable,IntWritable,VectorWritable> {  
        
        public void reduce(IntWritable itemIndex1,Iterable<IntWritable> itemPrefs,Context context) throws IOException, InterruptedException{  
            // RandomAccessSparseVector(int cardinality, int initialCapacity)   
            Vector itemVector=new RandomAccessSparseVector(Integer.MAX_VALUE,10);  
            for(IntWritable itemPref:itemPrefs){  
                int itemIndex2=itemPref.get();  
                itemVector.set(itemIndex2, itemVector.get(itemIndex2)+1.0);  
            }  
            context.write(itemIndex1, new VectorWritable(itemVector));  
          System.out.println(itemIndex1+"  ,"+itemVector);  
        }  
    }  

}
