package com.ganqiang.recsys.cf.old;

import java.io.IOException;  

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.util.GenericOptionsParser;  
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable;  
import org.apache.mahout.math.VarLongWritable;  
import org.apache.mahout.math.VectorWritable;  

import java.util.List;  

import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable;  
import org.apache.mahout.math.VarLongWritable;  
import org.apache.mahout.math.Vector;  
import org.apache.mahout.math.VectorWritable;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.mahout.math.VarLongWritable;  
import org.apache.mahout.math.Vector;  
import org.apache.mahout.math.VectorWritable;  

import java.io.IOException;  
import java.net.URI;  
import java.util.ArrayList;  
import java.util.Collections;  
import java.util.Iterator;  
import java.util.List;  
import java.util.PriorityQueue;  
import java.util.Queue;  
  

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.SequenceFile;  
import org.apache.hadoop.io.Writable;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.util.ReflectionUtils;  
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable;  
import org.apache.mahout.cf.taste.impl.common.FastMap;  
import org.apache.mahout.cf.taste.impl.recommender.ByValueRecommendedItemComparator;  
import org.apache.mahout.cf.taste.impl.recommender.GenericRecommendedItem;  
import org.apache.mahout.cf.taste.recommender.RecommendedItem;  
import org.apache.mahout.math.VarLongWritable;  
import org.apache.mahout.math.Vector;  
import org.apache.mahout.math.VectorWritable; 


//map：做乘法，单个用户对某个item的评分乘以某个item的 
//map输出：         3   {107:2.5,106:5.0,105:5.0,104:10.0,103:10.0,102:7.5,101:12.5}  
//combine：做加法，将map输出的结果userid相同的相加，即矩阵的中点积的相加过程
//combine输出： 1   {107:5.0,106:18.0,105:15.5,104:33.5,103:39.0,102:31.5,101:44.0}  
//reduce：过滤跟排序。过滤掉用户已评价的项目，并按预测评分高的排序
//reduce输出：     1   [104:33.5,106:18.0,105:15.5,107:5.0]
public class Step5 {

    public static final String INPUT_PATH = "hdfs://localhost:9000/output/four";
    public static final String OUTPUT_PATH = "hdfs://localhost:9000/output/five";
    public static final String JOB1OUTPATH = "hdfs://localhost:9000/output/first/part-r-00000";

   public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {  
        // TODO Auto-generated method stub  
        Configuration conf1 = new Configuration();  

        Job job1 = new Job(conf1, "wiki  job five");  
        job1.setNumReduceTasks(1);  
        job1.setJarByClass(Step5.class);  
        job1.setInputFormatClass(SequenceFileInputFormat.class);  
        job1.setMapperClass(WikiMapper5.class);  
        job1.setMapOutputKeyClass(VarLongWritable.class);  
        job1.setMapOutputValueClass(VectorWritable.class);  
          
        job1.setCombinerClass(WiKiCombiner5.class);  
        job1.setReducerClass(WiKiReducer5.class);  
        job1.setOutputKeyClass(VarLongWritable.class);  
        job1.setOutputValueClass(RecommendedItemsWritable.class);  
    //   job1.setOutputFormatClass(SequenceFileOutputFormat.class);  
        SequenceFileInputFormat.addInputPath(job1, new Path(INPUT_PATH));  
  
        FileOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH));     
        if(!job1.waitForCompletion(true)){  
            System.exit(1); // run error then exit  
        }  
    }  
    
    
    public static class WikiMapper5 extends Mapper<IntWritable ,VectorAndPrefsWritable,VarLongWritable,VectorWritable>{  
        
        public void map(IntWritable key,VectorAndPrefsWritable vectorAndPref,Context context) throws IOException, InterruptedException{  
            Vector coo=vectorAndPref.getVector();  
            List<Long> userIds=vectorAndPref.getUserIDs();  
            List<Float> prefValues=vectorAndPref.getValues();  
            //System.out.println("alluserids:"+userIds);  
            for(int i=0;i<userIds.size();i++){  
                long userID=userIds.get(i);  
                float prefValue=prefValues.get(i);  
                Vector par=coo.times(prefValue);  
                context.write(new VarLongWritable(userID), new VectorWritable(par));  
                System.out.println(",userid:"+userID+",vector:"+par);  //  if the user id = 3 is the same as my paper then is right  
            }  
        //  System.out.println();     
        }  
    }  
    
    public static class WiKiCombiner5 extends Reducer<VarLongWritable,VectorWritable,VarLongWritable,VectorWritable> {  
        public void reduce(VarLongWritable key, Iterable<VectorWritable> values,Context context) throws IOException, InterruptedException{  
            Vector partial=null;  
            for(VectorWritable v:values){  
                partial=partial==null?v.get():partial.plus(v.get());  
            }  
            context.write(key, new VectorWritable(partial));  
            System.err.println("userid:"+key.toString()+",vecotr:"+partial);//   here also should be the same as my paper's result  
        }  
}  
    
    
    public static class WiKiReducer5 extends Reducer<VarLongWritable,VectorWritable,VarLongWritable,RecommendedItemsWritable> {  
        
        private int recommendationsPerUser=10;  
        private String path=JOB1OUTPATH;  
          
        private static FastMap<Integer,String> map=new FastMap<Integer,String>();  
        public void setup(Context context) throws IOException{  
            Configuration conf=new Configuration();  
            FileSystem fs=FileSystem.get(URI.create(path), conf);  
            Path tempPath=new Path(path);  
            SequenceFile.Reader reader=null;  
            try {  
                reader=new SequenceFile.Reader(fs, tempPath, conf);  
                Writable key=(Writable)ReflectionUtils.newInstance(reader.getKeyClass(),conf);  
                Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);   
            //  long position = reader.getPosition();    
                while (reader.next(key, value)) {    
                    map.put(Integer.parseInt(key.toString()), value.toString());  
            //      System.out.println(key.toString()+","+value.toString());  
                //    position = reader.getPosition(); // beginning of next record    
                }  
            } catch (Exception e) {  
                // TODO Auto-generated catch block  
                e.printStackTrace();  
            }    
        }  
          
        public void reduce(VarLongWritable key, Iterable<VectorWritable> values,Context context) throws IOException, InterruptedException{  
              
                int userID=(int)key.get();  
                Vector rev=null;  
                for(VectorWritable vec:values){  
                    rev=rev==null? vec.get():rev.plus(vec.get());  
                }  
                Queue<RecommendedItem>topItems=new PriorityQueue<RecommendedItem>( recommendationsPerUser+1,  Collections.reverseOrder(ByValueRecommendedItemComparator.getInstance())   );  
                Iterator<Vector.Element>recommendationVectorIterator=  rev.nonZeroes().iterator();  
                while(recommendationVectorIterator.hasNext()){  
                    Vector.Element e=recommendationVectorIterator.next();  
                    int index=e.index();  
                    System.out.println("Vecotr.element.indxe:"+index);  //  test here  find the index is item id or not  ** test result : index is item  
                    if(!hasItem(userID,String.valueOf(index))){  
                        float value=(float) e.get();  
                        if(topItems.size()<recommendationsPerUser){  
                            //  here only set index  
                            topItems.add(new GenericRecommendedItem(index,value));  
                        }else if(value>topItems.peek().getValue()){  
                            topItems.add(new GenericRecommendedItem(index,value));  
                            topItems.poll();  
                        }  
                    }  
                }  
                List<RecommendedItem>recom=new ArrayList<RecommendedItem>(topItems.size());  
                recom.addAll(topItems);  
                Collections.sort(recom,ByValueRecommendedItemComparator.getInstance());  
                context.write(key, new RecommendedItemsWritable(recom));          
            }  
          
        public static boolean hasItem(int user,String item){  // to check whether the user has rate the item  
            boolean flag=false;  
            String items=map.get(user);  
            if(items.contains(item)){  
                flag=true;  
            }  
            return flag;  
        }  
    }  

}
