package h_wc_p;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PredictionCF extends Configured implements Tool {
	public static class Predictionmapper 
	     extends Mapper<Object, Text, IntWritable, Text>{
		public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException{
			//String str1 = new String(value.toString());
			//String[] s = str1.split("	| ");
		    //int uid = Integer.parseInt(s[0]);
		   // int mid = Integer.parseInt(s[1]);
	        //float rating = Float.parseFloat(s[2]);
		    StringTokenizer s = new StringTokenizer(value.toString());
		    //int uid = s.nextToken();
		     
		    	
			context.write(new IntWritable(Integer.parseInt(s.nextToken())), new Text(s.nextToken() + ' ' + s.nextToken()));
			
		}
	}
	public static class Predictiongeneratemapper
	     extends Mapper<Object, Text, IntWritable, Text> {
		StringTokenizer s;
		String s1;
		String s2;
		String s3;
		public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException{
			//String s1 = "userrecord.dat";
			//String s2 = "itemsaverating.dat";
			//BufferedReader reader1 = new BufferedReader(new FileReader(s1));
			//BufferedReader reader2 = new BufferedReader(new FileReader(s2));
			 s = new StringTokenizer(value.toString());
			 s1 = s.nextToken();
			 s2 = s.nextToken();
			 s3 = s.nextToken();
			 context.write(new IntWritable(Integer.parseInt(s1)), new Text(s2 + ' ' + s3));
			 context.write(new IntWritable(Integer.parseInt(s2)), new Text(s1 + ' ' + s3));
		}
	}
	public static class Predictionreducer
	     extends Reducer<IntWritable, Text, IntWritable, Text>{
		public void reduce(IntWritable key, Iterable<Text> value, Context context)
		throws IOException, InterruptedException{
			Text t = new Text();
			String s = new String();
			for(Text val : value)
				s = s + ' ' + val;
			t.set(s);
			context.write(key, t);
				
		}
	}
	public static class PredictionGenereducer
	     extends Reducer<IntWritable, Text, Text, FloatWritable>{
		BufferedReader reader1 = null;
		BufferedReader reader2 = null;
		HashMap<Integer, Float> map1 = new HashMap<Integer, Float>();//average ratings
		@Override
		protected void setup(Context context)//setup is a funtion which initiated the reduce task
	    throws IOException, InterruptedException{
		    super.setup(context);
		    System.out.println("end in here!");
		   /* Path[] localcachefiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		    if(localcachefiles != null)
		    {
		    	for(int i = 0;i < localcachefiles.length;i ++)
		    	{
		    		
		    	}
		    }
		    reader1 = new BufferedReader(new FileReader(new File("d1")));
			reader2 = new BufferedReader(new FileReader(new File("d2")));*/
		    String str2;
		    File f = new File("d2");
		    File f1 = new File("d1");
		    if(f.exists() || f1.exists()) {
		    	System.out.println("does exit");
		    }else 
		    	System.out.println("file does not exits");
			reader2 = new BufferedReader(new FileReader(f));
			System.out.println("end after reader");
			while((str2 = reader2.readLine()) != null){
				//System.out.println(str2);
				String[] ss = str2.split(" |	"); 
				map1.put(Integer.parseInt(ss[0]), Float.parseFloat(ss[ss.length - 1]));
				//context.write(new Text(ss[0]), new FloatWritable(Float.parseFloat(ss[ss.length - 1])));////////
			}
			reader2.close(); 
		}
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException{
			String allvalues = new String();
			for(Text val : values)
				allvalues = allvalues + val.toString() + ' ';
			String[] itemrating = allvalues.split(" ");
			/*String s1 = "userrecord.data";
			String s2 = "itemsaverating.data";*/
			BufferedReader reader1 = new BufferedReader(new FileReader("d1"));
		
			String str1;
			/*String str2;
			HashMap<Integer, Float> map1 = new HashMap<Integer, Float>();//average ratings
			while((str2 = reader2.readLine()) != null){
				//System.out.println(str2);
				String[] ss = str2.split(" |	"); 
				map1.put(Integer.parseInt(ss[0]), Float.parseFloat(ss[ss.length - 1]));
				//context.write(new Text(ss[0]), new FloatWritable(Float.parseFloat(ss[ss.length - 1])));////////
			}
			reader2.close();*/
			while((str1 = reader1.readLine()) != null){
				StringTokenizer st = new StringTokenizer(str1.toString());
				String uid = st.nextToken();
				HashMap<Integer, Float> map = new HashMap<Integer, Float>();//the items of user i 
				while(st.hasMoreTokens()){
     				map.put(Integer.parseInt(st.nextToken()), Float.parseFloat(st.nextToken()));
				    //context.write(new Text("123"), new FloatWritable(1.0f));//////
				}
				float ssum = 0.0f;//fenmu de sum
				float multisum = 0.0f;//fenzi de sum
			    if(!map.containsKey(key.get())){//if user i do not have the item key, then predict;
				    //for(Text val : values){
			    	int len = itemrating.length - 1;
			    	for(int i = 0;i < len;i = i + 2){
					    //String[] itemandsimi = val.toString().split(" ");
					    if(map.containsKey(Integer.parseInt(itemrating[i])) && Math.abs(Float.parseFloat(itemrating[i + 1])) > 0.5f){// if user has the item [0], and the similarity is more than 0.5f, then compute the prediction
						   multisum = multisum + Float.parseFloat(itemrating[i + 1]) * (map.get(Integer.parseInt(itemrating[i])) - map1.get(Integer.parseInt(itemrating[i])));
						   ssum = ssum + Math.abs(Float.parseFloat(itemrating[i + 1]));
					    }//right here, itemrating[i] should be itemrating[i+1]**************
				     }
			    	if(ssum != 0.0f){
				       float p = map1.get(key.get()) + multisum / ssum;
				       context.write(new Text(uid + ' ' + key.toString()), new FloatWritable(p));
			    	} 	
			    	//else context.write(new Text("i"), new FloatWritable(0.0f));
			    	}
			    //else context.write(new Text("j"), new FloatWritable(1.1f));
			}
			reader1.close();
		}
	}
	public int run(String[] args) throws IOException {
		/*Configuration conf = new Configuration();
		Job job = new Job(conf, " predictioncf");
		job.setJarByClass(PredictionCF.class);
		job.setMapperClass(Predictionmapper.class);
		job.setReducerClass(Predictionreducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("input02/u.data"));
		FileOutputFormat.setOutputPath(job, new Path("output15"));
		try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); 
		}*/
		Configuration conf1 = new Configuration();
		//conf1.set("mapred.create.symlink", "yes");
		DistributedCache.createSymlink(conf1);
		try {
			//DistributedCache.addCacheFile(new URI("hdfs://jsjcloudmaster:9160/user/yc/MoviePair8/part-r-00000#d1"), conf1);
			//DistributedCache.addCacheFile(new URI("hdfs://jsjcloudmaster:9160/user/yc/AverageRatings8/part-r-00000#d2"), conf1);
			DistributedCache.addCacheFile(new URI("hdfs://jsjcloudmaster:9160/user/yc/output/TestMovieSet/part-r-00000#d1"), conf1);
			DistributedCache.addCacheFile(new URI("hdfs://jsjcloudmaster:9160/user/yc/output/AverageRatings1/part-r-00000#d2"), conf1);
		} catch (URISyntaxException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		//conf1.set("mapred.cache.files", "inputdata/userrecord.dat#d1");
		//conf1.set("mapred.cache.files", "inputdata/itemsaverating.dat#d2");
		Job job1 = new Job(conf1, " predicgenecf");
		job1.setJarByClass(PredictionCF.class);
		job1.setMapperClass(Predictiongeneratemapper.class);
		job1.setReducerClass(PredictionGenereducer.class);
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(FloatWritable.class);
		job1.setNumReduceTasks(6);
		FileInputFormat.addInputPath(job1, new Path("hdfs://jsjcloudmaster:9160/user/yc/output/similarity1/part-r-00000"));
		FileOutputFormat.setOutputPath(job1, new Path("hdfs://jsjcloudmaster:9160/user/yc/output/based-Predition"));
		try {
			job1.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}
	public static void main(String[] args) throws Exception{
		  int exitCode = ToolRunner.run(new PredictionCF(), args);
				  System.exit(exitCode);
	}
} 