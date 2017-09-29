import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.lang.*;

public class Stripes {

	public static class MyMapWritable extends MapWritable {
		public MyMapWritable() {}
    		
		
		public String toString() {
			StringBuilder result = new StringBuilder();
			Set<Writable> keySet = this.keySet();
			for (Object key : keySet) {
				result.append("{" + key.toString() + " = " + this.get(key) + "}");
			}
			return result.toString();
		}
	}
							

	public static class StripesMapper extends Mapper<Object, Text,Text, MyMapWritable>{

		private MyMapWritable OMap = new MyMapWritable();
		private Text word = new Text();
  		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
 			String[] tokens = value.toString().split("\\s+");
 			int n = tokens.length;
 			for(int i=0;i<n-1;i++)
			{	
				word.set(tokens[i]);
				OMap.clear();
				for (int j=i+1;j<n;j++)
				{
					Text neighbor = new Text(tokens[j]);
					if(OMap.containsKey(neighbor)){
						IntWritable count = (IntWritable)OMap.get(neighbor);
						count.set(count.get()+1);
					}else{
						OMap.put(neighbor,new IntWritable(1));
					}
				}
				context.write(word,OMap);
				
			}
				
		}
	}   
  
  
	public static class StripesReducer extends Reducer<Text,MyMapWritable,Text,MyMapWritable> {
		private MyMapWritable resultmap = new MyMapWritable();
    		
		public void reduce(Text Key, Iterable<MyMapWritable> values, Context context) throws IOException,InterruptedException {
    			resultmap.clear();
			for (MyMapWritable val : values) {
				MyMapWritable mapWritable = val;
				Set<Writable> keys = mapWritable.keySet();
				for (Writable key : keys) {
					IntWritable count1 = (IntWritable) mapWritable.get(key);
					if (resultmap.containsKey(key)) {
						IntWritable count2 = (IntWritable) resultmap.get(key);
						count2.set(count2.get() + count1.get());
					}else {
						resultmap.put(key, count1);
					}
				}
			}
				 
			context.write(Key,resultmap);  
		}
    
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//conf.setInt("io.sort.mb",45);
		Job job = Job.getInstance(conf, "stripes");
		job.setJarByClass(Stripes.class);
		job.setMapperClass(StripesMapper.class);
		job.setCombinerClass(StripesReducer.class);
		job.setReducerClass(StripesReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MyMapWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
  

  
  
  
  
  
  
