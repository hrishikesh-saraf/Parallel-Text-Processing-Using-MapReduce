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
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.regex.*;

public class FA1{
	static HashMap<String,String[]> map = new HashMap<String, String[]>();
	
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
	
	public static class FA1Mapper extends Mapper<Object, Text,Text,Text>{

		
		private Text word = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
			String str[] = value.toString().split("\\t");
			if(str.length<2)
			{System.out.println("Cannot process at:"+value.toString());}
			else{
			String data = str[0];
			String sentence = str[1];
			String[] tokens = sentence.split("\\s+");
			for(int i=0;i<tokens.length;i++)
			{
				String token = tokens[i];
				token=token.toLowerCase();
			
				Pattern pt = Pattern.compile("[^a-zA-Z0-9]");
				Matcher match= pt.matcher(token);
	    			while(match.find()){
		 			String s= match.group();
					token=token.replaceAll("\\"+s, "");
		 		}
		 		
		 		token =token.replaceAll("v", "u");
				token =token.replaceAll("j", "i");
			
				Text Word = new Text();
				Word.set(token);
				Text Data = new Text();
				Data.set(data);
				context.write(Word,Data);
				if(map.containsKey(token)==true)
		    		{
		    			String[] lemmas = map.get(token);
		    			for(int j=0;j<lemmas.length;j++)
		    			{
		    				Text lemma = new Text();
		    				lemma.set(lemmas[j]);
		    				context.write(lemma,Data);
		    			}
		    		}		 				 		  
		
			}}
		}
	}	
		
		
	public static class FA1Reducer extends Reducer<Text,Text,Text,Text> {
				

		public void reduce(Text Key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
			
			String result="";
			for (Text val : values) {
			
				result=result+val.toString()+"  ";
				
			}
			Text Result=new Text();
			Result.set(result);
			context.write(Key,Result);
		}
	}		
		
	
	
	
	
	
	
	
	
	
	public static void main(String[] args) throws Exception {
		
		
		BufferedReader br = new BufferedReader(new FileReader("new_lemmatizer.csv"));
	    	String line =  null;
	    	HashMap<String,String[]> map = new HashMap<String, String[]>();
	    
	    	while((line=br.readLine())!=null){
	        	String str[] = line.split(",");
	        	String root =str[0];
	        	String[] arr=new String[str.length-1];
	        	for(int i=1;i<str.length;i++){
	        	    arr[i-1] = str[i];
	            
	        	}
	        	map.put(root, arr);
	    	}
	    	
		
		
		
		
		Configuration conf = new Configuration();
		//conf.setInt("io.sort.mb",45);
		Job job = Job.getInstance(conf, "fa1");
		job.setJarByClass(FA1.class);
		job.setMapperClass(FA1Mapper.class);
		job.setCombinerClass(FA1Reducer.class);
		job.setReducerClass(FA1Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
