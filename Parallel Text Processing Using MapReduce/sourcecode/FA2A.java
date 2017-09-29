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
import java.util.List;
import java.util.ArrayList;

public class FA2A{
	static HashMap<String,String[]> map = new HashMap<String, String[]>();
	
	public static String normalize(String a)
	{
		a=a.toLowerCase();
		a=a.replaceAll("v", "u");
		a=a.replaceAll("j", "i");
		Pattern pt = Pattern.compile("[^a-zA-Z0-9\\s]");
		Matcher match= pt.matcher(a);
			while(match.find()){
 			String s= match.group();
			a=a.replaceAll("\\"+s, "");
 		}
 		return a;
	}
	
	public static List<String> getcombinations(String[] words)
	{
		
		List<String> combinations = new ArrayList<String>();
		for(int i =0;i<words.length-1;i++)
		{
			for(int j=i+1;j<words.length;j++)
			{
				if(words[i].equals("")==false)
				{
					String temp = words[i]+","+words[j];
					combinations.add(temp);
				}
			}
		}
	
	
		return combinations;
		
	}
	
	
	public static List<String> getcombinations2(String a, String b)
	{
		
		List<String> combinations =new ArrayList<String>();
		String[] A = new String[1]; String[] B = new String[1];
		if(map.containsKey(a)==true){A = map.get(a);}
		else{A[0]=a;}
		if(map.containsKey(b)==true){B = map.get(b);}
		else{B[0]= b;}
		
		for(int i=0;i<A.length;i++)
		{
			for(int j=0;j<B.length;j++)
			{
				String temp = A[i]+","+B[j];
				combinations.add(temp);			
			}
		}	
		
		return combinations;
	}
	
	
	
	public static class FA2AMapper extends Mapper<Object, Text,Text,Text>{

		
		private Text word = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
			String str[] = value.toString().split(">");
			
			if(str.length==2)
			{
				String found="";
				Pattern pt = Pattern.compile("\\<(.*?)\\>");
				Matcher match= pt.matcher(value.toString());
				while(match.find()){
 					found= match.group();
			
 				}
 				
 				String data=found;
 				String sentence=str[1];			
			
				Text Data = new Text();
				Data.set(data);
			
				sentence=normalize(sentence);
				String[] tokens = sentence.split("\\s+");
				List<String> combinations = getcombinations(tokens);
				for(int i = 0; i<combinations.size();i++)
				{	
					Text pair = new Text();
					pair.set(combinations.get(i));
					context.write(pair,Data);
					String[] tuple = combinations.get(i).split(",");	
					List<String> combinations2 = getcombinations2(tuple[0],tuple[1]);
					for(int j =0; j<combinations2.size();j++)
					{
						pair = new Text();
						pair.set(combinations2.get(j));
						context.write(pair,Data);			
				
					}	
			
				}
		}
	}
	}		
	
	
	public static class FA2AReducer extends Reducer<Text,Text,Text,Text> {
				

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
		Job job = Job.getInstance(conf, "fa2a");
		job.setJarByClass(FA2A.class);
		job.setMapperClass(FA2AMapper.class);
		job.setCombinerClass(FA2AReducer.class);
		job.setReducerClass(FA2AReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
	
	
	
