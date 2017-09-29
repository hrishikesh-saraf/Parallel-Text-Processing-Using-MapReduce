import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Pairs {
								

  public static class PairsMapper extends Mapper<Object, Text,Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text pair = new Text();
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] tokens = value.toString().split("\\s+");
      int n = tokens.length;
      for(int i=0;i<n-1;i++)
	{
	 for (int j=i+1;j<n;j++)
	 {
	  String word1=tokens[i];
	  String word2=tokens[j];
	  if(word1.compareTo(word2)>0)
	  {
	  pair.set(word1+","+word2);
	  context.write(pair,one);
	  }
	  else
	  {
	  pair.set(word2+","+word1);
	  context.write(pair,one);
	 }
        }
       }
     }    
  }

  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    //conf.setInt("io.sort.mb",45);
    Job job = Job.getInstance(conf, "pairs");
    job.setJarByClass(Pairs.class);
    job.setMapperClass(PairsMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
