import java.io.IOException;
import java.util.StringTokenizer;
import java.io.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import java.util.Set;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StripesNew{
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, MyMapWritable >{

		


	private MyMapWritable map = new MyMapWritable();
	

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
     
	String a = value.toString();
	InputStream in = new ByteArrayInputStream(a.getBytes("UTF-8"));
	BufferedReader br = new BufferedReader(new InputStreamReader(in));
	String tweet;
	while((tweet = br.readLine())!= null){
		String[] str = tweet.split("\\s+");
		for(int i = 0; i<str.length; i++)
			{
				Text term = new Text();
				term.set(str[i]);
				map.clear();
				for(int j = 0; j<str.length; j++)
				{
					String new_term = str[j];	
					if(i != j)
					{
						if(map.containsKey(new_term)){
                   					IntWritable count = (IntWritable)map.get(new_term);
                 					count.set(count.get()+1);
               						 }else{
								Text t = new Text();
								t.set(new_term);
                  						 map.put(t,new IntWritable(1));
               						 }
						

					}

				}
			
				context.write(term, map);

			}
		

		
	}
      }
    }

	public static class IntSumReducer
       extends Reducer<Text,MyMapWritable,Text,MyMapWritable> {
    private MyMapWritable result = new MyMapWritable();

    public void reduce(Text key, Iterable<MyMapWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      result.clear();
      for (MyMapWritable val : values) {
        Set<Writable> keys = val.keySet();
		for(Writable loc_key : keys)
		{
			IntWritable count = (IntWritable)val.get(loc_key);
			if(result.containsKey(loc_key)){
				IntWritable count_new = (IntWritable)result.get(loc_key);
				count_new.set(count.get() + count_new.get());
			}else{
				result.put(loc_key, count);

			}		


		}
      }
      
      context.write(key, result);
    }
  }


	public static class MyMapWritable extends MapWritable {
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        Set<Writable> keySet = this.keySet();

        for (Object key : keySet) {
            result.append("{" + key.toString() + " = " + this.get(key) + "}");
        }
        return result.toString();
    }
}

	public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word co-occurence");
    job.setJarByClass(StripesNew.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MyMapWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}