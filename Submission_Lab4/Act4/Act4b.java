import java.io.IOException;
import java.util.StringTokenizer;
import java.io.*;
import java.util.*;


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

public class Act4b{

private static HashMap<String,ArrayList<String>> map1 = new HashMap<String,ArrayList<String>>();
	
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, MyMapWritable >{

		


	private MyMapWritable map = new MyMapWritable();
	

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
     
	String a = value.toString();
	InputStream in = new ByteArrayInputStream(a.getBytes("UTF-8"));
	BufferedReader br = new BufferedReader(new InputStreamReader(in));
	String latin;
		while((latin = br.readLine())!= null){
		
			//System.out.println("BEFORE SPLITTING: " + latin); 
			String[] str = latin.toString().split(">");
			Text v = new Text();
			v.set(str[0] + ">");
			str[1] = str[1].replaceAll("[^a-zA-Z0-9 ]","");
			str[1] = str[1].toLowerCase();
			//System.out.println("AFTER SPLITTING: " + str[1]);
			String[] sentc = str[1].split("\\s+");
			
			
		for(int i = 0; i<sentc.length; i++)
			{
				Text term = new Text(sentc[i]);
				//term.set(str[i]);
				map.clear();
				for(int j = 0; j<sentc.length; j++)
				{
					String new_term = sentc[j];
					for(int k =0; k<sentc.length;k++)
					{
					String final_term = sentc[k];	
					if((i != j) && (j!=k))
					{
					
					
					
							if(map.containsKey(new_term)){
                   					Text count = (Text)map.get(new_term);
                 					count.set(count + " :: " + v);
               						 }else{
								Text t = new Text(new_term + " , " + final_term);
								//t.set(new_term);
                  						 map.put(t,v);
								}

					}
					}

				}
				//System.out.println("The Map is" + map);
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
      String t = key.toString();
      t = t.replace("j","i");
        t = t.replace("v","u");
        Text p = new Text();
        p.set(t);
      for (MyMapWritable val : values) {
        Set<Writable> keys = val.keySet();
		for(Writable loc_key : keys)
		{
			Text count = (Text)val.get(loc_key);
			if(result.containsKey(loc_key)){
				Text count_new = (Text)result.get(loc_key);
				count_new.set(count + " :: " + count_new);
			}else{
				//Text z = new Text(loc_key.toString());
				result.put(loc_key, count);

			}		


		}
      }
      
      
      
      
      

      
      context.write(p, result);
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


	public static void MapIni() throws IOException, FileNotFoundException{
	   BufferedReader br2 = new BufferedReader(new FileReader("/home/hadoop/Downloads/new_lemmatizer.csv"));
        String lem;
        
        while((lem = br2.readLine())!= null)
        	{
        		String[] lemma = lem.split("\\,");
        		ArrayList<String> ar = new ArrayList<String>();
        		for(int i = 1; i<lemma.length; i++)
        			{
        				ar.add(lemma[i]);
        			}
        		map1.put(lemma[0],ar);
        	}
	}


	

	public static void main(String[] args) throws Exception {
	Act4b ob = new Act4b();
	ob.MapIni();
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word co-occurence");
    job.setJarByClass(Act4b.class);
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