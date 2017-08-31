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

public class LatinCo{

 private static HashMap<String,ArrayList<String>> map = new HashMap<String,ArrayList<String>>();
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text >{

		


	
	

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
			//System.out.println("AFTER SPLITTING SENTC: " + str[1] + " :       " + sentc[0]);
			//System.out.println("AFTER SPLITTING SENTC: " + str[1] + " :       " + sentc[1]);
			//System.out.println("AFTER SPLITTING SENTC: " + str[1] + " :       " + sentc[2]);
			for(int i = 0; i<sentc.length; i++)
				{
					
					Text k = new Text(sentc[i]);
					//k.set(sentc[i]);
					//System.out.println("SENDING FROM MAPPER: KEY: " + k + " AND LOC: " + v); 
					context.write(k,v);
				}
			
				
			
		

		
	}
      }
    }

	public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
       
       //private static HashMap<String,ArrayList<String>> map = new HashMap<String,ArrayList<String>>();
       
       
       /*protected void Setup(Context context) throws IOException, InterruptedException{
       
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
        		map.put(lemma[0],ar);
        	}
       
       }*/
    
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
        
        	String t = key.toString();
        	t = t.replace("j","i");
        	t = t.replace("v","u");
        	Text p = new Text();
        	p.set(t);
     		String loca = "";
     		 for (Text loc : values){
        	
        	 loca = loca.concat(loc.toString() + "::");
        	
        	}
        	Text location = new Text();
        	location.set(loca);
     		
     		if(map.containsKey(t))
     		{
     			ArrayList<String> temp = map.get(t);
     			for(int i = 0; i<temp.size(); i++)
     			{
     				String c = temp.get(i);
     				Text lee = new Text();
     				lee.set(c);
     				//System.out.println("IF:   SENDING FROM REDUCER: KEY: " + lee + " AND LOC: " + location);
     				context.write(lee, location);
     			}
     		}
     		else
     		{
     			//System.out.println("ELSE:   SENDING FROM REDUCER: KEY: " + p + " AND LOC: " + location);
     			context.write(p,location);
     		}
       
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
        		map.put(lemma[0],ar);
        	}
	}

	

	public static void main(String[] args) throws Exception {
	LatinCo ob = new LatinCo();
	ob.MapIni();
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word co-occurence");
    job.setJarByClass(LatinCo.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}