package dataRreHandle;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Filter {
	
	static String INPUT_PATH = "hdfs://localhost:9000/input/dataprehandle/data.txt"; 
	static String OUTPUT_PATH = "hdfs://localhost:9000/output/dataprehandle/D_Filter";
	
	public static final class FilterMapper extends Mapper<Object, Text, Text, Text> {
		
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
			
			String line = value.toString();
	        String[] values = line.split("\\|");
			double longitude = Double.valueOf(values[1]);
			double latitude = Double.valueOf(values[2]);
			if(longitude >= 8.1461259 && longitude <= 11.1993265 && latitude >= 56.5824856 && latitude <= 57.750511) {
				context.write(value, new Text(""));
			}
		}
		
	}

	
	public static void main(String[] arg) throws Exception{
		
		Path outputpath=new Path(OUTPUT_PATH);
		Path inputpath=new Path(INPUT_PATH);
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf, "filter");	
		FileInputFormat.setInputPaths(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputpath);
		job.setJarByClass(Filter.class);
		job.setMapperClass(FilterMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
			
	}
	
}