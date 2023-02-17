package dataRreHandle;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Normalization {
	
	static String INPUT_PATH = "hdfs://localhost:9000/output/dataprehandle/D_Filter2/part-r-00000"; 
	static String OUTPUT_PATH = "hdfs://localhost:9000/output/dataprehandle/D_Filter3";
	
	public static final class NormalizationMapper extends Mapper<Object, Text, Text, Text> {
		
		private static double min = 0;
		private static double max = 0;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			try {
				byte[] buf = readFile("hdfs://localhost:9000/output/dataprehandle/RatingMaxMin/part-r-00000");
				String tem = new String(buf);
				String[] mm = tem.split("\t");
				max = Double.valueOf(mm[0]);
				min = Double.valueOf(mm[1]);
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
			
			String line = value.toString();
	        String[] values = line.split("\\|");
	        
	        if(values[6].equals("?")) {
	        	context.write(value, new Text(""));
	        } else {
		        double rating = Double.valueOf(values[6]);
		        rating = (rating - min) / (max - min);
		        line = line.replace(values[6], String.valueOf(rating));
	        	context.write(new Text(line), new Text(""));
	        }
		}
	}
	
	
	public static byte[] readFile(String path) throws Exception {
		
		Path filePath = new Path(path);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		if(fs.exists(filePath)) {
			FSDataInputStream is = fs.open(filePath);
			FileStatus status = fs.getFileStatus(filePath);
			byte[] buf = new byte[Integer.parseInt(String.valueOf(status.getLen()))];
			is.readFully(0, buf);
			is.close();
			return buf;
		} else {
			throw new Exception("file not exists");
		}
		
	}
	
	public static void main(String[] arg) throws Exception{
		
		Path outputpath=new Path(OUTPUT_PATH);
		Path inputpath=new Path(INPUT_PATH);
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf, "normalization");	
		FileInputFormat.setInputPaths(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputpath);
		job.setJarByClass(Normalization.class);
		job.setMapperClass(NormalizationMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
			
	}
	
}
