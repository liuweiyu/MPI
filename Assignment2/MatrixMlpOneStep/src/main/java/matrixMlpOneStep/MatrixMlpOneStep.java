package matrixMlpOneStep;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MatrixMlpOneStep { 
	public final static String TOKEN = ",";
	
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int m = Integer.parseInt(conf.get("m"));
            int n = Integer.parseInt(conf.get("n"));
            String record = value.toString();
            String[] elements = record.split(TOKEN);
            Text outputKey = new Text();
            Text outputValue = new Text();
            if (elements[0].equals("M")) {
                for (int k = 0; k < n; k++) {
                    outputKey.set(elements[1] + TOKEN + k);
                    outputValue.set("M" + TOKEN + elements[2] + TOKEN + elements[3]);
                    context.write(outputKey, outputValue);
                }
            } else {
                for (int i = 0; i < m; i++) {
                    outputKey.set(i + TOKEN + elements[2]);
                    outputValue.set("N" + TOKEN + elements[1] + TOKEN + elements[3]);
                    context.write(outputKey, outputValue);
                }
            }
        }
    }
 
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] elements;
            HashMap<Integer, Float> MIndexValueMap = new HashMap<Integer, Float>();
            HashMap<Integer, Float> NIndexValueMap = new HashMap<Integer, Float>();
            for (Text element : values) {
            	elements = element.toString().split(TOKEN);
                if (elements[0].equals("M")) {
                	MIndexValueMap.put(Integer.parseInt(elements[1]), Float.parseFloat(elements[2]));
                } else {
                	NIndexValueMap.put(Integer.parseInt(elements[1]), Float.parseFloat(elements[2]));
                }
            }
            int p = Integer.parseInt(context.getConfiguration().get("p"));
            float tmpResult = 0;
            float m_ij;
            float n_jk;
            for (int j = 0; j < p; j++) {
                m_ij = MIndexValueMap.containsKey(j) ? MIndexValueMap.get(j) : 0.0f;
                n_jk = NIndexValueMap.containsKey(j) ? NIndexValueMap.get(j) : 0.0f;
                tmpResult += m_ij * n_jk;
            }
            if (tmpResult != 0.0f) {
                context.write(null, new Text(key.toString() + TOKEN + Float.toString(tmpResult)));
            }
        }
    }    
    
    //args: matrix 1's col num; matrix 1's row num; matrix 2's row num; input file; outpufile
    public static void main(String[] args) throws Exception {        	
        Configuration conf = new Configuration();
        // A is an m-by-p matrix; B is an p-by-n matrix.
        conf.set("m", args[0]);
        conf.set("p", args[1]);
        conf.set("n", args[2]);
 
        Job job = new Job(conf, "MatrixMlpOneStep");
        job.setJarByClass(MatrixMlpOneStep.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
 
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.addInputPath(job, new Path(args[3]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));
        
        System.exit(job.waitForCompletion(true)? 0 : 1);
    }    
}
