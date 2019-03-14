import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

public class JSONWordCount{
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context)
            throws IOException,InterruptedException {
        String segment[] = value.toString().split(",");
        context.write(new Text(segment[8]), new IntWritable(1));
            
        }
    }

    public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException,InterruptedException {
            int sum = 0;
            for(IntWritable x: values) {
                sum+=x.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }


    public static void main(String[] args) throws Exception{
        long st = System.currentTimeMillis();
        Configuration conf= new Configuration();
        Job job = Job.getInstance(conf, "JSONWordCount");

        job.setJarByClass(JSONWordCount.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(JSONInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        conf.set("START_TAG_KEY", "{");
        conf.set("END_TAG_KEY", "}");

        Path outputPath = new Path("/home/mqp/Desktop/out");
        outputPath.getFileSystem(conf).delete(outputPath, true);

        FileInputFormat.addInputPath(job, new Path("/home/mqp/Desktop/airfield.json"));
        FileOutputFormat.setOutputPath(job, new Path("/home/mqp/Desktop/out"));

        boolean exitCode = job.waitForCompletion(true);
        long et = System.currentTimeMillis();
        long tt = et - st;
        System.out.println("Total execution time for Problem 2 is " + tt + " ms.");
        System.exit(exitCode? 0: 1);

    }
}
