import java.io.*;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.*;
import java.util.HashMap;
import java.lang.*;
import org.apache.hadoop.fs.FileUtil;
public class K_means_output2 {

    public static class check_difference {
        public static boolean check (Path oldpath, Path newpath,FileSystem fs) throws IOException {
            BufferedReader old_centroid1 = new BufferedReader(new InputStreamReader(fs.open(oldpath)));
            BufferedReader new_centroid1 = new BufferedReader(new InputStreamReader(fs.open(newpath)));
            String lines;
            HashMap<String,String> old_1 = new HashMap<>();
            HashMap<String,String> new_1 = new HashMap<>();
            while ((lines = old_centroid1.readLine()) != null) {
                String[] list_1 = lines.split(",");
                old_1.put(list_1[0], list_1[1] + "," + list_1[2]);
            }
            while ((lines = new_centroid1.readLine()) != null) {
                String[] list_1 = lines.split(",");
                new_1.put(list_1[0], list_1[1] + "," + list_1[2]);
            }
            Double difference;
            for(String keys : new_1.keySet()){
                for(String keys2 : old_1.keySet()){
                    if(keys.equals(keys2)){
                        String[] new_list = new_1.get(keys).split(",");
                        String[] old_list = old_1.get(keys2).split(",");
                        difference = Math.sqrt(Math.pow((Double.parseDouble(new_list[0])-Double.parseDouble(old_list[0])),2)+Math.pow((Double.parseDouble(new_list[1])-Double.parseDouble(old_list[1])),2));
                        if (difference>5){
                            return true;
                        }
                    }
                }
            }
            return false;
        }
    }

    public static class map1
            extends Mapper<Object, Text, Text, Text> {
        String K_means;
        HashMap<String, String> centroid = new HashMap<>();
        @Override
        public void setup(Mapper<Object, Text, Text, Text>.Context context)
                throws IOException{
            Configuration configuration = context.getConfiguration();
            K_means = configuration.get("K_means");
            FileSystem fileSystem = FileSystem.get(configuration);
            BufferedReader ip = new BufferedReader(new InputStreamReader(fileSystem.open(new Path(K_means))));
            String lines;
            while ((lines = ip.readLine()) != null) {
                System.out.println(lines);
                String[] list_1 = lines.split(",");
                centroid.put(list_1[0], list_1[1] + "," + list_1[2]);
            }
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Text centroid_id = new Text();
            double min = Double.POSITIVE_INFINITY;
            double distance;
            String id = "";
            String[] points = value.toString().split(",");
            for(String keys : centroid.keySet()){
                String[] centroid_list = centroid.get(keys).split(",");
                double X_centroid = Double.parseDouble(centroid_list[0]);
                double y_centroid = Double.parseDouble(centroid_list[1]);
                distance = Math.sqrt(Math.pow((Double.parseDouble(points[0])-X_centroid),2)+Math.pow((Double.parseDouble(points[1])-y_centroid),2));
                if(distance < min){
                    min = distance;
                    id = keys;
                }
            }
            centroid_id.set(id+","+"centroid: "+ centroid.get(id));
            String result = value.toString()+",1";
            context.write(centroid_id,new Text(result));

        }
    }

    public static class map2
            extends Mapper<Object, Text, Text, Text> {
        String K_means;
        HashMap<String, String> centroid = new HashMap<>();

        @Override
        public void setup(Mapper<Object, Text, Text, Text>.Context context)
                throws IOException {
            Configuration configuration = context.getConfiguration();
            K_means = configuration.get("K_means");
            FileSystem fileSystem = FileSystem.get(configuration);
            BufferedReader ip = new BufferedReader(new InputStreamReader(fileSystem.open(new Path(K_means))));
            String lines;
            while ((lines = ip.readLine()) != null) {
                System.out.println(lines);
                String[] list_1 = lines.split(",");
                centroid.put(list_1[0], list_1[1] + "," + list_1[2]);
            }
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Text centroid_id = new Text();
            double min = Double.POSITIVE_INFINITY;
            double distance;
            String id = "";
            String[] points = value.toString().split(",");
            for (String keys : centroid.keySet()) {
                String[] centroid_list = centroid.get(keys).split(",");
                double X_centroid = Double.parseDouble(centroid_list[0]);
                double y_centroid = Double.parseDouble(centroid_list[1]);
                distance = Math.sqrt(Math.pow((Double.parseDouble(points[0]) - X_centroid), 2) + Math.pow((Double.parseDouble(points[1]) - y_centroid), 2));
                if (distance < min) {
                    min = distance;
                    id = keys;
                }
            }
            centroid_id.set(id + "," + "centroid: " + centroid.get(id));
            context.write(centroid_id, value);
        }
    }


    public static class Combiner
            extends Reducer<Text, Text, Text, Text> {
        private Text local_centroid_out = new Text();
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            double X_sum = 0.0;
            double Y_sum = 0.0;
            int a = 0;
            for (Text v : values) {
                String[] list_1 = v.toString().split(",");
                X_sum += Double.parseDouble(list_1[0]);
                Y_sum += Double.parseDouble(list_1[1]);
                a += Integer.parseInt(list_1[2]);
            }
            local_centroid_out.set(X_sum+","+Y_sum+","+a);
            context.write(key,local_centroid_out );
        }
    }

    public static class Reducer1
            extends Reducer<Text, Text, Text, NullWritable> {
        private Text centroid_out = new Text();
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            double X_sum = 0.0;
            double Y_sum = 0.0;
            int a = 0;
            for (Text v : values) {
                String[] list_1 = v.toString().split(",");
                X_sum += Double.parseDouble(list_1[0]);
                Y_sum += Double.parseDouble(list_1[1]);
                a += Double.parseDouble(list_1[2]);
            }
            String updated_centroid = X_sum/a+","+Y_sum/a;
            centroid_out.set(key.toString().split(",")[0]+","+updated_centroid);
            context.write(centroid_out, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Random rand = new Random();
        int K = Integer.parseInt(args[0]);
        PrintWriter writer = new PrintWriter("yyang19.txt", "UTF-8");
        for(int i =1;i<(K+1);i++){
            int a = rand.nextInt(10000);
            int b = rand.nextInt(10000);
            writer.println(i+","+a+","+b); }
        writer.close();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path sPath = new Path("yyang19.txt");
        String filename = "hdfs://localhost:9000/Project2/input3/";
        Path dPath = new Path(filename);
        fs.copyFromLocalFile(false, sPath, dPath);
        conf.set("K_means", filename);
        for(int i = 0; i < Integer.parseInt(args[1]); i++){
            Job job = Job.getInstance(conf, "Problem_3");
            job.setJarByClass(K_means_output2.class);
            job.setMapperClass(map1.class);
            job.setReducerClass(Reducer1.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            job.setNumReduceTasks(5);
            job.setCombinerClass(Combiner.class);
            FileInputFormat.addInputPath(job, new Path(args[2]));
            FileOutputFormat.setOutputPath(job, new Path(args[3]+i));
            job.waitForCompletion(true);
            FileUtil.copyMerge(fs, new Path(args[3]+i), fs, new Path(args[3]+i+"result.txt"),true, conf, null);
            if(check_difference.check(new Path(filename), new Path(args[3]+i+"result.txt"),fs)){
                filename = args[3]+i+"result.txt";
                conf.set("K_means", filename);
            }
            else{
                break;
            }

        }
        Configuration conf2 = new Configuration();
        conf2.set("K_means", filename);
        Job job2 = Job.getInstance(conf2, "Problem_3");
        job2.setJarByClass(K_means_output2.class);
        job2.setMapperClass(map2.class);
        job2.setNumReduceTasks(0);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]+"finalresult"));
        job2.waitForCompletion(true);
    }

}

