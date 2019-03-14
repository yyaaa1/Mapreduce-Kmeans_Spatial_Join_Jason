import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Spatial_Join_Window {

    public static class map1
            extends Mapper<Object, Text, Text, Text> {
        private String check_point = new String();
        private int a;
        private int b;
        private int c;
        private int d;
        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            check_point = configuration.get("window_parameter");
            if(!check_point.equals("fulljoin")) {
                a = Integer.parseInt(check_point.split(",")[0]);
                b = Integer.parseInt(check_point.split(",")[1]);
                c = Integer.parseInt(check_point.split(",")[2]);
                d = Integer.parseInt(check_point.split(",")[3]);
            }
        }

        private Text X = new Text();
        private Text Y = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] list_1 = value.toString().split(",");
            if(!check_point.equals("fulljoin")) {
                if(Integer.parseInt(list_1[0]) >= a && Integer.parseInt(list_1[0]) <= c && Integer.parseInt(list_1[1]) <= d && Integer.parseInt(list_1[1]) >= b){
                    X.set(list_1[0]);
                    Y.set(list_1[1]);
                    context.write(X, Y); }
            }
            else{
                X.set(list_1[0]);
                Y.set(list_1[1]);
                context.write(X, Y);

            }
        }
    }

    public static class map2
            extends Mapper<Object, Text, Text, Text> {
        private Text X = new Text();
        private Text p_Y = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] list_1 = value.toString().split(",");
            p_Y.set(list_1[0]+" "+list_1[2]+" "+list_1[4]);
            for (int v = Integer.parseInt(list_1[1]);v<(Integer.parseInt(list_1[3])+1);v++){
                X.set(Integer.toString(v));
                context.write(X, p_Y);
            }
        }
    }





    public static class Reducer1
            extends Reducer<Text, Text, Text, Text> {
        private Text r = new Text();
        private Text x = new Text();
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String a = ("a");
            String b = new String();
            for (Text v : values) {
                char c = v.toString().charAt(0);
                String a_s = String.valueOf(c);
                if(a_s.equals("r")){
                    b = b+v+",";
                }
                else{
                    a = v.toString()+","+a;

                }
            }
            String[] list_1 = b.split(",");
            String[] list_3 = a.split(",");
            for(String v1:list_3){
                if(!v1.equals("a")){
                for (String v:list_1) {
                String[] list_2 = v.split(" ");
                int upper = Integer.parseInt(list_2[2]);
                int lower = Integer.parseInt(list_2[1]);
                if (Integer.parseInt(v1) >= lower && Integer.parseInt(v1) <= upper) {
                    r.set(list_2[0]);
                    x.set("(" + key.toString() + "," + v1 + ")");
                    context.write(r, x);
                }
                }
            }
            }

        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if(args.length == 7){
            conf.set("window_parameter", args[3]+","+args[4]+","+args[5]+","+args[6]);}
        else{conf.set("window_parameter","fulljoin");}
        Job job = Job.getInstance(conf, "Spatial_Join_Window");
        Path p1=new Path(args[0]);
        Path p2=new Path(args[1]);
        Path p3=new Path(args[2]);
        job.setJarByClass(Spatial_Join_Window.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(Reducer1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, p1, TextInputFormat.class, map1.class);
        MultipleInputs.addInputPath(job, p2, TextInputFormat.class, map2.class);
        FileOutputFormat.setOutputPath(job, p3);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
