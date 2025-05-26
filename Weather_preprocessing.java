import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherPreprocessing {

    public static class Map extends Mapper<LongWritable, Text, Text, NullWritable> {
        public void map(LongWritable key, Text value, Context context) {
            try {
                String line = value.toString();
                if(line.trim().isEmpty() || line.startsWith("station_id")) return;

                String[] fields = line.split(",");
                if(fields.length < 12) return;

                float temperature = Float.parseFloat(fields[3]);
                float humidity = Float.parseFloat(fields[4]);
                float rainfall = Float.parseFloat(fields[11]);

                // Filter out invalid values
                if (temperature < -50 || temperature > 60) return;
                if (humidity < 0 || humidity > 100) return;
                if (rainfall < 0) return;

                context.write(new Text(line), NullWritable.get());
            } catch (Exception e) {
                // Skip lines with parsing errors
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "weather preprocessing");
        job.setJarByClass(WeatherPreprocessing.class);
        job.setMapperClass(Map.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
