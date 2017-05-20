import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


public class CoOccurrenceMatrixGenerator {

    public static class MatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key,
                        Text value,
                        Context context) throws IOException, InterruptedException {
            // value: userID\tmovie1:rating,movie2:rating,...

            String line = value.toString().trim();
            String[] user_movieRatings = line.split("\t");
            if (user_movieRatings.length != 2) {
                return;
            }

            // [1,2,3] -> 1,1 1,2 1,3 2,1 2,2 2,3 3,1 3,2 3,3
            String[] movieRatings = user_movieRatings[1].split(",");
            for (int i = 0; i < movieRatings.length; i++) {
                String movieA = movieRatings[i].trim().split(":")[0];
                for (int j = 0; j < movieRatings.length; j++) {
                    String movieB = movieRatings[j].trim().split(":")[0];
                    context.write(new Text(movieA + "&" + movieB), new IntWritable(1));
                }
            }
        }
    }


    public static class MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key,
                           Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum = sum + value.get();
            }
            // movieA&movieB sum
            context.write(key, new IntWritable(sum));
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(DataDividerByUser.class);

        job.setMapperClass(MatrixGeneratorMapper.class);
        job.setReducerClass(MatrixGeneratorReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}