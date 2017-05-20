import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


public class DataDividerByUser {

    public static class DataDividerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        public void map(LongWritable key,
                        Text value,
                        Context context) throws IOException, InterruptedException {
            // input: user,movie,rating
            String line = value.toString().trim();
            String[] userMovieRating = line.split(",");
            int userID = Integer.parseInt(userMovieRating[0]);
            String movieID = userMovieRating[1];
            String rating = userMovieRating[2];

            context.write(new IntWritable(userID), new Text( movieID + ":" + rating));
        }
    }


    public static class DataDividerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        public void reduce(IntWritable key,
                           Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            // input key: userID
            // input value: list of movies
            StringBuilder sb = new StringBuilder();
            while (values.iterator().hasNext()) {
                sb.append("," + values.iterator().next());
            }

            // sb = ,movie1:5.6,movie2:7.8,...
            context.write(key, new Text(sb.toString().replaceFirst(",", "")));
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(DataDividerByUser.class);

        job.setMapperClass(DataDividerMapper.class);
        job.setReducerClass(DataDividerReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}