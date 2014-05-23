package webreduce.hadoop;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import webreduce.data.Dataset;

import com.google.gson.JsonSyntaxException;

/*
 * MapReduce job that maps all tables in the corpus to their first line,
 * then checks each set of tables with same first line for duplicates by comparing
 * part of their content.
 *
 * Can also be used as a template for other Hadoop jobs running on DWTC.
 */
public class Deduplicate extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Deduplicate(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		// Creates a new job configuration for this Hadoop job.
		Configuration conf = getConf();

		conf.setBoolean("mapreduce.map.output.compress", true);
		conf.set("mapreduce.map.failures.maxpercent", "10");
		conf.set("mapreduce.max.map.failures.percent", "10");
		conf.set("mapred.max.map.failures.percent", "10");
		conf.set("mapred.map.failures.maxpercent", "10");

		String inputPrefixes = args[0];
		String outputFile = args[1];
		Job job = Job.getInstance(conf);

		FileInputFormat.addInputPath(job, new Path(inputPrefixes));
		FileOutputFormat.setOutputPath(job, new Path(outputFile));
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job,
				org.apache.hadoop.io.compress.GzipCodec.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(DeduplicateMapper.class);
		job.setReducerClass(DeduplicateReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setJarByClass(Deduplicate.class);
		job.setNumReduceTasks(500);

		job.submit();
		return 0;
	}

	public static class DeduplicateMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		Text outKey = new Text();

		public enum DeduplicateCounters {
			JSON_SYNTAX_EXCEPTION
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Dataset ds;
			try {
				ds = Dataset.fromJson(value.toString());
			} catch (JsonSyntaxException ex) {
				context.getCounter(DeduplicateCounters.JSON_SYNTAX_EXCEPTION)
						.increment(1);
				return;
			}
			String newKey = StringUtils.join(ds.getAttributes(), " ");
			outKey.set(newKey);
			context.write(outKey, value);
		}

	}

	public static class DeduplicateReducer extends
			Reducer<Text, Text, NullWritable, Text> {

		Set<String> cache = null;
		Text outValue = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			cache = new HashSet<String>();
			for (Text t : values) {
				String oldValue = t.toString();
				Dataset ds = Dataset.fromJson(oldValue);
				StringBuilder sb = new StringBuilder();
				for (String[] c : ds.getRelation()) {
					sb.append(c[0]);
					sb.append(c[1]);
				}
				String contentSample = sb.toString();
				boolean contained = cache.add(contentSample);
				if (!contained) {
					outValue.set(oldValue);
					context.write(NullWritable.get(), outValue);
				}
			}
		}

	}

}
