import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.*;

import org.apache.hadoop.io.LongWritable;

public class Twoinputs extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		
			int res = ToolRunner.run(new Twoinputs(), args);
			System.exit(res);
		
	}

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 3) {
			System.out
					.println("Usage: <Department Emp Strength input> <Department Name input> <output>");
			return -1;
		}

		Configuration cconf = new Configuration();

		JobConf conf = new JobConf(cconf, getClass());
		conf.setJobName("Join 'Department Emp Strength input' with 'Department Name input'");

		Path AInputPath = new Path(args[0]);
		Path BInputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);

		MultipleInputs.addInputPath(conf, AInputPath, TextInputFormat.class,
				JoinAMapper.class);
		MultipleInputs.addInputPath(conf, BInputPath, TextInputFormat.class,
				JoinBMapper.class);

		FileOutputFormat.setOutputPath(conf, outputPath);

		conf.setPartitionerClass(KeyPartitioner.class);
		conf.setOutputValueGroupingComparator(TextPair.FirstComparator.class);

		conf.setMapOutputKeyClass(TextPair.class);

		conf.setReducerClass(JoinReducer.class);

		conf.setOutputKeyClass(Text.class);

		removeDir(args[2], cconf);
		JobClient.runJob(conf);

		return 0;
	}

	public static class KeyPartitioner implements Partitioner<TextPair, Text> {
		@Override
		public void configure(JobConf job) {
		}

		@Override
		public int getPartition(TextPair key, Text value, int numPartitions) {
			return (key.getFirst().hashCode() & Integer.MAX_VALUE)
					% numPartitions;
		}
	}

	private static void removeDir(String pathToDirectory, Configuration conf)
			throws IOException {
		Path pathToRemove = new Path(pathToDirectory);
		FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(pathToRemove)) {
			fileSystem.delete(pathToRemove, true);
		}
	}
}

class JoinAMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, TextPair, Text> {

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<TextPair, Text> output, Reporter reporter)
			throws IOException {
		String valueString = value.toString();
		String[] SingleNodeData = valueString.split(" ");
		output.collect(new TextPair(SingleNodeData[0], "0"), new Text(
				SingleNodeData[1]));
	}
}

class JoinBMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, TextPair, Text> {

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<TextPair, Text> output, Reporter reporter)
			throws IOException {

		String valueString = value.toString();
		System.out.println("line------------> "+valueString);
		String[] SingleNodeData = valueString.split(" ");
		System.out.println("------>"+Arrays.deepToString(SingleNodeData));
		output.collect(new TextPair(SingleNodeData[0], "1"), new Text(
				SingleNodeData[1]));
	}
}

class JoinReducer extends MapReduceBase implements
		Reducer<TextPair, Text, Text, Text> {

	@Override
	public void reduce(TextPair key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		Text nodeId = new Text(values.next());
		while (values.hasNext()) {
			Text node = values.next();
			Text outValue = new Text(nodeId.toString() + "," + node.toString());
			output.collect(key.getFirst(), outValue);
		}
	}
}
