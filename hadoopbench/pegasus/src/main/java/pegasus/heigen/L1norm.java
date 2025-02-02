/***********************************************************************
    PEGASUS: Peta-Scale Graph Mining System
    Authors: U Kang, Duen Horng Chau, and Christos Faloutsos

This software is licensed under Apache License, Version 2.0 (the  "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-------------------------------------------------------------------------
File: L1norm.java
 - Compute L1 norm with block based method.
Version: 2.0
***********************************************************************/

package pegasus.heigen;

import java.io.*;
import java.util.*;
import java.text.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import pegasus.PegasusUtils;

// y = y + ax
public class L1norm extends Configured implements Tool 
{
	public static class MapStage1 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, DoubleWritable>
    {
		public void map (final LongWritable key, final Text value, final OutputCollector<IntWritable, DoubleWritable> output, final Reporter reporter) throws IOException
		{
			String line_text = value.toString();
			int tabpos = line_text.indexOf("\t");
			double raw_val = 0;

			if( tabpos > 0 ) {
				if( line_text.charAt(tabpos+1) == 'v') {
					raw_val = Math.abs(Double.parseDouble(line_text.substring(tabpos+2)));
				} else {
					raw_val = Math.abs(Double.parseDouble(line_text.substring(tabpos+1)));
				}
			} else {
				raw_val = 1.0;
			}

			output.collect(new IntWritable(0), new DoubleWritable(Math.abs(raw_val)));
		}
	}

		
    //////////////////////////////////////////////////////////////////////
    // command line interface
    //////////////////////////////////////////////////////////////////////
	protected int nreducers = 1;

    // Main entry point.
    public static void main (final String[] args) throws Exception
    {
		final int result = ToolRunner.run(new Configuration(), new Saxpy(), args);

		System.exit(result);
    }


    // Print the command-line usage text.
    protected static int printUsage ()
    {
		System.out.println("L1norm <in_path>");

		ToolRunner.printGenericCommandUsage(System.out);

		return -1;
    }

	// submit the map/reduce job.
    public int run (final String[] args) throws Exception
    {
		if( args.length != 1 ) {
			return printUsage();
		}

		Path in_path = new Path(args[0]);

		System.out.println("\n-----===[PEGASUS: A Peta-Scale Graph Mining System]===-----\n");
		System.out.println("[PEGASUS] Computing L1norm. in_path=" + in_path.getName() + "\n");

		final FileSystem fs = FileSystem.get(getConf());
		Path l1norm_output = new Path("l1norm_output");
		fs.delete(l1norm_output);
		JobClient.runJob(configL1norm(in_path, l1norm_output));

		System.out.println("\n[PEGASUS] L1norm computed. Output is saved in HDFS " + l1norm_output.getName() + "\n");

		return 0;
    }

	// Configure l2 norm
    protected JobConf configL1norm (Path in_path, Path out_path) throws Exception
    {
		final JobConf conf = new JobConf(getConf(), L1norm.class);
		conf.setJobName("L1norm");
		
		conf.setMapperClass(MapStage1.class);
		conf.setReducerClass(PegasusUtils.RedSumDouble.class);
		conf.setCombinerClass(PegasusUtils.RedSumDouble.class);

		FileInputFormat.setInputPaths(conf, in_path);  
		FileOutputFormat.setOutputPath(conf, out_path);  

		conf.setNumReduceTasks( 1 );

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(DoubleWritable.class);

		return conf;
    }
}

