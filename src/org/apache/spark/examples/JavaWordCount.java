/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public final class JavaWordCount
{
	private static final Pattern SPACE = Pattern.compile(" ");
	private static final Pattern COMMA = Pattern.compile("，");

	public static void main(String[] args) throws Exception
	{

		if (args.length < 1)
		{
			System.err.println("Usage: JavaWordCount <file>");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile(args[0], 1);

		JavaDoubleRDD rdd = lines.mapToDouble(new DoubleFunction<String>()
		{

			@Override
			public double call(String t) throws Exception
			{
				// TODO Auto-generated method stub
				return 0;
			}
		});

		// JavaPairRDD<String, Integer> ones = lines.flatMapToPair(new
		// PairFlatMapFunction<String, String, Integer>()
		// {
		//
		// @Override
		// public Iterable<Tuple2<String, Integer>> call(String s) throws
		// Exception
		// {
		// char[] chars = s.toCharArray();
		// Tuple2<String, Integer>[] tuples = new Tuple2[chars.length];
		// String[] ss = new String[chars.length];
		// for (int i = 0; i < chars.length; i++)
		// {
		// char c = chars[i];
		// // if (c >= 0 && c <= 255)
		// // continue;
		// ss[i] = String.valueOf(c);
		// tuples[i] = new Tuple2<String, Integer>(ss[i], 1);
		// }
		// return Arrays.asList(tuples);
		// }
		// });

		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>()
		{
			@Override
			public Iterable<String> call(String s)
			{
				char[] chars = s.toCharArray();
				String[] ss = new String[chars.length];
				for (int i = 0; i < chars.length; i++)
				{
					char c = chars[i];
					// if (c >= 0 && c <= 255)
					// continue;
					ss[i] = String.valueOf(c);
				}
				// return Arrays.asList(COMMA.split(s));
				System.out.println(Arrays.asList(ss));
				return Arrays.asList(ss);
			}
		});

		JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>()
		{
			@Override
			public Tuple2<String, Integer> call(String s)
			{
				return new Tuple2<String, Integer>(s, 1);
			}
		});
		ones = ones.combineByKey(new Function<Integer, Integer>()
		{
			@Override
			public Integer call(Integer i1)
			{
				return i1;
			}
		}, new Function2<Integer, Integer, Integer>()
		{
			@Override
			public Integer call(Integer i1, Integer i2)
			{
				return i1 + i2;
			}
		}, new Function2<Integer, Integer, Integer>()
		{
			@Override
			public Integer call(Integer i1, Integer i2)
			{
				return i1 + i2;
			}
		});

		// ones = ones.sortByKey(false);

		ones = ones.partitionBy(new Partitioner()
		{

			@Override
			public int numPartitions()
			{
				// TODO Auto-generated method stub
				return 10;
			}

			@Override
			public int getPartition(Object arg0)
			{
				// TODO Auto-generated method stub
				return (arg0.hashCode() & Integer.MAX_VALUE) % numPartitions();
				// return 0;
			}
		});

		// JavaPairRDD<String, Iterable<Integer>> onesGroup = ones.groupByKey();
		// JavaPairRDD<String, Integer> counts = onesGroup.reduceByKey(new
		// Function2<Iterable<Integer>, Iterable<Integer>, Integer>()
		// {
		// int sum = 0;
		//
		// @Override
		// public Integer call(Iterable<Integer> i1, Iterable<Integer> i2)
		// {
		// for (Integer i : i1)
		// {
		// sum += i;
		// }
		// for (Integer i : i2)
		// {
		// sum += i;
		// }
		// return new Integer(sum);
		// }
		// });

		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>()
		{
			@Override
			public Integer call(Integer i1, Integer i2)
			{
				return i1 + i2;
			}
		});

		List<Tuple2<String, Integer>> output = counts.collect();
		// counts.saveAsTextFile("warehouse/mr_dw/word_count");
		for (Tuple2<?, ?> tuple : output)
		{
			System.out.println(tuple._1() + ": " + tuple._2());
		}
		ctx.stop();
	}
}
