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

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.List;

/**
 * Computes an approximation to pi Usage: JavaSparkPi [slices]
 */
public final class JavaSparkPi2
{

	public static void main(String[] args) throws Exception
	{
		SparkConf sparkConf = new SparkConf().setAppName("JavaSparkPi").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		int slices = (args.length == 1) ? Integer.parseInt(args[0]) : 2;
		int n = 100000 * slices;
		List<Integer> l = new ArrayList<Integer>(n);
		for (int i = 0; i < n; i++)
		{
			l.add(i);
		}

		JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

		double count = dataSet.map(new Function<Integer, Double>()
		{
			@Override
			public Double call(Integer integer)
			{
				// double x = Math.random() * 2 - 1;
				// double y = Math.random() * 2 - 1;
				// return (x * x + y * y < 1) ? 1 : 0;
				// return 1.0 / (2 * integer + 1) * (integer % 2 == 0 ? 1 : -1);
				return (1.0 / (2 * integer + 1) * (4 / Math.pow(5.0, (2 * integer + 1)) - 1.0 / Math.pow(239.0, (2 * integer + 1))))
						* (integer % 2 == 0 ? 1 : -1);
			}
		}).reduce(new Function2<Double, Double, Double>()
		{
			@Override
			public Double call(Double integer, Double integer2)
			{
				return integer + integer2;
			}
		});

		System.out.println("Pi is roughly " + 4.0 * count);

		jsc.stop();
	}
}
