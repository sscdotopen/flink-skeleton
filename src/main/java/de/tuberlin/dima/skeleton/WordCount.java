/**
 * Flink Skeleton
 * Copyright (C) 2015  Sebastian Schelter
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package de.tuberlin.dima.skeleton;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

public class WordCount {

  public static void main(String[] args) throws Exception {

    List<String> poem = Arrays.asList(
        "Three Rings for the Elven-kings under the sky,",
        "Seven for the Dwarf-lords in their halls of stone,",
        "Nine for Mortal Men doomed to die,",
        "One for the Dark Lord on his dark throne",
        "In the Land of Mordor where the Shadows lie.",
        "One Ring to rule them all, One Ring to find them,",
        "One Ring to bring them all, and in the darkness bind them,",
        "In the Land of Mordor where the Shadows lie.");


    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSet<String> sentences = env.fromCollection(poem);

    DataSet<Tuple2<String, Integer>> words = sentences.flatMap(
        new FlatMapFunction<String, Tuple2<String, Integer>>() {
          @Override
          public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
            StringTokenizer tokenizer = new StringTokenizer(line, ",. -");
            while (tokenizer.hasMoreTokens()) {
              String word = tokenizer.nextToken().toLowerCase();
              collector.collect(new Tuple2<String, Integer>(word, 1));
            }
          }
        });

    DataSet<Tuple2<String, Integer>> wordCounts = words.groupBy(0).sum(1);

    wordCounts.print();

    env.execute();
  }
}
