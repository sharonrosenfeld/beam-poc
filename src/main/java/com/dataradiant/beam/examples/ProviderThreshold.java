/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataradiant.beam.examples;

import org.apache.beam.runners.flink.FlinkRunner;
import org.json.*;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ProviderThreshold {

  public static class ExtractResultsFn extends DoFn<String, KV<String,List<Integer>>> {

    @Override
    public void processElement(ProcessContext c) {

      String providerId, channelId, result;
      String line = c.element();

      try{
        JSONObject obj = new JSONObject(line);
        providerId = obj.getString("provider-id");
        channelId = obj.getString("channel-id");
        result = obj.getString("result");
      }catch (JSONException e){
          return;
      }

      List<Integer> values = new ArrayList<Integer>();
      values.add(1);
      if (result.equals("105")){
        values.add(1);
      }else {
        values.add(0);
      }
      c.output(KV.of (providerId+"-"+channelId,values));
    }
  }

  public static class PercentageFn extends Combine.CombineFn<List<Integer>, PercentageFn.Accum, Integer> {
    public class Accum implements Serializable{
      int total = 0;
      int a105 = 0;
    }

    @Override
    public Accum createAccumulator() { return new Accum(); }

    @Override
    public Accum addInput(Accum accum, List<Integer> input) {
      accum.a105+=input.get(1);
      accum.total++;
      return accum;
    }

    @Override
    public Accum mergeAccumulators(Iterable<Accum> accums) {
      Accum merged = createAccumulator();
      for (Accum accum : accums) {
        merged.total += accum.total;
        merged.a105 += accum.a105;
      }
      return merged;
    }

    @Override
    public Integer extractOutput(Accum accum) {
      return (int)((((double)accum.a105 / accum.total)) * 100);
    }
  }


  public static class FindOutages extends PTransform<PCollection<String>,
                    PCollection<KV<String, Integer>>> {
    @Override
    public PCollection<KV<String, Integer>> apply(PCollection<String> lines) {

      PCollection<KV<String,List<Integer>>> results =
              lines.apply(ParDo.of(new ExtractResultsFn()));

      //group by & merge for each provider-channel
      PCollection<KV<String,Integer>> percentages = results.apply(Combine.<String,List<Integer>,Integer>perKey(new PercentageFn()));

      //filter
      PCollection<KV<String,Integer>> outages = percentages
              .apply(Filter.by(new SerializableFunction<KV<String,Integer>, Boolean>() {
                @Override
                public Boolean apply(KV<String,Integer> input) {
                  System.out.println(input.getKey()+"--"+input.getValue());
                  return input.getValue() > 95;
                }
              }));

      return outages;
    }
  }

      /** A SimpleFunction that converts a Word and Count into a printable string. */
  public static class FormatAsTextFn extends SimpleFunction<KV<String, Integer>, String> {
    @Override
    public String apply(KV<String, Integer> input) {
      return input.getKey() + ": " + input.getValue();
    }
  }

  /**
   * Options supported by {@link ProviderThreshold}.
   * <p>
   * Inherits standard configuration options.
   */
  public interface Options extends PipelineOptions, FlinkPipelineOptions {
    @Description("Path of the file to read from")
    @Default.String("/tmp/fpos.txt")
    String getInput();
    void setInput(String value);

    @Description("Path of the file to write to")
    @Default.String("/tmp/alerts.txt")
    String getOutput();
    void setOutput(String value);

    @Description("Fixed window duration, in minutes")
    @Default.Integer(10)
    Integer getWindowSize();
    void setWindowSize(Integer value);
  }

  public static void main(String[] args) {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(Options.class);
//    options.setRunner(FlinkRunner.class);
//    options.setParallelism(2);

    Pipeline p = Pipeline.create(options);

    p.apply("ReadLines", TextIO.Read.from(options.getInput()))
           // .apply(Window.<String>into(
             //       FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))))
            .apply(new FindOutages())
        .apply(MapElements.via(new FormatAsTextFn()))
        .apply("WriteOutages", TextIO.Write.to(options.getOutput()));

    p.run();
  }

}