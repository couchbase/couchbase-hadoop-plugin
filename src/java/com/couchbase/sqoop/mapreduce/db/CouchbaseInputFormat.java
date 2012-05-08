/**
 * Copyright 2011-2012 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.sqoop.mapreduce.db;

import com.cloudera.sqoop.config.ConfigurationHelper;
import com.couchbase.client.CouchbaseClient;
import com.couchbase.sqoop.mapreduce.CouchbaseImportMapper;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/**
 * A InputFormat that reads input data from Couchbase Server
 * <p>
 * CouchbaseInputFormat emits LongWritables containing a record
 * number as key and DBWritables as value.
 *
 */
public class CouchbaseInputFormat<T extends DBWritable> extends
    InputFormat<Text, T> implements Configurable {

  private String tableName;

  private CouchbaseConfiguration dbConf;

  @Override
  public void setConf(Configuration conf) {
    dbConf = new CouchbaseConfiguration(conf);
    dbConf.setMapperClass(CouchbaseImportMapper.class);
    tableName = dbConf.getInputTableName();
  }

  @Override
  public Configuration getConf() {
    return dbConf.getConf();
  }

  public CouchbaseConfiguration getDBConf() {
    return dbConf;
  }

  @Override
  /** {@inheritDoc} */
  public RecordReader<Text, T> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return createRecordReader(split, context.getConfiguration());
  }

  @SuppressWarnings("unchecked")
  public RecordReader<Text, T> createRecordReader(InputSplit split,
      Configuration conf)
    throws IOException, InterruptedException {
    Class<T> inputClass = (Class<T>) (dbConf.getInputClass());
    return new CouchbaseRecordReader<T>(inputClass, (CouchbaseInputSplit)split,
      conf, getDBConf(), tableName);
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException,
      InterruptedException {
    List<URI> baseUris = new LinkedList<URI>();
    baseUris.add(URI.create(dbConf.getUrlProperty()));
    // Tell things using Spy's logging to use log4j compat, hadoop uses log4j
    Properties spyLogProperties = System.getProperties();
    spyLogProperties.put("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.Log4JLogger");
    System.setProperties(spyLogProperties);
    CouchbaseClient client = new CouchbaseClient(baseUris, dbConf.getUsername(),
        dbConf.getPassword());
    int numVBuckets = client.getNumVBuckets();
    client.shutdown();

    int chunks = ConfigurationHelper.getJobNumMaps(job);
    int itemsPerChunk = numVBuckets / chunks;
    int extraItems = numVBuckets % chunks;

    List<InputSplit> splits = new ArrayList<InputSplit>();

    int splitIndex = 0;
    short[] curSplit = nextEmptySplit(itemsPerChunk, extraItems);
    extraItems--;

    for (short i = 0; i < numVBuckets + 1; i++) {
      if (splitIndex == curSplit.length) {
        CouchbaseInputSplit split = new CouchbaseInputSplit(curSplit);
        splits.add(split);
        curSplit = nextEmptySplit(itemsPerChunk, extraItems);
        extraItems--;
        splitIndex = 0;
      }
      curSplit[splitIndex] = i;
      splitIndex++;
    }
    return splits;
  }

  private short[] nextEmptySplit(int itemsPerChunk, int extraItems) {
    if (extraItems > 0) {
      return new short[itemsPerChunk + 1];
    } else {
      return new short[itemsPerChunk];
    }
  }
}
