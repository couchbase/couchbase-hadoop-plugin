/**
 * Copyright 2011 Couchbase, Inc.
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

import com.couchbase.client.TapClient;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import javax.naming.ConfigurationException;


import net.spy.memcached.tapmessage.MessageBuilder;
import net.spy.memcached.tapmessage.ResponseMessage;
import net.spy.memcached.tapmessage.TapStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A RecordReader that reads records from a tap stream
 * Emits LongWritables containing the record number as
 * key and DBWritables as value.
 */
public class CouchbaseRecordReader<T extends DBWritable>
    extends RecordReader<LongWritable, T> {

  private static final Log LOG =
    LogFactory.getLog(CouchbaseRecordReader.class);

  private Class<T> inputClass;

  private Configuration conf;

  private LongWritable key = null;

  private T value = null;

  private CouchbaseConfiguration dbConf;

  private String tableName;

  private TapClient client;

  private CouchbaseInputSplit split;

  public CouchbaseRecordReader(Class<T> inputClass, CouchbaseInputSplit split,
      Configuration conf, CouchbaseConfiguration dbConfig, String table) {
    this.inputClass = inputClass;
    this.conf = conf;
    this.dbConf = dbConfig;
    this.tableName = table;
    this.split = split;
    try {
      String user = dbConf.getUsername();
      String pass = dbConf.getPassword();
      String url = dbConf.getUrlProperty();
      this.client = new TapClient(Arrays.asList(new URI(url)), user,
          pass);
    } catch (URISyntaxException e) {
      LOG.error("Bad URI Syntax: " + e.getMessage());
      client.shutdown();
    }
  }

  @Override
  public void close() throws IOException {
    client.shutdown();
  }

  @Override
  public LongWritable getCurrentKey() throws IOException,
        InterruptedException {
    LOG.trace("Key: " + key);
    return key;
  }

  @Override
  public T getCurrentValue() throws IOException, InterruptedException {
    LOG.trace("Value: " + value.toString());
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    // Since we don't know how many messages are coming progress doesn't
    // make much sense so either we're all the way done or not done at all.
    if (client.hasMoreMessages()) {
      return 0;
    }
    return 1;
  }

  @Override
  public void initialize(InputSplit splits, TaskAttemptContext context)
    throws IOException, InterruptedException {
    try {
      MessageBuilder builder = new MessageBuilder();
      if (tableName.equals("DUMP")) {
        builder.doDump();
        builder.supportAck();
        builder.specifyVbuckets(((CouchbaseInputSplit)split).getVBuckets());
        client.tapCustom(null, builder.getMessage());
      } else if (tableName.startsWith("BACKFILL_")) {
        String time = tableName.substring("BACKFILL_".length(),
            tableName.length());
        builder.doBackfill(0);
        builder.supportAck();
        builder.specifyVbuckets(((CouchbaseInputSplit)split).getVBuckets());
        TapStream tapStream = client.tapCustom(null, builder.getMessage());
        createTapStreamTimeout(tapStream, (new Long(time)).intValue());
      }
    } catch (ConfigurationException e) {
      LOG.error("Couldn't Configure Tap Stream: " + e.getMessage());
      client.shutdown();
    } catch (NumberFormatException e) {
      LOG.error("Bad Backfill Time: " + e.getMessage() + "\n(Ex. BACKFILL_5");
      client.shutdown();
    }
  }

  private void createTapStreamTimeout(final TapStream tapStream,
      final long duration) {
    if (duration > 0) {
      Runnable r = new Runnable() {
        @Override
        public void run() {
          try {
            Thread.sleep(TimeUnit.MILLISECONDS.convert(duration,
                TimeUnit.MINUTES));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Tap stream closing early. Reason: "
              + e.getMessage());
          }
          tapStream.cancel();
        }
      };
      new Thread(r).start();
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    ResponseMessage message;
    while ((message = client.getNextMessage()) == null) {
      if (!client.hasMoreMessages()) {
        return false;
      }
    }

    byte[] mkey = null;
    byte[] mvalue = null;
    ByteBuffer buf;
    int bufLen = 4;

    mkey = message.getKey().getBytes();
    bufLen += mkey.length;

    mvalue = message.getValue();
    bufLen += mvalue.length;
    buf = ByteBuffer.allocate(bufLen);

    if (key == null) {
      key = new LongWritable();
    }
    if (value == null) {
      value = ReflectionUtils.newInstance(inputClass, conf);
    }

    key.set(client.getMessagesRead());
    if (mkey != null) {
      buf.put((byte)0);
      buf.put((byte)mkey.length);
      for (int i = 0; i < mkey.length; i++) {
        buf.put(mkey[i]);
      }
    }

    if (mvalue != null) {
      buf.put((byte)0);
      buf.put((byte)mvalue.length);
      for (int i = 0; i < mvalue.length; i++) {
        buf.put(mvalue[i]);
      }
    }

    ByteArrayInputStream in = new ByteArrayInputStream(buf.array());
    DataInputStream dataIn = new DataInputStream(in);
    ((Writable)value).readFields(dataIn);
    dataIn.close();
    return true;
  }
}
