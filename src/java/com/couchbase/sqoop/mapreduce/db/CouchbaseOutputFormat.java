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

import com.cloudera.sqoop.mapreduce.db.DBConfiguration;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import net.spy.memcached.internal.OperationFuture;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * A OutputFormat that sends the reduce output to a Couchbase server.
 * <p>
 * {@link CocuhbaseOutputFormat} accepts &lt;key,value&gt; pairs, where
 * key has a type extending DBWritable. Returned {@link RecordWriter}
 * writes <b>only the key</b> to the database with a batch tap stream.
 */
public class CouchbaseOutputFormat<K extends DBWritable, V>
    extends OutputFormat<K, V> {
  public static final Log LOG = LogFactory.getLog(
      CouchbaseOutputFormat.class.getName());

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException,
      InterruptedException {
    Configuration conf = context.getConfiguration();

    // Sanity check all the configuration values we need.
    if (null == conf.get(DBConfiguration.URL_PROPERTY)) {
      throw new IOException("Database connection URL is not set.");
    }
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new FileOutputCommitter(FileOutputFormat.getOutputPath(context),
        context);
  }

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new CouchbaseRecordWriter(
        new CouchbaseConfiguration(context.getConfiguration()));
  }

  /**
   * A RecordWriter that writes the reduce output to a Couchbase
   * server.
   */
  public class CouchbaseRecordWriter extends RecordWriter<K, V> {

    final class KV {
      private String key;
      private Object value;
      private Future<Boolean> status;

      private KV(String key, Object value, Future<Boolean> status) {
        this.key = key;
        this.value = value;
        this.status = status;
      }
    }

    private CouchbaseClient client;

    private BlockingQueue<KV> opQ;

    public CouchbaseRecordWriter(CouchbaseConfiguration dbConf) {
      String user = dbConf.getUsername();
      String pass = dbConf.getPassword();
      String url = dbConf.getUrlProperty();
      opQ = new LinkedBlockingQueue<KV>();
      CouchbaseConnectionFactoryBuilder cfb =
        new CouchbaseConnectionFactoryBuilder();
      cfb.setOpTimeout(10000);  // wait up to 10 seconds for an op to succeed
      cfb.setOpQueueMaxBlockTime(60000); // wait up to 1 minute to enqueue
      try {
        List<URI> baselist = Arrays.asList(new URI(url));
        client = new CouchbaseClient(cfb.buildCouchbaseConnection(baselist,
            user, user, pass));
      } catch (IOException e) {
        client.shutdown();
        LOG.fatal("Problem configuring CouchbaseClient for IO.", e);
      } catch (URISyntaxException e) {
        client.shutdown();
        LOG.fatal("Could not configure CouchbaseClient with URL supplied: "
          + url, e);
      }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
      while (opQ.size() > 0) {
        drainQ();
      }
      client.shutdown();
    }

    private void drainQ() throws IOException {
      Queue<KV> list = new LinkedList<KV>();
      opQ.drainTo(list, opQ.size());

      KV kv;
      while ((kv = list.poll()) != null) {
        try {
          if (!kv.status.get().booleanValue()) {
            opQ.add(new KV(kv.key, kv.value, client.set(kv.key, 0, kv.value)));
            LOG.info("Failed");
          }
        } catch (RuntimeException e) {
          LOG.fatal("Failed to export record. " + e.getMessage());
          throw new IOException("Failed to export record", e);
        } catch (InterruptedException e) {
          LOG.fatal("Interrupted during record export. " + e.getMessage());
          throw new IOException("Interrupted during record export", e);
        } catch (ExecutionException e) {
          LOG.fatal("Aborted execution during record export. "
            + e.getMessage());
          throw new IOException("Aborted execution during record export", e);
        }
      }
    }

    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
      String keyToAdd = null;
      Object valueToAdd = null;

      if (opQ.size() > 10000) {
        drainQ();
      }

      keyToAdd = key.toString();
      valueToAdd = value;
      OperationFuture<Boolean> arecord = null;

      try {
        arecord = client.set(keyToAdd, 0, valueToAdd);
        opQ.add(new KV(keyToAdd, valueToAdd, arecord));
      } catch (IllegalArgumentException e) {
        LOG.error("Failed to write record " + arecord.getKey(), e);
        LOG.error("Status of failed record is " + arecord.getStatus());
        throw new IOException("Failed to write record", e);
      }
    }
  }
}
