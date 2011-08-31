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

import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.mapreduce.db.DBInputFormat.NullDBWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/**
 * A container for configuration property names for jobs with Couchbase
 * input/output.
 *
 * The job can be configured using the static methods in this class,
 * {@link CouchbaseInputFormat}, and {@link CouchbaseOutputFormat}.
 * Alternatively, the properties can be set in the configuration with proper
 * values.
 */
public class CouchbaseConfiguration {

  private Configuration conf;

  public CouchbaseConfiguration(Configuration job) {
    this.conf = job;
  }

  public Configuration getConf() {
    return conf;
  }

  public Class<?> getInputClass() {
    return conf.getClass(DBConfiguration.INPUT_CLASS_PROPERTY,
        NullDBWritable.class);
  }

  public void setInputClass(Class<? extends DBWritable> inputClass) {
    conf.setClass(DBConfiguration.INPUT_CLASS_PROPERTY, inputClass,
        DBWritable.class);
  }

  public String getUsername() {
    return conf.get(DBConfiguration.USERNAME_PROPERTY, "default");
  }

  public void setUsername(String username) {
    conf.set(DBConfiguration.USERNAME_PROPERTY, username);
  }

  public String getPassword() {
    return conf.get(DBConfiguration.PASSWORD_PROPERTY, "");
  }

  public void setPassword(String password) {
    conf.set(DBConfiguration.PASSWORD_PROPERTY, password);
  }

  public String getUrlProperty() {
    return conf.get(DBConfiguration.URL_PROPERTY);
  }

  public void setUrlProperty(String url) {
    conf.set(DBConfiguration.URL_PROPERTY, url);
  }

  public String getInputTableName() {
    return conf.get(DBConfiguration.INPUT_TABLE_NAME_PROPERTY);
  }

  public void setOutputTableName(String tableName) {
    conf.set(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY, tableName);
  }
}
