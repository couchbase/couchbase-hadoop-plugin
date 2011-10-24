#bin/bash

# Copyright 2011 Couchbase, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

function check_file_exists {
  if [ -f $1 ]; then
    echo "$1 FOUND"
  else
    echo "$1 NOT FOUND"
    echo
    echo "Install Failed"
    exit 1
  fi
}

f_config=$PWD/couchbase-config.xml
f_manager=$PWD/couchbase-manager.xml
f_plugin=$PWD/couchbase-hadoop-plugin-1.0.jar

if [ $# -ne 1 ]; then
  echo "usage: ./install.sh path_to_sqoop_home"
  exit 1;
fi

sqoop_home=$1

echo
echo "---Checking for install files---"

check_file_exists $f_config
check_file_exists $f_manager
check_file_exists $f_plugin

mkdir -p $sqoop_home/lib/
mkdir -p $sqoop_home/conf/managers.d/

echo
echo "Installing files to Sqoop"
cp -f $f_config $sqoop_home/conf/
cp -f $f_manager $sqoop_home/conf/managers.d/
cp -f *.jar $sqoop_home/lib/


echo
echo "Install Successful!"
