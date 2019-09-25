#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

case "$1" in
    scheduler)
        while true; do
	    echo "Starting scheduler run at "$(date)
	    airflow "$@"
	    exit_code=$(echo $?)
	    if [ $exit_code != 0]; then
	        echo "Scheduler had a fatal error. Exit code: "$exit_code
	    fi
	done
        ;;
    worker)
        echo "Starting worker"
        airflow "$@"
        ;;
    webserver)
        echo "Starting webserver"
        airflow "$@"
        ;;
    initdb)
        airflow db init
        ;;
    dev)
        # For dev purposes run in this mode to run no services and do things in
        # a bash shell without killing the container's entrypoint.
        sleep infinity
        ;;
    *)
        "$@"
        ;;
esac
