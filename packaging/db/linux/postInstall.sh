#!/bin/sh
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

USERNAME='@USERNAME@'
GROUPNAME='@GROUPNAME@'

setup_directories() {
#  echo "setup directories"
  [ -d '@CONF_DIR@' ] ||  install -d '@CONF_DIR@' --owner="${USERNAME}" --group="${GROUPNAME}" --mode=0770
  [ -d '@PID_DIR@' ] ||  install -d '@PID_DIR@' --owner="${USERNAME}" --group="${GROUPNAME}" --mode=0770
  [ -d '@LOG_DIR@' ] ||  install -d '@LOG_DIR@' --owner="${USERNAME}" --group="${GROUPNAME}" --mode=0770
  [ -d '@WORK_DIR@' ] ||  install -d '@WORK_DIR@' --owner="${USERNAME}" --group="${GROUPNAME}" --mode=0770

  /bin/chown -R "${USERNAME}:${GROUPNAME}" '@INSTALL_DIR@'
  /bin/chown -R "${USERNAME}:${GROUPNAME}" '@LOG_DIR@'
  /bin/chown -R "${USERNAME}:${GROUPNAME}" '@CONF_DIR@'
  /bin/chown -R "${USERNAME}:${GROUPNAME}" '@PID_DIR@'
}

setup_service_files() {
  if command -v initctl >/dev/null && initctl version | grep upstart >/dev/null; then
    ln -sf '@INSTALL_DIR@/@PRODUCT_NAME@.conf' /etc/init/
    initctl reload-configuration
  elif command -v systemctl >/dev/null && systemctl --version | grep systemd >/dev/null; then
    mkdir -p /usr/lib/systemd/system
    ln -sf '@INSTALL_DIR@/@PRODUCT_NAME@.service' /usr/lib/systemd/system/
    systemctl daemon-reload
    systemctl enable '@INSTALL_DIR@/@PRODUCT_NAME@.service' >/dev/null 2>&1
  else
    echo
    echo "We could not detect Upstart or Systemd. You can start the process manually using @INSTALL_DIR@/start.sh"
    echo
  fi
}

setup_host_name() {
  if command -v hostname >/dev/null && command -v sed >/dev/null; then
    sed -i "s/NODE_NAME=node1/NODE_NAME=$(hostname)/" @CONF_DIR@/vars.env
  fi
}

escape_forward_slashes() {
  echo "$1" | sed 's;/;\\/;g'
}

replace_or_append_property_in_file() (
  LHS="$(escape_forward_slashes "$1")"
  RHS="$(escape_forward_slashes "$2")"
  sed -i "/^$LHS=/{h;s/=.*/=$RHS/};\${x;/^$/{s//$LHS=$RHS/;H};x}" "$3"
)

replace_or_append_properties() (
  replace_or_append_property() {
    LHS="$(echo "$1" | cut -d'=' -f1)"
    RHS="$(echo "$1" | cut -d'=' -f2-)"
    replace_or_append_property_in_file "$LHS" "$RHS" "$2"
  }
  while IFS='' read -r LINE || [ -n "${LINE}" ]; do
    case "$LINE" in
      *=*) replace_or_append_property "$LINE" "$2" ;;
    esac
  done < "$1"
)

persist_properties() {
  if [ -f '@CONF_DIR@/backup/vars.env' ]; then
    if [ -f '@CONF_DIR@/vars.env.rpmnew' ]; then
      mv '@CONF_DIR@/vars.env.rpmnew' '@CONF_DIR@/vars.env'
    fi
    replace_or_append_properties '@CONF_DIR@/backup/vars.env' '@CONF_DIR@/vars.env'
  fi
  if [ -f '@CONF_DIR@/backup/ignite-config.conf' ]; then
    cp -f "@CONF_DIR@/backup/ignite-config.conf" "@CONF_DIR@/ignite-config.conf"
  fi
}

setup_directories
setup_service_files
persist_properties
setup_host_name

echo
echo "  @PRODUCT_DISPLAY_NAME@ installed successfully."
echo
