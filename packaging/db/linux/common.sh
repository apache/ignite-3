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

source_user_group() {
  USERNAME="@USERNAME@"
  GROUPNAME="@GROUPNAME@"

  if [ ! "$(getent passwd ${USERNAME})" ]; then
    /usr/sbin/useradd "${USERNAME}"
  fi

  if [ ! "$(getent group ${GROUPNAME})" ]; then
    sudo groupadd "${GROUPNAME}"
    sudo usermod -a -G "${GROUPNAME}" "${USERNAME}"
  fi

  if [ -f "@CONF_DIR@/@VARS_FILE@" ]; then . "@CONF_DIR@/@VARS_FILE@"; fi
}

pre_uninstall() {
  if command -v initctl >/dev/null && initctl version | grep upstart >/dev/null; then
    if test -f '@PID_DIR@/@PRODUCT_NAME@.pid'; then
      initctl stop '@PRODUCT_NAME@' >/dev/null 2>&1
    fi
  elif command -v systemctl >/dev/null && systemctl --version | grep systemd >/dev/null; then
    if (systemctl is-active --quiet '@PRODUCT_NAME@'); then
      systemctl stop '@PRODUCT_NAME@' >/dev/null 2>&1
    fi
    if (systemctl is-enabled --quiet '@PRODUCT_NAME@'); then
      systemctl disable '@PRODUCT_NAME@'
    fi
  else
    pid=$(pgrep -f '@PRODUCT_NAME@')
    [ -n "$pid" ] && kill "$pid"
    sleep 5
    pid=$(pgrep -f '@PRODUCT_NAME@')
    [ -n "$pid" ] && kill -9 "$pid"
  fi
}

post_uninstall_remove() {
  if command -v initctl >/dev/null && initctl version | grep upstart >/dev/null; then
    /bin/rm -f '/etc/init/@PRODUCT_NAME@.conf' >/dev/null 2>&1
    /bin/rm -rf '@PID_DIR@' >/dev/null 2>&1
    initctl reload-configuration
    echo "  @PRODUCT_DISPLAY_NAME@ uninstalled successfully."
  elif command -v systemctl >/dev/null && systemctl --version | grep systemd >/dev/null; then
    /bin/rm -f '/usr/lib/systemd/system/@PRODUCT_NAME@.service' >/dev/null 2>&1
    systemctl daemon-reload
    echo "  @PRODUCT_DISPLAY_NAME@ uninstalled successfully."
  fi
}

post_uninstall_purge() {
  post_uninstall_remove
  /bin/rm -rf '@LOG_DIR@' >/dev/null 2>&1
}

post_uninstall_upgrade() {
  if command -v initctl >/dev/null && initctl version | grep upstart >/dev/null; then
    initctl restart '@PRODUCT_NAME@'
    echo "  @PRODUCT_DISPLAY_NAME@ upgraded successfully."
  elif command -v systemctl >/dev/null && systemctl --version | grep systemd >/dev/null; then
    systemctl restart '@PRODUCT_NAME@'
    echo "  @PRODUCT_DISPLAY_NAME@ upgraded successfully."
  fi
}
