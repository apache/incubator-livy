#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

function exit_with_usage {
  cat << EOF
usage: update-version.sh [-r <rc#>] [-s <version>]

Updates the Livy version on the current branch.

If "-r" is used, generates two commits, one to set the version for the RC,
one for set the version to the next dev version. A tag is also created
pointing to the specific commit for the RC.

If "-s" is used, the version is set to the given version string. A single
commit is generated.

The generated commits and tags need to be pushed to the upstream repository
manually.
EOF
  exit 1
}

if [ ! -f LICENSE ]; then
  echo "This script must be run in the repository's root directory."
  exit 1
fi

OP=$1
VER=$2

set -e

set_version() {
  local VER="$1"
  local MSG="$2"
  local CURRENT=$(mvn -B help:evaluate -Dexpression=project.version | grep -v '^\[')

  mvn -q -B versions:set -Pthriftserver -DgenerateBackupPoms=false "-DnewVersion=$VER" 1>/dev/null

  # Some other files where the version is hardcoded.
  local NON_POMS=(
    docs/_data/project.yml
    docs/programmatic-api.md
    python-api/setup.py
  )

  for f in ${NON_POMS[@]}; do
    local TMP="$f.__tmp__"
    sed -e "s/$CURRENT/$VER/" "$f" > "$TMP"
    mv "$TMP" "$f"
  done

  GIT_EDITOR=true git commit -a -m "[BUILD] $MSG"
}

set_rc_version() {
  local RC="$1"
  local CURRENT
  local RC_VER
  local DEV_VER
  local TAG
  local SHORT_VERSION

  if [ -z "$RC" ] || [[ $RC = rc0 ]] || [[ ! $RC =~ rc[0-9]+ ]]; then
    echo "Empty or invalid RC: $RC" 1>&2
    exit_with_usage
  fi

  CURRENT=$(mvn -B help:evaluate -Dexpression=project.version | grep -v '^\[')
  SHORT_VERSION=${CURRENT/-SNAPSHOT/}
  SHORT_VERSION=${SHORT_VERSION/-incubating/}
  MINOR_VERSION=$(echo $SHORT_VERSION | cut -d. -f 1-2)
  REL_VERSION=$(echo $SHORT_VERSION | cut -d. -f 3)

  if [ "$RC" = "rc1" ]; then
    RC_VER="$MINOR_VERSION.$REL_VERSION"
    DEV_VER="$MINOR_VERSION.$((REL_VERSION + 1))"
  else
    RC_VER="$MINOR_VERSION.$((REL_VERSION -1 ))"
    DEV_VER="$MINOR_VERSION.$REL_VERSION"
  fi

  RC_VER="$RC_VER-incubating"
  DEV_VER="$DEV_VER-incubating-SNAPSHOT"
  TAG="v$RC_VER-$RC"

  echo "RC: $RC_VER, DEV: $DEV_VER, TAG: $TAG"

  set -x
  set_version "$RC_VER" "Update version for $RC_VER $RC."
  git tag -f "$TAG"
  set_version "$DEV_VER" "Update version for development version $DEV_VER."
}

case $OP in
  -r)
    set_rc_version "$VER"
    ;;
  -s)
    set_version "$VER" "Bump Livy version to $VER."
    ;;
esac
