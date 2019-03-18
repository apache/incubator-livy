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
usage: release-build.sh

Creates build deliverables from a Livy commit:
  - The source archive, staged in the svn dev repo.
  - The binary archive, staged in the svn dev repo.
  - Maven artifacts, staged in the ASF's Nexus repo.

The following environment variables are used as input:

GIT_REF - Release tag or commit to build from
RELEASE_RC - Release RC identifier used when Publishing

ASF_USERNAME - Username of ASF committer account
ASF_PASSWORD - Password of ASF committer account

GPG_PASSPHRASE - Passphrase for GPG key
EOF
  exit 1
}

set -e

if [[ $@ == *"help"* ]]; then
  exit_with_usage
fi

# Explicitly set locale in order to make `sort` output consistent across machines.
# See https://stackoverflow.com/questions/28881 for more details.
export LC_ALL=C

# Setup env

# Commit ref to checkout when building
if [ -z "$GIT_REF" ]; then
  read -p "Choose git branch/tag [master]: " GIT_REF
  GIT_REF=${GIT_REF:-master}
  echo $GIT_REF
fi

# Set RELEASE_RC
if [ -z "$RELEASE_RC" ]; then
  read -p "Choose RC [rc1]: " RELEASE_RC
  RELEASE_RC=${RELEASE_RC:-rc1}
  echo $RELEASE_RC
fi

# Get ASF Login
if [ -z "$ASF_USERNAME" ]; then
  read -p "ASF username: " ASF_USERNAME
  echo $ASF_USERNAME
fi

if [ -z "$ASF_PASSWORD" ]; then
  read -s -p "ASF password: " ASF_PASSWORD
  echo
fi

# Destination directory on remote server
RELEASE_STAGING_LOCATION="https://dist.apache.org/repos/dist/dev/incubator/livy"

LIVY_REPO=${LIVY_REPO:-https://gitbox.apache.org/repos/asf/incubator-livy.git}
DO_STAGING=${DO_STAGING:-1}
GPG="gpg --no-tty --batch"
NEXUS_ROOT=https://repository.apache.org/service/local/staging
NEXUS_PROFILE=91529f2f65d84e # Profile for Livy staging uploads
BASE_DIR=$(pwd)

MVN="mvn"

# Use temp staging dir for release process
rm -rf release-staging
mkdir release-staging
cd release-staging

git clone $LIVY_REPO incubator-livy
cd incubator-livy
git checkout $GIT_REF
git_hash=`git rev-parse --short HEAD`
echo "Checked out Livy git hash $git_hash"

LIVY_VERSION=$($MVN help:evaluate -Dexpression=project.version \
  | grep -v INFO | grep -v WARNING | grep -v Download)

git clean -d -f -x
rm .gitignore
rm -rf .git
cd ..

ARCHIVE_NAME_PREFIX="apache-livy-$LIVY_VERSION"
SRC_ARCHIVE="$ARCHIVE_NAME_PREFIX-src.zip"
BIN_ARCHIVE="$ARCHIVE_NAME_PREFIX-bin.zip"

create_source_archive() {(
  echo "======================================="
  echo "Creating and signing the source archive"
  cp -r incubator-livy $ARCHIVE_NAME_PREFIX
  zip -r $SRC_ARCHIVE $ARCHIVE_NAME_PREFIX
  echo "" | $GPG --passphrase-fd 0 --armour --output $SRC_ARCHIVE.asc --detach-sig $SRC_ARCHIVE
  echo "" | $GPG --passphrase-fd 0 --print-md SHA512 $SRC_ARCHIVE > $SRC_ARCHIVE.sha512
  rm -rf $ARCHIVE_NAME_PREFIX
)}

create_binary_archive() {(
  echo "================================================"
  echo "Creating and signing regular binary distribution"
  cp -r incubator-livy $ARCHIVE_NAME_PREFIX-bin
  cd $ARCHIVE_NAME_PREFIX-bin
  $MVN -Pthriftserver -Dmaven.repo.local=$tmp_repo clean install -DskipTests -DskipITs
  # generate-third-party requires a previous install to work, so the above command is required.
  $MVN -Pthriftserver -Pgenerate-third-party -Dmaven.repo.local=$tmp_repo clean package -DskipTests
  cp assembly/target/$BIN_ARCHIVE ..
  cd ..
  echo "" | $GPG --passphrase-fd 0 --armour --output $BIN_ARCHIVE.asc --detach-sig $BIN_ARCHIVE
  echo "" | $GPG --passphrase-fd 0 --print-md SHA512 $BIN_ARCHIVE > $BIN_ARCHIVE.sha512
  rm -rf $ARCHIVE_NAME_PREFIX-bin
)}

create_maven_staging() {(
  echo "================================="
  echo "Creating local maven staging repo"
  cd $tmp_repo/org/apache/livy

  # Remove any extra files generated during install
  find . -type f |grep -v \.jar |grep -v \.pom | xargs rm

  # Nexus artifacts must have .asc, .md5 and .sha1 - and it really doesn't like anything else.
  echo "Creating hash and signature files"
  for file in $(find . -type f)
  do
    echo "" | $GPG --passphrase-fd 0 --output $file.asc \
      --detach-sig --armour $file;
    if [ $(command -v md5) ]; then
      # Available on OS X; -q to keep only hash
      md5 -q $file > $file.md5
    else
      # Available on Linux; cut to keep only hash
      md5sum $file | cut -f1 -d' ' > $file.md5
    fi
    sha1sum $file | cut -f1 -d' ' > $file.sha1
  done
)}

stage_archives() {(
  echo "======================================================="
  echo "Uploading release archives to $RELEASE_STAGING_LOCATION"
  svn co --depth=empty $RELEASE_STAGING_LOCATION svn-livy
  mkdir -p svn-livy/$LIVY_VERSION-$RELEASE_RC

  echo "Copying release tarballs to local svn directory"
  cp $SRC_ARCHIVE* svn-livy/$LIVY_VERSION-$RELEASE_RC/
  cp $BIN_ARCHIVE* svn-livy/$LIVY_VERSION-$RELEASE_RC/

  cd svn-livy
  svn add $LIVY_VERSION-$RELEASE_RC
  svn ci -m "Apache Livy $LIVY_VERSION-$RELEASE_RC"
)}

stage_artifacts() {(
  echo "=================================="
  echo "Uploading Maven artifacts to Nexus"
  cd $tmp_repo

  repo_request="<promoteRequest><data><description>Apache Livy $LIVY_VERSION (commit $git_hash)</description></data></promoteRequest>"
  out=$(curl -X POST -d "$repo_request" -u $ASF_USERNAME:$ASF_PASSWORD \
    -H "Content-Type:application/xml" -v \
    $NEXUS_ROOT/profiles/$NEXUS_PROFILE/start)
  staged_repo_id=$(echo $out | sed -e "s/.*\(orgapachelivy-[0-9]\{4\}\).*/\1/")

  nexus_upload=$NEXUS_ROOT/deployByRepositoryId/$staged_repo_id
  echo "Uploading files to $nexus_upload"
  for file in $(find org/apache/livy -type f)
  do
    # strip leading ./
    file_short=$(echo $file | sed -e "s/\.\///")
    dest_url="$nexus_upload/$file_short"
    echo "Uploading $file_short"
    curl -u $ASF_USERNAME:$ASF_PASSWORD --upload-file $file_short $dest_url
  done

  echo "Closing nexus staging repository"
  repo_request="<promoteRequest><data><stagedRepositoryId>$staged_repo_id</stagedRepositoryId><description>Apache Livy$LIVY_VERSION (commit $git_hash)</description></data></promoteRequest>"
  curl -X POST -d "$repo_request" -u $ASF_USERNAME:$ASF_PASSWORD \
    -H "Content-Type:application/xml" -v \
    $NEXUS_ROOT/profiles/$NEXUS_PROFILE/finish
  echo "Closed Nexus staging repository: $staged_repo_id"
)}

# Symlink as much as possible from ~/.m2/repository to avoid re-downloading from online
# repos. Basically symlink everything but org/apache/livy.
populate_tmp_repo() {(
  local_repo="$HOME/.m2/repository"
  if [ -d $local_repo ]; then
    mkdir -p $tmp_repo/org/apache
    for path in org org/apache org/apache/livy; do
      parent=$(dirname $path)
      name=$(basename $path)

      for e in $local_repo/$parent/*; do
        if [[ ! -d $e ]]; then
          continue
        fi
        e=$(basename $e)
        if [[ $e != $name ]]; then
          src=$(cd $local_repo/$parent/$e && pwd)
          ln -s $src $tmp_repo/$parent/$e
        fi
      done
    done

    if [ -d $local_repo/.cache ]; then
      ln -s $local_repo/.cache $tmp_repo/.cache
    fi
  fi
)}

tmp_dir=$(mktemp -d livy-repo-XXXXX)
tmp_repo=$(cd $tmp_dir && pwd)

populate_tmp_repo
create_source_archive
create_binary_archive
create_maven_staging

if [[ $LIVY_VERSION =~ .*-SNAPSHOT ]]; then
  echo "Refusing to stage a SNAPSHOT version."
  exit 1
fi

if [[ $DO_STAGING = 1 ]]; then
  # stage_archives
  stage_artifacts
fi

rm -rf $tmp_repo
