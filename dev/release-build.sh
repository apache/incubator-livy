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
usage: release-build.sh <package|publish-release>
Creates build deliverables from a Livy commit.

Top level targets are
  package: Create binary packages and copy them to home.apache
  publish-release: Publish a release to Apache release repo

All other inputs are environment variables

GIT_REF - Release tag or commit to build from
LIVY_VERSION - Release identifier used when Publishing
RELEASE_RC - Release RC identifier used when Publishing

ASF_USERNAME - Username of ASF committer account
ASF_PASSWORD - Password of ASF committer account

GPG_PASSPHRASE - Passphrase for GPG key
EOF
  exit 1
}

set -e

if [ $# -eq 0 ]; then
  exit_with_usage
fi

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

if [ -z "$LIVY_VERSION" ]; then
  LIVY_VERSION=$($MVN help:evaluate -Dexpression=project.version \
    | grep -v INFO | grep -v WARNING | grep -v Download)
fi

git clean -d -f -x
rm .gitignore
rm -rf .git
cd ..

ARCHIVE_NAME_PREFIX="apache-livy-$LIVY_VERSION"
SRC_ARCHIVE="$ARCHIVE_NAME_PREFIX-src.zip"
BIN_ARCHIVE="$ARCHIVE_NAME_PREFIX-bin.zip"

if [[ "$1" == "package" ]]; then
  # Source and binary tarballs
  echo "Packaging release tarballs"
  cp -r incubator-livy $ARCHIVE_NAME_PREFIX
  zip -r $SRC_ARCHIVE $ARCHIVE_NAME_PREFIX
  echo "" | $GPG --passphrase-fd 0 --armour --output $SRC_ARCHIVE.asc --detach-sig $SRC_ARCHIVE
  echo "" | $GPG --passphrase-fd 0 --print-md SHA512 $SRC_ARCHIVE > $SRC_ARCHIVE.sha512
  rm -rf $ARCHIVE_NAME_PREFIX

  # Updated for binary build
  make_binary_release() {
    cp -r incubator-livy $ARCHIVE_NAME_PREFIX-bin
    cd $ARCHIVE_NAME_PREFIX-bin

    $MVN clean package -DskipTests -Dgenerate-third-party

    echo "Copying and signing regular binary distribution"
    cp assembly/target/$BIN_ARCHIVE .
    echo "" | $GPG --passphrase-fd 0 --armour --output $BIN_ARCHIVE.asc --detach-sig $BIN_ARCHIVE
    echo "" | $GPG --passphrase-fd 0 --print-md SHA512 $BIN_ARCHIVE > $BIN_ARCHIVE.sha512

    cp $BIN_ARCHIVE* ../
    cd ..
  }

  make_binary_release

  svn co --depth=empty $RELEASE_STAGING_LOCATION svn-livy
  mkdir -p svn-livy/$LIVY_VERSION-$RELEASE_RC

  echo "Copying release tarballs to local svn directory"
  cp ./$SRC_ARCHIVE* svn-livy/$LIVY_VERSION-$RELEASE_RC/
  cp ./$BIN_ARCHIVE* svn-livy/$LIVY_VERSION-$RELEASE_RC/

  cd svn-livy
  svn add $LIVY_VERSION-$RELEASE_RC
  svn ci -m "Apache Livy $LIVY_VERSION-$RELEASE_RC"

  exit 0
fi

if [[ "$1" == "publish-release" ]]; then
  tmp_dir=$(mktemp -d livy-repo-XXXXX)
  # the following recreates `readlink -f "$tmp_dir"` since readlink -f is unsupported on MacOS
  cd $tmp_dir
  tmp_repo=$(pwd)
  cd ..

  cd incubator-livy
  # Publish Livy to Maven release repo
  echo "Publishing Livy checkout at '$GIT_REF' ($git_hash)"
  echo "Publish version is $LIVY_VERSION"
  # Coerce the requested version
  $MVN versions:set -DnewVersion=$LIVY_VERSION

  # Using Nexus API documented here:
  # https://support.sonatype.com/entries/39720203-Uploading-to-a-Staging-Repository-via-REST-API
  echo "Creating Nexus staging repository"
  repo_request="<promoteRequest><data><description>Apache Livy $LIVY_VERSION (commit $git_hash)</description></data></promoteRequest>"
  out=$(curl -X POST -d "$repo_request" -u $ASF_USERNAME:$ASF_PASSWORD \
    -H "Content-Type:application/xml" -v \
    $NEXUS_ROOT/profiles/$NEXUS_PROFILE/start)
  staged_repo_id=$(echo $out | sed -e "s/.*\(orgapachelivy-[0-9]\{4\}\).*/\1/")
  echo "Created Nexus staging repository: $staged_repo_id"

  $MVN -Dmaven.repo.local=$tmp_repo -DskipTests -DskipITs clean install

  pushd $tmp_repo/org/apache/livy

  # Remove any extra files generated during install
  find . -type f |grep -v \.jar |grep -v \.pom | xargs rm

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

  nexus_upload=$NEXUS_ROOT/deployByRepositoryId/$staged_repo_id
  echo "Uploading files to $nexus_upload"
  for file in $(find . -type f)
  do
    # strip leading ./
    file_short=$(echo $file | sed -e "s/\.\///")
    dest_url="$nexus_upload/org/apache/livy/$file_short"
    echo "  Uploading $file_short"
    curl -u $ASF_USERNAME:$ASF_PASSWORD --upload-file $file_short $dest_url
  done

  echo "Closing nexus staging repository"
  repo_request="<promoteRequest><data><stagedRepositoryId>$staged_repo_id</stagedRepositoryId><description>Apache Livy$LIVY_VERSION (commit $git_hash)</description></data></promoteRequest>"
  out=$(curl -X POST -d "$repo_request" -u $ASF_USERNAME:$ASF_PASSWORD \
    -H "Content-Type:application/xml" -v \
    $NEXUS_ROOT/profiles/$NEXUS_PROFILE/finish)
  echo "Closed Nexus staging repository: $staged_repo_id"
  popd
  rm -rf $tmp_repo
  cd ..
  exit 0
fi

cd ..
rm -rf release-staging
echo "ERROR: expects to be called with 'package', 'publish-release'"
