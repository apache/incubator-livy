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

for env in ASF_USERNAME ASF_PASSWORD GPG_PASSPHRASE RELEASE_RC; do
  if [ -z "${!env}" ]; then
    echo "ERROR: $env must be set to run this script"
    exit_with_usage
  fi
done

# Explicitly set locale in order to make `sort` output consistent across machines.
# See https://stackoverflow.com/questions/28881 for more details.
export LC_ALL=C

# Commit ref to checkout when building
GIT_REF=${GIT_REF:-master}

# Destination directory on remote server
RELEASE_STAGING_LOCATION="https://dist.apache.org/repos/dist/dev/incubator/livy"

GPG="gpg --no-tty --batch"
NEXUS_ROOT=https://repository.apache.org/service/local/staging
NEXUS_PROFILE=91529f2f65d84e # Profile for Livy staging uploads
BASE_DIR=$(pwd)

MVN="mvn"

rm -rf incubator-livy
git clone https://git-wip-us.apache.org/repos/asf/incubator-livy.git
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

if [[ "$1" == "package" ]]; then
  # Source and binary tarballs
  echo "Packaging release tarballs"
  cp -r incubator-livy livy-$LIVY_VERSION-src
  zip -r livy-$LIVY_VERSION-src.zip livy-$LIVY_VERSION-src
  echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --armour --output livy-$LIVY_VERSION-src.zip.asc \
    --detach-sig livy-$LIVY_VERSION-src.zip
  echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --print-md MD5 livy-$LIVY_VERSION-src.zip > \
    livy-$LIVY_VERSION-src.zip.md5
  echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --print-md \
    SHA512 livy-$LIVY_VERSION-src.zip > livy-$LIVY_VERSION-src.zip.sha512
  rm -rf livy-$LIVY_VERSION-src

  # Updated for binary build
  make_binary_release() {
    cp -r incubator-livy livy-$LIVY_VERSION-bin

    cd livy-$LIVY_VERSION-bin

    $MVN clean install -DskipTests -DskipITs
    $MVN clean package -DskipTests -Dgenerate-third-party

    echo "Copying and signing regular binary distribution"
    cp assembly/target/livy-$LIVY_VERSION-bin.zip .
    echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --armour \
      --output livy-$LIVY_VERSION-bin.zip.asc \
      --detach-sig livy-$LIVY_VERSION-bin.zip
    echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --print-md \
      MD5 livy-$LIVY_VERSION-bin.zip > \
      livy-$LIVY_VERSION-bin.zip.md5
    echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --print-md \
      SHA512 livy-$LIVY_VERSION-bin.zip > \
      livy-$LIVY_VERSION-bin.zip.sha512

    cp livy-$LIVY_VERSION-bin.zip* ../
    cd ..
  }

  make_binary_release

  svn co $RELEASE_STAGING_LOCATION svn-livy
  mkdir -p svn-livy/$LIVY_VERSION-$RELEASE_RC

  echo "Copying release tarballs to local svn directory"
  cp ./livy-$LIVY_VERSION-src.zip* svn-livy/$LIVY_VERSION-$RELEASE_RC/
  cp ./livy-$LIVY_VERSION-bin.zip* svn-livy/$LIVY_VERSION-$RELEASE_RC/

  cd svn-livy
  svn add $LIVY_VERSION-$RELEASE_RC
  svn ci -m "Apache Livy $LIVY_VERSION-$RELEASE_RC"

  exit 0
fi

if [[ "$1" == "publish-release" ]]; then
  tmp_dir=$(mktemp -d livy-repo-XXXXX)
  tmp_repo=`readlink -f "$tmp_dir"`

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
    echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --output $file.asc \
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
rm -rf incubator-livy
echo "ERROR: expects to be called with 'package', 'publish-release'"
