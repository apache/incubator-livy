#!/bin/bash
CONFIGFILE_TUPLES="/opt/configfiles/livy.conf ${LIVY_HOME}/conf/livy.conf
/opt/configfiles/livy-client.conf ${LIVY_HOME}/conf/livy-client.conf
/opt/configfiles/spark-blacklist.conf ${LIVY_HOME}/conf/spark-blacklist.conf
/opt/configfiles/livy-env.sh ${LIVY_HOME}/conf/livy-env.sh
/opt/configfiles/spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf
/opt/configfiles/spark-env.sh ${SPARK_HOME}/conf/spark-env.sh"

echo "$CONFIGFILE_TUPLES" | while read src dst; do
    if [ -e "$src" ]; then
        cat "$src" >> "$dst"
    fi
done

#
# Fill in configuration data from environment variables
#
env_to_conf() {
    prefix=$1
    file=$2
    sep=$3

    for prop in $(env | grep "^${prefix}_"); do
        env=$(echo "$prop" | sed 's/=.*//')
        val=$(echo "$prop" | sed "s/${env}=//" )
        key=$(echo "$env" | sed -e "s/^${prefix}_//" -e 's/__/-/g' -e 's/_/./g')
        echo "$key" $sep "$val" >> "$file"
    done
}

env_to_conf LIVY_CONF "${LIVY_HOME}/conf/livy.conf" '='
env_to_conf LIVY_CLIENT_CONF "${LIVY_HOME}/conf/livy-client.conf" '='
env_to_conf SPARK_CONF "${SPARK_HOME}/conf/spark-defaults.conf"

#
# Start Livy Server
#
exec "${LIVY_HOME}/bin/livy-server"