rm -r ~/livy-server/*

cp assembly/target/apache-livy-0.8.0-incubating-SNAPSHOT-bin.zip ~/livy-server/
unzip ~/livy-server/apache-livy-0.8.0-incubating-SNAPSHOT-bin.zip -d ~/livy-server/
mkdir ~/livy-server/apache-livy-0.8.0-incubating-SNAPSHOT-bin/logs

kubectl cp ~/livy-server/apache-livy-0.8.0-incubating-SNAPSHOT-bin test/sparkhead-0:/ -c hadoop-livy-sparkhistory
kubectl cp ~/livy-server/apache-livy-0.8.0-incubating-SNAPSHOT-bin test/sparkhead-1:/ -c hadoop-livy-sparkhistory
