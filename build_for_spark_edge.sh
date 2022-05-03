mvn clean package -B -V -e -Pspark-3.0 -Pthriftserver -DskipTests -DskipITs -Dmaven.javadoc.skip=true
echo "upload zip to s3://prophecy-dependencies"
ls assembly/target | grep zip