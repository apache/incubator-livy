prepare:
	# python 2.7
	pip install setuptools --upgrade
	pip install flake8 flaky pytest requests-kerberos requests cloudpickle
	mvn clean install -B -V -e -Pspark-2.4.8 -Pthriftserver -DskipTests -DskipITs -Dmaven.javadoc.skip=true
	mvn clean package -B -V -e -Pspark-2.4.8 -Pthriftserver -DskipTests -DskipITs -Dmaven.javadoc.skip=true -Dgenerate-third-party
