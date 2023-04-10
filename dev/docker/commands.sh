curl --no-progress-meter -X POST -H "Content-Type: application/json" --data '{"kind": "spark"}' http://127.0.0.1:8998/sessions

curl --no-progress-meter -X POST -d '{ "kind": "spark", "code": "sc.parallelize(1 to 10).count()" }' -H "Content-Type: application/json" \
http://127.0.0.1:8998/sessions/1/statements

