curl -X PUT -d '{"dest":"10.0.1.0","len":24, "name":"Route1", "priority":1}' localhost:7767/apps/lpm/10.0.1.0


curl -X PUT -d '{"dest":"10.0.1.107","len":32, "name":"Route2", "priority":3}' localhost:7767/apps/lpm/10.0.1.107


curl -X PUT -d '{"dest":"10.0.0.0","len":8, "name":"Route3", "priority":2}' localhost:7767/apps/lpm/10.0.0.0

curl -X PUT -d '{"dest":"10.4.0.10","len":16, "name":"Route4", "priority":2}' localhost:7767/apps/lpm/10.4.0.10

