curl -X PUT -d '{"dest":"10.0.1.0","mask":"255.255.255.0","gateway":"10.0.1.100","iface":"eth1"}' localhost:7767/apps/lpm/10.0.1.0


curl -X PUT -d '{"dest":"10.0.1.107","mask":"255.255.255.255","gateway":"10.0.1.107","iface":"eth2"}' localhost:7767/apps/lpm/10.0.1.107


curl -X PUT -d '{"dest":"10.0.0.0","mask":"255.0.0.0","gateway":"10.0.1.245","iface":"eth3"}' localhost:7767/apps/lpm/10.0.0.0
