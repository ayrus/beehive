

LPM
-------------

 - Run lpm.go 
 - Invoke populate.sh to populate some routing table entries
 - LPM lookups can now be done, for instance:

```bash
curl -X GET localhost:7767/apps/lpm/10.0.1.106
```

with a reply (which is the LPM for this request):
```JSON
{
  "request": "10.0.1.106",
  "route": {
    "dest": "10.0.1.0",
    "mask": "255.255.255.0",
    "gateway": "10.0.1.100",
    "iface": "eth1"
  }
}
```
