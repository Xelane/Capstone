# How to run
```
go run cmd/server/main.go --id node1 --config config/cluster.yaml
go run cmd/server/main.go --id node2 --config config/cluster.yaml
go run cmd/server/main.go --id node3 --config config/cluster.yaml
```

# Add the following Firewall rules
```
PowerShell (admin): New-NetFirewallRule -DisplayName "CapstoneRaft" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 9001,9002,9003
```