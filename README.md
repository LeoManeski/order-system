<b>Run</b>
```
docker run -d --name redis-stack -p 6379:6379 redis/redis-stack-server

docker run -d --name nats-js -p 4222:4222 -p 8222:8222 nats -js

go run ./validator

go run ./payment

go run ./shipping

go run ./orchestrator
```

<b>Test</b>
```
echo '{"id":"001","item":"Pizza","amount":25}' | nats pub orders.create
```
