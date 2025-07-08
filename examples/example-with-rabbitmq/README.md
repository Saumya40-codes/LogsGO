This setup is same as original one in examples/ root, only difference, is it setups up a rabbitmq service and configures logsGo accordingly.

some logs when running `docker compose up`

```

~/Code/apps/LogsGO/examples/example-with-rabbitmq$ docker compose up
[+] Running 4/4
 ✔ Container minio        Running                                                                                                                                                                                                         0.0s 
 ✔ Container rabbitmq     Running                                                                                                                                                                                                         0.0s 
 ✔ Container logsgo       Running                                                                                                                                                                                                         0.0s 
 ✔ Container example_app  Recreated                                                                                                                                                                                                       0.1s 
Attaching to example_app, logsgo, minio, rabbitmq
rabbitmq     | 2025-07-08 15:00:37.365110+00:00 [info] <0.981.0> accepting AMQP connection <0.981.0> (172.18.0.5:42666 -> 172.18.0.3:5672)
rabbitmq     | 2025-07-08 15:00:37.380253+00:00 [info] <0.981.0> connection <0.981.0> (172.18.0.5:42666 -> 172.18.0.3:5672): user 'guest' authenticated and granted access to vhost '/'
example_app  | 2025/07/08 15:00:37 Uploaded batch to queue: 8 logs
logsgo       | 2025/07/08 15:00:37 Inserted 8 logs from queue
example_app  | 2025/07/08 15:00:57 Uploaded batch to queue: 6 logs
logsgo       | 2025/07/08 15:00:57 Inserted 6 logs from queue
example_app  | 2025/07/08 15:01:17 Uploaded batch to queue: 6 logs
logsgo       | 2025/07/08 15:01:17 Inserted 6 logs from queue
rabbitmq     | 2025-07-08 15:01:37.391511+00:00 [warning] <0.981.0> closing AMQP connection <0.981.0> (172.18.0.5:42666 -> 172.18.0.3:5672, vhost: '/', user: 'guest'):
rabbitmq     | 2025-07-08 15:01:37.391511+00:00 [warning] <0.981.0> client unexpectedly closed TCP connection
example_app exited with code 0
```
