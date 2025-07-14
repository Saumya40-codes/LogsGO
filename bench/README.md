LogsGo was tested against ingesting 1 Million logs sample, here is the results of that:

```
benchtest_app  | 
benchtest_app  | 🎯 LOAD TEST RESULTS
benchtest_app  | ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
benchtest_app  | 📈 Performance Metrics:
benchtest_app  |    • Total Logs Sent: 1,000,000
benchtest_app  |    • Total Errors: 0
benchtest_app  |    • Success Rate: 100.00%
benchtest_app  |    • Total Time: 2m47.749s
benchtest_app  |    • Logs per Second: 5961.26
benchtest_app  |    • Batches Success: 500
benchtest_app  |    • Batches Failed: 0
benchtest_app  |    • Avg Batch Latency: 6.417s
benchtest_app  |    • Min Batch Latency: 97ms
benchtest_app  |    • Max Batch Latency: 14.988s
benchtest_app  | ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
benchtest_app  | ✅ SUCCESS: All logs processed successfully!
```

As you can see max batch latency is bit high, this is due to the default configuration involving locks, flushing, etc. which no doubt can be improved. Not to mention, the flush interval from memory store -> local store was kept very low in this case (i.e. 4m ) and it was flushed in two turns

```
logsgo         | 2025/07/14 05:25:07 Flushed 999447 logs to disk.
logsgo         | 2025/07/14 05:29:09 Flushed 553 logs to disk.
```

If ~2m 48s for 1M log ingestion is still costly, the opt-in support for message queue can be used, which does it in blink of the eye. Of course now all load will be on logsGo server ingestion workers to do the job, but the main part is decoupling here. The result of the same: 

```
benchtest_logsgo  | 
benchtest_logsgo  | 
benchtest_logsgo  | 🎯 LOAD TEST RESULTS
benchtest_logsgo  | ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
benchtest_logsgo  | 📈 Performance Metrics:
benchtest_logsgo  |    • Total Logs Sent: 1,000,000
benchtest_logsgo  |    • Total Errors: 0
benchtest_logsgo  |    • Success Rate: 100.00%
benchtest_logsgo  |    • Total Time: 1.309s
benchtest_logsgo  |    • Logs per Second: 763694.55
benchtest_logsgo  |    • Batches Success: 500
benchtest_logsgo  |    • Batches Failed: 0
benchtest_logsgo  |    • Avg Batch Latency: 34ms
benchtest_logsgo  |    • Min Batch Latency: 0s
benchtest_logsgo  |    • Max Batch Latency: 253ms
benchtest_logsgo  | ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
benchtest_logsgo  | ✅ SUCCESS: All logs processed successfully!
```
