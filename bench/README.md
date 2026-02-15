LogsGo was tested against ingesting 1 Million logs sample, here is the results of that:

```
benchtest_logsgo  | 
benchtest_logsgo  | 🎯 LOAD TEST RESULTS
benchtest_logsgo  | ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
benchtest_logsgo  | 📈 Performance Metrics:
benchtest_logsgo  |    • Total Logs Sent: 1,000,000
benchtest_logsgo  |    • Total Errors: 0
benchtest_logsgo  |    • Success Rate: 100.00%
benchtest_logsgo  |    • Total Time: 14.318s
benchtest_logsgo  |    • Logs per Second: 69838.64
benchtest_logsgo  |    • Batches Success: 500
benchtest_logsgo  |    • Batches Failed: 0
benchtest_logsgo  |    • Avg Batch Latency: 562ms
benchtest_logsgo  |    • Min Batch Latency: 49ms
benchtest_logsgo  |    • Max Batch Latency: 963ms
benchtest_logsgo  | ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
benchtest_logsgo  | ✅ SUCCESS: All logs processed successfully!
```

If ~15s for 1M log ingestion is still costly, the opt-in support for message queue can be used, which does it in blink of the eye. Of course now all load will be on logsGo server ingestion workers to do the job, but the main part is decoupling here. The result of the same: 

```
benchtest_logsgo  | 
benchtest_logsgo  | 🎯 LOAD TEST RESULTS
benchtest_logsgo  | ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
benchtest_logsgo  | 📈 Performance Metrics:
benchtest_logsgo  |    • Total Logs Sent: 1,000,000
benchtest_logsgo  |    • Total Errors: 0
benchtest_logsgo  |    • Success Rate: 100.00%
benchtest_logsgo  |    • Total Time: 492ms
benchtest_logsgo  |    • Logs per Second: 2031233.13
benchtest_logsgo  |    • Batches Failed: 0
benchtest_logsgo  |    • Avg Batch Latency: 10ms
benchtest_logsgo  |    • Min Batch Latency: 1ms
benchtest_logsgo  |    • Max Batch Latency: 47ms
benchtest_logsgo  | ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
benchtest_logsgo  | ✅ SUCCESS: All logs processed successfully!
```
