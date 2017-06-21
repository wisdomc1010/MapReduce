# MapReduce
# 不规则的日志文件，重新规范
# 原日志内容
  
2017-04-21 16:17:17
[INFO]-[Thread: Thread-1]-[com.unitech.lingpipe.util.Log.logInfo(Log.java:14)]: 插入结束

2017-04-21 16:17:17
[INFO]-[Thread: Thread-1]-[com.unitech.lingpipe.util.Log.logInfo(Log.java:14)]: flag3

2017-04-21 16:17:17
[INFO]-[Thread: Thread-1]-[com.unitech.lingpipe.util.Log.logInfo(Log.java:14)]: files.length2

2017-04-21 16:17:34
[INFO]-[Thread: Thread-0]-[com.unitech.lingpipe.util.Log.logInfo(Log.java:14)]: 相同标题查询开始

# 规范后日志内容
2017-04-21 16:17:17	[INFO]-[Thread: Thread-1]-[com.unitech.lingpipe.util.Log.logInfo(Log.java:14)]: 插入结束		1<br/>
2017-04-21 16:17:17	[INFO]-[Thread: Thread-1]-[com.unitech.lingpipe.util.Log.logInfo(Log.java:14)]: flag3		1<br/>
2017-04-21 16:17:17	[INFO]-[Thread: Thread-1]-[com.unitech.lingpipe.util.Log.logInfo(Log.java:14)]: files.length2		1<br/>
2017-04-21 16:17:34	[INFO]-[Thread: Thread-0]-[com.unitech.lingpipe.util.Log.logInfo(Log.java:14)]: 相同标题查询开始		1<br/>
2017-04-21 16:17:34	[INFO]-[Thread: Thread-0]-[com.unitech.lingpipe.util.Log.logInfo(Log.java:14)]: 相同标题查询结束		1
