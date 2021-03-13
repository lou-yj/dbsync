# 配置说明

项目配置采用`yaml`配置文件格式, 完整配置示例如下:

```
sys:
  batch: 10000
  partition: 20
  maxPollWait: 5000
  cleanInterval: 60000
  syncTriggerInterval: 60000
  retryInterval: 60000
  dataKeepHours: 24
  workDirectory: "."
  stateDirectory: "status"
  endpointPort: 8080

db:
  - name: db1
    type: postgresql
    driver: org.postgresql.Driver
    url: jdbc:postgresql://127.0.0.1:5432/test
    user: admin
    password: password
    sysSchema: dbsync
    createIndex: true
  - name: db2
    type: mysql
    driver: com.mysql.cj.jdbc.Driver
    url: jdbc:mysql://127.0.0.1:3306/test
    user: admin
    password: password
    sysSchema: dbsync
    createIndex: true

sync:
  - sourceDb: db2
    targetDb: db1
    sourceSchema: test
    sourceTable: test01
    sourceKeys: f1,f2

monitor:
  - matches:
      heartbeatLostOver: 5
      syncBlockedOver: 100
      syncErrorOver: 10
      syncPendingOver: 1000
    action: webhook
    params:
      url: http://x.x.x.x:xxxx/path
```

其中

- `sys`为系统配置
- `db` 为数据库配置, 数组格式, 支持配置多个
- `sync` 为同步配置, 数组格式, 支持配置多个
- `monitor` 为监控配置

## 系统配置

系统配置参数说明:

- batch 为抽取器每一批抽取的批次大小,默认`10000`
- partition 分区队列的分区数, 对应同步线程的并发数, 默认`100`
- maxPollWait 抽取器抽取最长等待时间,抽取器会根据数据量大小伸缩等待时间, 单位`毫秒`, 默认`60000`
- cleanInterval 数据清理周期, 单位毫秒,默认`1小时`
- dataKeepHours 数据保留时长, 超过时长的数据会被清理掉,默认为`24小时`
- syncTriggerInterval `trigger`检查周期,单位`毫秒`,默认为`30分钟`
- maxRetry 最大尝试次数,默认为`Int.MaxValue`
- retryInterval 异常重试时间间隔
- pollBlockInterval 同步线程抽取等待时长
- workDirectory 工作目录,默认为当前目录
- stateDirectory 状态目录名, 相对于`workDirectory`,默认为`state`目录
- endpointPort 监听端口,默认`8080`

## 数据库配置

数据库配置参数说明:

- name 为数据库名, 全局唯一
- sysSchema 系统`schema`名
- type 数据库类型, 如`postgresql,mysql`
- driver 驱动类型
- url 数据库连接地址
- user 数据库用户名
- password 数据库密码
- maxPoolSize 数据库连接池最大大小, 默认`15`
- createIndex 是否对同步目标表自动创建索引, 默认`false`

## 同步配置

同步配置参数说明:

- sourceDb 源数据库名
- targetDb 目标数据库名, 可多选, 多个目标数据库名用逗号分隔
- sourceSchema 源schema名
- sourceTable 源table名
- sourceKeys 源主键列名, 多列用逗号分隔
- targetSchema 目标schema名
- targetTable 目标table名
- insertCondition 当操作为`insert`时执行同步过滤条件,满足条件的数据才会被同步
- updateCondition 当操作为`update`时执行同步过滤条件,满足条件的数据才会被同步
- deleteCondition 当操作为`delete`时执行同步过滤条件,满足条件的数据才会被同步

## 监控配置

系统提供`同步状态`和`工作线程状态`的监控. 详情参看`Endpoints端点`

监控配置可配置1-N个监控策略, 目前支持的动作有: `发送告警`和`自动重启`

监控配置参数说明:

- matches.heartbeatLostOver 当工作线程心跳丢失超过N次时执行
- matches.syncBlockedOver 当阻塞的数据量超过N个时执行
- matches.syncErrorOver 当同步失败的数据量超过N个时执行
- matches.syncPendingOver 当同步等待的数据量超过N个时执行
- action 动作类型, 目前支持`restart`和`webhook`方式. 可扩展
- params 动作参数. 针对每种动作类型有不同参数

### 动作`webhook`, 配置如下

```
params:
  url: http://x.x.x.x:xxxx/path
```

其中`url`为接收告警的地址

告警数据格式如下:

```

> POST /path HTTP/1.1
> Content-Type: application/json
> Content-Length: 1056

{
	"reason": "告警原因",
	"syncStatus": {
		"pending": 0,
		"blocked": 0,
		"error": 0,
		"success": 0,
		"others": 0
	},
	"components": {
		"blocked-handler": {
			"statistics": {},
			"total": 0,
			"heartbeatLost": 0,
			"heartbeatInterval": 120000,
			"lastHeartbeat": "2021-03-13 15:04:59",
			"status": "GREEN"
		},
		"cleanWorker": {
			"heartbeatLost": 206,
			"heartbeatInterval": 5,
			"lastHeartbeat": "2021-03-13 15:04:59",
			"status": "RED"
		}
	}
}
```

其中:

- reason 为原因
- syncStatus 为当前同步状态
- components 为当前工作线程状态