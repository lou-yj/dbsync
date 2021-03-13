# Endpoints

## 查看系统状态

```
GET /status/sys HTTP/1.1
```

响应示例:

```
{
  "componentStatus": "GREEN",
  "syncStatus": {
    "name": "N/A",
    "pending": 0,
    "blocked": 0,
    "error": 0,
    "success": 0,
    "others": 0
  },
  "running": true,
  "restartReason": "Component cleanWorker heartbeat lost over 10",
  "uptime": "2021-03-13 15:08:24"
}
```

## 查看同步状态

```
GET /status/sync HTTP/1.1
```

响应示例:

```
[
  {
    "name": "db1",
    "pending": 0,
    "blocked": 0,
    "error": 0,
    "success": 0,
    "others": 0
  },
  {
    "name": "db2",
    "pending": 0,
    "blocked": 0,
    "error": 0,
    "success": 9,
    "others": 0
  }
]
```

## 查看工作线程状态

```
GET /status/component HTTP/1.1
```

响应示例:

```
{
  "blocked-handler": {
    "statistics": {
      
    },
    "total": 0,
    "heartbeatLost": 0,
    "heartbeatInterval": 120000,
    "lastHeartbeat": "2021-03-13 15:11:17",
    "status": "GREEN"
  },
  "cleanWorker": {
    "heartbeatLost": 422,
    "heartbeatInterval": 5,
    "lastHeartbeat": "2021-03-13 15:11:16",
    "status": "RED"
  },
  "poller-db1": {
    "statistics": {
      
    },
    "total": 0,
    "heartbeatLost": 0,
    "heartbeatInterval": 5000,
    "lastHeartbeat": "2021-03-13 15:11:15",
    "status": "GREEN"
  }
  ...
  ...
}
```

## 查看数据库连接池状态

```
GET /status/datasource HTTP/1.1
```

响应示例:

```
{
  "db1": {
    "pollingCount": 1,
    "maxActive": 5,
    "name": "db1",
    "url": "jdbc:postgresql://x.x.x.x:xxxx/db",
    "errorCount": 0,
    "activeCount": 0,
    "user": "user",
    "waitCount": 0
  },
  "db2": {
    "pollingCount": 1,
    "maxActive": 15,
    "name": "db2",
    "url": "jdbc:mysql://x.x.x.x:xxxx/db",
    "errorCount": 0,
    "activeCount": 0,
    "user": "user",
    "waitCount": 0
  }
}
```

## 查看配置

```
GET /config HTTP/1.1
```

## 重新加载配置

重新加载配置文件并立即生效

```
GET /config/reload HTTP/1.1
```

## 重启服务

```
GET /control/restart HTTP/1.1
```