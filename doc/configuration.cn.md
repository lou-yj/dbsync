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
```

其中

- `sys`为系统配置
- `db` 为数据库配置, 数组格式, 支持配置多个
- `sync` 为同步配置, 数组格式, 支持配置多个




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

