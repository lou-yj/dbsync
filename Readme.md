#总体架构

![title](dbsync.png)


数据同步使用`数据库Trigger`机制捕获源数据库数据变化, 通过通过`数据抽取器`对各个数据源进行数据抽取, 使用Hash对数据进行分片放入分区队列中. 数据同步线程从分区队列中拉取数据进行数据同步.

数据同步服务提供的功能:

- 多数据源支持: 支持从多个数据源同步到多个数据源, 支持级联同步, 二叉树同步.
- 多种数据库类型支持: 数据同步服务通过插件提供了对多种数据库的支持, 目前实现的有`postgresql`插件, 其他数据库支持可通过扩展插件实现
- 强顺序性保障: 数据库操作天然要求强顺序性, 乱序同步可能导致数据不一致问题. 因此同步操作严格按照数据顺序同步,即使遇到数据异常.
- 异常重试机制: 对异常数据进行重试, 重试时为了保证顺序性, 对影响到的数据进行阻塞操作, 阻塞粒度为`一亿亿分之一`,即在绝大多数情况下只会对同一条数据的多次操作才会导致异常阻塞.
- 支持自动批量同步, 对相同类型的操作强顺序性批量同步.
- 支持自动检测`Trigger`版本及重建, 支持自动唯一索引检测并重建
- 支持数据自动清理
- 支持自动创建及修复同步系统表
- 支持端点监控
- 支持数据分片及集群部署


##数据抽取及分区

每个源数据库对应一个或多个抽取线程,对数据表`sync_data`中的数据进行抽取. 数据按partition字段分区, 每个抽取线程负责分区的一段(分区段).
抽取完成后将抽取状态存入抽取状态表`sync_polled`中.

对于抽取到的数据, 按照`源数据库+源schema+源表+主键`计算HASH值, 并将HASH值取模`同步线程数`放入分区队列中.

##数据同步

每个数据同步线程负责分区队列中的一个分区和阻塞队列中的一个分区, 为保证数据的强顺序性, 同步线程每次抽取先从阻塞队列抽取数据再从分区队列抽取数据. 将抽取的数据按照同步策略进行同步, 按照操作类型和目标数据库尽可能的组合成批量操作以保证同步效率.


##异常重试

当遇到同步异常时, 将异常数据放到重试队列进行后续重试操作, 同时使用异常数据的HASH值设置BLOCK标记,从分区队列中拉取数据是相同HASH值的数据将被阻塞并放置到阻塞队列(保证强顺序性),其他数据正常处理(保证高可用性).

当重试机制成功重试某一条数据时,将该数据的HASH标记从阻塞标记中移除, 并激活相同HASH的阻塞数据处理.

阻塞粒度为`一亿亿分之一`,即在绝大多数情况下只会对同一条数据的多次操作才会导致异常阻塞.


#系统表

##数据捕获表(sync_data)

`sync_data`表用于对源表的增删改操作的捕获, 表结构如下:

|列名|类型|说明|默认值|
|-|
|id|长整形|自增主键|
|sourceDb|字符型|源数据库名|
|targetDb|字符型|目标数据库名|
|schema|字符型|源捕获数据的schema名|
|table|字符型|捕获数据的表名|
|operation|字符型|操作类型,增删改|
|data|字符型|捕获到的数据|
|partition|数值型|数据分区,用于分布式同步|
|createTime|时间戳|数据插入时间|now|


##抽取状态表(sync_polled)

`sync_polled`用于表示被抽取器抽取的数据

|列名|类型|说明|默认值|
|-|
|dataId|长整形|数据id|
|createTime|时间戳|抽取时间|now|

##同步状态表(sync_data_status)

`sync_data_status`用来表示数据的同步状态

|列名|类型|说明|默认值|
|-|
|dataId|长整形|数据id|
|status|字符型|同步状态,`OK`代表成功,`ERR`代表异常,`BLK`代表阻塞|
|message|字符型|说明信息|
|retry|数值型|重试次数|0|
|createTime|时间戳|同步时间|now|


##触发器版本表(sync_trigger_version)

`sync_trigger_version`表中保存当前系统中触发器的版本

|列名|类型|说明|默认值|
|-|
|schema|字符型|schema名|
|table|字符型|表名|
|trigger|字符型|触发器名|
|function|字符型|函数名|
|version|字符型|触发器版本|
|createTime|时间戳|创建时间|now|



#项目配置


项目主配置定义为

```
case class AppConfig(
                      sys: SysConfig,
                      db: List[DatabaseConfig],
                      sync: List[SyncConfig]
                    )
```

其中

- `sys`为系统配置
- `db` 为数据库配置
- `sync` 为同步配置


##系统配置

系统配置定义为

```
case class SysConfig(var batch: Int = 10000,
                     var partition: Int = 10,
                     var maxPollWait: Long = 60000,
                     var cleanInterval: Long = 3600000,
                     var syncTriggerInterval: Long = 1800000,
                     var dataKeepHours: Int = 24,
                     var maxRetry: Int = Int.MaxValue,
                     var retryInterval: Int = 10000,
                     var pollBlockInterval: Long = 1000,
                     var workDirectory: String = ".",
                     var stateDirectory: String = "state",
                     var endpointPort: Int = 8080)
```

其中:

- batch 为抽取器每一批抽取的批次大小
- partition 分区队列的分区数, 对应同步线程的并发数
- maxPollWait 抽取器抽取最长等待时间,抽取器会根据数据量大小伸缩等待时间
- cleanInterval 数据清理周期, 单位毫秒
- dataKeepHours 数据保留时长, 超过时长的数据会被清理掉
- syncTriggerInterval `trigger`检查周期
- maxRetry 最大尝试次数
- retryInterval 异常重试时间间隔
- pollBlockInterval 同步线程抽取等待时长
- workDirectory 工作目录
- stateDirectory 状态目录名, 相对于`workDirectory`
- endpointPort 监听端口


##数据库配置

数据库配置定义为

```
case class DatabaseConfig(name: String, 
                          sysSchema: String, 
                          `type`: String,
                          driver: String, 
                          url: String,
                          user: String, 
                          password: String,
                          var maxPoolSize: Int = 15,
                          var createIndex: Boolean = true)
```

其中:

- name 为数据库名
- sysSchema 系统`schema`名
- type 数据库类型, 如`postgresql`
- driver 驱动类型
- url 数据库连接地址
- user 数据库用户名
- password 数据库密码
- maxPoolSize 数据库连接池最大大小
- createIndex 是否对同步目标表自动创建索引


##同步配置

同步配置定义如下

```
case class SyncConfig(sourceDb: String, 
                      targetDb: String,
                      sourceSchema: String, 
                      sourceTable: String,
                      sourceKeys: String,
                      var targetSchema: String, 
                      var targetTable: String,
                      var insertCondition: String, 
                      var updateCondition: String, 
                      var deleteCondition: String)
```


其中:

- sourceDb 源数据库名
- targetDb 目标数据库名,多个目标数据库名用逗号分隔
- sourceSchema 源schema名
- sourceTable 源table名
- sourceKeys 源主键列名, 多列用逗号分隔
- targetSchema 目标schema名
- targetTable 目标table名
- insertCondition 当操作为`insert`时执行同步过滤条件,满足条件的数据才会被同步
- updateCondition 当操作为`update`时执行同步过滤条件,满足条件的数据才会被同步
- deleteCondition 当操作为`delete`时执行同步过滤条件,满足条件的数据才会被同步


##配置文件示例

配置文件采用`yaml`格式, 示例如下

```
sys:
  batch: 10000
  partition: 20
  maxPollWait: 5000
  cleanInterval: 60000
  syncTriggerInterval: 60000
  retryInterval: 60000
  dataKeepHours: 24
  workDirectory: "d:/temp"


db:
  - name: pgdata
    type: postgresql
    driver: org.postgresql.Driver
    url: jdbc:postgresql://x.x.x.x:x/xx
    user: xxx
    password: xxx
    sysSchema: dbsync
    createIndex: true
  - name: srv
    type: postgresql
    driver: org.postgresql.Driver
    url: jdbc:postgresql://x.x.x.x:x/xx
    user: xxx
    password: xxx
    sysSchema: dbsync
    createIndex: true

sync:
  - sourceDb: pgdata
    targetDb: srv
    sourceSchema: dbsync
    sourceTable: test01
    sourceKeys: source,part_name,sub_chain
```

#服务部署

项目打包

    maven install

启动脚本

```
java -cp dbsync-1.0-jar-with-dependencies.jar com.louyj.tools.dbsync.DbSyncLanucher app.yaml
```

