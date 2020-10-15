# 系统表

## 数据捕获表(sync_data)

`sync_data`表用于对源表的增删改操作的捕获, 表结构如下:

|列名|类型|说明|默认值|
| ---- | ----- | ----- | ----- |
|id|长整形|自增主键|
|sourceDb|字符型|源数据库名|
|targetDb|字符型|目标数据库名|
|schema|字符型|源捕获数据的schema名|
|table|字符型|捕获数据的表名|
|operation|字符型|操作类型,增删改|
|data|字符型|捕获到的数据|
|partition|数值型|数据分区,用于分布式同步|
|createTime|时间戳|数据插入时间|now|


## 抽取状态表(sync_polled)

`sync_polled`用于表示被抽取器抽取的数据

|列名|类型|说明|默认值|
| ---- | ----- | ----- | ----- |
|dataId|长整形|数据id|
|createTime|时间戳|抽取时间|now|

## 同步状态表(sync_data_status)

`sync_data_status`用来表示数据的同步状态

|列名|类型|说明|默认值|
| ---- | ----- | ----- | ----- |
|dataId|长整形|数据id|
|status|字符型|同步状态,`OK`代表成功,`ERR`代表异常,`BLK`代表阻塞|
|message|字符型|说明信息|
|retry|数值型|重试次数|0|
|createTime|时间戳|同步时间|now|


## 触发器版本表(sync_trigger_version)

`sync_trigger_version`表中保存当前系统中触发器的版本

| 列名 | 类型 | 说明 | 默认值 |
| ---- | ----- | ----- | ----- |
| schema| 字符型 | schema名 |
|table|字符型|表名|
|trigger|字符型|触发器名|
|function|字符型|函数名|
|version|字符型|触发器版本|
|createTime|时间戳|创建时间|now|
