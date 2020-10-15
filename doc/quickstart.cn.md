# 快速入门

## 下载release包

进入![release页面](https://github.com/lou-yj/dbsync/releases)下载最新的发布包。解压后目录结构如下：

```
.
├── bin
├── config
├── lib
├── logs
└── state
```

其中: 

- bin 脚本目录
- config 配置文件目录
- lib 依赖包目录
- logs 日志文件目录
- state 内部状态存储目录


## 编辑配置文件

`config`目录下`app-demo.yaml`为配置模板文件, 可以使用模板文件快速创建配置

```
cd config
cp app-demo.yaml app.yaml
```

配置文件中假设已有两个数据库`db1`和`db2`

- `db1`为`postgresql`数据库,地址为`127.0.0.1:5432`,数据库名为`test`,用户名为`admin`,密码为`password`
- `db2`为`mysql`数据库,地址为`127.0.0.1:3306`,数据库名为`test`,用户名为`admin`,密码为`password`

如数据库信息不一致需要修改配置文件中相关数据库信息

详细配置请参考![配置说明](configuration.cn.md)


## 创建同步表

连接数据库`db1`和`db2`, 分别创建要同步的表

```
create table test.test01 (
f1 varchar(128),
f2 varchar(128),
f3 varchar(128)
)
```

## 启动服务

```
cd dbsync-*
bin/start.sh
```

查看服务日志

```
tail -f logs/dbsync-*.log
```

## 测试数据同步

向`db1`中`test.test01`表中添加数据库, 观察`db2`中数据同步状态

