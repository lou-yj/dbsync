# 通用数据库实时同步工具

![title](doc/dbsync.png)

## 功能列表

- 多种数据库类型支持: 目前支持的数据库有`postgresql`和`mysql`, 其他数据库支持可通过扩展实现.
- 多数据源支持: 支持从多个数据源同步到多个数据源, 支持级联同步, 二叉树同步.
- 强顺序性保障: 数据库操作天然要求强顺序性, 乱序同步可能导致数据不一致问题. 因此同步操作严格按照数据顺序同步,即使遇到数据异常.
- 异常重试机制: 对异常数据进行重试, 重试时为了保证顺序性, 对影响到的数据进行阻塞操作, 阻塞粒度为`一亿亿分之一`,即在绝大多数情况下只会对同一条数据的多次操作才会导致异常阻塞.
- 支持自动批量同步, 对相同类型的操作强顺序性批量同步.
- 支持自动检测`Trigger`版本及重建, 支持自动唯一索引检测并重建
- 支持数据自动清理
- 支持自动创建及修复同步系统表
- 支持端点监控

## 相关文档

[快速入门](doc/quickstart.cn.md)

[配置说明](doc/configuration.cn.md)

[架构及实现原理](doc/architecture.cn.md)

[系统表定义](doc/systable.cn.md)




