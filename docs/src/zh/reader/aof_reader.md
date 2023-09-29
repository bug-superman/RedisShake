# aof_reader

## 介绍

可以使用 `aof_reader` 来从 AOF 文件中读取数据，然后写入目标端。常见于从备份文件中恢复数据，可以不读取时间戳之后的数据。

## 配置

```toml
[aof_reader]
aoffilepath="/tmp/appendonly.aof.manifest"
aoftimestamp="0"
```

* 应传入绝对路径。

主要流程如下：
- 首先检查manifest是否存在 来判断AOF是 multi-part AOF 还是单AOF
- 解析mainifest文件，对multi-part AOF文件进行逐个解析。 单aof直接进行单个文件加载。
- 如果AOF文件有RDB格式前缀 则按RDB文件格式处理 否则按AOF文件处理， 解析aof文件并且支持按时间戳恢复数据
[module-aof.ipg](/public/module-aof.jpg) 