# MIT 6.830 Simple DB总结
## Lab1
### Exercise 1
这一部分主要涉及到元组的定义，一共有两个类
- TupleDesc.class 这个类是定义了每个属性（字段）的类型和名字的信息
- Tuple.class 这个类根据上面类的定义定义元组，并设置相关的值

也就是说，一个元组由多个属性构成，比如：

(name string,age int, area string)

TupleDesc类定义了一个元组中所有属性的基本信息，Tuple类根据TupleDesc类的定义中
定义的信息为元组中的每一个元素赋值

TupleDesc类中的信息对一个表来说也是至关重要的

⚠️注意：在重写equlas方法后也要重写hashCode方法，因为如果不重写
hashCode方法有可能导致两个相同的Object得到不同的hashCode，这样
在使用Map的时候有可能导致key重复的情况

### Exercise 2

这一部分涉及到数据库中所有表的元数据的存储
- Catalog.class这个类在内存中保存了数据库当前所有可用表的元数据，
包括表文件以及表中属性的定义(TupleDesc.class类)。
  
这一部分通过HashMap分别实现根据表名和表id查询数据表中的相关信息

### Exercise 3

这一部分实现BufferPool中的getPage()方法

在SimpleDB中BufferPool相当于Cache，数据库中的信息全部持久化
存储在磁盘(Disk)中，数据库在操作时将一部分所需的数据页保存在缓存
(cache/buffer pool)中，在读取时，如果缓存命中，则直接从缓存中
取出所需的数据页进行读写，同时将读写后的数据也放回到缓存中，再从
缓存持久化到磁盘中。

getPage()方法的逻辑是，可能有多个不同的事务并发的访问getPage()方法
因此为了避免不同的事务竞争需要加锁，而对于同一事务的话则无需加锁。

此外对于不同page的操作也需要加锁，因为如果不加锁可能导致不同的操作同时
修改同一个page造成错误。对于page的操作有读和写两种，需要根据不同的访问
权限加读锁或写锁，此外，每个page都需要一个锁，因此使用map结构来存储每个
page的锁。

### Exercise 4

一个headFile由多个page组成，一个page由多个slot组成，一个slot存储一个tuple

此外每个page都有一个header，该header由一个bitmap组成，每个slot一个bit

如果某个元组对应的位为1，则表示该元组有效；如果为 0，则元组无效（例如，已被删除或从未初始化。）


