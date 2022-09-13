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

每个page的组织形式是 header(bitmap) + slots

每个page中slots的数量的计算公式是：

_tuples per page_ = floor((_page size_ * 8) / (_tuple size_ * 8 + 1))

_page size_ 表示页面的大小

_tuple size_ 表示一个元组的大小

得到slots后我们可以计算header中bitmap的大小

headerBytes = ceiling(tupsPerPage/8)

这里的tupsPerPage为上面计算得到的_tuples per page_

⚠️注意：由于Java采用的是大端的方式进行存储，对于每一个byte，低位地址存放的是
一个字节的最高位，因此对于header中的bitmap来说，第一个byte的最高位表示第7个slot是否
为空，第一个byte的最低位表示第0个slot是否为空

我们在判断某一位是否为1时可以通过移位运算和位与运算实现，比如我们想要判断某个byte的第i位（从小到大）
是否为1，我们可以通过以下代码实现：
```java

    if((byte)((b >> p) & 0x1)==1){
        ...
    }

```

### Exercise 5

该部分实现HeapFile类，一个HeapFile存储一个数据库表中的所有数据。

在SimpleDB中，一个HeapFile由一组HeapPage组成。同时一个HeapFile关联
一个在磁盘中存储的文件以及一个TupleDesc对象。

在该部分我们要实现根据tableId和pageId即HeapPageId来实现随机访问一个HeapPage，
这是通过FileInputStream实现的

同时我们还要自定一个一个HeapFileIterator类，并让它实现DbFileIterator。该迭代器使用
BufferPool中的getPage方法从磁盘中读取page，并将page导入到bufferPool中，同时返回一个
HeapPage中所有的Tuple

## Lab2

### Exercise 1

这一部分实现数据库查询中的JOIN和Filter两个操作

在实现这两个操作之前首先要实现这两个操作的判别器(Predicate和JoinPredicate)

所谓判别器就是判断两个元组中的某个属性（Integer或String）的大于、小于、等于等关系

在Predicate中传入要判别第几个属性、判别的类型(Op枚举类)、以及要对比的右边的值

比如对于(name,age)这个元组，我们要实现 
 
```sql
    SELECT * FROM USER WHERE age > 18
```

那么我们需要
```java
Predicate p = new Predicate(1,GREATER,50(Field))
```
之后再调用Predicate.filter(Field field)方法，传入一个属性
与之比较即可

而在JoinPredicate中是对两个元组中的属性进行比较，因此我们直接传入两个不同的属性以及判别的类型即可。

完成判别器之后我们可以使用判别器来实现Filter和Join操作

Filter使用在SELECT ...  WHERE 中，主要是对不符合条件的元组进行过滤

在Filter中我们根据WHERE子句的条件构造一个判别器，然后传入一个元组的迭代器
然后迭代访问每一个元组，通过判别器进行判断，如果符合要求则返回。
Filter也是一个迭代器。

而对JOIN来说，它使用在 SELECT ... FROM ... JOIN ON... WHERE ...
需要将不同的表根据条件连接起来。

⚠️注意：使用JOIN返回一个新的类型的Tuple，它的TupleDesc包含两个表中的内容

我们可以通过迭代访问两个表中的元组实现JOIN操作，时间复杂度O(n^2)

### Exercise 2

这一部分是实现 GROUP BY 和 一些常见的聚合(Aggregator)函数，比如
SUM、MAX、MIN、COUNT等。

其思路是按一个field分组，并对另一个field执行相关的聚合操作。
显然我们可以使用Map这一数据结构来实现，用分组的field作为key，用
执行聚合的field作为value。

在Aggregate类中，我们传入一个child迭代器，这个迭代器中包含了所有
我们需要聚类以及聚合的元组，然后我们根据聚类的field的类型new一个相对应
的类型的aggreator进行聚合，然后得到一个结果的迭代器，用这个迭代器返回
聚合得到的结果。
