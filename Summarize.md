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


### Exercise 3

这一部分主要是实现对元组的修改和新增
对于元组的修改和新增在最底层是通过HeapPage中对元组进行
修改和更新实现的，然后在HeapFile中调用HeapPage中的新增和
修改的方法，同时要加一些逻辑判断，比如在HeapFile中新增元组的
时候，需要找到一个含有空白的slot的page，然后在对应的page中实现
对元组对修改。

而用户在应用层调用新增和修改的API时，是直接去调用BufferPool中的
新增和修改的方法，不是去直接修改文件中的内容，而是先修改cacahe中的
page，同时对修改后的page进行标记，将其标记为dirty。然后再将cacahe
中的page刷新到disk中去。

下面简单整理一下整个流程的逻辑：
- HeapPage：
  实现insertTuple和deleteTuple两个方法，在page中删除相应的元组
  ，具体的做法，insert时先根据header找一个空的slot，将其标记为使用，然后将tuple
  放到相应的位置。delete时删除相应位置的tuple，并将header中的slot标记为未使用。
- HeapFile：
  实现inertTuple和deleteTuple两个方法，insert时从BufferPool中按顺序读取page，判断
  page是否已满，如果未满则调用page的insertTuple方法。如果全部page都满了，则创建一个新的page
  注意需要加锁。delete时从BufferPool中读取tuple所在的page，然后调用page的deleteTuple方法
- BufferPool:
  insert和delete都时根据catalog找到对应的表的HeapFile，然后调用heapfile的insert或者delete
  方法，注意在bufferpool中要将修改后的page进行更新，同时记录事务和dirty page的映射关系，以便将
  对buffer pool的修改在之后刷回到disk中

### Exercise 4

这一部分是实现执行器中的Delete和Insert方法。在构造函数中会
传入一个child的Tuple迭代器，每次调用BufferPool中的insert和
delete方法将迭代器中的tuple进行插入或删除。完成之后返回一个Tuple，
这个Tuple只有一个Field，就是本次操作的记录的数量。

### Exercise 5

在之前的Lab1 中，BufferPool如果满了我们直接抛出一个异常。而在该部分
我们要实现page的置换。当BufferPool满之后选择一个page从缓存中删除，然后
将新的page放到缓存中。
我们使用LRU策略实现页面置换策略
需要使用一个Map和一个双向链表数据结构来实现该策略
在双向链表中的每个节点中保存一个page，同时map记录pageid和
节点的映射关系。
当在bufferpool中访问一个page后，首先根据pageid找到node
然后在双向链表中删除node，并将该node放到链表的首位
当在链表中新增一个节点且当前链表已满时，将链表最后（最久未使用）
的节点删除，同时如果该page被修改，将其刷新到disk中，然后将新增
的节点放到链表的首位
以上方法实现LRU策略的时间复杂度是O(1)

## Lab 3

这部分实验是实现一个数据库优化器，对查询、JOIN等操作的执行进行优化

为什么需要对查询进行优化？

SQL 是一种结构化查询语言，它只告诉数据库”想要什么”，但是它不会告诉
数据库”如何获取”这个结果，这个"如何获取"的过程是由数据库的“大脑”查询
优化器来决定的。在数据库系统中，一个查询通常会有很多种获取结果的方法，
每一种获取的方法被称为一个"执行计划"。给定一个 SQL，查询优化器首先会
枚举出等价的执行计划。

其次，查询优化器会根据统计信息和代价模型为每个执行计划计算一个“代价”，
这里的代价通常是指执行计划的执行时间或者执行计划在执行时对系统资源
(CPU + IO + NETWORK)的占用量。最后，查询优化器会在众多等价计划中
选择一个"代价最小"的执行计划。

### Exercise 1

这一部分是实现一个IntHistogram(直方图)通过它实现selectivity estimation
(选择性估计)。直方图根据数据的范围和桶的数量(min,max,buckets)来构造，每次
输入一个数，就找到这个数所在的桶，并将其中的计数值加一，在进行选择性估计时

根据以下公式来实现：

估计等于某一个值的选择性概率：(h / w) / ntups

其中h和w分别表示该值所在的桶的计数值(h,高)和该桶的范围(w,宽度)

ntups是所有桶中计数值的和

估计某个范围的值的选择性概率：

b_f = h_b / ntups

(b_right - const) / w_b  or (const - b_left) / w_b

Thus, bucket b contributes (b_f x b_part)

也就是某个范围内的概率之和，对一个范围内每个值的概率积分

在实现的时候，通过一个bucketRange数组记录每个桶的范围的右边界
然后通过一个groups数组来记录min-max范围内每个整数所在的bucket的编号
从而实现线性复杂度的记录。

⚠️注意：在进行线性复杂度的估计时，要考虑一些边界情况：

比如输入的数在min-max范围之内

比如输入的数在桶的边界上

这些边界情况都需要额外处理。

### Exercise 2

这一部分是实现表中相关信息的统计。

有一个Map，维护了所有的表的统计信息

对每一个表而言，它的统计信息有
- estimateScanCost： 扫描这个表的所有Page所消耗的IO的
时间，对于扫描每一个Page而言，这里假设每个Page的IO的时间是一个常数
因此实际上我们只需要拿到这个表有多少个page就可以了，其次我们还需要

- estimateTableCardinality：拿到这个表中所有元组的数量，然后乘
一个因子即可。
- estimateSelectivity：首先我们要这个表所有的tuple进行遍历
对每个属性根据其Type建立一个统计直方图，然后把每个元组中的每个属性放到
放到直方图中进行统计即可


### Exercise 3

这一部分对Join操作的Cost(花费)和基数(Join操作后元组的个数)进行统计

需要实现两个方法

- estimateJoinCost(LogicalJoinNode j, int card1, int card2, double cost1, double cost2)：
这个方法计算JOIN操作的花费，计算的公式为：
joincost(t1 join t2) = scancost(t1) + ntups(t1) x scancost(t2) //IO cost+ ntups(t1) x ntups(t2)  //CPU cost

- estimateJoinCardinality(LogicalJoinNode j, int card1, int card2, boolean t1pkey, boolean t2pkey)：
这个方法计算JOIN操作的计数，计算的规则如下：
  - 如果不是等值操作，那么基数为 (int)(card1*card2*0.3)
  - 如果是等值操作，且两个表JOIN的属性都不是主键，那么基数为card1+card2
  - 如果有主键，那么基数等于非主键的card
  

### Exercise 4

这一部分是实现 Selinger 优化器，通过这个优化器来构建JOIN
操作是执行计划(执行顺序)

我们需要实现  List<LogicalJoinNode> orderJoins(Map<String, TableStats> stats,
Map<String, Double> filterSelectivities,  
boolean explain)方法

算法的伪代码如下：
```text
1. j = set of join nodes
2. for (i in 1...|j|):
3.     for s in {all length i subsets of j}
4.       bestPlan = {}
5.       for s' in {all length d-1 subsets of s}
6.            subplan = optjoin(s')
7.            plan = best way to join (s-s') to subplan
8.            if (cost(plan) < cost(bestPlan))
9.               bestPlan = plan
10.      optjoin(s) = bestPlan
11. return optjoin(j)
```
需要注意的是，在我们获得bestPlan之后，我们需要手动将这个bestPlan
的相关信息加入到planCache中去

## Lab 4

这一部分是实现数据库的事务，事务在执行时需要获得相应的读写锁，执行后
释放锁，从而保证事务的ACID特性。

在这一部分简化对数据库的思考，认为事务或数据库在运行过程中不会出现故障
因此在这一部分不需要考虑事务的回滚等操作，只需要保证事务的ACID特性即可

### Exercise 1 2
在这一部分锁的级别都是PageLock，不需要在表级别上进行加锁，因此，所有的锁
的获取的操作都是在BufferPool中的getPage()方法中实现的。

在getPage()方法中，首先获取对应的读写锁，然后再去从bufferPool中或者从disk
中读取相应的配置。

在BufferPool中新增一个LockManager类，对应每一个Page的锁的管理

LockManager类中记录了当前获取Page锁的transactionId，获取锁的类型(共享锁or排他锁)
以及相应的加锁解锁方法。

在BufferPool中新增一个Map保存每一个Page和其对应的LockManager的对应关系

同时新增一个Map记录每一个transactionId以及它所持有的锁的PageId的集合，
方便在事务完成之后释放事务在执行过程中所持有的Page的锁

⚠️：在PageFile中的insertTuple方法中，当当前Table所有的Page的slot都满了之后，在
执行新增操作时需要创建一个空的page，把tuple放到这个新建的page中。

在并发场景下，可能有多个事务同时执行insert操作，因此可能同时发现当前的Table中所有的
Page都满了，因此同时创建多个空的Page。

为了解决这个问题，在Table级别加一个锁，当一个insertTuple需要创建一个新的Page时，
首先获取这个Table的锁，然后再次检查当前的Table中是否有Page有空的Slot，如果有空的
Slot，说明同时有其他的事务创建了一个新的Page，那么它就无需再去创建一个新的Page，否则
它再新建一个新的Page。

⚠️：在BufferPool中需要实现锁的升级。具体来说，当只有一个事务访问一个Page时，它首先获取
一个Page的共享锁，然后再获取这个Page的排他锁时。它无需等待先释放共享锁，而是可以直接将
这个Page的共享锁升级为排他锁。

一开始的逻辑是首先进行逻辑判断，当满足逻辑时，首先释放这个Page的排他锁，然后再加共享锁。
但是在Java中抛出异常，在线程B中无法释放在线程A中获取的锁。意思是一个锁只能在同一个线程
中获取和释放。

因此将逻辑改为在LockerManager类中新增一个lockUpgrade方法，在锁升级时，将LockerManager中
Lock变量重新new一个，然后加排他锁同时将事务的权限更新为READ_WRITE

### Exercise 3 4 
Exercise 3 中需要对BufferPool的evictPage方法进行修改，在Lab4 之前，实现evictPage的逻辑是
采用LRU策略从BufferPool保存的Page中选出一个Page，将其移出BufferPool，如果这个Page是脏页，
还要对其进行刷新。
而在Exercise中，evictPage方法不能将dirty的Page移除。因此在实现的时候要找一个clean的Page移出
如果BufferPool中所有的Page都是脏页，那么抛出异常

Exercise 4 实现transactionComplete()，当一个事务完成之后，需要对脏页进行刷新，也需要释放事务在
执行中获取的锁。如果这个事务没有提交，应该丢弃事务对Page进行的修改。具体的做法是将BufferPool中的dirty
page替换为从disk中获取的旧的Page。

### Exercise 5

这一部分实现死锁检测，本来应该是通过检测环路来实现。
偷懒用超时来做了 ：）
---
注意⚠️：在本实验中事务的并发控制采用两阶段锁来实现(2PL)，两阶段锁是一个悲观锁
在事务开始时，事务必须首先拿到所有资源的锁才可以正常执行。事务在执行的过程中不能再去
获取新的资源的锁。
在事务完成后，事务一次性释放所有资源的锁。并且在锁释放之后不能再释放新的锁
---

## LAB 5

这一部分是实现一个基于B+树的数据库存储文件，在Lab 1-4 中我们使用HeapFile对数据库中的
信息进行存储，使用HeapFile的缺点是每一次我们找一个Tuple或者一个Page都要遍历每一个page。
因此时间复杂度是线性复杂度O(n)。

使用B+树后我们可以根据一些字段建立索引，通过树的查找来搜索一个Page或者一个Tuple，将搜素的
时间复杂度由O(n)降为O(logn)

B+ 树的节点有两种类型，索引节点和 叶子节点。索引节点按序存放索引字段，m阶B+树节点有m+1个孩子
叶子节点和HeapPage一样存放数据库中的Tuple。

在simple DB数据库中，除了叶子节点和索引节点外还增加了BTreeHeaderPage和BTreeRootPtrPage。
BTreeHeaderPage和HeapPage中的header类似，它用来追踪BTreeFile正在使用的Page。
每个BTreeFile开始都有一个BTreeRootPtrPage，它指向B+树的根节点。

### Exercise 1

这一部分实现findLeafPage()方法，它可以根据索引来找到我们需要的Page。这个方法是递归实现的，

首先通过BTreeFile.getPage()方法得到一个BTreePage。如果它的类型是叶节点，那么直接返回即可。

如果是索引节点，我们需要遍历这个节点中的每一个entry，然后比较entry中的索引值与我们传入的查找值，
找到第一个大于查找值的entry，通过递归调用findLeafPage()查找它的左孩子节点。

如果遍历结束后没找到符合的entry，就递归调用findLeafPage()查找最后一个entry的右孩子节点。

特殊的，如果传入的查找索引值为null，我们递归调用findLeafPage，查找每个叶子节点第一个entry的左孩子，
返回最左边的第一个叶节点。

在查找的过程中，对于访问的每一个索引节点我们要对Page加读锁，而对于找到的叶节点，我们根据我们的需要来加锁。


### Exercise 2

这一部分是实现当叶子节点和索引节点满之后的分裂，下面简单说一下思路，明天看情况详细整理。

说一下思路，首先拿到这个page最多的元组的个数，然后除2，得到要分到一个新的page的元组的个数

把要移动的元组从旧的page中删除，加入到新的page中。

更新旧元组和新元组的左右兄弟的指针

得到向索引节点插入的key

对比索引节点插入的key与要插入的新key，选择一个page插入这个新key

把这两个page放到dirtypage中

将要向索引节点插入的key插入到索引节点中，如果索引节点已满，要分裂索引节点。

### Exercise 3 4 

这两个部分是在B+树删除tuple的时候用于对page中的索引或者tuple进行重新分配。

当一个page中的tuple因删除小于总容量的一半的时候就需要对B+树中的节点进行重新分配。

- 对于叶子节点来说，如果它的兄弟page中的tuple数量大于总容量的一半，那么将一个tuple
  从兄弟page移到这个page中，并更新parent索引节点中的entry；否则，将两个page中的tuple
  合并到一起，并删除parent索引节点中的entry。

从上边可以看出，删除tuple可能导致索引page中entry的删除。同理，当一个索引page的tuple数量小于
总容量的一半时也需要对其进行重新分配，其分配的策略与叶节点相似。

对索引page的重新分配可能导致不断向上重新分配直到根节点

索引page和叶节点重新分配的不同在于，叶节点重新分配时需要将一个副本加到索引page中，而索引page重新
分配则不需要

本实验中需要特别注意，在在合并叶节点两个page为一个page后，兄弟指针的修改逻辑。

page->right = page->right->right

同时不能忘记

page->right->right->left = page

最后nextKeyLockTest可以过。

但是由于在上一个Lab中死锁检测偷懒使用超时策略BTreeDeadLockTest过不了。。。

不打算改了 嘻嘻嘻 ^_^

## Lab 6

这一部分是实现事务失败后的回滚，以及数据库崩溃后根据log来进行重做

在这一部分中加入了一个LogFile文件，日志中记录了每个事务的相关操作，

log的类型一共有五种：
- BEGIN 表示一个事务开始，log中记录了事务的id
- COMMIT 表示事务成功提交，log中记录了事务的id
- ABORT 表示事务中断，log中记录了事务的id
- UPDATE RECORDS 记录事务对page的进行修改后（脏页），的before-image以及after-image
  用于之后对事务进行redo或者undo
- CHECKPOINT 检查点log中记录了当前仍然活跃的日志（即事务begin但是还没有提交或者abort）的第一个record的位置，

⚠️注意： UPDATE RECORDS 日志是在 flushPage()方法中进行记录的，即在将脏页刷到disk之前，首先要将脏页的
before-image和after-image记录到日志中，然后再调用file.writePage()方法，将脏页写入磁盘，这样可以保证
每个对磁盘中进行修改的page都有对应的信息保存在log中，以便后续的redo和undo

### Exercise 1 2

Exercise 1 中实现rollback操作，即传入一个事务，该事务所有的操作都要撤销，即undo。

这个比较简单，就是遍历log中的所有信息，找到这个事务修改的所有的page，然后将page的data设为
before-image即可

Exercise 2 中实现redo操作，即在数据库崩溃之后可以根据日志进行相关操作的修复。

具体的逻辑是，从检查点开始，检查点之后已经commit的事务，我们要将它修改的所有page的
数据设为它的after-image。而剩余未commit的事务，我们要将它修改的所有page的数据设为
它的before-image

⚠️注意：事务在提交时，在BufferPool.transactionComplete()需要对所有脏页进行刷盘，在
刷盘完成后将page的before-image更新为当前的data。

但是在测试中，有的测试代码中在调用BufferPool.transactionComplete()之前首先调用了
BufferPool.flushAllPages()方法，调用这个方法将所有的脏页刷盘，并将dirtyPage从dirtyPageMap
中移除，导致在BufferPool.transactionComplete()中无法从dirtyPageMap中拿到page，并设置before-image
从而测试失败

解决方案是在设置一个flushedButNotCompleteMap，用于记录虽然刷盘但是事务没提交的page。
在BufferPool.flushAllPages()中每从dirtyPageMap中移除一个pageId，就将这个pageId
加入到flushedButNotCompleteMap中。

而在BufferPool.transactionComplete()中，如果无法从dirtyPageMap中拿到page，再去从
flushedButNotCompleteMap拿page。同时在事务完成后，将page从flushedButNotCompleteMap删除

---
2022.9.25 10:46  6.830 LAB 完结 撒花🎉

