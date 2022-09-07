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

⚠️注意：在重写equlas方法后也要重写hashCode方法，因为如果不重写
hashCode方法有可能导致两个相同的Object得到不同的hashCode，这样
在使用Map的时候有可能导致key重复的情况