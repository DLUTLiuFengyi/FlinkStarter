### FlatMap

flink的FlatMapFunction的flatMap函数是void类型的，
它用Collector<String> out参数输出

### RichFunction

RichFunction与常规函数的区别在于其可以获取运行环境的上下文，
并拥有一些生命周期方法

生命周期：
* 一个算子被调用前open方法会被调用
* close是生命周期最后一个调用的方法
* getRuntimeContext()提供了函数的运行时上下文的一些信息，如函数执行的并行度、任务名字、state状态等

### state机制

* value state
  * 状态是单个值
* list state
  * 状态是一组数据的列表
* map state
  * 状态是一组kv对
* reducing state & aggregating state
  * 状态是一个用于聚合操作的列表

#### 状态后端

主要负责：1. 本地的状态管理 2. 将checkpoint写入远程存储

正常情况：在本地内存

### window分为两类
#### 增量聚合

来一条数据就计算一次，保持一个简单的状态。
ReduceFunction，AggregateFunction

用state来保持，隔一段时间才会有结果（等到窗口时间结束）

sum max min都是增量聚合

实时性更好

#### 全窗口聚合

先把窗口所有数据收集起来，等到计算的时候会遍历所有数据。
ProcessWindowFunction，WindowFunction

等窗口时间结束时，将窗口内所有数据进行计算

* 排序、统计当前数据的中位数，用全窗口聚合
* 更灵活，适用场景更多，因为process能获取到上下文

### ProcessFunction

常规转换算子无法访问事件的时间戳信息（不是生成时间而是事件时间）和watermark信息

low-level的转换算子：
* 访问事件时间、watermark、注册定时事件（当前要做一个操作，但这个操作不是现在马上要做的）
* 输出一些特定事件（超时事件）
* Flink SQL就是用ProcessFunction实现的

### Time

* Processing Time
  * 执行相应操作机器的系统时间。当程序在process time运行时
  所有基于时间的操作（时间窗口）将使用当前运行机器的系统时间。
  * 不需要流和物理机之间的协调，性能最好延迟最低
  但在分布式和异步环境下，不确定性，因为易收到系统之间操作记录传输速度以及中断影响，导致数据延迟
* Event Time
  * 每个独立事件在其生成设备上发生的时间，通常进入flink前就嵌入在记录中的时间
  * event time必须指定如何生成event的watermark，保证事件时间有序性
  * 假设所有数据都已到达，事件时间操作将按照预期进行，即使在处理无序延迟的事件或重新处理历史数据，
  也会产生正确一致的结果。例如，每小时事件时间窗口将包含所有带有属于该小时的事件时间戳的记录，
  而不管它们到达的顺序如何，也不管它们是在什么时候处理的
* Ingestion Time
  * 事件进入flink的时间，在源操作中每个记录都会获得源的当前时间作为时间戳，
  后续基于时间的操作（window）会依赖这个时间戳
  * 处在事件时间和处理时间之间，与处理时间相比成本更高点，但可提供更可预测的结果
  * 摄入时间使用固定的时间戳（在源处指定），不能处理任何无序事件或延迟事件（event time可以），
  但程序无需指定如何产生水印

### watermark

Flink衡量event time 进度的机制是watermark，watermark流作为数据流的一部分，携带了一个时间戳t。

一个Watermark(t)声明了数据流中的事件时间已经到t了，也就是说数据流中不应该再有时间戳t'<=t的元素了。

一旦watermark到达一个操作，这个操作就可以将事件时间的时钟提前到watermark值的前面。

为此，流程序可能会显式地预期一些延迟元素。
延迟元素是在系统时间(如水印所示)已经超过延迟元素的时间戳时间之后到达的元素。

### SlotSharingGroup

1、降低数据交换开销
2、方便用户配置资源
3、均衡负载

如果flink window操作比较复杂，source操作抽取数据，window操作的slot来不及处理，
造成数据堆积，从而导致slot的空闲浪费。

解决：解决办法：使用共享slot，一条数据从source -> transformation -> sink都在同一个slot中，
当其他slot空闲时，可以共用slot来用于复杂的window计算操作。