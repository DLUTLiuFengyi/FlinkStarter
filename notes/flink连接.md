union, cogroup, join

## union

* 将多个DataStream[T]合并为一个新的DataStream[T]，或者2个DataSet[T]合并成一个DataSet[T]
* 不去重，先进先出，混在一起
* 只能union类型相同的流或数据集

stream1.union(stream2,stream3,stream4)）结果是一个新Datastream

## cogroup

将两个数据流或集合按照key进行group，然后将相同的key的数据进行处理。

与join的区别在于它在一个流/数据集中没有找到与另一个匹配的数据还是会输出。

如果在一个流中没有找到与另一个流的window中匹配的数据，任何输出结果，即只输出一个流的数据。