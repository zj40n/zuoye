## Introduction

This is the Merge Sort home work for PingCAP Talent Plan Online of week 1.

There are 16, 000, 000 int64 values stored in an unordered array. Please
supplement the `MergeSort()` function defined in `mergesort.go` to sort this
array.

Requirements and rating principles:
* (30%) Pass the unit test.
* (20%) Performs better than `sort.Slice()`.
* (40%) Have a document to describe your idea and record the process of performance optimization with `pprof`.
* (10%) Have a good code style.

NOTE: **go 1.12 is required**

## How to use

Please supplement the `MergeSort()` function defined in `mergesort.go` to accomplish
the home work.

**NOTE**:
1. There is a builtin unit test defined in `mergesort_test.go`, however, you still
   can write your own unit tests.
2. There is a builtin benchmark test defined in `bench_test.go`, you should run
   this benchmark to ensure that your parallel merge sort is fast enough.


How to test:
```
make test
```

How to benchmark:
```
make bench
```   


# 多线程MergeSort 文档    Author:赵晋 Email:372586300@qq.com  
## 调研
给定问题为内存中多线程归并排序，内存足够大，主要瓶颈为CPU资源。   
经过查阅资料，我确定了以下几项可以优化的点:   
1. 对于多个有序数组，可以正反两个方向同时进行归并，即从小到大归并，和从大到小归并。   
2. 对于有序数组数量为2时，可以使用基于二分查找的方式获取中间点，复杂度为O(logN)，N为两数组中较小的长度。再从中间截断，可以拆分为左右两部分。当待归并的有序数组数量大于2时，需要的复杂度较高，不考虑。
3. 对于有序数组数量大于2，如果两两归并，效率是不如直接整体归并的。
4. 对于最后归并完成后的拷贝回原数组的操作，可以对使用多线程加速，将归并好的数组分段，充分利用CPU，加速拷贝。  
## 总体思想  
因为在调研中提到的第二点和第三点，即当归并任务中有序数组数量为2时，可以在复杂度不高的情况下，将任务继续拆分，这样可以充分利用多线程的优势。当CPU空闲核心小于等于2时，此时应该直接整体归并，反之，则应该使用两两归并。所以我决定根据CPU数量和当前处理中的任务数实现一个自适应任务分配算法。  

如下图所示 : 若CPU数量只有1个，直接内部排序返回。否则将目标数组分成两个部分，即部分1和部分2，处理上分为Phase1Manager,Phase2Manager，其中Phase1Manager先对第一部分进行分段，段数为CPU数量，对于每段利用GO自带的内部排序方法进行排序。同时开启Phase2Manager接收排序处理后的数组，根据CPU数量和当前处理中的任务数的情况，进行任务分配,启动归并排序。

![](https://i.postimg.cc/FHPJ1bL3/phase1.png)   
![](https://i.postimg.cc/YSZGzp8S/phase2.png)   
## 具体实现   

上文提到，根据和CPU核心数量和当前任务数进行任务分配，每时间段内t，查看有没有返回的任务，若不存在则继续等待，若存在返回的任务。则再等待时间t，直到，存在接受到的任务数量大于等于2并且下一个t时刻没有新任务到来时结束。此时通过算法公式判断：   
公式为：设workCounts2为当前处理中的任务数，CpuNums为cpu核心数，  
1. 当worksCount2>cpuNums/2时，由于每个任务可能有正反双向处理，即占用两个核心，此时CPU符合已满，则对这批任务进行整体归并，不使用两两归并拆分加速。
2. 当worksCount2<=cpuNums/2，此时表示CPU有余力，于是将这批任务进行两两归并处理，并进行适当拆分加速。    

最后全部处理完毕后，将归并后的数组，拷贝回原数组，利用多线程加速。
## pprof参数调优
我的CPU为4核心，开启了超线程，go识别为8核心。最初，参数上存在误估，任务等待时间即上文提到的t，设的值为0.2秒，偏大，经过调整，调小为0.01秒。先前测试结果为：多线程归并0.95秒，单线程归并2.7秒左右，结果调整，多线程归并排序时间的降低为0.71秒。结果如下图所示：   
![](https://i.postimg.cc/X7k566N2/Adjust2.png)   利用go-torch工具，可视化cpu利用率后，如下图所示：
![](https://i.postimg.cc/zv1y40sZ/Flame-Graph1.png)    
经查看发现，CPU使用上，多路归并排序只有10%，而phase1Manager和phase2Manager的内部排序占比为45%左右，瓶颈主要在内部排序。
关闭超线程后，go识别为4核心。测试结果为:   多线程归并0.9秒，单线程归并2.7秒。结果如下图所示：   
![](https://i.postimg.cc/jSq7s3LR/Adjust3.png) 
利用pprof输出pdf，结果如下图所示:
![](https://i.postimg.cc/9M94xvNY/pdf1.png)  
手动改变算法中的CPU核心数为8测试，结果并没有明显变化。结果如下图所示:
![](https://i.postimg.cc/jjLnfWH9/Adjust4.png)

