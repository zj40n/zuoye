package main

import (
	"math"
	"runtime"
	"sort"
	"sync"
	"time"
)

//第二阶段 任务 信息定义
type phase2WorkInfo struct {
	//任务编号
	num int
	//是否属于第二部分任务(即调用sort64 单线程排序的第二部分)
	fromPart2 bool
	//排序后数组的原先的任务编号 若上述fromPart2为true则 忽略该项
	srcNums []int
	//排序后数组是否属于 第二部分任务 用于debug
	srcFromPart2 []bool
	//待归并后的排序完毕的数组
	workArrSlice [][]int64
	//为归并准备的临时存储空间，将workArrSlice排序为一个整体写入此空间
	tempTargetArrSlice []int64
	//排序的方向 false为正向 true为反向 用语 任务分发后 的childMergeSortPhase2排序
	direction bool
	//排序的最终截止位置 用语 任务分发后 的childMergeSortPhase2排序，到targetPos 截止
	targetPos int
}

//第一阶段 任务 信息定义
type phase1WorkInfo struct {
	//任务编号
	num int
	//待排序数组
	workArrSlice []int64
}

// 以下类型 和三个定义的函数 为了  int64数组 调用sort内部排序 自定义
type internalSortDef []int64

func (array internalSortDef) Len() int           { return len(array) }
func (array internalSortDef) Less(i, j int) bool { return array[i] < array[j] }
func (array internalSortDef) Swap(i, j int)      { array[i], array[j] = array[j], array[i] }

//单线程 int64 排序函数
func sort64(array []int64) {
	sort.Sort(internalSortDef(array))
}

//第一阶段单线程排序函数 排序完毕后 将排序完毕的work写回chPhase1BackToMannager2
func sortPhase1(work phase1WorkInfo, chPhase1BackToMannager2 chan phase1WorkInfo) {
	sort64(work.workArrSlice)
	chPhase1BackToMannager2 <- work
}

//第二阶段单线程排序函数 排序完毕后 将排序完毕的work写回work.tempTargetArrSlice，并将work写回chPhase2BackToMannager2
func sortPhase2(work phase2WorkInfo, chPhase2BackToMannager2 chan phase2WorkInfo) {
	sort64(work.workArrSlice[0])
	work.tempTargetArrSlice = work.workArrSlice[0]
	chPhase2BackToMannager2 <- work
}

//在两个有序数组内 寻找第targetNum的数，并分割，返回index1为array1数组的最后一个小于第targetNum的数的数字的下标,index2为array2数组的最后一个小于第targetNum的数的数字的下标,ok为是否分割成功
//返回index1 index2 为-2 时 说明该数组的全部都小于第targetNum个数，为-1时，说明该数组没有小于第targetNum个数的数字，其他情况为具体下标
func findKthCut(targetNum int, array1 []int64, array2 []int64) (index1 int, index2 int, ok bool) {
	length1 := len(array1)
	length2 := len(array2)
	//flag=1时length1 < length2 flag=2时length2<length1
	var flag = 0
	//当两数组的长度均大于targetNum时flag2=1 其他为0
	var flag2 = 0
	var start = 0
	var end = 0
	var mid = 0
	if length1+length2 < targetNum {
		return 0, 0, false
	}
	if length1 == 0 {
		return -2, targetNum - 1, true
	}
	if length2 == 0 {
		return targetNum - 1, -2, true
	}
	if length1 >= targetNum && length2 >= targetNum {
		flag2 = 1
		flag = 1
	} else {
		if length1 < length2 {
			if targetNum-1-length2 > 0 {
				start = targetNum - length2 - 1
			}
			flag = 1
		} else {
			if targetNum-1-length1 > 0 {
				start = targetNum - length1 - 1
			}
			flag = 2
		}
	}
	var theOtherCut = 0
	if flag2 == 1 {

		if array1[0] >= array2[targetNum-1] {
			if length2 != targetNum-1 {
				return -1, targetNum - 1, true
			}
			return -1, -2, true
		}
		if array2[0] >= array1[targetNum-1] {
			if length1 != targetNum-1 {
				return targetNum - 1, -1, true
			}
			return -2, -1, true
		}
		end = targetNum - 1
	} else {
		if flag == 1 {
			if array1[0] >= array2[targetNum-1] {
				return -1, targetNum - 1, true
			}
			end = length1 - 1
		}
		if flag == 2 {
			if array2[0] >= array1[targetNum-1] {
				return targetNum - 1, -1, true
			}
			end = length2 - 1
		}
	}
	if targetNum == 1 {
		if array1[0] > array2[0] {
			if length2 != 1 {
				return -1, 0, true
			}
			return -1, -2, true
		}
		if array2[0] >= array1[0] {
			if length1 != 1 {
				return 0, -1, true
			}
			return -2, -1, true
		}
	}
	if flag == 1 {
		for start <= end {
			mid = (start + end) / 2
			theOtherCut = targetNum - mid - 2
			if mid == length1-1 {
				return -2, theOtherCut, true
			}
			if theOtherCut == length2-1 {
				if array2[theOtherCut] <= array1[mid+1] && array2[theOtherCut] >= array1[mid] {
					return mid, -2, true
				}
				if array2[theOtherCut] > array1[mid+1] {
					start = mid + 1
				}
			} else {
				if array1[mid] <= array2[theOtherCut+1] && array2[theOtherCut] <= array1[mid+1] {
					return mid, theOtherCut, true
				}
				if array1[mid] > array2[theOtherCut+1] {
					end = mid - 1
					if end == -1 {
						return -1, targetNum - 1, true
					}
				}
				if array2[theOtherCut] > array1[mid+1] {
					start = mid + 1
				}
			}

		}
	}
	if flag == 2 {
		end = length2 - 1
		for start <= end {
			mid = (start + end) / 2
			theOtherCut = targetNum - mid - 2
			if mid == length2-1 {
				return theOtherCut, -2, true
			}
			if theOtherCut == length1-1 {
				if array1[theOtherCut] <= array2[mid+1] && array1[theOtherCut] >= array2[mid] {
					return -2, mid, true
				}
				if array1[theOtherCut] > array2[mid+1] {
					start = mid + 1
				}
			} else {
				if array2[mid] <= array1[theOtherCut+1] && array1[theOtherCut] <= array2[mid+1] {
					return theOtherCut, mid, true
				}
				if array2[mid] > array1[theOtherCut+1] {
					end = mid - 1
					if end == -1 {
						return targetNum - 1, -1, true
					}

				}
				if array1[theOtherCut] > array2[mid+1] {
					start = mid + 1
				}
			}

		}
	}
	return -1, -1, false
}

//第二阶段mergeSort2Manager分配任务 的具体函数 indexes1 indexes2存储了findKthCut分割情况的数组 根据indexes1,indexes2的情况分配任务
func alloWorkForMergeSort(work phase2WorkInfo, array1 []int64, array2 []int64, tempTargetSlice []int64, indexes1 []int, indexes2 []int, perCut int, wg *sync.WaitGroup, cutCount int) {
	originArray1 := array1
	originArray2 := array2
	var works = make([]phase2WorkInfo, len(indexes1)+1)
	for j := 0; j < len(indexes1)+1; j++ {
		works[j].workArrSlice = make([][]int64, len(work.workArrSlice))
	}
	var flag = 0
	var i = 0
	for i = 0; i < len(indexes1); i++ {
		if i == 0 {
			if indexes1[i] == -2 && indexes2[i] == -2 {
				works[i].workArrSlice[0] = originArray1
				works[i].workArrSlice[1] = originArray2
			}
			if indexes1[i] == -2 {
				works[i].workArrSlice[0] = originArray1
				works[i].workArrSlice[1] = originArray2[:indexes2[i]+1]
			} else {
				if indexes2[i] == -2 {
					works[i].workArrSlice[0] = originArray1[:indexes1[i]+1]
					works[i].workArrSlice[1] = originArray2
				} else {
					works[i].workArrSlice[0] = originArray1[:indexes1[i]+1]
					works[i].workArrSlice[1] = originArray2[:indexes2[i]+1]
				}
			}
		} else {
			if indexes1[i] == -2 && indexes2[i] == -2 {
				works[i].workArrSlice[0] = originArray1
				works[i].workArrSlice[1] = originArray2
			}
			if indexes1[i] == -2 {
				works[i].workArrSlice[0] = originArray1[indexes1[i-1]+1:]
				works[i].workArrSlice[1] = originArray2[indexes2[i-1]+1 : indexes2[i]+1]
			} else {
				if indexes2[i] == -2 {
					works[i].workArrSlice[0] = originArray1[indexes1[i-1]+1 : indexes1[i]+1]
					works[i].workArrSlice[1] = originArray2[indexes2[i-1]+1:]
				} else {
					works[i].workArrSlice[0] = originArray1[indexes1[i-1]+1 : indexes1[i]+1]
					works[i].workArrSlice[1] = originArray2[indexes2[i-1]+1 : indexes2[i]+1]
				}
			}
		}
		works[i].num = work.num
		works[i].tempTargetArrSlice = tempTargetSlice[:perCut]
		tempTargetSlice = tempTargetSlice[perCut:]
		works[i].direction = false
		works[i].targetPos = perCut/2 - 1

		if works[i].targetPos >= 0 {
			wg.Add(1)
			go childMergeSortPhase2(works[i], wg)
		}
		works[i].direction = true
		works[i].targetPos = perCut / 2
		wg.Add(1)
		go childMergeSortPhase2(works[i], wg)

		if indexes1[i] == -2 {
			copyDirectionFalse(tempTargetSlice, array2, 0, indexes2[i]+1, len(tempTargetSlice)-1)
			flag = 1
			break
		}
		if indexes2[i] == -2 {
			copyDirectionFalse(tempTargetSlice, array1, 0, indexes1[i]+1, len(tempTargetSlice)-1)
			flag = 1
			break
		}
	}
	if flag == 0 {
		works[i].workArrSlice[0] = originArray1[indexes1[i-1]+1:]
		works[i].workArrSlice[1] = originArray2[indexes2[i-1]+1:]

		works[i].num = work.num
		works[i].tempTargetArrSlice = tempTargetSlice
		works[i].direction = false
		works[i].targetPos = len(tempTargetSlice)/2 - 1
		if works[i].targetPos >= 0 {
			wg.Add(1)
			go childMergeSortPhase2(works[i], wg)
		}
		works[i].direction = true
		works[i].targetPos = len(tempTargetSlice) / 2
		wg.Add(1)
		go childMergeSortPhase2(works[i], wg)
	}

}

//第二季二段 归并排序的Manager，其中fast为是否开启加速，即采取分割的手段，对两个任务数组进行分割 fastLevel为空闲的核心数，fastLevel = cpuNums - worksCount2*2
func mergeSortPhase2Manager(work phase2WorkInfo, chPhase2BackToMannager2 chan phase2WorkInfo, fast bool, fastLevel int) {
	if len(work.workArrSlice) == 1 {
		chPhase2BackToMannager2 <- work
	} else {
		//cutCount为Log2fastLevel+1 为分割的次数
		cutCount := math.Ilogb((float64(fastLevel) + 1))
		var wg sync.WaitGroup
		if fast == false || cutCount <= 1 {
			wg.Add(1)
			work.direction = false
			work.targetPos = len(work.tempTargetArrSlice)/2 - 1
			go childMergeSortPhase2(work, &wg)
			wg.Add(1)
			work.direction = true
			work.targetPos = len(work.tempTargetArrSlice) / 2
			go childMergeSortPhase2(work, &wg)
			wg.Wait()
			chPhase2BackToMannager2 <- work
			return
		}
		originArray1 := work.workArrSlice[0]
		originArray2 := work.workArrSlice[1]
		indexes1 := make([]int, cutCount-1)
		indexes2 := make([]int, cutCount-1)

		perCut := len(work.tempTargetArrSlice) / cutCount
		if perCut == 0 {
			wg.Add(1)
			work.direction = false
			work.targetPos = len(work.tempTargetArrSlice) - 1
			go childMergeSortPhase2(work, &wg)
			wg.Wait()
			chPhase2BackToMannager2 <- work
			return
		}
		array1 := work.workArrSlice[0]
		array2 := work.workArrSlice[1]
		remainArray1 := work.workArrSlice[0]
		remainArray2 := work.workArrSlice[1]
		tempTargetSlice := work.tempTargetArrSlice
		var sum1 = 0
		var sum2 = 0
		var flag = 0
		for i := 0; i < cutCount-1; i++ {
			if flag == 1 {
				break
			}
			index1, index2, ok := findKthCut(perCut, remainArray1, remainArray2)
			if ok {
				if index1 != -2 {
					remainArray1 = remainArray1[index1+1:]
					indexes1[i] = index1 + sum1
					sum1 += index1 + 1
				} else {
					indexes1[i] = -2
					flag = 1
				}
				if index2 != -2 {
					remainArray2 = remainArray2[index2+1:]
					indexes2[i] = index2 + sum2
					sum2 += index2 + 1
				} else {
					indexes2[i] = -2
					flag = 1
				}
			}
		}
		alloWorkForMergeSort(work, array1, array2, tempTargetSlice, indexes1, indexes2, perCut, &wg, cutCount)
		wg.Wait()
		work.workArrSlice[0] = originArray1
		work.workArrSlice[1] = originArray2
		chPhase2BackToMannager2 <- work
	}

}

//真正的 归并排序执行者 根据work.direction判断是正向还是反向归并，根据work.targetPos判断归并的结束位置
func childMergeSortPhase2(work phase2WorkInfo, wg *sync.WaitGroup) {
	defer wg.Done()
	var n = len(work.workArrSlice)
	var finishedCount = 0
	lengths := make([]int, n)
	finished := make([]bool, n)
	indexes := make([]int, n)
	var i = 0
	for i := 0; i < n; i++ {
		lengths[i] = len(work.workArrSlice[i])
		if lengths[i] == 0 {
			finished[i] = true
			finishedCount++
		} else {
			finished[i] = false
		}
	}
	if finishedCount == n {
		return
	}
	if work.direction == false {
		for t := 0; t <= work.targetPos; t++ {
			if finishedCount == n-1 {
				for i = 0; i < n; i++ {
					if finished[i] == false {
						break
					}
				}
				copyDirectionFalse(work.tempTargetArrSlice, work.workArrSlice[i], t, indexes[i], work.targetPos)
			}
			var min int64 = 0
			minIndex := 0
			var first = true
			for i = 0; i < n; i++ {
				if finished[i] == true {
					continue
				}
				if first {
					first = false
					min = work.workArrSlice[i][indexes[i]]
					minIndex = i
					continue
				}
				if work.workArrSlice[i][indexes[i]] < min {
					min = work.workArrSlice[i][indexes[i]]
					minIndex = i
				}
			}
			work.tempTargetArrSlice[t] = min
			indexes[minIndex]++
			if indexes[minIndex] == lengths[minIndex] {
				finished[minIndex] = true
				finishedCount++
			}
		}
	} else {
		for i := 0; i < n; i++ {
			indexes[i] = lengths[i] - 1
		}
		for t := len(work.tempTargetArrSlice) - 1; t >= work.targetPos; t-- {
			if finishedCount == n-1 {
				for i = 0; i < n; i++ {
					if finished[i] == false {
						break
					}
				}
				copyInverse(work.tempTargetArrSlice, work.workArrSlice[i], t, indexes[i], work.targetPos)
			}

			var max int64 = 0
			maxIndex := 0
			var first = true
			for i = 0; i < n; i++ {
				if finished[i] == true {
					continue
				}
				if first {
					first = false
					max = work.workArrSlice[i][indexes[i]]
					maxIndex = i
					continue
				}
				if work.workArrSlice[i][indexes[i]] > max {
					max = work.workArrSlice[i][indexes[i]]
					maxIndex = i
				}
			}
			work.tempTargetArrSlice[t] = max
			indexes[maxIndex]--
			if indexes[maxIndex] == -1 {
				finished[maxIndex] = true
				finishedCount++
			}
		}
	}
}

//最终的多线程拷贝函数,从source[sourceStart]开始向target[targetStart]拷贝 直至target[targetEnd]
func copyFinal(target []int64, source []int64, targetStart int, sourceStart int, targetEnd int, wgFinal *sync.WaitGroup) {
	defer wgFinal.Done()
	j := sourceStart
	for i := targetStart; i <= targetEnd; {
		target[i] = source[j]
		i++
		j++
	}
}

//childMergeSortPhase2使用的 拷贝函数
func copyDirectionFalse(target []int64, source []int64, targetStart int, sourceStart int, targetEnd int) {
	j := sourceStart
	for i := targetStart; i <= targetEnd; {
		target[i] = source[j]
		i++
		j++
	}
}

//childMergeSortPhase2使用的 反向拷贝函数
func copyInverse(target []int64, source []int64, targetStart int, sourceStart int, targetEnd int) {
	j := sourceStart
	for i := targetStart; i >= targetEnd; {
		if source[j] == 0 {
			// fmt.Println("ERROR 0")
		}
		target[i] = source[j]
		i--
		j--
	}
}

//将接收到的phase1DoneInfo1 转换为phase2WorkInfo
func phase1WriteToWorkInfoPhase2(phase1DoneInfo1 []phase1WorkInfo, workInfoPhase2Part1 []phase2WorkInfo, phase2Num int, j int, start int, end int) {
	var sum = 0
	length := end - start
	workInfoPhase2Part1[j].srcNums = make([]int, length)
	workInfoPhase2Part1[j].workArrSlice = make([][]int64, length)
	workInfoPhase2Part1[j].srcFromPart2 = make([]bool, length)
	var k = 0
	for i := start; i < end; i++ {
		sum += len(phase1DoneInfo1[i].workArrSlice)
		workInfoPhase2Part1[j].workArrSlice[k] = phase1DoneInfo1[i].workArrSlice
		workInfoPhase2Part1[j].srcNums[k] = phase1DoneInfo1[i].num
		workInfoPhase2Part1[j].srcFromPart2[k] = false
		k++
	}
	workInfoPhase2Part1[j].tempTargetArrSlice = make([]int64, sum)
	workInfoPhase2Part1[j].num = phase2Num
	workInfoPhase2Part1[j].fromPart2 = false
}

//将接收到的phase1DoneInfo2 转换为phase2WorkInfo 若fromPart2为true，说明此任务为第二部分单线程内部排序的任务，只有一个任务数组，讲phase2DoneInfo[i].workArrSlice[0]赋给workArrSlice，其他情况，将tempTargetArrSlice赋给workArrSlice
func phase2WriteToWorkInfoPhase2(phase2DoneInfo []phase2WorkInfo, workInfoPhase2Part1 []phase2WorkInfo, phase2Num int, j int, start int, end int) {
	var sum = 0
	length := end - start
	var k = 0
	workInfoPhase2Part1[j].srcNums = make([]int, length)
	workInfoPhase2Part1[j].workArrSlice = make([][]int64, length)
	workInfoPhase2Part1[j].srcFromPart2 = make([]bool, length)
	for i := start; i < end; i++ {
		if phase2DoneInfo[i].fromPart2 {
			sum += len(phase2DoneInfo[i].workArrSlice[0])
			workInfoPhase2Part1[j].workArrSlice[k] = phase2DoneInfo[i].workArrSlice[0]
			workInfoPhase2Part1[j].srcFromPart2[k] = true
		} else {
			sum += len(phase2DoneInfo[i].tempTargetArrSlice)
			workInfoPhase2Part1[j].workArrSlice[k] = phase2DoneInfo[i].tempTargetArrSlice
			workInfoPhase2Part1[j].srcFromPart2[k] = false
		}
		workInfoPhase2Part1[j].srcNums[k] = phase2DoneInfo[i].num
		k++
	}
	workInfoPhase2Part1[j].tempTargetArrSlice = make([]int64, sum)
	workInfoPhase2Part1[j].num = phase2Num
	workInfoPhase2Part1[j].fromPart2 = false

}

//1阶段内部排序的Manager，将任务根据cpuNums分配给sortPhase1，也开启internalSortManager2
func internalSortManager1(array []int64, wg *sync.WaitGroup, cpuNums int) {
	var targetNum = 0
	if len(array) == 0 || cpuNums == 0 {
		return
	}
	var lenPerSlice = 0
	switch {
	case cpuNums == 1:
		{
			sort64(array)
			wg.Done()
		}
	case cpuNums >= 2:
		{
			targetNum = 2 * cpuNums
			lenPerSlice = (len(array)) / targetNum
			if lenPerSlice == 0 {
				sort64(array)
				wg.Done()
				return
			}
			var workInfoPhase1 phase1WorkInfo
			chPhase1BackToMannager2 := make(chan phase1WorkInfo)
			var wgForCloseChPhase1BackToMannager2 sync.WaitGroup
			wgForCloseChPhase1BackToMannager2.Add(1)
			go internalSortManager2(array, chPhase1BackToMannager2, cpuNums, wg, &wgForCloseChPhase1BackToMannager2)
			for i := 1; i <= cpuNums; i++ {
				arrayPhase1 := array[(i-1)*lenPerSlice : i*lenPerSlice]
				workInfoPhase1.num = i
				workInfoPhase1.workArrSlice = arrayPhase1
				go sortPhase1(workInfoPhase1, chPhase1BackToMannager2)
			}
			wgForCloseChPhase1BackToMannager2.Wait()
			close(chPhase1BackToMannager2)
		}
	}
}

//2阶段内部排序的Manager
func internalSortManager2(array []int64, chPhase1BackToMannager2 chan phase1WorkInfo, cpuNums int, wg *sync.WaitGroup, wgForCloseChPhase1BackToMannager2 *sync.WaitGroup) {
	defer wg.Done()
	phase1DoneInfo := make([]phase1WorkInfo, cpuNums)
	var worksCount1 = cpuNums
	phase2DoneInfo := make([]phase2WorkInfo, cpuNums*4)

	chPhase2BackToMannager2 := make(chan phase2WorkInfo, 2)
	workInfoPhase2Part1 := make([]phase2WorkInfo, cpuNums*4)
	var state = 0
	var i = 0
	var j = 0
	var targetNum = 0
	var lenPerSlice = 0
	var worksCount2 = 0
	var phase2Num = cpuNums + 1
	//首先根据cpuNums判断处理方式
	if cpuNums < 4 {
		state = 1
	} else {
		state = 2
	}
	targetNum = 2 * cpuNums
	lenPerSlice = (len(array)) / targetNum
	//若state也就是cpNums<4 等待第一部分任务处理完毕后将第二部分任务分发
	if state == 1 {
		for ; i < cpuNums; i++ {
			phase1DoneInfo[i] = <-chPhase1BackToMannager2
			worksCount1--
		}
		wgForCloseChPhase1BackToMannager2.Done()
		worksCount2 += cpuNums
		for i := cpuNums + 1; i <= 2*cpuNums; i++ {
			var workInfoPhase2Part2 phase2WorkInfo
			workInfoPhase2Part2.workArrSlice = make([][]int64, 1)
			if i != 2*cpuNums {
				arrayPhase2 := array[(i-1)*lenPerSlice : i*lenPerSlice]
				workInfoPhase2Part2.workArrSlice[0] = arrayPhase2
			} else {
				arrayPhase2 := array[i*lenPerSlice:]
				workInfoPhase2Part2.workArrSlice[0] = arrayPhase2
			}
			workInfoPhase2Part2.num = phase2Num
			phase2Num++
			workInfoPhase2Part2.fromPart2 = true

			go sortPhase2(workInfoPhase2Part2, chPhase2BackToMannager2)
		}
		phase1WriteToWorkInfoPhase2(phase1DoneInfo, workInfoPhase2Part1, phase2Num, j, 0, cpuNums)
		worksCount2++
		phase2Num++
		go mergeSortPhase2Manager(workInfoPhase2Part1[j], chPhase2BackToMannager2, false, 0)
		j++
	}
	var flag = 0
	var t = cpuNums + 1
	var remainForphase1 = cpuNums
	var originT = t
	var originI = i
	//若state为2 cpuNums>=4，每等待四个任务到来，开启四路归并处理，同时发起2个sortPhase2，来填补空闲的CPU
	if state == 2 {
		for {
			if flag == 0 {
				for ; i < originI+4; i++ {
					phase1DoneInfo[i] = <-chPhase1BackToMannager2
					worksCount1--
				}
				originI = i
				worksCount2 += 2
				for ; t <= originT+1; t++ {
					var workInfoPhase2Part2 phase2WorkInfo
					workInfoPhase2Part2.workArrSlice = make([][]int64, 1)
					arrayPhase2 := array[(t-1)*lenPerSlice : t*lenPerSlice]
					workInfoPhase2Part2.workArrSlice[0] = arrayPhase2
					workInfoPhase2Part2.num = phase2Num
					phase2Num++
					workInfoPhase2Part2.fromPart2 = true
					go sortPhase2(workInfoPhase2Part2, chPhase2BackToMannager2)
				}
				originT = t
				phase1WriteToWorkInfoPhase2(phase1DoneInfo, workInfoPhase2Part1, phase2Num, j, i-4, i)
				worksCount2++
				phase2Num++
				go mergeSortPhase2Manager(workInfoPhase2Part1[j], chPhase2BackToMannager2, false, 0)
				j++

				remainForphase1 = remainForphase1 - 4
				if remainForphase1 <= 4 {
					flag = 1
					continue
				}
			}
			if flag == 1 {
				for ; i < cpuNums; i++ {
					phase1DoneInfo[i] = <-chPhase1BackToMannager2
					worksCount1--
				}
				wgForCloseChPhase1BackToMannager2.Done()

				for ; t <= 2*cpuNums; t++ {
					var workInfoPhase2Part2 phase2WorkInfo
					workInfoPhase2Part2.workArrSlice = make([][]int64, 1)
					if t != 2*cpuNums {
						arrayPhase2 := array[(t-1)*lenPerSlice : t*lenPerSlice]
						workInfoPhase2Part2.workArrSlice[0] = arrayPhase2
					} else {
						arrayPhase2 := array[(t-1)*lenPerSlice:]
						workInfoPhase2Part2.workArrSlice[0] = arrayPhase2
					}
					workInfoPhase2Part2.num = phase2Num
					phase2Num++
					workInfoPhase2Part2.fromPart2 = true
					worksCount2++
					go sortPhase2(workInfoPhase2Part2, chPhase2BackToMannager2)
				}
				if remainForphase1 > 0 {
					phase1WriteToWorkInfoPhase2(phase1DoneInfo, workInfoPhase2Part1, phase2Num, j, i-remainForphase1, i)
					worksCount2++
					phase2Num++
					go mergeSortPhase2Manager(workInfoPhase2Part1[j], chPhase2BackToMannager2, false, 0)
					j++
				}
				break
			}
		}

	}
	//等待2阶段 任务处理完毕，发起新任务，继续等待
	var k = 0
	var originK = 0
	var timeToEnd = false
	var remain = 0
	timer := time.NewTimer(200 * time.Millisecond)
	var fastLevel = 0
FORSELECT:
	for {
		select {
		case workTemp := <-chPhase2BackToMannager2:
			{
				phase2DoneInfo[k] = workTemp
				k++
				worksCount2--
				timer.Reset(10 * time.Millisecond)
			}
		case <-timer.C:
			if k-originK < 2 {
				continue
			}
			switch {
			//此时cpu满负荷 不需要加速
			case worksCount2 > cpuNums/2:
				{
					phase2WriteToWorkInfoPhase2(phase2DoneInfo, workInfoPhase2Part1, phase2Num, j, originK, k)
					phase2Num++
					originK = k
					worksCount2++
					go mergeSortPhase2Manager(workInfoPhase2Part1[j], chPhase2BackToMannager2, false, 0)
					j++
				}
				//此时cpu有空闲 可以加速
			case worksCount2 <= cpuNums/2:
				{
					for {
						remain = k - originK
						if remain > 1 {
							phase2WriteToWorkInfoPhase2(phase2DoneInfo, workInfoPhase2Part1, phase2Num, j, originK, originK+2)
							phase2Num++
							originK += 2
							fastLevel = cpuNums - worksCount2*2
							worksCount2++
							go mergeSortPhase2Manager(workInfoPhase2Part1[j], chPhase2BackToMannager2, true, fastLevel)
							j++
						} else {
							break
						}
					}
					for {
						workTemp := <-chPhase2BackToMannager2
						phase2DoneInfo[k] = workTemp
						k++
						worksCount2--
						//最后一个任务处理结束 可以结束
						if timeToEnd {
							lenPerSliceFinal := len(workTemp.tempTargetArrSlice) / cpuNums
							var wgFinal sync.WaitGroup
							wgFinal.Add(cpuNums)
							for i := 1; i <= cpuNums; i++ {
								if i == cpuNums {
									go copyFinal(array, workTemp.tempTargetArrSlice, (i-1)*lenPerSliceFinal, (i-1)*lenPerSliceFinal, len(workTemp.tempTargetArrSlice)-1, &wgFinal)
									break
								}
								go copyFinal(array, workTemp.tempTargetArrSlice, (i-1)*lenPerSliceFinal, (i-1)*lenPerSliceFinal, i*lenPerSliceFinal-1, &wgFinal)
							}
							wgFinal.Wait()
							break FORSELECT
						}
						if k-originK == 2 {
							phase2WriteToWorkInfoPhase2(phase2DoneInfo, workInfoPhase2Part1, phase2Num, j, originK, originK+2)
							phase2Num++
							originK += 2

							fastLevel = cpuNums - worksCount2*2
							worksCount2++
							//此时workCount2为1 说明为最后一个任务 当下次任务处理完毕可以结束
							if worksCount2 == 1 {
								timeToEnd = true
							}
							go mergeSortPhase2Manager(workInfoPhase2Part1[j], chPhase2BackToMannager2, true, fastLevel)
							j++
						}

					}

				}

			}

		}
	}
	close(chPhase2BackToMannager2)
}

// MergeSort performs the merge sort algorithm.
// Please supplement this function to accomplish the home work.
func MergeSort(src []int64) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	cpuNums := runtime.NumCPU()
	var wg sync.WaitGroup
	wg.Add(1)
	go internalSortManager1(src, &wg, cpuNums)
	wg.Wait()
}
