package main

import (
	"fmt"
	"strconv"
	"sync"
)

// See https://github.com/dariocasas/concurrency
// See https://go.dev/blog/pipelines (Sameer Ajmani)

const concurrency = 4

var data = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}

func f(in int) string {
	s := strconv.Itoa(in * 10)
	fmt.Printf("executing f(%d)=%s\n", in, s)
	return s
}

func main() {

	done := make(chan struct{})
	defer func() {
		close(done)
		fmt.Printf("fim")
	}()

	// Stage 1 (source or producer) (only outbound channel)
	// generates input data
	sourceChan := generateSourceChannel(data...)

	// Stage 2
	// fan out
	spreadChans := fanOut(sourceChan, concurrency)

	// Stage 3
	// run workers
	outChannels := runWorkers(spreadChans, f)

	// Stage 4
	// fan in
	sinkChan := merge(done, outChannels...)

	// Stage 5 (sink or consumer)(only inbound channel)
	for n := range sinkChan {
		fmt.Printf("a  %s \n", n)
	}

}

func runWorkers(inC []<-chan int, f func(int) string) []<-chan string {
	var outC []<-chan string
	for i := 0; i < len(inC); i++ {
		outC = append(outC, worker(inC[i], f))
	}
	return outC
}

func worker(in <-chan int, f func(int) string) <-chan string {

	out := make(chan string)
	go func() {
		for n := range in {
			// process
			s := f(n)
			out <- s
		}
		close(out)
	}()
	return out
}

func fanOut(in <-chan int, concurrency int) []<-chan int {

	var outC []chan int

	for i := 0; i < concurrency; i++ {
		c := make(chan int)
		outC = append(outC, c)
	}

	// distributes chan values in X separated channels
	go func() {
		i := 0
		for v := range in {
			outC[i] <- v
			i++
			if i == concurrency {
				i = 0
			}
		}
		for i := 0; i < concurrency; i++ {
			close(outC[i])
		}
	}()

	// copy out channels to read-only channels
	resultChan := make([]<-chan int, concurrency)
	for n, ch := range outC {
		resultChan[n] = ch
	}
	return resultChan

}

func merge(done chan struct{}, cs ...<-chan string) <-chan string {
	var wg sync.WaitGroup
	out := make(chan string, 1)

	wg.Add(len(cs))

	output := func(c <-chan string) {
		defer func() {
			wg.Done()
		}()
		for n := range c {
			select {
			case out <- n:
			case <-done:
			}
		}
	}

	for _, c := range cs {
		go output(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func generateSourceChannel(nums ...int) <-chan int {
	out := make(chan int, len(nums))
	for _, n := range nums {
		out <- n
	}
	close(out)
	return out
}
