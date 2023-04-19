package exe_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/expbuild/expbuild/pkg/exe"
)

func TestScheduler_Wait(t *testing.T) {
	fmt.Println("start test")

	scheduler, _ := exe.MakeScheduler()
	go func() {
		time.Sleep(1 * time.Second)
		fmt.Println("notifying")
		for _, waiter := range scheduler.RunningJobs {
			fmt.Println("there is some event waiting")
			for _, ch := range waiter.NotifyChannels {
				fmt.Println("there is some channel")
				ch <- 1
			}
		}
	}()
	go scheduler.Wait("123")
	go scheduler.Wait("123")
	go scheduler.Wait("123")
	go scheduler.Wait("123")
	go scheduler.Wait("123")
	for {
		time.Sleep(1 * time.Second)
	}
	//t.Fail()

}
