package main

import (
	"os/exec"
	"strconv"

	"github.com/en-vee/alog"
)

func defaultRowChecker(row map[string]any) (bool, string) {
	if b, ok := row["bedrooms"]; ok {
		bs := strconv.Itoa(int(row["bedrooms"].(int32)))

		return b.(int32) > 2, "The row " + row["_id"].(string) + " has more than 2 rooms: " + bs
	}

	return false, ""
}

func simpleRowChecker(row map[string]any) (bool, string) {
	if mn, ok := row["minimum_nights"]; ok {
		mni, _ := strconv.Atoi(mn.(string))
		return mni < 2, ""
	}

	return false, ""
}

func acceptAllRowsChecker(row map[string]any) (bool, string) {
	return true, ""
}

func shellExecRowChecker(row map[string]any) (bool, string) {
	app := "echo"

	arg0 := "wololo"

	cmd := exec.Command(app, arg0)

	_, err := cmd.Output()

	if err != nil {
		alog.Error(err.Error())
		return false, ""
	}

	return true, ""
}
