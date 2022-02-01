package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	IfraGoSDK "github.com/ifraiot/IfraGoSDK"
	"github.com/joho/godotenv"
)

type empData struct {
	Node      string
	Battery   float64
	F1        float64
	A1        float64
	F2        float64
	A2        float64
	F3        float64
	A3        float64
	F4        float64
	A4        float64
	F5        float64
	A5        float64
	City      string
	Temp      float64
	Timestamp time.Time
}

type TimeStamp struct {
	Node string
	Time time.Time
}

var timeStamps []TimeStamp

func main() {

	godotenv.Load(".env")
	for {

		currentTime := time.Now()
		fileName := fmt.Sprintf("Log/SensorNode_%s.csv", currentTime.Format("20060102"))
		// fmt.Printf(fileName)

		csvFile, err := os.Open(fileName)
		if err != nil {
			fmt.Println(err)
		}

		ifra := IfraGoSDK.NewIFRA(
			Env("MQTT_TOPIC"),
			Env("MQTT_PASSWORD"),
			Env("MQTT_PASSWORD"))

		fmt.Println("Successfully Opened CSV file")

		csvLines, err := csv.NewReader(csvFile).ReadAll()
		if err != nil {
			fmt.Println(err)
			fmt.Println(err)
			time.Sleep(60 * time.Second)
			continue
		}

		node1, err := FindLastData(csvLines, "E5C6")
		if err != nil {
			fmt.Println(err)
			time.Sleep(60 * time.Second)
			continue
		}
		node2, err := FindLastData(csvLines, "E592")
		if err != nil {
			fmt.Println(err)
			time.Sleep(60 * time.Second)
			continue
		}
		node3, err := FindLastData(csvLines, "E555")
		if err != nil {
			fmt.Println(err)
			time.Sleep(60 * time.Second)
			continue
		}

		if len(node1) > 20 && canSend(node1, timeStamps) {

			data := empData{
				Node:      node1[0],
				Battery:   ParseFloat(node1[6]),
				F1:        ParseFloat(node1[8]),
				A1:        ParseFloat(node1[10]),
				F2:        ParseFloat(node1[12]),
				A2:        ParseFloat(node1[14]),
				F3:        ParseFloat(node1[16]),
				A3:        ParseFloat(node1[18]),
				F4:        ParseFloat(node1[20]),
				A4:        ParseFloat(node1[22]),
				F5:        ParseFloat(node1[24]),
				A5:        ParseFloat(node1[26]),
				Timestamp: ParseDateTime(node1[38]),
				Temp:      ParseFloat(node1[32]),
			}
			fmt.Println(data.Node, ":canSend")
			t := ParseDateTime(node1[38])
			SetLastSend(node1[0], t)
			fmt.Println(data.Node+"BatteryVoltage", data.Battery)
			ifra.AddMeasurement(data.Node+"BatteryVoltage", data.Battery)
			ifra.AddMeasurement(data.Node+"F1", data.F1)
			ifra.AddMeasurement(data.Node+"A1", data.A1)
			ifra.AddMeasurement(data.Node+"F2", data.F2)
			ifra.AddMeasurement(data.Node+"A2", data.A2)
			ifra.AddMeasurement(data.Node+"F3", data.F3)
			ifra.AddMeasurement(data.Node+"A3", data.A3)
			ifra.AddMeasurement(data.Node+"F4", data.F4)
			ifra.AddMeasurement(data.Node+"A4", data.A4)
			ifra.AddMeasurement(data.Node+"F5", data.F5)
			ifra.AddMeasurement(data.Node+"A5", data.A5)
			ifra.AddMeasurement(data.Node+"Temp", data.Temp)
			ifra.Send()
		}

		if len(node2) > 20 && canSend(node2, timeStamps) {
			data := empData{
				Node:      node2[0],
				Battery:   ParseFloat(node2[6]),
				F1:        ParseFloat(node2[8]),
				A1:        ParseFloat(node2[10]),
				F2:        ParseFloat(node2[12]),
				A2:        ParseFloat(node2[14]),
				F3:        ParseFloat(node2[16]),
				A3:        ParseFloat(node2[18]),
				F4:        ParseFloat(node2[20]),
				A4:        ParseFloat(node2[22]),
				F5:        ParseFloat(node2[24]),
				A5:        ParseFloat(node2[26]),
				Timestamp: ParseDateTime(node2[38]),
				Temp:      ParseFloat(node2[32]),
			}
			fmt.Println(data.Node, ":canSend")
			t := ParseDateTime(node2[38])
			SetLastSend(node2[0], t)
			fmt.Println(data.Node+"BatteryVoltage", data.Battery)
			ifra.AddMeasurement(data.Node+"BatteryVoltage", data.Battery)
			ifra.AddMeasurement(data.Node+"F1", data.F1)
			ifra.AddMeasurement(data.Node+"A1", data.A1)
			ifra.AddMeasurement(data.Node+"F2", data.F2)
			ifra.AddMeasurement(data.Node+"A2", data.A2)
			ifra.AddMeasurement(data.Node+"F3", data.F3)
			ifra.AddMeasurement(data.Node+"A3", data.A3)
			ifra.AddMeasurement(data.Node+"F4", data.F4)
			ifra.AddMeasurement(data.Node+"A4", data.A4)
			ifra.AddMeasurement(data.Node+"F5", data.F5)
			ifra.AddMeasurement(data.Node+"A5", data.A5)
			ifra.AddMeasurement(data.Node+"Temp", data.Temp)
			ifra.Send()
		}

		if len(node3) > 20 && canSend(node3, timeStamps) {
			data := empData{
				Node:      node3[0],
				Battery:   ParseFloat(node3[6]),
				F1:        ParseFloat(node3[8]),
				A1:        ParseFloat(node3[10]),
				F2:        ParseFloat(node3[12]),
				A2:        ParseFloat(node3[14]),
				F3:        ParseFloat(node3[16]),
				A3:        ParseFloat(node3[18]),
				F4:        ParseFloat(node3[20]),
				A4:        ParseFloat(node3[22]),
				F5:        ParseFloat(node3[24]),
				A5:        ParseFloat(node3[26]),
				Timestamp: ParseDateTime(node3[38]),
				Temp:      ParseFloat(node3[32]),
			}
			fmt.Println(data.Node, ":canSend")
			t := ParseDateTime(node3[38])
			SetLastSend(node3[0], t)
			fmt.Println(data.Node+"BatteryVoltage", data.Battery)
			ifra.AddMeasurement(data.Node+"BatteryVoltage", data.Battery)
			ifra.AddMeasurement(data.Node+"F1", data.F1)
			ifra.AddMeasurement(data.Node+"A1", data.A1)
			ifra.AddMeasurement(data.Node+"F2", data.F2)
			ifra.AddMeasurement(data.Node+"A2", data.A2)
			ifra.AddMeasurement(data.Node+"F3", data.F3)
			ifra.AddMeasurement(data.Node+"A3", data.A3)
			ifra.AddMeasurement(data.Node+"F4", data.F4)
			ifra.AddMeasurement(data.Node+"A4", data.A4)
			ifra.AddMeasurement(data.Node+"F5", data.F5)
			ifra.AddMeasurement(data.Node+"A5", data.A5)
			ifra.AddMeasurement(data.Node+"Temp", data.Temp)
			ifra.Send()
		}

		csvFile.Close()
		ifra.Disconnect()
		time.Sleep(60 * time.Second)
	}

}

func ParseDateTime(str string) time.Time {

	layout := "2006/01/02 15:04:05"
	//str := "2014-11-12T11:45:26.371Z"
	t, err := time.Parse(layout, str)

	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(t)
	return t
}
func ParseFloat(f string) float64 {
	if s, err := strconv.ParseFloat(f, 64); err == nil {
		return s
	}
	return 0
}

func Env(key string) string {

	// load .env file
	err := godotenv.Load(".env")

	if err != nil {
		log.Fatalf("Error loading .env file")
	}

	return os.Getenv(key)
}

func FindLastData(csvLines [][]string, node string) ([]string, error) {
	for i := len(csvLines) - 1; i >= 0; i-- {
		if csvLines[i][0] == node {
			return csvLines[i], nil
		}
	}
	return []string{}, errors.New("Not Found")
}

func canSend(node []string, timeStamps []TimeStamp) bool {

	var hasFound bool
	var lastestSent time.Time
	for _, ts := range timeStamps {
		if ts.Node == node[0] {
			hasFound = true
			lastestSent = ts.Time
			break
		}
	}

	if hasFound {
		nodeTime := ParseDateTime(node[38])
		if nodeTime.Unix() > lastestSent.Unix() {

			return true
		}
		fmt.Println(node[0], ":Waiting to send")
		return false
	}
	return true
}

func SetLastSend(node string, newTime time.Time) {

	for i := 0; i < len(timeStamps); i++ {
		if timeStamps[i].Node == node {
			timeStamps[i].Time = newTime
			return
		}
	}

	timeStamps = append(timeStamps, TimeStamp{
		Node: node,
		Time: newTime,
	})
}
