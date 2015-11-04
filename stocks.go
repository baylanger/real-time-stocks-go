package main

import (
	"encoding/json"
	"fmt"
	"github.com/anovikov1984/go/messaging"
	"log"
	"math"
	"math/rand"
	"os"
	"strings"
	"time"
)

const (
	CHAT_CHANNEL    = "stock-chat"
	CHANNEL_GROUP   = "stockblast"
	HISTORY_CHANNEL = "MSFT"
	GRANT_TTL       = 1
)

var (
	stocks        []Stock
	auths         Auths
	stockNames    string
	bootstrapAuth string
)

func main() {
	LoadConfig()
	SetUpChannelGroup()
	GrantPermissions()
	RunStocks()
}

func LoadConfig() {
	// Auths
	file, err := os.Open("auths.json")
	if err != nil {
		log.Fatal(err)
	}

	auths = Auths{}

	decoder := json.NewDecoder(file)

	err = decoder.Decode(&auths)
	if err != nil {
		log.Fatal(err)
	}

	// Stocks
	file, err = os.Open("stocks.json")
	if err != nil {
		log.Fatal(err)
	}

	decoder = json.NewDecoder(file)

	err = decoder.Decode(&stocks)
	if err != nil {
		log.Fatal(err)
	}

	// Stock names
	stockNamesArray := make([]string, 0, len(stocks))
	for _, v := range stocks {
		stockNamesArray = append(stockNamesArray, v.Name)
	}

	stockNames = strings.Join(stockNamesArray, ",")

	// Authentication key for management instance inside GrantPermissions()
	// function
	bootstrapAuth = auths.Auth + "-bootstrap"
}

func SetUpChannelGroup() {
	errorChannel := make(chan []byte)
	successChannel := make(chan []byte)
	done := make(chan bool)

	pubnub := messaging.NewPubnub(auths.Pub, auths.Sub, auths.Secret, "",
		false, "")

	pubnub.SetAuthenticationKey(bootstrapAuth)

	// Remove Group
	go pubnub.ChannelGroupRemoveGroup(CHANNEL_GROUP, successChannel, errorChannel)
	go handleResponse(successChannel, errorChannel,
		messaging.GetNonSubscribeTimeout(), done)

	<-done

	// Create it from the scratch
	go pubnub.ChannelGroupAddChannel(CHANNEL_GROUP, stockNames,
		successChannel, errorChannel)
	go handleResponse(successChannel, errorChannel,
		messaging.GetNonSubscribeTimeout(), done)

	<-done
}

func GrantPermissions() {
	errorChannel := make(chan []byte)
	successChannel := make(chan []byte)

	done := make(chan bool)

	pubnub := messaging.NewPubnub(auths.Pub, auths.Sub, "", "",
		false, "")

	pubnub.SetAuthenticationKey(bootstrapAuth)

	// Allow current Pubnub instance to managet the channel group
	go pubnub.GrantChannelGroup(CHANNEL_GROUP, false, true, GRANT_TTL,
		bootstrapAuth, successChannel, errorChannel)
	go handleResponse(successChannel, errorChannel,
		messaging.GetNonSubscribeTimeout(), done)

	<-done

	// Allow unauthorized users to subscribe to stockblast channel group
	go pubnub.GrantChannelGroup(CHANNEL_GROUP, true, false, GRANT_TTL,
		"", successChannel, errorChannel)
	go handleResponse(successChannel, errorChannel,
		messaging.GetNonSubscribeTimeout(), done)

	<-done

	// Unauthorized users can both read and write on chat channel
	go pubnub.GrantSubscribe(CHAT_CHANNEL, true, true, GRANT_TTL, "",
		successChannel, errorChannel)
	go handleResponse(successChannel, errorChannel,
		messaging.GetNonSubscribeTimeout(), done)

	<-done

	// Unauthorized users can only read history
	go pubnub.GrantSubscribe(HISTORY_CHANNEL, true, false, GRANT_TTL, "",
		successChannel, errorChannel)
	go handleResponse(successChannel, errorChannel,
		messaging.GetNonSubscribeTimeout(), done)

	<-done

	// Allow stock tickers authorized by auths.Auth key to publish to stock
	// channels
	go pubnub.GrantSubscribe(stockNames, false, true, GRANT_TTL,
		auths.Auth, successChannel, errorChannel)
	go handleResponse(successChannel, errorChannel,
		messaging.GetNonSubscribeTimeout(), done)

	<-done
}

func RunStocks() {
	done := make(chan bool)

	for _, stock := range stocks {
		go func(st Stock) {
			fmt.Printf("Starting up %s\n", st.Name)
			st.RunCycle()
		}(stock)
	}

	<-done
}

type Auths struct {
	Pub    string `json:"publish_key"`
	Sub    string `json:"subscribe_key"`
	Secret string `json:"secret_key"`
	Auth   string `json:"auth_key"`
}

type Stock struct {
	Name         string
	InitialPrice float64
	CurrentPrice float64
	MinTrade     int
	MaxTrade     int
	Volatility   int
	MaxDelta     int
}

func (stock *Stock) RunCycle() {
	cycle := make(chan bool)
	i := 0
	pubnub := messaging.NewPubnub(auths.Pub, auths.Sub, auths.Secret, "",
		false, "")
	pubnub.SetAuthenticationKey(auths.Auth)

	for {
		go stock.UpdateValuesAndPublish(pubnub, cycle)
		<-cycle
		fmt.Printf("Iteration #%d", i)
		i++
	}
}

func (stock *Stock) UpdateValuesAndPublish(pubnub *messaging.Pubnub,
	cycle chan bool) {
	if stock.CurrentPrice == 0 {
		stock.CurrentPrice = stock.InitialPrice
	}

	rand.Seed(int64(time.Now().Nanosecond()))

	change := float64(rand.Intn(stock.Volatility)-stock.Volatility/2) / 100
	stock.CurrentPrice = stock.CurrentPrice + float64(change)
	delta := stock.CurrentPrice - stock.InitialPrice
	percentage := Roundn((1-stock.InitialPrice/stock.CurrentPrice)*100, 2)
	vol := Randn(stock.Volatility, 1000) * 10

	streamMessage := StreamMessage{
		Time:       time.Now().Format("03:04:05pm"),
		Price:      fmt.Sprintf("%.2f", stock.CurrentPrice),
		Delta:      fmt.Sprintf("%.2f", delta),
		Percentage: fmt.Sprintf("%.2f", percentage),
		Vol:        vol}

	if math.Abs(percentage) > float64(stock.MaxDelta)/100 {
		stock.CurrentPrice = stock.InitialPrice
	}

	errorChannel := make(chan []byte)
	successChannel := make(chan []byte)
	done := make(chan bool)

	go pubnub.Publish(stock.Name, streamMessage, successChannel, errorChannel)
	go handleResponse(successChannel, errorChannel,
		messaging.GetNonSubscribeTimeout(), done)

	sleep := Randn(stock.MinTrade, stock.MaxTrade)
	time.Sleep(time.Duration(sleep) * time.Microsecond)

	cycle <- <-done
}

type StreamMessage struct {
	Time       string `json:"time"`
	Price      string `json:"price"`
	Delta      string `json:"delta"`
	Percentage string `json:"perc"`
	Vol        int    `json:"vol"`
}

// Handlers
func handleResponse(successChannel, errorChannel chan []byte, timeout uint16,
	finishedChannel chan bool) {

await:
	for {
		select {
		case success, ok := <-successChannel:
			if !ok {
				break await
			}

			fmt.Printf("%s\n", success)
			break await
		case failure, ok := <-errorChannel:
			if !ok {
				break await
			}

			fmt.Printf("ERROR: %s\n", failure)
			break await
		case <-time.After(time.Second * 3):
			fmt.Println("Request timeout")
			break await
		}
	}

	finishedChannel <- true
}

// Helpers
func Roundn(val float64, precision int) float64 {
	shift := math.Pow(10, float64(precision))
	return math.Floor(val*shift+.5) / shift
}

func Randn(min int, max int) int {
	return rand.Intn(max-min) + min
}
