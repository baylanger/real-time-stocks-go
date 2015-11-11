package main

import (
	"encoding/json"
	"fmt"
	"github.com/anovikov1984/go/messaging"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	BOOTSTRAP_INSTANCE_SUFFIX = "-bootstrap"
	CONFIG_PATH_ENV_VAR       = "PUBNUB_STOCKS_CONFIG"
	CONFIG_FILE               = "/config.json"
	STOCKS_FILE               = "/stocks.json"
	TIME_FORMAT               = "03:04:05pm"
)

var (
	stocks        []Stock
	config        Config
	stockNames    string
	bootstrapAuth string
)

func main() {
	LoadConfig()
	SetUpChannelGroup()
	GrantPermissions()
	RunStocks()
	ServeHttp()
}

func LoadConfig() {
	configPath := os.Getenv(CONFIG_PATH_ENV_VAR)

	// Fallback to local config files
	if configPath == "" {
		configPath = "."
	}

	fmt.Printf("Line is %s", configPath)

	// Auths
	file, err := os.Open(configPath + CONFIG_FILE)
	if err != nil {
		log.Fatal(err)
	}

	config = Config{}

	decoder := json.NewDecoder(file)

	err = decoder.Decode(&config)
	if err != nil {
		log.Fatal(err)
	}

	// Stocks
	file, err = os.Open(configPath + STOCKS_FILE)
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
	bootstrapAuth = config.Keys.Auth + BOOTSTRAP_INSTANCE_SUFFIX
}

func SetUpChannelGroup() {
	errorChannel := make(chan []byte)
	successChannel := make(chan []byte)
	done := make(chan bool)

	pubnub := messaging.NewPubnub(config.Keys.Pub, config.Keys.Sub, "", "",
		false, "")

	pubnub.SetAuthenticationKey(bootstrapAuth)

	// Remove Group
	go pubnub.ChannelGroupRemoveGroup(config.StocksChannelGroup,
		successChannel, errorChannel)
	go handleResponse(successChannel, errorChannel,
		messaging.GetNonSubscribeTimeout(), done)

	<-done

	// Create it from the scratch
	go pubnub.ChannelGroupAddChannel(config.StocksChannelGroup, stockNames,
		successChannel, errorChannel)
	go handleResponse(successChannel, errorChannel,
		messaging.GetNonSubscribeTimeout(), done)

	<-done
}

func GrantPermissions() {
	errorChannel := make(chan []byte)
	successChannel := make(chan []byte)

	done := make(chan bool)

	pubnub := messaging.NewPubnub(config.Keys.Pub, config.Keys.Sub,
		config.Keys.Secret, "", false, "")

	pubnub.SetAuthenticationKey(bootstrapAuth)

	// Allow current Pubnub instance to managet the channel group
	go pubnub.GrantChannelGroup(config.StocksChannelGroup,
		false, true, config.GrantTTL,
		bootstrapAuth, successChannel, errorChannel)
	go handleResponse(successChannel, errorChannel,
		messaging.GetNonSubscribeTimeout(), done)

	<-done

	// Allow unauthorized users to subscribe to stockblast channel group
	go pubnub.GrantChannelGroup(config.StocksChannelGroup,
		true, false, config.GrantTTL,
		"", successChannel, errorChannel)
	go handleResponse(successChannel, errorChannel,
		messaging.GetNonSubscribeTimeout(), done)

	<-done

	// Unauthorized users can both read and write on chat channel
	go pubnub.GrantSubscribe(config.ChatChannel, true, true, config.GrantTTL, "",
		successChannel, errorChannel)
	go handleResponse(successChannel, errorChannel,
		messaging.GetNonSubscribeTimeout(), done)

	<-done

	// Unauthorized users can only read history
	go pubnub.GrantSubscribe(config.HistoryChannel,
		true, false, config.GrantTTL, "", successChannel, errorChannel)
	go handleResponse(successChannel, errorChannel,
		messaging.GetNonSubscribeTimeout(), done)

	<-done

	// Allow stock tickers authorized by auths.Auth key to publish to stock
	// channels
	go pubnub.GrantSubscribe(stockNames, false, true, config.GrantTTL,
		config.Keys.Auth, successChannel, errorChannel)
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

type Config struct {
	Port               string `json:"port"`
	GrantTTL           int    `json:"grant_ttl"`
	StocksChannelGroup string `json:"stocks_channel_goroup"`
	HistoryChannel     string `json:"history_channel"`
	ChatChannel        string `json:"chat_channel"`
	Keys               struct {
		Pub    string `json:"publish_key"`
		Sub    string `json:"subscribe_key"`
		Secret string `json:"secret_key"`
		Auth   string `json:"auth_key"`
	}
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
	pubnub := messaging.NewPubnub(config.Keys.Pub, config.Keys.Sub, "", "",
		false, "")
	pubnub.SetAuthenticationKey(config.Keys.Auth)

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
		Time:       time.Now().Format(TIME_FORMAT),
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

// Exposing keys for clients throught HTTP
func GetConfigsHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(
		fmt.Sprintf("{\"publish_key\": \"%s\", \"subscribe_key\": \"%s\"}",
			config.Keys.Pub, config.Keys.Sub)))
}

func ServeHttp() {
	publicPath := os.Getenv("PUBNUB_STOCKS_PUBLIC")

	// Fallback to the local public folder
	if publicPath == "" {
		publicPath = "./public"
	}

	http.Handle("/", http.FileServer(http.Dir(publicPath)))
	http.HandleFunc("/get_configs", GetConfigsHandler)

	err := http.ListenAndServe(fmt.Sprintf(":%s", config.Port), nil)
	if err != nil {
		log.Fatal(err)
	}
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
