package main

import(
	"encoding/json"
	"fmt"
	"log"
	"order-system/model"
	"time"
	"github.com/nats-io/nats.go"
)
func main(){
	nc, err:=nats.Connect(nats.DefaultURL)
	if err!=nil{
		log.Fatal(err)
	}
	defer nc.Close()

	_, err = nc.Subscribe("orders.pay", func(msg *nats.Msg){
		var order model.Order
		if err:=json.Unmarshal(msg.Data, &order); err!=nil{
			fmt.Printf("failed to unmarshal order:%v\n", err)
			return
		}

		fmt.Printf("Processing payment for order %s: %d\n", order.ID, order.Amount)
		order.Status="paid"
		fmt.Printf("order %s paid\n", order.ID)

		data, err:=json.Marshal(order)
		if err!=nil{
			fmt.Printf("fialed to marshal order:%v\n", err)
			return
		}
		if err:=msg.Respond(data); err!=nil{
			fmt.Printf("failed to respond:%v\n", err)
		}
	})
	if err!=nil{
		log.Fatal("failed to pay:", err)
	}

	_, err=nc.Subscribe("orders.pay.compensate", func (msg *nats.Msg)  {
		var order model.Order
		if err:=json.Unmarshal(msg.Data, &order); err!=nil{
			fmt.Printf("failed to unmarshal order:%v\n", err)
			return
		}

		fmt.Printf("refunding order: %s: %d\n", order.ID, order.Amount)
		order.Status="refunded"

		data, err:=json.Marshal(order)
		if err!=nil{
			fmt.Printf("failed to respond:%v\n", err)
		}
		if err:=msg.Respond(data); err!=nil{
			fmt.Printf("failed to respond:%v\n", err)
		}
	})
	if err!=nil{
		log.Fatal("failed to compensate:", err)
	}

	fmt.Println("payment service listening")
	time.Sleep(1*time.Hour)
}