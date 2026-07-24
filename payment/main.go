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

	nc.Subscribe("orders.pay", func(msg *nats.Msg){
		var order model.Order
		json.Unmarshal(msg.Data, &order)

		fmt.Printf("Processing payment for order %s: %d\n", order.ID, order.Amount)
		order.Status="paid"
		fmt.Printf("order %s paid\n", order.ID)

		data, _:=json.Marshal(order)
		msg.Respond(data)
	})
	nc.Subscribe("orders.pay.compensate", func (msg *nats.Msg)  {
		var order model.Order
		json.Unmarshal(msg.Data, &order)

		fmt.Printf("refunding order: %s: %d\n", order.ID, order.Amount)
		order.Status="refuded"

		data, _:=json.Marshal(order)
		msg.Respond(data)
	})
	fmt.Println("payment service listening")
	time.Sleep(1*time.Hour)
}