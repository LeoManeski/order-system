package main
import(
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
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

	r:=rand.New(rand.NewSource(time.Now().UnixNano()))
	nc.Subscribe("orders.ship", func(msg *nats.Msg){
		var order model.Order
		json.Unmarshal(msg.Data, &order)
		fmt.Printf("shipping order %s\n", order.ID)

		if r.Intn(2)==0{
			order.Status="shipping_failed"
			fmt.Printf("ordeer %s Shipping Failed\n", order.ID)
		}else{
			order.Status="shipped"
			fmt.Printf("order %s shipped\n", order.ID)
		}
		data, _:=json.Marshal(order)
		msg.Respond(data)
	})
	fmt.Println("shipping service listengin")
	time.Sleep(1*time.Hour)
}