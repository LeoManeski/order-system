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
	_, err=nc.Subscribe("orders.ship", func(msg *nats.Msg){
		var order model.Order
		if err:=json.Unmarshal(msg.Data, &order); err!=nil{
			fmt.Printf("failed to unmarshal order:%v\n", err)
			return
		}
		fmt.Printf("shipping order %s\n", order.ID)

		if r.Intn(2)==0{
			order.Status="shipping_failed"
			fmt.Printf("ordeer %s Shipping Failed\n", order.ID)
		}else{
			order.Status="shipped"
			fmt.Printf("order %s shipped\n", order.ID)
		}
		data, err:=json.Marshal(order)
		if err!=nil{
			fmt.Printf("failed to marshal order: %v\n", err)
			return
		}
		if err:=msg.Respond(data); err!=nil{
			fmt.Printf("failed to respond: %v\n", err)
		}
	})
	if err!=nil{
		log.Fatal("failed to ship", err)
	}
	fmt.Println("shipping service listengin")
	time.Sleep(1*time.Hour)
}