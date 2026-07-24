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

	nc.Subscribe("orders.validate", func(msg *nats.Msg){
		var order model.Order
		json.Unmarshal(msg.Data, &order)
		fmt.Printf("Validating order %s: %s ($%d)\n", order.ID, order.Item, order.Amount)

		if order.Item==""||order.Amount<=0{
			order.Status="validation_failed"
			fmt.Printf("Order: %s INVALID\n", order.ID)
		}else{
			order.Status="validated"
			fmt.Printf("Order %s VALID\n", order.ID)
		}
		data, _:=json.Marshal(order)
		msg.Respond(data)
	})
	fmt.Println("validator service listeninng")
	time.Sleep(1*time.Hour)
}