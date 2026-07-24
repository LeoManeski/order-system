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

	_, err = nc.Subscribe("orders.validate", func(msg *nats.Msg){
		var order model.Order
		if err:=json.Unmarshal(msg.Data, &order); err!=nil{
			fmt.Printf("failed to unmarshal order:%v\n", err)
			return
		}
		fmt.Printf("Validating order %s: %s ($%d)\n", order.ID, order.Item, order.Amount)

		if order.Item==""||order.Amount<=0{
			order.Status="validation_failed"
			fmt.Printf("Order: %s INVALID\n", order.ID)
		}else{
			order.Status="validated"
			fmt.Printf("Order %s VALID\n", order.ID)
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
		log.Fatal("failed to subscribe:", err)
	}
	
	fmt.Println("validator service listeninng")
	time.Sleep(1*time.Hour)
}