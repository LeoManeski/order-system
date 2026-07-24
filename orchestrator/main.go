package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"order-system/model"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
)
var ctx=context.Background()
var rdb *redis.Client

func main(){
	rdb=redis.NewClient(&redis.Options{
		Addr:"localhost:6379",
	})
	if _, err:=rdb.Ping(ctx).Result(); err!=nil{
		log.Fatal("redis failed:", err)
	}
	fmt.Println("connected to redis")
	nc, err:=nats.Connect(nats.DefaultURL)
	if err!=nil{
		log.Fatal(err)
	}
	defer nc.Close()
	fmt.Println("connected to NATS")
	fmt.Println("Orchestrator listening")

	nc.Subscribe("orders.create", func(msg *nats.Msg){
		var order model.Order
		json.Unmarshal(msg.Data, &order)
		fmt.Printf("new order: %s - %s ($%d)\n", order.ID, order.Item, order.Amount)
	
		//validating
		order.Status="validating"
		rdb.HSet(ctx, "order:"+order.ID, "status",order.Status)

		data, _:=json.Marshal(order)
		resp, err:=nc.Request("orders.validate", data, 5*time.Second)
		if err!=nil{
			fmt.Printf("validation timout for %s\n", order.ID)
			return
		}
		json.Unmarshal(resp.Data, &order)
		rdb.HSet(ctx, "order:"+order.ID, "status:",order.Status)
		fmt.Printf("order %s: %s\n", order.ID, order.Status)
		
		//paying
		if order.Status!="validated"{
			fmt.Printf("order %s failed validating\n", order.ID)
			return
		}
		
		order.Status="paying"
		rdb.HSet(ctx, "order:"+order.ID, "status",order.Status)

		data, _=json.Marshal(order)
		resp, err=nc.Request("orders.pay", data, 5*time.Second)
		if err!=nil{
			fmt.Printf("payment timout for %s\n", order.ID)
			return
		}
		json.Unmarshal(resp.Data, &order)
		rdb.HSet(ctx, "order:"+order.ID, "status:",order.Status)
		fmt.Printf("order %s: %s\n", order.ID, order.Status)

		//shipping
		if order.Status!="paid"{
			fmt.Printf("order %s payment failed\n", order.ID)
			return
		}		
		order.Status="shipping"
		rdb.HSet(ctx, "order:"+order.ID, "status", order.Status)

		data, _=json.Marshal(order)
		resp, err=nc.Request("orders.ship", data, 5*time.Second)
		if err!=nil{
			fmt.Printf("Shipping timeout for %s\n", order.ID)
			return
		}
		json.Unmarshal(resp.Data, &order)
		rdb.HSet(ctx, "order:"+order.ID, "status",order.Status)
		fmt.Printf("order %s: %s\n", order.ID, order.Status)

		if order.Status=="shipped"{
			fmt.Printf("order %s completed\n", order.ID)
		}else{
			fmt.Printf("order %s shipping failed, need compensation!\n", order.ID)

			data, _ = json.Marshal(order)
			resp,err=nc.Request("orders.pay.compensate", data, 5*time.Second)
			if err!=nil{
				fmt.Printf("Compensation timout for %s\n", order.ID)
				return
			}
			json.Unmarshal(resp.Data, &order)
			order.Status="COmpensated"
			rdb.HSet(ctx, "order:"+order.ID, "status",order.Status)
			fmt.Printf("order %s is Compensated, money refunded\n", order.ID)
		}
	})

	select{}
}