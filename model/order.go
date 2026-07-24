package model
type Order struct{
	ID string `json:"id"`
	Item string `json:"item"`
	Amount int `json:"amount"`
	Status string `json:"status"`
}
