package models

import (
	//"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Transactions struct{
	ID primitive.ObjectID `json:"id" bson:"_id,required"`
	AccID int `json:"account_id" bson:"account_id,required"`
	Count int `json:"count" bson:"transaction_count"`
	StartDate primitive.DateTime `json:"startdate" bson:"bucket_start_date"`
	EndDate primitive.DateTime `json:"bucket_end_date" bson:"bucket_end_date"`
	//Transaction [] InnerTransaction `json:"transaction" bson:"transactions"`

}

type InnerTransaction struct{
	Date primitive.DateTime `json:"date" bson:"date"`
	Amount int `json:"amount" bson:"amount"`
	TransactionCode string `json:"code" bson:"transaction_code"`
	Symbol string `json:"symbol" json:"symbol"`
	Price string `json:"price" bson:"price"`
	Total string `json:"tot" bson:"total"` 

}
