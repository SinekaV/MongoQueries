package models

import "go.mongodb.org/mongo-driver/bson/primitive"

type Product struct{
	Id primitive.ObjectID `json:"id" bson:"_id"`
	Name string `json:"name" bson:"name,reqired"`
	Price float64 `json:"price" bson:"price,reqired"`
	Description string `json:"description" bson:"description,omitempty"`

}