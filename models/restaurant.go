
package models

import (
    "time"
    "go.mongodb.org/mongo-driver/bson/primitive"
)

type Res struct {
    ID           primitive.ObjectID `json:"id" bson:"_id"`
    Address      Address            `json:"address" bson:"address,required"`
    Borough      string             `json:"borough",bson:"borough,omitempty"`
    Cuisine      string             `json : "cuisine" , bson : "cuisine,omitempty"`
    Grades       []Grade            `json:"grades" bson:"grade,required"`
    Name         string             `json:"name" bson:"name,required"`
    RestaurantID string             `json:"restaurant_id" bson:"RestaurantID,required"`
}

type Address struct {
    Building string    `json:"building" bson:"Building,required"`
    Coord    []float64 `json:"coord" bson:"coord,required"`
    Street   string    `json:"street" bson:"Street,required"`
    Zipcode  string    `json:"zipcode" bson:"Zipcode,required"`
}

type Grade struct {
    Date  time.Time `json:"date" bson:"Date,required"`
    Grade string    `json:"grade" bson:"Grade,required"`
    Score int       `json:"score" bson:"Score,required"`
}