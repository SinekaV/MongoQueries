package main

import (
	
	"fmt"
    //"mongodb/models"
	"mongodb/services"

	//"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	
)
var (//mongoclient will act as a global variable
	mongoClient *mongo.Client
)
func main(){
	// fmt.Println("MongoDB successfully connected...")
	// products,_:=services.FindRes()
	// for _,product:=range products{
	// 	fmt.Println(product)
	// }


	fmt.Println("MongoDB successfully connected...")
	products,_:=services.FindTrans()
	for _,product:=range products{
		fmt.Println(product)
	}
	// products:=[]interface{}{
	// 	models.Product{ID:primitive. NewObjectID(),Name:"Oneplus",Price:100000,Description:"Budget Phone"},
	// 	models.Product{ID:primitive. NewObjectID(),Name:"Vivo",Price:100000,Description:"China Phone"}}
	// services.InsertproductList(products)
	//client:=config.ConnectDatabase()
	//collection:=config
}