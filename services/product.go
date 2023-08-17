package services

import (
	"context"
	"fmt"
	"mongodb/config"
	"mongodb/models"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func ProductContext() *mongo.Collection{
   client,_:= config.ConnectDatabase()
	
    return config.GetCollection(client,"inventory","products")
}

func InsertProduct(product models.Product) (*mongo.InsertOneResult,error){
    //var product models.Product
    //product.Name="iphone"
   //   product.Price=115000
   // product.Description="It is an awesome phone"
    ctx,_:= context.WithTimeout(context.Background(),10*time.Second)
    //client,_:=config.ConnectDataBase()
    //var productCollection *mongo.Collection = config.GetCollection(client,"inventory","products")
    //result,err := productCollection.InsertOne(ctx,product)
    result,err :=ProductContext().InsertOne(ctx,product)
    if err !=nil{
        fmt.Println(err)
    }
    fmt.Println(result)
    return result, nil
}

func InsertproductList(products []interface{})(*mongo.InsertManyResult,error){
    ctx,_:=context.WithTimeout(context.Background(),10*time.Second)
    result,err:= ProductContext().InsertMany(ctx,products)
    if err !=nil{
        fmt.Println(err)
    }
    fmt.Println(result)
    return result, nil
}

func FindProducts()([]*models.Product,error){
    ctx,_ := context.WithTimeout(context.Background(),10*time.Second)
    filter :=bson.D{{"name","samsung"}}//covert into mongodb understandable code
    result,err := ProductContext().Find(ctx,filter)//fetch all the products
    if err !=nil{
        fmt.Println(err.Error())
        return nil,err
    }
        //fmt.Println(result)
        //build the array of products for the cursor that we received
        var products []*models.Product
        for result.Next(ctx){
            product :=&models.Product{}
            err:=result.Decode(product)
            if err!=nil{
                return nil,err
            }
            products=append(products,product)
        }
        if err :=result.Err();err !=nil{
            return nil,err
        }
        if len(products) ==0{
           return []*models.Product{},nil
        }
        //return products,nil
    
    return products,nil
}
