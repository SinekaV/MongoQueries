package services

import (
	"context"
	"encoding/json"
	"fmt"
	"mongodb/config"
	//"mongodb/models"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	//"go.mongodb.org/mongo-driver/mongo/options"
)

/*Function name:TranscationContext
parameters:none
return :mongo collection
purpose:Connecting to the database and return the mongo colection*/


func TransactionContext()*mongo.Collection{
	client,_:= config.ConnectDatabase()
	
    return config.GetCollection(client,"sample_analytics","transactions")

}

// func FindTrans() ([]*models.Transactions, error) {
//     ctx, _ := context.WithTimeout(context.Background(), 100*time.Second)
//    // filter := bson.D{}//M-map;D-Document
//    //filter := bson.M{"transaction_count":bson.D{{"$gt",85},{"$lt",90}}}//nested queries --map
//    filter := bson.M{"transaction_count":bson.D{{"$gt",85},{"$lt",90}}}
// 	options:=options.Find().SetSort(bson.D{{"transaction_count",1}}).SetSkip(30).SetLimit(10)
//     //result, err := ProductCont().Find(ctx, filter, options.Find().SetLimit(10))
// 	result, err := TransactionContext().Find(ctx, filter, options)
//     if err != nil {
//         fmt.Println(err.Error())
//         return nil, err
//     } else {
//         var trans []*models.Transactions
//         for result.Next(ctx) {//looping.. cursor
//             post := &models.Transactions{}
//             err := result.Decode(post)//unmarshall (any to 'go' structure)
//             if err != nil {
//                 return nil, err
//             }

//             trans = append(trans, post)
//         }
//         if err := result.Err(); err != nil {
//             return nil, err
//         }
//         return trans, nil
//     }

// }
func FetchAggregatedTransactions(){
    ctx,_:=context.WithTimeout(context.Background(),100*time.Second)
    matchStage:=bson.D{{"$match",bson.D{{"account_id",278866}}}}
    groupStage:=bson.D{
        {
            "$group",bson.D{
                {"_id","$account_id"},
                {"total_count",bson.D{{"$sum","$transaction_count"}}},
            }}}
            result,err:=TransactionContext().Aggregate(ctx,mongo.Pipeline{matchStage,groupStage})//pipeline of queries
            if err!=nil{
                fmt.Println(err.Error())

            }else{
                var showsWithInfo []bson.M
                if err=result.All(ctx,&showsWithInfo);err!=nil{
                    panic(err)
                }
                formatted_data,err:=json.MarshalIndent(showsWithInfo,""," ")
                if err!=nil{
                    fmt.Println(err.Error())
                }else{
                    fmt.Println(string(formatted_data))
                }
            }
}

func UpdateTransaction(initialValue int,newValue int)(*mongo.UpdateResult,error){
    ctx,_:=context.WithTimeout(context.Background(),100*time.Second)
    filter:=bson.D{{"account_id",initialValue}}
    update:=bson.D{{"$set",bson.D{{"account_id",newValue}}}}
    result,err:=TransactionContext().UpdateOne(ctx,filter,update)
    if err!=nil{
        fmt.Println(err.Error())
        return nil,err
    }
    return result,nil
}

