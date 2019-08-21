//mongoimport --host localhost --db fit5148_db --collection fire --type csv --headerline --file env3/FireData-Part1.csv

//mongoimport --host localhost --db fit5148_db --collection climate --type csv --headerline --file env3/ClimateData-Part1.csv

//1
show dbs
use fit5148_db
show collections

//2
db.climate.createIndex({"Date":1})
db.climate.find({"Date":"2017-12-15"}).pretty()

//3
db.fire.createIndex({"Surface Temperature (Celcius)":1})

db.fire.find({"Surface Temperature (Celcius)":{$gte:65,$lte:100}},{"Latitude":1,"Longitude":1,"Confidence":1,"Surface Temperature (Celcius)":1}).pretty()


db.fire.aggregate([{$lookup: {from:"climate",localField:"Date",foreignField:"Date",as:"Climate"}},{$out:"Task4"}])

db.Task4.createIndex({"Date":1})

//4
db.Task4.find({"Date":{"$in":["2017-12-15","2017-12-16"]}},{"Date":1,"Surface Temperature (Celcius)":1,"Climate.Air Temperature(Celcius)":1,"Climate.Relative Humidity":1,"Climate.Max Wind Speed":1}).pretty()

//5
db.Task4.createIndex({"Confidence":1})
db.Task4.find({"Confidence":{$gte:80,$lte:100}},{"Datetime":1,"Climate.Air Temperature(Celcius)":1,"Surface Temperature (Celcius)":1,"Confidence":1}).pretty()

//6
db.fire.aggregate([{$sort:{"Surface Temperature (Celcius)":-1}},{$out:"Task6"}])
db.Task6.find().limit(10).pretty()

//7
db.Task6.aggregate([{$group:{_id:"$Date","total_number":{$sum:1}}},{$sort:{"_id":-1}}])

//8
db.Task6.aggregate([{$group:{_id:"$Date","Average":{$avg:"$Surface Temperature (Celcius)"}}},{$sort:{"_id":-1}}])


