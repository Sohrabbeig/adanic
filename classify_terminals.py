from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client.adanalas
merchant_category = db.merchant_category
terminals = db.terminals

s = []  # merchants with two categories

l = list(merchant_category.aggregate(
    [
        {"$group": {"_id": {"merchant": "$merchant"}, "category": {"$addToSet": "$category"}}},
        {"$match": {"category": {"$size": 2}}}
    ]
))

for i in l:
        s.append([i["_id"]["merchant"], i["category"][0]])

for j in s:
    merchant_category.update_many({"merchant": j[0]}, {"$set": {"category": j[1]}})

print(s)
