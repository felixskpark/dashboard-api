from flask import Flask, Response
from flask_restful import abort, Api, Resource
from pymongo import MongoClient
from bson import ObjectId, json_util
import os
from flask.json.provider import DefaultJSONProvider

# Flask 앱 생성
app = Flask(__name__)
api = Api(app)

# MongoDB 연결
MONGO_URI = os.getenv("MONGO_URI")
mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client.get_database("apiTest")

# 커스텀 JSON Provider (ObjectId 직렬화 지원)
class CustomJSONProvider(DefaultJSONProvider):
  def default(self, obj):
    if isinstance(obj, ObjectId):
      return str(obj)
    return super().default(obj)

app.json = CustomJSONProvider(app)

# 숫자 변환 안전 함수
def safe_int(val):
  try:
    return int(str(val).replace(",",""))
  except (TypeError, ValueError):
    return 0

# ====================API 리소스====================
class StoreSales(Resource):
  def get(self):
    col = mongo_db["storeSales"]
    results = list(col.find({}, {"_id":0}))
    if not results:
      abort(404, message="No StoreSales data found")
    return Response(
      json_util.dumps(results, ensure_ascii=False),
      mimetype="application/json; charset=utf-8"
    )

class DeptSales(Resource):
  def get(self):
    col = mongo_db["storeSales"]
    results = list(col.find({}, {"_id":0}))
    if not results:
      abort(404, message="No DeptSales data found")
    response_data = {}
    for doc in results:
      if doc.get("DISP_IDX") is not None:
        doc["DISP_IDX"] = f"M{doc['DISP_IDX']}"
      final_results = {k: (str(v) if isinstance(v, ObjectId) else v) for k, v in doc.items() if k !="_id"}
      key = final_results.get("DISP_IDX")
      if key:
        response_data[key] = final_results
    return Response(json_util.dumps(response_data, ensure_ascii=False),
                    mimetype="application/json; charset=utf-8")

class SaleCompare(Resource):
  def get(self):
    col = mongo_db["storeSales"]
    results = list(col.find({}, {"_id":0}))
    if not results:
      abort(404, message="No SaleCompare data found")
    transformed = [{"DEPT_NM": item.get("DEPT_NM"),
                    "SALE_BUDGET": safe_int(item.get("SALE_BUDGET")),
                    "SALE_AMT": safe_int(item.get("SALE_AMT"))}
                    for item in results]
    return Response(json_util.dumps(transformed, ensure_ascii=False),
                    mimetype="application/json; charset=utf-8")

class TimeSales(Resource):
  def get(self):
    col = mongo_db["salesForecast"]
    results = list(col.find({}, {"_id":0}))
    if not results:
      abort(404, message="No TimeSales data found")
    return Response(json_util.dumps(results, ensure_ascii=False),
                    mimetype="application/json; charset=utf-8")

# ====================라우팅====================
api.add_resource(StoreSales, "/sales")
api.add_resource(DeptSales, "/dept")
api.add_resource(SaleCompare, "/compare")
api.add_resource(TimeSales, "/time")

# ====================실행====================
if __name__ == "__main__":
  app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=True)