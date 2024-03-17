from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
from kafka import KafkaProducer
import json


app = FastAPI()


producer = KafkaProducer(bootstrap_servers=['kafka:29092'],
                         api_version=(0, 10, 0),
                         value_serializer=lambda x: json.dumps(x)
                         .encode('utf-8'))


class Item(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    price: float


items = {}


@app.post("/items/")
async def create_item(item: Item):
    if item.id in items:
        raise HTTPException(status_code=400, detail="Item already exists")
    items[item.id] = item
    producer.send(topic='test.events',
                  value={"action": "create", "item": item.dict()})
    return item


@app.put("/items/{item_id}", response_model=Item)
async def update_item(item_id: int, item: Item):
    if item_id not in items:
        raise HTTPException(status_code=404, detail="Item not found")
    items[item_id] = item
    producer.send(topic='test.events',
                  value={"action": "update", "item": item.dict()})
    return item


@app.delete("/items/{item_id}")
async def delete_item(item_id: int):
    if item_id not in items:
        raise HTTPException(status_code=404, detail="Item not found")
    del items[item_id]
    producer.send(topic='test.events',
                  value={"action": "delete", "item_id": item_id})
    return {"result": "Item deleted"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
