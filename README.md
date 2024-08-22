## Weather collector

### To run separately in terminal, install pre-requisite dependencies
From root project folder
```
pip install -r requirements.txt

python3 src/scheduler/scheduler.py
python3 src/processor/processor.py
python3 src/api/api.py
```


for running test 
```
 python3 -m pytest -v
```

### To run with containers

```
docker-compose up --build
```



---
**Reminder**

For manual execution, run mongodb container before and export MONGO_DATABASE_URL with connection string

---