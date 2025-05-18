# Big-Data-HW-12

Author: Davyd Ilnytskyi

---

# Design

Data Fetching | Python Server -> Kafka Queue -> Spark Server -> Kafka Queue -> Spark -> Cassandra -> WebServer

### Data fetching - Python Server

Main reason:
- Easy to use

### Data transfering - Kafka Queues

Main reason:
- Easy to use

### Data processing - Spark Node
Main reason:
- Easy to use
- Easy to work with streaming data

### Webserver



----
# Work demonstration

1. http://localhost:7080/agg_one
![alt text](./images/agg-one.png)
2. http://localhost:7080/agg_two
![alt text](./images/agg-two.png)
3. http://localhost:7080/agg_three
![alt text](./images/agg-three.png)
4. http://localhost:7080/adhoc_one
![alt text](./images/adhoc-one.png)
5. http://localhost:7080/adhoc_two?user_id=123
![alt text](./images/adhoc-two.png)
6. http://localhost:7080/adhoc_three?specified_domain=id.wikipedia.org
![alt text](./images/adhoc-three.png)
7. http://localhost:7080/adhoc_four?page_id=165480052
![alt text](./images/adhoc-four.png)
8. http://localhost:7080/adhoc_five?start_date=2025-05-17&end_date=2025-5-18
![alt text](./images/adhoc-five.png)