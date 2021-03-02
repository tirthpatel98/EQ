# Steps to run the code

1. `docker-compose up -d`
2. `docker exec -it <master_container_name> /bin/bash`
3. `cd /tmp/data`
4. `pip install -r requirements.txt`
5. `spark-submit DataSample.py`
