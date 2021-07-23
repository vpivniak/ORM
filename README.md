# ORM
pip3 install -r requirements.txt
docker exec -it database bash
docker-compose up -d
docker cp schema.sql database:/schema.sql
psql -h localhost -d orm_base -U user
psql -h localhost -p 5433 -d orm_base -U user -f schema.sql