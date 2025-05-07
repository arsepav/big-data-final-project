#!/bin/bash

bash DownloadScript.sh

if [ $? -ne 0 ]; then
  echo "ERROR: Download failed!"
  exit 1
fi

python3 build_projectdb.py


if [ $? -ne 0 ]; then
  echo "ERROR: Python script failed!"
  exit 1
fi

hdfs dfs -rm -r -f /user/team27/project/warehouse/books
hdfs dfs -rm -r -f /user/team27/project/warehouse/reviews

sqoop import \
  --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team27_projectdb \
  --username team27 \
  --password "$(head -n 1 ~/secrets/password.psql.pass)" \
  --compression-codec=snappy \
  --compress \
  --as-avrodatafile \
  --query 'SELECT title, description, authors::VARCHAR, image, preview_link, publisher, published_date, info_link, categories::VARCHAR, ratings_count FROM books WHERE $CONDITIONS' \
  --split-by title \
  --target-dir /user/team27/project/warehouse/books \
  --m 1


sqoop import \
  --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team27_projectdb \
  --username team27 \
  --password "$(head -n 1 ~/secrets/password.psql.pass)" \
  --compression-codec=snappy \
  --compress \
  --as-avrodatafile \
  --warehouse-dir /user/team27/project/warehouse \
  --table reviews \
  --m 1


rm -f ~/data/books_data.csv ~/data/Books_rating.csv ~/data/reviews_filtered.csv ~/data/books_processed.csv
echo "stage 1 completed"
