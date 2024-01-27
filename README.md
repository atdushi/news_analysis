# Анализ публикуемых новостей

![Logo](./images/logo2.png)

## Постановка задачи

Создать ETL-процесс формирования витрин данных для анализа публикаций новостей.

<details>
  <summary>Подробное описание задачи</summary>

- Разработать скрипты загрузки данных в 2-х режимах:
    - Инициализирующий – загрузка полного слепка данных источника
    - Инкрементальный – загрузка дельты данных за прошедшие сутки

- Организовать правильную структуру хранения данных
    - Сырой слой данных
    - Промежуточный слой
    - Слой витрин

В качестве результата работы программного продукта необходимо написать скрипт, который формирует витрину данных следующего содержания

- Общая часть витрин:
  - Суррогатный ключ категории
  - Название категории
- Витрина 1:
  - Общее количество новостей из всех источников по данной категории за все время
  - Количество новостей данной категории для каждого из источников за все время
- Витрина 2:
  - Общее количество новостей из всех источников по данной категории за последние сутки
  - Количество новостей данной категории для каждого из источников за последние сутки
- Витрина 3:
  - Среднее количество публикаций по данной категории в сутки
  - День, в который было сделано максимальное количество публикаций по данной категории
- Витрина 4:
  - Количество публикаций новостей данной категории по дням недели

**Дополнение**:

Т.к. в разных источниках названия и разнообразие категорий могут отличаться, вам необходимо привести все к единому виду.

**Источники**:

- https://lenta.ru/rss/
- https://www.vedomosti.ru/rss/news
- https://tass.ru/rss/v2.xml

</details>

## План реализации

![График1](images/diagram.drawio.png)

Приведём все к единому виду. Для этого возьмём за основу категории из датасета [lenta.ru](https://github.com/yutkin/Lenta.Ru-News-Dataset/releases). Обучим модель и по заголовкам новостей будем определять их категорию.

Oozie запускает RSS-парсер один раз в день. Каждой новости сопоставляется категория, и новость отправляется в Kafka.

В зависимости от режима загрузки (переменная **data_loading_mode** в скрипте **parser.py**), Spark либо берёт новости из Kafka, либо из HBase, делает дополнительные преобразования и сохраняет их в HDFS, откуда на них смотрит ClickHouse.

Итоговая витрина строится в ClickHouse по запросу.

Моё окружение:

- [Hadoop 3.2.1](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation) - нужен для организации озера данных на основе HDFS. 
- [Oozie 5.2.1](https://oozie.apache.org/docs/5.2.1/DG_QuickStart.html) - простой планировщик. Ещё [инструкция](https://www.cloudduggu.com/oozie/installation/) по установке.
- [Kafka 3.3.1](./kafka/) - no comments.
- [Spark 3.3.1](https://spark.apache.org/downloads.html) - быстрая обработка данных, лучше чем MapReduce.
- [ClickHouse 22.11.2](https://clickhouse.com/docs/ru/getting-started/install/) - можно настроить на папку в HDFS как в Hive Metastore. Быстро делает выборки.
- [HBase 2.5.2](https://hbase.apache.org/book.html#quickstart) - масштабируемая база данных. Ещё [инструкция](https://kontext.tech/article/628/spark-connect-to-hbase) по установке. Скачивать лучше [hbase-2.5.2-hadoop3-bin.tar.gz](https://dlcdn.apache.org/hbase/2.5.2/hbase-2.5.2-hadoop3-bin.tar.gz), чтобы были все необходимые библиотеки.
- [Apache Drill 1.20.3](https://drill.apache.org/) - SQL запросы к HBase

## HDFS

### Инициализация

```bash
hdfs namenode -format

start-dfs.sh

hdfs dfsadmin -safemode leave

hadoop fs -mkdir -p oozie/apps/ssh

hadoop fs -mkdir /news

# если нужно остановить, то stop-dfs.sh
```

### Структура

```bash
├── news        # сырые данные в виде json файлов
└── user
  └── {user.name}
    └── oozie   # файлы с задачами для oozie
```

## Oozie

Скопируем файлы **coordinator.xml** и **workflow.xml** из папки [oozie](./oozie/) в HDFS папку **hdfs://localhost:9000/user/${user.name}/oozie/apps/ssh** и локально запустим job.

### Инициализация

```bash
hadoop fs -put coordinator.xml oozie/apps/ssh
hadoop fs -put workflow.xml oozie/apps/ssh

oozied.sh start

# если нужно остановить, то oozied.sh stop
```
### Запуск

```bash
oozie job -oozie http://localhost:11000/oozie -config ./job.properties -run
```

Запущенную задачу можно увидеть по адресу http://localhost:11000/oozie/ во вкладке Coordinator Jobs

![oozie](./images/oozie.png)

## Kafka

Kakfa развернём с помощью [docker-compose.yml](./kafka/docker-compose.yml). Работать будем с топиком **foobar**. Некоторые команды, которые можно запустить на образе kafka из docker:

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --list

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic foobar --from-beginning
```

## HBase

### Инициализация HDFS

```bash
./bin/start-all.sh
./bin/start-dfs.sh
./bin/start-yarn.sh
./bin/hdfs dfsadmin -safemode leave
```

### Запуск HBase

```bash
$ ./bin/start-hbase.sh
$ ./bin/hbase thrift start
$ ./bin/hbase shell

hbase:001:0> create 'news', 'cf'
```

### Коннектор

В качестве коннектора возьмём [HBase connector](https://github.com/apache/hbase-connectors/tree/master/spark). 

Для начала его нужно собрать. Соберём с учётом нужных версий:

```bash
mvn -Dspark.version=3.3.1 -Dscala.version=2.12.17 -Dscala.binary.version=2.12 -Dhbase.version=2.5.2 -Dhadoop.profile=3.0 -Dhadoop-three.version=3.2.1 -DskipTests -Dcheckstyle.skip -U clean package
```

## Drill

Проверим таблицу news с помощью Apache Drill.

https://drill.apache.org/docs/querying-hbase/

```bash
./bin/drill-embedded
```

Web UI находится по адресу http://localhost:8047/

На вкладке storage включим hbase.

На вкладке query введём запрос:

```sql
select 
    CONVERT_FROM(news.cf.category, 'UTF8') as category,
    CONVERT_FROM(news.cf.title, 'UTF8') as title,
    CONVERT_FROM(news.cf.site, 'UTF8') as site,
    CONVERT_FROM(news.cf.pub_date, 'UTF8') as pub_date,
    CONVERT_FROM(news.cf.day_of_week, 'UTF8') as day_of_week
from hbase.news limit 10;
```

<details>
  <summary>Screenshots</summary>
  <img src="/pubnews/images/drill.png"/>
  <img src="/pubnews/images/drill_query"/>
  <img src="/pubnews/images/drill_result"/>
</details>

## Spark

В зависимости от переменной **DATA_LOADING_MODE** Spark будет работать либо в режиме начальной загрузки из HBase таблицы **news**, либо в режиме непрерывного стриминга из Kafka топика **foobar**.

Сохраняет данные в папку **hdfs:///news** каждые 10 секунд.

Параллельно данные выводятся в консоль:

![spark](./images/spark.png)

## ClickHouse

- **news** - таблица с новостями
- **news_topic** - Kafka топик
- **news_target** - таблица из Kafka топика
- **vw_news** - материализованное представление

![ER](images/er.png)

Скрипт иницилизации [init.sql](./clickhouse/init.sql)

Скрипты итоговых витрин [result.sql](./clickhouse/result.sql)

## Результаты разработки

В результате был создан проект со следующей структурой:

```bash
├── clickhouse      # скрипты для ClickHouse
├── docs            # документация
├── images          # диаграммы, картинки
├── kafka           # скрипты для Kafka
├── lenta.ru        # модель для предсказания категории
├── oozie           # задачи для Oozie
├── rss             # парсер новостей
└── spark           # исходный код для Spark
```
<details>
  <summary>Примеры витрин данных</summary>

- Витрины 1, 2:

  ![dataset12](./images/dataset_1_2.png)

- Витрина 3:

  ![dataset3](./images/dataset_3.png)

- Витрина 4:

  ![dataset4](./images/dataset_4.png)

</details>
