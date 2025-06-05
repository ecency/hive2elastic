# hive2elastic

hive2elastic synchronises Hive posts & comments to an elasticsearch index and keeps it updated.

Check https://github.com/ecency/h2e new indexer for further info.

## Before start

Some additional database objects have to be created on hive's database.

**Follow steps below:**

1- Stop hive. Make sure all hive processes stopped.

2- Create database objects on hive's database.

```
CREATE TABLE __h2e_posts
(
    post_id INTEGER PRIMARY KEY
);
```

```
INSERT INTO __h2e_posts (post_id) SELECT id FROM hive_posts;
```

```
CREATE OR REPLACE FUNCTION __fn_h2e_posts()
  RETURNS TRIGGER AS
$func$
BEGIN   
    IF NOT EXISTS (SELECT post_id FROM __h2e_posts WHERE post_id = NEW.id) THEN
    	INSERT INTO __h2e_posts (post_id) VALUES (NEW.id);
	END IF;
	RETURN NEW;
END
$func$ LANGUAGE plpgsql;
```

```
CREATE TRIGGER __trg_h2e_posts
AFTER INSERT OR UPDATE ON hive_posts
FOR EACH ROW EXECUTE PROCEDURE __fn_h2e_posts();
```

3- Start hive

*Make sure database credentials that you use has delete permission on __h2e_posts table*

**Elasticsearch** 

You can find detailed installation instructions [here](https://www.elastic.co/guide/en/elasticsearch/reference/current/install-elasticsearch.html)


## Installation

```
$ git clone https://github.com/esteemapp/hive2elastic
$ cd hive2elastic
$ pip3 install -e .
```

### Alternative
```
$ python3 setup.py build
$ python3 setup.py install
```

## Configuration

You can configure hive2elastic by these arguments/environment variables:


|	Argument	|	Environment Variable	|	Description | Default|
|	--------	|	--------	|	--------	|  --------	|  
|	--db-url	|	DB_URL	|	Connection string for hive database	| -- | 
|	--es-url	|	ES_URL	|	Elasticsearch server address	| -- | 
|	--es-index	|	ES_INDEX	|	 Index name on elasticsearch	| hive_posts | 
|	--es-type	|	ES_TYPE	|	 Type name on elasticsearch index | hive_posts | 
|	--bulk-size	|	BULK_SIZE	|	 Number of documents to index in a single loop | 500 | 
|	--max-workers	|	MAX_WORKERS	|  Max workers for document preparation process | 2 | 


## Example configuration and running

```
export DB_URL=postgresql://username:passwd@localhost:5432/hive 
export ES_URL=http://localhost:9200/
export BULK_SIZE=2000                 
export MAX_WORKERS=4

hive2elastic_post
```
