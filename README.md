#hive2elastic

## Installation

Run `pip3 install -e .` to install.

**Before start, there some additions to implement on hive database. follow steps below:**

1- stop hive

make sure hive processes stopped

2- Create db objects on hive database

```
CREATE TABLE __h2e_posts
(
    post_id INTEGER PRIMARY KEY
);
```

```
CREATE OR REPLACE FUNCTION __fn_h2e_posts()
  RETURNS TRIGGER AS
$func$
BEGIN   
    IF NOT EXISTS (SELECT post_id FROM __h2e_posts WHERE post_id = NEW.post_id) THEN
    	INSERT INTO __h2e_posts (post_id) VALUES (NEW.post_id);
	END IF;
	RETURN NEW;
END
$func$ LANGUAGE plpgsql;
```

```
CREATE TRIGGER __trg_h2e_posts
AFTER INSERT OR UPDATE ON hive_posts_cache
FOR EACH ROW EXECUTE PROCEDURE __fn_h2e_posts();
```

**make sure you database user has delete permission on __h2e_posts table**

3- start hive
