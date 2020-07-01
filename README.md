# Teradata SQL Runner

The Simplest tool to run SQL scripts against Teradata without TTU (just pure python).

Thanks to [Vladimir Filimonov](https://github.com/z0diak) and [Eugeniy Balashov](https://github.com/evgeniy-balashov) for the initial implementation.

```
usage: td_sql_runner.py [-h] --repo REPO --dbparam DBPARAM [--debug]
                        [--stoponexception]

Optional arguments

optional arguments:
  -h, --help         show this help message and exit
  --repo REPO        A required repository path
  --dbparam DBPARAM  DB parameters file
  --debug            Enables debug output
  --stoponexception  Stop deployment in case of exception (default=false)

```

Your SQL script may use Jinja2 templates, make sure variables are present in dbparam file.


Using Docker image (mount the volume with `db.txt` file and `scripts` directory to `src` inside the container, or provide custom CMD line)
```shell script
> docker build -t td-sql-runner . 

> ls -R /myapp/src
db.txt  scripts

/myapp/src/scripts:
test.sql

> docker run -v /myapp/src/:/src/ td-sql-runner 
```