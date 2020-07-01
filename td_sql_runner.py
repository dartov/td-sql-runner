import logging
import argparse
import os.path
import re
import teradatasql
import sys

from jinja2 import Template

def isString(value):
    return isinstance(value, str)


def sqlsplit(sql, delimiter=";"):
    tokens = re.split("(--|'|\n|" + re.escape(delimiter) + "|\"|/\*|\*/)",
                      sql if isString(sql) else delimiter.join(sql))
    statement = []
    inComment = False
    inLineComment = False
    inString = False
    inQuote = False
    for t in tokens:
        if not t:
            continue
        if inComment:
            if t == "*/":
                inComment = False
        elif inLineComment:
            if t == "\n":
                inLineComment = False
        elif inString:
            if t == '"':
                inString = False
        elif inQuote:
            if t == "'":
                inQuote = False
        elif t == delimiter:
            sql = "".join(statement).strip()
            if sql:
                yield sql
            statement = []
            continue
        elif t == "'":
            inQuote = True
        elif t == '"':
            inString = True
        elif t == "/*":
            inComment = True
        elif t == "--":
            inLineComment = True
        statement.append(t)
    sql = "".join(statement).strip()
    if sql:
        yield sql


def load_tokens(token_file):
    myvars = {}
    logging.debug("Loading tokens from {}".format(token_file))
    with open(token_file, encoding='utf-8-sig') as param_file:
        for line in param_file:
            line = line.strip()
            if "=" in line:
                name, value = line.split('=')
                myvars[name] = str(value).strip()
            elif line == "" or line.startswith("#"):
                continue
            else:
                logging.warning('{} tokens file has string with = sign missing: {}'.format(token_file, line))
    return myvars


def read_n_run_files(dir, db_param_file, stoponexception):

    logging.debug("-------------------------------------------")
    logging.debug("           Deploying Statements            ")
    logging.debug("-------------------------------------------")

    no_parsing_file = [".proc", ".macro"]
    myvars = load_tokens(db_param_file)

    listFiles = list()
    for root, dirs, files in os.walk(dir):
        for f in files:
            listFiles.append(os.path.join(root, f))
            listFiles.sort()

    with teradatasql.connect(None,
                             host=myvars.get("host"),
                             user=myvars.get("user"),
                             password=myvars.get("password"),
                             tmode=myvars.get("tmode", "DEFAULT"),
                             logmech=myvars.get("logmech", "TDNEGO")) as con:
        with con.cursor() as cur:
            try:
                for i in listFiles:
                    extension = os.path.splitext(i)[1]

                    file = open(i, encoding='utf-8-sig')
                    tmpl = Template(file.read().lstrip())
                    sql_statement = tmpl.render(myvars)
                    file_name = file.name
                    file.close()

                    logging.info("Deploying file {}".format(file_name))
                    try:
                        if extension in no_parsing_file:
                            logging.debug("Deploying without parsing because extension is on the list")
                            try:
                                cur.execute(sql_statement)
                            except:
                                tt, value, tb = sys.exc_info()
                                raise value
                        else:
                            logging.debug("Deploying with parsing because extension on the list")
                            cnt = 0
                            for x in sqlsplit(sql_statement):
                                try:
                                    cnt = cnt + 1
                                    logging.info("Running statement #{}".format(cnt))
                                    logging.debug("Statement #{} text: {}".format(cnt, x))
                                    result = cur.execute(x)
                                    logging.info("Statement #{} affected {} rows".format(cnt, result.rowcount))
                                except:
                                    tt, value, tb = sys.exc_info()
                                    if stoponexception:
                                        raise value
                                    else:
                                        logging.exception("Deployment exception:".format(str(value)))
                                        pass
                    except:
                        tt, value, tb = sys.exc_info()
                        if stoponexception:
                            raise value
                        else:
                            logging.exception("Deployment exception:".format(str(value)))
                            pass
            except:
                tt, value, tb = sys.exc_info()
                if stoponexception:
                    raise value
                else:
                    pass


def exception_logger(type, value, tb):
    logging.exception("Uncaught exception: {0}".format(str(value)))
    exit(1)


if __name__ == '__main__':

    #CLI params settings
    parser = argparse.ArgumentParser(description="Optional arguments", epilog="")
    parser.add_argument("--repo", type=str, required=True, help="A required repository path")
    parser.add_argument('--dbparam', type=str, required=True, help='DB parameters file')
    parser.add_argument("--debug", help="Enables debug output", required=False, default=False, action="store_true")
    parser.add_argument("--stoponexception", required=False, action="store_true", default=False, help="Stop deployment in case of exception (default=false)")
    args = parser.parse_args()

    repodir = args.repo
    db_param_file = args.dbparam
    stoponexception = args.stoponexception

    logging_level = logging.DEBUG if args.debug else logging.INFO
    logging.getLogger('').handlers = []
    logging.basicConfig(level=logging_level, format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        handlers=[logging.StreamHandler()])

    sys.excepthook = exception_logger

    logging.info("------------------------------------------------------------")
    logging.info("------------- Teradata SQL Runner Tool starting -------------")
    logging.info("------------------------------------------------------------")

    logging.info("Parameters given: ")
    for arg in vars(args):
        logging.info("   " + str(arg) + ": " + str(getattr(args, arg)))

    read_n_run_files(repodir, db_param_file, stoponexception)
