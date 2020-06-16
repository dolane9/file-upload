from flask import Flask, flash, request, redirect, url_for, send_from_directory, session
from flask_sqlalchemy import SQLAlchemy
from psycopg2 import errors
from werkzeug.utils import secure_filename
import json
import os
import psycopg2

UPLOAD_FOLDER = '\\uploads'
ALLOWED_EXTENSIONS = {'csv', 'txt'}

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

DB_URI = (f"postgresql://{os.environ['fu_pg_user']}:{os.environ['fu_pg_pw']}"
          f"@{os.environ['fu_pg_host']}/{os.environ['fu_pg_db']}")


# Helper functions
def simple_query(query, commit=False, get_result=True):
    """Execute an SQL query on the database.

    Parameters
    ----------
    query : str
        The SQL query to get results for
    commit : bool
        Whether the query should be committed on the database after execution
    get_result : bool
        Whether the result of the query should be requested

    Returns
    -------
    list of tuples
        Each row returned from executing the query is represented by a tuple in
        the list. Each column value is a separate element in the tuple. None is
        returned if `get_result` is set to False.
    """
    # Setup connection - details stored in environment variables
    conn = psycopg2.connect("dbname={} user={} password={} host={}".format(
        os.environ['fu_pg_db'], os.environ['fu_pg_user'],
        os.environ['fu_pg_pw'], os.environ['fu_pg_host']))

    # Retrieve data
    cur = conn.cursor()
    cur.execute(query)

    if commit:
        conn.commit()

    if get_result:
        result = cur.fetchall()
    else:
        result = None

    cur.close()
    conn.close()

    return result


def execute_query_with_values(values_query, values, post_values_query='',
                              values_format='(%s,%s)'):
    """Formats an SQL query containing the `VALUES` command and executes on the
    database.

    This function is intended to be used when multiple rows are to be inserted
    into the database. The values supplied are added to the query in a format
    that is understood by the database.

    Parameters
    ----------
    values_query : str
        The SQL query which contains the `VALUES` command
    values : tuple of (str,)
        The raw values to be used by the `VALUES` command
    post_values_query : str
        Any SQL to be appended to `values_query` (the default is a blank string
        which means no additional SQL is appended to the query)
    values_format : str
        The format the values should follow in the SQL query

    Returns
    -------
    tuple of (str, `flask.wrappers.Response`)
        str: The query sent to the database to execute.
        `flask.wrappers.Response`: A response containing the error while
            executing the query on the database (None if no error occurred).
    """
    # Setup connection to database
    conn, cur = con_to_app_db()

    # Construct query
    values_psql = values_to_psql(cur, values, values_format)
    query = values_query + ' ' + values_psql + '' + post_values_query

    # Catch common database related errors and return message to caller
    try:
        cur.execute(query)
    except errors.ForeignKeyViolation as e:
        # Foreign Key violated, most likely trying to insert team or
        # team_member, return error message
        print(e)
        results = '{"psycopg2.errors.ForeignKeyViolation": "' + str(e) + '"}'
        response = app.response_class(
            response=json.dumps(results, indent=4, sort_keys=True, default=str),
            status=422,
            mimetype='application/json'
        )
        return query, response
    except errors.NotNullViolation as e:
        # A null value was supplied for a column that does not allow null
        # values - most likely user did not supply league_id or user_id
        print(e)
        results = '{"psycopg2.errors.NotNullViolation": "' + str(e) + '"}'
        response = app.response_class(
            response=json.dumps(results, indent=4, sort_keys=True, default=str),
            status=422,
            mimetype='application/json'
        )
        return query, response

    conn.commit()
    cur.close()
    conn.close()

    return query, None


def con_to_app_db():
    """Create a connection to the database and opens a cursor that uses this
    connection.

    Returns
    -------
    tuple of (`psycopg2.extensions.connection`, `psycopg2.extensions.cursor`,)
        `psycopg2.extensions.connection`: The connection object to the database.
        `psycopg2.extensions.cursor`: A cursor opened using the connection in
            the first element of this tuple.
    """
    # Setup connection - details stored in environment variables
    conn = psycopg2.connect("dbname={} user={} password={} host={}".format(
        os.environ['fu_pg_db'], os.environ['fu_pg_user'],
        os.environ['fu_pg_pw'], os.environ['fu_pg_host']))

    cur = conn.cursor()

    return conn, cur


def values_to_psql(cursor, values, format_='(%s,%s)'):
    """Returns a query string for the `VALUE` containing the `values` supplied
     in the `f`ormat supplied.

    Parameters
    ----------
    cursor : `psycopg2.extensions.connection`
        A cursor opened on a connection to a database
    values : tuple of (str,)
        The raw values to be used by the `VALUES` command
    format_ : str
        The format of the values for the SQL query

    Returns
    -------
    str
        The values supplied formatted to fit into a `VALUE` command in an SQL
        query.
    """
    return ','.join(cursor.mogrify(format_, x).decode("utf-8") for x in values)


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        # Check if the post request has the file part
        if 'file' not in request.files:
            return redirect(request.url)
        file = request.files['file']
        # If user does not select file, browser also submit an empty part without filename
        if file.filename == '':
            return redirect(request.url)
        if file and allowed_file(file.filename):
            # Save file to local folder
            filename = secure_filename(file.filename)
            nf_path = os.getcwd() + app.config['UPLOAD_FOLDER']
            nf_path2 = os.path.join(nf_path, filename)
            file.save(nf_path2)

            # Import file to table in db
            q = (f"COPY public.iris(sepal_length,sepal_width,petal_length,petal_width,species) "
                 f"FROM '{nf_path2}' DELIMITER ',' CSV HEADER;")
            print(q)
            res = simple_query(q, commit=True, get_result=False)
            print(res)

            print('end')
            return redirect(url_for('upload_file'))
    return '''
    <!doctype html>
    <title>Upload new File</title>
    <h1>Upload new File</h1>
    <form method=post enctype=multipart/form-data>
      <input type=file name=file>
      <input type=submit value=Upload>
    </form>
    '''


@app.route('/uploads/<filename>')
def uploaded_file(filename):
    print('in uf func')
    print(os.getcwd() + app.config['UPLOAD_FOLDER'])
    return send_from_directory(os.getcwd() + app.config['UPLOAD_FOLDER'], filename)


if __name__ == '__main__':
    app.run()
