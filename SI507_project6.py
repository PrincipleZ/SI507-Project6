# Import statements
import psycopg2
import psycopg2.extras
import csv

# Write code / functions to set up database connection and cursor here.


def db_connect_cursor():
    try:
        db_connection = psycopg2.connect("dbname='yuanzez_507project6'")
        print("Success connecting to database")
    except:
        print("Unable to connect to the database")
        sys.exit(1)

    db_cursor = db_connection.cursor(
        cursor_factory=psycopg2.extras.RealDictCursor)

    return db_connection, db_cursor

conn, cur = db_connect_cursor()


# Write code / functions to create tables with the columns you want and
# all database setup here.
def setup_database():
    cur.execute(
        """CREATE TABLE IF NOT EXISTS "States"("ID" SERIAL UNIQUE, "Name" VARCHAR(40) UNIQUE)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS "Sites"("ID" SERIAL, "Name" VARCHAR(128) UNIQUE, "Type" VARCHAR(128), "State_ID" INT, FOREIGN KEY ("State_ID") REFERENCES "States"("ID"), "Location" VARCHAR(255), "Description" TEXT)""")
    conn.commit()
    print("Setup databse complete")
# Write code / functions to deal with CSV files and insert data into the
# database here.


def read_from_csv(filename, state_dict):
    res = []
    state = filename[:-4].title()
    state_num = state_dict[state]

    with open(filename, newline='') as f:
        reader = csv.DictReader(f)
        for i in reader:
            res.append(
                (i['NAME'],
                 i['TYPE'],
                    state_num,
                    i['LOCATION'],
                    i['DESCRIPTION']))
    return res


def insert_state(state_list):
    for i in state_list:
        cur.execute(
            """INSERT INTO "States"("Name") VALUES ('{0}') on conflict do nothing""".format(
                i.title()))
    conn.commit()
    cur.execute('SELECT "ID", "Name" from "States"')
    results = cur.fetchall()
    res = {}
    for i in results:
        res[i['Name']] = i['ID']
    return res


def insert(data):
    for i in data:
        cur.execute(
            """INSERT INTO "Sites"("Name", "Type", "State_ID", "Location", "Description") VALUES(%s, %s, %s, %s, %s) on conflict do nothing""",
            i)
    conn.commit()
# Make sure to commit your database changes with .commit() on the database
# connection.

# Write code to be invoked here (e.g. invoking any functions you wrote above)
setup_database()
state_dict = insert_state(['arkansas', 'california', 'michigan'])
ar_data = read_from_csv("arkansas.csv", state_dict)
ca_data = read_from_csv("california.csv", state_dict)
mi_data = read_from_csv("michigan.csv", state_dict)
insert(ar_data + ca_data + mi_data)

# Write code to make queries and save data in variables here.
print("---Query the database for all of the locations of the sites.---")
cur.execute('SELECT "Location" FROM "Sites"')
all_locations = cur.fetchall()
# print(all_locations)

print("---Query the database for all of the names of the sites whose descriptions include the word beautiful---")
cur.execute(
    """SELECT "Name" FROM "Sites" WHERE "Description" LIKE '%beautiful%' """)
beautiful_sites = cur.fetchall()
# print(beautiful_sites)

print("---Query the database for the total number of sites whose type is National Lakeshore---")
cur.execute(
    """SELECT count(*) FROM "Sites" WHERE "Type" = 'National Lakeshore'""")
natl_lakeshores = cur.fetchall()
# print(natl_lakeshores)

print("---Query your database for the names of all the national sites in Michigan---")
cur.execute("""SELECT "Sites"."Name" FROM "Sites" INNER JOIN "States" ON "Sites"."State_ID" = "States"."ID" WHERE "States"."Name" = 'Michigan'""")
michigan_names = cur.fetchall()
# print(michigan_names)

print("---Query your database for the total number of sites in Arkansas---")
cur.execute("""SELECT count(*) FROM "Sites" INNER JOIN "States" ON "Sites"."State_ID" = "States"."ID" WHERE "States"."Name" = 'Arkansas'""")
total_number_arkansas = cur.fetchall()
# print(total_number_arkansas)

# We have not provided any tests, but you could write your own in this
# file or another file, if you want.
