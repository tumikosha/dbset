from dbset import connect

def test_dummy():
    # Connect to database
    # db = connect('postgresql://localhost/mydb')
    db = connect('sqlite:///db.sqlite')

    # Get table
    users = db['users']

    # Insert data
    pk = users.insert({'name': 'John', 'age': 30})

    # Find with filters
    for user in users.find(age={'>=': 18}):
        print(user)

    # Close connection
    db.close()