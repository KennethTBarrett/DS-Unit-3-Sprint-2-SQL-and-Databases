# Working with MongoDB has been different in the following ways:
# 1. Functional vs. Relational approach, feels more 'programmatic'
# 2. MongoDB clearly requires a stronger train of logistics in development

# I can see MongoDB being significantly easier in connecting, however
# I can also see it being significantly more difficult programmatically,
# and there definitely has to be a stronger sense of logic in the individual
# developer, as well as a strong chain of logistics for the entire
# development team.

import pymongo

client = pymongo.MongoClient("mongodb+srv://admin:PasswordSNIP" +
                             "@cluster0-dc2kq.mongodb.net/test?" +
                             "retryWrites=true&w=majority")
db = client.test

# Count how many documents
db.test.count_documents({'x': 1})

# Insert a document
db.test.insert_one({'x': 1})

# Count how many documents again
db.test.count_documents({'x': 1})

# Find one matching this data
db.test.find_one({'x': 1})

# Find with cursor
curs = db.test.find({'x': 1})

# Tell us where the documents are
list(curs)

# Practicing insert_many

first_doc = {
    'favorite animal': ['Dog', 'Cat']
}

second_doc = {
    'favorite animal': 'Snakes',
    'favorite color': 'Cyan'
}

third_doc = {
    'favorite animal': 'Red Panda'
}

db.test.insert_many([first_doc, second_doc, third_doc])

# Did it work?
list(db.test.find())

# Make more documents
more_docs = []
for i in range(10):
    doc = {'even': i % 2 == 0}
    doc['value'] = i
    more_docs.append(doc)

db.test.insert_many(more_docs)

# Find just even objects.
list(db.test.find({'even': False}))

# Let's find the line with Red Panda
list(db.test.find({'favorite animal': 'Red Panda'}))

# Try updating an entry
db.test.update_one({'value': 3},
                   {'$inc': {'value': 5}})

# Check if it's updated.
list(db.test.find())

# Update many
db.test.update_many({'even': True},
                    {'$inc': {'value': 100}})
list(db.test.find({'even': True}))

# Delete many
db.test.delete_many({'even': False})
list(db.test.find())

rpg_character = (1, "King Kyle", 10, 3, 0, 0, 0)  # To turn into a dictionary

# Wrap into a simple dictionary so that the insert_one method works
db.test.insert_one({'rpg_character': rpg_character})

list(db.test.find())

db.test.insert_one({
    'sql_id': rpg_character[0],
    'name': rpg_character[1],
    'hp': rpg_character[2],
    'level': rpg_character[3]
})

list(db.test.find())
