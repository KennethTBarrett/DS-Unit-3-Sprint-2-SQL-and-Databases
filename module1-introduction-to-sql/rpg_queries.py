import sqlite3

conn = sqlite3.connect('rpg_db.sqlite3')

c = conn.cursor()

# Here we will be finding the number of characters
char_query = "SELECT COUNT(character_id) FROM charactercreator_character;"
number_chars = c.execute(char_query).fetchone()
print(f'Number of Characters: {number_chars[0]}')

# How many of each subclass?

# Mage
mage_query = "SELECT COUNT(character_ptr_id) FROM charactercreator_mage;"
number_mages = c.execute(mage_query).fetchone()
print(f'Number of Characters in Subclass - Mage: {number_mages[0]}')

# Necromancer
necro_query = "SELECT COUNT(mage_ptr_id) FROM charactercreator_necromancer;"
number_necro = c.execute(necro_query).fetchone()
print(f'Number of Characters in Subclass - Necromancer: {number_necro[0]}')

# Cleric
cleric_query = "SELECT COUNT(character_ptr_id) FROM charactercreator_cleric;"
number_cleric = c.execute(cleric_query).fetchone()
print(f'Number of Characters in Subclass - Cleric: {number_cleric[0]}')

# Fighter
fighter_query = "SELECT COUNT(character_ptr_id) FROM charactercreator_fighter;"
number_fighter = c.execute(fighter_query).fetchone()
print(f'Number of Characters in Subclass - Fighter: {number_fighter[0]}')

# Thief
thief_query = "SELECT COUNT(character_ptr_id) FROM charactercreator_thief;"
number_thief = c.execute(thief_query).fetchone()
print(f'Number of Characters in Subclass - Thief: {number_thief[0]}')


# How many non-weapon vs. weapon items?

# Items
items_query = "SELECT COUNT(item_id) FROM armory_item;"
number_items = c.execute(items_query).fetchone()

print(f'Number of Non-Weapon Items: {number_items[0]}')

# Weapons
weapons_query = "SELECT COUNT(item_ptr_id) FROM armory_weapon;"
number_weapons = c.execute(weapons_query).fetchone()

print(f'Number of Weapons: {number_weapons[0]}')

# What is the total number of items?
total_items = (number_weapons[0] + number_items[0])
print(f'Total Number of Items: {total_items}')

# How many items does each character have? (First 20 rows)
first_twenty_items_query = ("SELECT SUM(item_id) FROM " +
                            "charactercreator_character_inventory " +
                            "GROUP BY character_id LIMIT 20;")
first_twenty_items = c.execute(first_twenty_items_query).fetchall()

print(f"Each Character's Number of Items (First 20 Characters):")
for i in range(0, 20):
    print(f'Character {i + 1}: {first_twenty_items[i][0]}')

# How many weapons does each character have? (First 20 rows)
first_twenty_weap_query = ("SELECT SUM(item_ptr_id) FROM " +
                           "charactercreator_character_inventory, "
                           "armory_weapon WHERE item_id = item_ptr_id " +
                           "GROUP BY character_id LIMIT 20;")
number_weap_per = c.execute(first_twenty_weap_query).fetchall()

print(f"Each Character's Number of Weapons (First 20 Characters):")
for i in range(0, 20):
    print(f'Character {i + 1}: {number_weap_per[i][0]}')

# On average, how many items does each character have?
avg_items_query = ("SELECT AVG(item_id) FROM (SELECT DISTINCT item_id, " +
                   "character_id FROM charactercreator_character_inventory);")
avg_item_per = c.execute(avg_items_query).fetchone()

print(f'Each Character has an Average of {round(avg_item_per[0])} items (entire database).')

# On average, how many weapons does each character have?
avg_weap_query = ("SELECT AVG(item_ptr_id) FROM (SELECT DISTINCT " +
                  "item_ptr_id, character_id FROM " +
                  "charactercreator_character_inventory, armory_weapon " +
                  "WHERE item_id = item_ptr_id);")
avg_weap_per = c.execute(avg_weap_query).fetchone()

print(f'Each Character has an Average of {round(avg_weap_per[0])} weapons (entire database).')
