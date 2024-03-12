from client import Database

database = Database()
cursor = database.cursor()

if input("Are you sure you want to delete? (Y/n) > ") != "Y":
    quit()

cursor.execute("DELETE FROM GREENHOUSE_DATA WHERE TIMESTAMP > ?", (170449744308,))
database.commit()