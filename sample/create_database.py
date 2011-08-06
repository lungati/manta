#!/usr/bin/env python

import os

def RunCommand(command):
    print command
    os.system(command)

print "Creating SQLite database..."
database_dir_path = os.path.join(os.path.dirname(__file__), "database")
sqlite_file = os.path.join(database_dir_path, "DB.sqlite")
if os.path.exists(sqlite_file):
    os.remove(sqlite_file)

os.chdir(database_dir_path)

RunCommand('sqlite3 DB.sqlite ".read Import.sql"')

print "Done. Database file is: " + sqlite_file
