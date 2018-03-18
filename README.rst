ccm_backup_reader
=================

ccm_backup_reader is a tool to read IBM Synergy/CCM backups.

Currently it is only tested with backups from Synergy 7.2.1 (schema version ``0114``). If you're using another version, your milage may vary.

Usage
-----

Extract your backup file, usually called a file with the extension .cpk, to a directory. This will give you a lot of files, and a file
called DBDump.Z.

Then, call ``scripts/ccm_backup_to_sqlite.py``, with the first argument the path to the DBDump.Z file. This will create a file, in the
current directory, called ``DBDump.sqlite3``. This will contain the database.
