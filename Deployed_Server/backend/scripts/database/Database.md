# Importing `tcna_cancer_db.sql` into a Database

This guide explains how to load the `tcna_cancer_db.sql` file into your database system. The file contains all the necessary SQL commands to create and populate the **Cancer Database**.

---

## Prerequisites

Before proceeding, ensure you have:

- **MariaDB** installed and running.
- Access to the MariaDB client (`mysql` command).
- The `tcna_cancer_db.sql` file available in your current working directory.
- A database user with privileges to create and modify databases.

---

## 1. Import into MariaDB

If you haven’t already created a database to import into, run:

### 1. Create a new database (optional)

```bash
mysql -u root -p -e "CREATE DATABASE tcna_cancer_db;"
```

### 2. Import the SQL File

Once the database exists, import the SQL dump file:
```bash
mysql -u root -p tcna_cancer_db < tcna_cancer_db.sql
```
root → your MariaDB username

tcna_cancer_db → name of the target database

tcna_cancer_db.sql → path to your SQL dump file

You’ll be asked to enter your password. The process may take a few moments depending on the size of the file.

### 3. Verify the Import

Log in to MariaDB:

```bash
mysql -u root -p tcna_cancer_db
```

Then inside MariaDB, run:

```bash
SHOW TABLES;
```

If you see the expected tables, the import was successful.
