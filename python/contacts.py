import sys
import sqlite3
from pathlib import Path
from datetime import datetime


class Contacts:
    def __init__(self, db_path):
        self.db_path = db_path
        if not db_path.exists():
            print("Migrating db")
            connection = sqlite3.connect(db_path)
            cursor = connection.cursor()
            cursor.execute(
                """
                CREATE TABLE contacts(
                  id INTEGER PRIMARY KEY,
                  name TEXT NOT NULL,
                  email TEXT NOT NULL
                )
              """
            )
            connection.commit()
            cursor.execute("""CREATE UNIQUE INDEX index_contacts_email ON contacts(email)""")
            connection.commit()
        self.connection = sqlite3.connect(db_path)
        self.connection.row_factory = sqlite3.Row

    def insert_contacts(self, contacts):
        print("Inserting contacts ...")
        # TODO
        cursor = self.connection.cursor()
        i = 0
        for contact in contacts:
            cursor.execute(
                """
                INSERT INTO contacts(name, email) VALUES (?, ?)
                """,
                contact
            )
            i += 1
            if i == 10000:
                i = 0
                self.connection.commit()

        self.connection.commit()

    def get_name_for_email(self, email):
        print("Looking for email", email)
        cursor = self.connection.cursor()
        start = datetime.now()
        cursor.execute(
            """
            SELECT * FROM contacts
            WHERE email = ?
            """,
            (email,),
        )
        row = cursor.fetchone()
        end = datetime.now()

        elapsed = end - start
        print("query took", elapsed.microseconds / 1000, "ms")
        if row:
            name = row["name"]
            print(f"Found name: '{name}'")
            return name
        else:
            print("Not found")

    def get_last_contact_id(self):
        cursor = self.connection.cursor()
        cursor.execute("""SELECT * FROM contacts ORDER BY id DESC LIMIT 1;""")
        row = cursor.fetchone()
        if row:
            return row["id"]
        return 0


def yield_contacts(num_contacts, last_contact_id):
    # TODO: Generate a lot of contacts
    # instead of just 3
    for i in range(last_contact_id+1, num_contacts + 1):
        yield (f"name-{i}", f"email-{i}@domain.tld")


def main():
    num_contacts = int(sys.argv[1])
    db_path = Path("contacts.sqlite3")
    contacts = Contacts(db_path)
    last_contact_id = contacts.get_last_contact_id()
    contacts.insert_contacts(yield_contacts(last_contact_id+num_contacts, last_contact_id))
    charlie = contacts.get_name_for_email(f"email-{num_contacts}@domain.tld")


if __name__ == "__main__":
    main()
