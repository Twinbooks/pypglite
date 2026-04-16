from pathlib import Path

from pypglite import PGlite
from pypglite.dbapi2 import connect


def main() -> None:
    data_dir = Path("./demo-pgdata")

    with PGlite(data_dir) as db:
        db.query("create table if not exists items (id serial primary key, name text)")
        db.query("insert into items (name) values ('native one'), ('native two')")
        print(db.query("select id, name from items order by id"))

    with connect(data_dir) as conn:
        with conn.cursor() as cur:
            cur.execute("select count(*) from items")
            print(cur.fetchone())


if __name__ == "__main__":
    main()
