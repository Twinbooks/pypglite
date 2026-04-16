from pglite import PGliteServer


def main() -> None:
    with PGliteServer(db="memory://") as db:
        db.query(
            """
            create table items (
              id serial primary key,
              name text not null
            )
            """
        )
        db.query("insert into items (name) values ('hello'), ('world')")
        result = db.query("select id, name from items order by id")
        print(result.rows)


if __name__ == "__main__":
    main()
