from etl.db import get_conn

def main():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT DATABASE();")
    print("Banco atual:", cur.fetchone()[0])

    cur.execute("SHOW FULL TABLES;")
    print("\nTabelas e Views:")
    for name, table_type in cur.fetchall():
        print(f"- {name} ({table_type})")

    conn.close()

if __name__ == "__main__":
    main()
