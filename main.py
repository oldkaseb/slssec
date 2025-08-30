async with pool.acquire() as con:
        # Execute each DDL statement separately...
        for stmt in [s.strip() for s in SCHEMA_SQL.split(";") if s.strip()]:
            await con.execute(stmt + ";")

        # Upsert groups with parameters separately
        await con.execute("""
            INSERT INTO groups (group_type, chat_id, title)
            VALUES ('main', $1, 'souls')
            ON CONFLICT (group_type) DO UPDATE
                SET chat_id = EXCLUDED.chat_id,
                    title = EXCLUDED.title
        """, MAIN_CHAT_ID)

        await con.execute("""
            INSERT INTO groups (group_type, chat_id, title)
            VALUES ('guard', $1, 'souls guard')
            ON CONFLICT (group_type) DO UPDATE
                SET chat_id = EXCLUDED.chat_id,
                    title = EXCLUDED.title
        """, GUARD_CHAT_ID)
