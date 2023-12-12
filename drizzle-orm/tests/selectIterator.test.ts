import { Pool } from 'pg';
import { describe, it } from 'vitest';
import { varchar } from '~/pg-core';
import { drizzle } from "~/node-postgres";
import { pgTable } from '~/pg-core/table';
import { SelectAsyncGenerator, SelectIterator } from '~/select-iterator';
import { sql } from '~/sql';
import exp from 'constants';

interface TestEntity {
    a: number;
    b: string;
}

class InternalIterator<T>  {
    cnt = 0;
    async next(): Promise<IteratorResult<TestEntity, TestEntity>> {
        try {
            return {
                done: this.cnt >= 5,
                value: { a: this.cnt, b: '2' }
            }
        } finally {
            this.cnt++;
        }
    }

    return(value: TestEntity | PromiseLike<TestEntity>): Promise<IteratorResult<TestEntity, TestEntity>> {
        return value instanceof Promise ? value : Promise.resolve(value);
    }

    throw(e: any): Promise<IteratorResult<TestEntity, TestEntity>> {
        return Promise.reject(e);
    }

    [Symbol.asyncIterator]() {
        return this;
    }
}

class TestIterator extends SelectIterator<TestEntity> {
    iterator(): SelectAsyncGenerator<TestEntity> {
        return new InternalIterator<TestEntity>();
    }
}


export const pgOpenseaListings = pgTable("OpenseaListings", {
    market_place: varchar("market_place").primaryKey(),
    slug: varchar("slug").primaryKey(),
});

describe.concurrent('select-iterator', () => {
    it('via iterator()', async ({ expect }) => {
        const ti = new TestIterator();
        let cnt = 0;
        for await (const i of ti.iterator()) {
            // test the type assertion because the
            // type of i is inferred as unknown
            expect(i.a).toEqual(cnt)
            expect(i.b).toEqual('2')
            cnt++;
        }
        expect(cnt).toBe(5);
    });

    // it('via implicit', async ({ expect }) => {
    //     const ti = new TestIterator();
    //     let cnt = 0;
    //     for await (const i of ti) {
    //         // test the type assertion because the
    //         // type of i is inferred as unknown
    //         expect(i.a).toEqual(cnt)
    //         expect(i.b).toEqual('2')
    //         cnt++;
    //     }
    //     expect(cnt).toBe(5);
    // });

    (process.env['DATABASE_URL'] ? it: it.skip)('via db and iterator', async ({ expect }) => {
        const pool = new Pool({
            connectionString: process.env['DATABASE_URL']!
        });
        const db = drizzle(pool);
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const mo = db
            .select({ market_place: pgOpenseaListings.market_place, bla: sql`'4'` })
            .from(pgOpenseaListings)
            .limit(126);
        const rows = await mo.execute();
        expect(rows.length).toBe(126);
        expect(rows[0]!.market_place).toEqual("opensea");
        expect(rows[0]!.bla).toEqual("4");

        let cnt = 0;
        for await (const row of mo.iterator()) {
            expect(row.market_place).toEqual("opensea");
            expect(row.bla).toEqual("4");
            cnt++;
        }
        expect(cnt).toBe(126);
        pool.end();
    });
});