const capturedQueries: string[] = []

jest.mock('trino-client', () => {
  const mockQuery = jest.fn().mockImplementation((sql: string) => {
    capturedQueries.push(sql)
    return Promise.resolve({
      [Symbol.asyncIterator]: async function* () {
        yield {
          data: [['1.0.0']],
          columns: [{ name: '_col0', type: 'varchar' }]
        }
      }
    })
  })

  return {
    Trino: {
      create: jest.fn().mockReturnValue({ query: mockQuery })
    },
    BasicAuth: jest.fn().mockImplementation((user, pass) => ({ type: 'basic', username: user, password: pass })),
  }
})

import fs from 'fs'
import os from 'os'
import path from 'path'
import { Trino as TrinoNodeClient } from 'trino-client'
import { TrinoClient } from '@commercial/backend/lib/db/clients/trino'
import { IDbConnectionServer } from '@/lib/db/backendTypes'
import { IDbConnectionDatabase } from '@/lib/db/types'

function makeServer(overrides: Partial<IDbConnectionServer['config']> = {}): IDbConnectionServer {
  return {
    db: {},
    config: {
      client: 'trino',
      host: 'localhost',
      port: 8080,
      user: 'testuser',
      password: null,
      readOnlyMode: false,
      osUser: 'testuser',
      ssh: null,
      sslCaFile: null,
      sslCertFile: null,
      sslKeyFile: null,
      sslRejectUnauthorized: false,
      ssl: false,
      domain: null,
      socketPath: null,
      socketPathEnabled: false,
      ...overrides,
    },
  } as IDbConnectionServer
}

function makeDatabase(): IDbConnectionDatabase {
  return {
    database: 'postgresql',
    connected: false,
    connecting: false,
    namespace: null,
  }
}

describe('TrinoClient SSL configuration (bug #3695)', () => {
  let tmpDir: string
  let caFile: string
  let certFile: string
  let keyFile: string

  beforeAll(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'trino-test-'))
    caFile = path.join(tmpDir, 'ca.pem')
    certFile = path.join(tmpDir, 'cert.pem')
    keyFile = path.join(tmpDir, 'key.pem')
    fs.writeFileSync(caFile, 'fake-ca')
    fs.writeFileSync(certFile, 'fake-cert')
    fs.writeFileSync(keyFile, 'fake-key')
  })

  afterAll(() => {
    fs.rmSync(tmpDir, { recursive: true })
  })

  beforeEach(() => {
    jest.clearAllMocks()
  })

  it('should pass ssl options to Trino.create() when ssl is true', async () => {
    const server = makeServer({ ssl: true })
    const client = new TrinoClient(server, makeDatabase())
    await client.connect()

    const createCall = (TrinoNodeClient.create as jest.Mock).mock.calls[0][0]
    expect(createCall.ssl).toBeDefined()
  })

  it('should use https protocol when ssl is true', async () => {
    const server = makeServer({ ssl: true })
    const client = new TrinoClient(server, makeDatabase())
    await client.connect()

    const createCall = (TrinoNodeClient.create as jest.Mock).mock.calls[0][0]
    expect(createCall.server).toContain('https')
  })

  it('should pass ssl cert/key/ca files when configured', async () => {
    const server = makeServer({
      ssl: true,
      sslCaFile: caFile,
      sslCertFile: certFile,
      sslKeyFile: keyFile,
    })
    const client = new TrinoClient(server, makeDatabase())
    await client.connect()

    const createCall = (TrinoNodeClient.create as jest.Mock).mock.calls[0][0]
    expect(createCall.ssl).toBeDefined()
    expect(createCall.ssl.ca).toBeDefined()
    expect(createCall.ssl.cert).toBeDefined()
    expect(createCall.ssl.key).toBeDefined()
  })

  it('should set rejectUnauthorized based on config', async () => {
    const server = makeServer({
      ssl: true,
      sslRejectUnauthorized: false,
    })
    const client = new TrinoClient(server, makeDatabase())
    await client.connect()

    const createCall = (TrinoNodeClient.create as jest.Mock).mock.calls[0][0]
    expect(createCall.ssl).toBeDefined()
    expect(createCall.ssl.rejectUnauthorized).toBe(false)
  })

  it('should not pass ssl options when ssl is false', async () => {
    const server = makeServer({ ssl: false })
    const client = new TrinoClient(server, makeDatabase())
    await client.connect()

    const createCall = (TrinoNodeClient.create as jest.Mock).mock.calls[0][0]
    expect(createCall.ssl).toBeUndefined()
  })
})

describe('TrinoClient read-only mode', () => {
  let client: TrinoClient

  beforeEach(async () => {
    jest.clearAllMocks()
    capturedQueries.length = 0
    client = new TrinoClient(makeServer({ readOnlyMode: true }), makeDatabase())
    await client.connect()
    capturedQueries.length = 0
  })

  it('should allow SHOW CATALOGS (listDatabases) in read-only mode', async () => {
    await expect(client.listDatabases()).resolves.toBeDefined()
    expect(capturedQueries[0]).toContain('show catalogs')
  })

  it('should allow SHOW SCHEMAS (listSchemas) in read-only mode', async () => {
    await expect(client.listSchemas(null)).resolves.toBeDefined()
    expect(capturedQueries[0]).toContain('show schemas')
  })

  it('should still block writes in read-only mode', async () => {
    await expect(client.driverExecuteSingle('DELETE FROM users'))
      .rejects.toThrow(/Read-Only Mode/)
  })
})

describe('TrinoClient query error surfacing', () => {
  let client: TrinoClient
  // The trino-client mock hands back a shared client object, so overriding
  // its query() leaks across tests unless restored.
  let originalQuery: any

  beforeEach(async () => {
    jest.clearAllMocks()
    capturedQueries.length = 0
    client = new TrinoClient(makeServer(), makeDatabase())
    await client.connect()
    originalQuery = (client as any).client.query
    capturedQueries.length = 0
  })

  afterEach(() => {
    ;(client as any).client.query = originalQuery
  })

  // Helper: make the underlying trino-client yield an arbitrary sequence of
  // QueryResult objects (Trino streams errors in the body, not via HTTP).
  function stubResults(...results: any[]) {
    ;(client as any).client.query = jest.fn().mockResolvedValue({
      [Symbol.asyncIterator]: async function* () {
        for (const r of results) yield r
      }
    })
  }

  it('throws when Trino reports an error in the response body', async () => {
    stubResults({
      error: {
        message: "line 1:8: Column 'foo' cannot be resolved",
        errorCode: 47,
        errorName: 'COLUMN_NOT_FOUND',
        errorType: 'USER_ERROR'
      }
    })

    await expect(client.driverExecuteSingle('SELECT foo FROM bar'))
      .rejects.toThrow(/COLUMN_NOT_FOUND/)
  })

  it('includes the human-readable message in the thrown error', async () => {
    stubResults({
      error: {
        message: "line 1:15: Table 'x.y.z' does not exist",
        errorCode: 1,
        errorName: 'TABLE_NOT_FOUND',
        errorType: 'USER_ERROR'
      }
    })

    await expect(client.driverExecuteSingle('SELECT * FROM x.y.z'))
      .rejects.toThrow(/does not exist/)
  })

  it('does not overwrite columns with a trailing result page that omits them', async () => {
    // Final Trino poll can arrive without columns; earlier ones carry them.
    stubResults(
      { columns: [{ name: 'n', type: 'bigint' }], data: [[1]] },
      { data: [[2]] }
    )

    const result = await client.driverExecuteSingle('SELECT n FROM t')
    expect(result.columns).toEqual([{ name: 'n', type: 'bigint' }])
    expect(result.rows).toEqual([{ n: 1 }, { n: 2 }])
  })

  it('still returns rows for a successful query', async () => {
    stubResults({ columns: [{ name: 'n', type: 'bigint' }], data: [[42]] })

    const result = await client.driverExecuteSingle('SELECT 42 AS n')
    expect(result.rows).toEqual([{ n: 42 }])
  })
})

describe('TrinoClient execution progress and cancel', () => {
  let client: TrinoClient
  let originalQuery: any

  beforeEach(async () => {
    jest.clearAllMocks()
    capturedQueries.length = 0
    client = new TrinoClient(makeServer(), makeDatabase())
    await client.connect()
    originalQuery = (client as any).client.query
    capturedQueries.length = 0
  })

  afterEach(() => {
    ;(client as any).client.query = originalQuery
  })

  function stubResults(...results: any[]) {
    ;(client as any).client.query = jest.fn().mockResolvedValue({
      [Symbol.asyncIterator]: async function* () {
        for (const r of results) yield r
      }
    })
  }

  const statsPage = (over: any = {}) => ({
    id: 'query_123',
    infoUri: 'https://trino/ui/query.html?query_123',
    stats: {
      state: 'RUNNING',
      progressPercentage: 40,
      processedRows: 1000,
      processedBytes: 2048,
      elapsedTimeMillis: 5000,
      queuedTimeMillis: 100,
      completedSplits: 4,
      runningSplits: 2,
      totalSplits: 10,
      nodes: 3,
      ...over
    }
  })

  it('reports progress for each page the engine sends', async () => {
    stubResults(
      statsPage(),
      { ...statsPage({ state: 'FINISHING', progressPercentage: 100 }), columns: [{ name: 'n', type: 'bigint' }], data: [[1]] }
    )

    const seen: any[] = []
    const query = await client.query('SELECT n FROM t')
    query.onProgress((p) => seen.push(p))
    await query.execute()

    expect(seen.length).toBe(2)
    expect(seen[0]).toMatchObject({
      state: 'RUNNING',
      percentage: 40,
      processedRows: 1000,
      totalSplits: 10,
      completedSplits: 4,
      nodes: 3,
      driverQueryId: 'query_123'
    })
    expect(seen[1].state).toBe('FINISHING')
  })

  it('executes fine when nothing subscribes to progress', async () => {
    stubResults({ ...statsPage(), columns: [{ name: 'n', type: 'bigint' }], data: [[7]] })

    const query = await client.query('SELECT n FROM t')
    await expect(query.execute()).resolves.toBeDefined()
  })

  it('cancels the running query on the server by its Trino query id', async () => {
    const cancel = jest.fn().mockResolvedValue({})
    ;(client as any).client.cancel = cancel

    // Hold the query open so cancel lands while it is still in flight.
    let release: () => void
    const gate = new Promise<void>((r) => { release = r })
    ;(client as any).client.query = jest.fn().mockResolvedValue({
      [Symbol.asyncIterator]: async function* () {
        yield statsPage()
        await gate
        yield { columns: [{ name: 'n', type: 'bigint' }], data: [[1]] }
      }
    })

    const query = await client.query('SELECT n FROM t')
    const running = query.execute().catch((e) => e)
    // Let the first page (which carries the query id) arrive
    await new Promise((r) => setTimeout(r, 10))

    await query.cancel()
    expect(cancel).toHaveBeenCalledWith('query_123')

    release()
    await running
  })

  it('reports progress for pages the library iterator never yields', async () => {
    // Faithful to trino-client: every poll goes through Client.request, but
    // the iterator only yields pages carrying rows — a query that sits in
    // QUEUED/RUNNING for minutes yields nothing at all until rows appear.
    const page = (state: string, over: any = {}) => ({
      id: 'q1',
      stats: { state, progressPercentage: 0, totalSplits: 0, ...over }
    })

    const polled = [page('RUNNING', { progressPercentage: 50 })]
    const inner = {
      request: jest.fn().mockImplementation(async () => polled.shift())
    }
    const fakeTrino = {
      client: inner,
      query: jest.fn().mockResolvedValue({
        iter: { queryResult: page('QUEUED') },
        [Symbol.asyncIterator]: async function* () {
          // The skipped poll still happens, it just isn't emitted
          await inner.request({ url: 'nextUri' })
          yield { ...page('FINISHING'), columns: [{ name: 'n', type: 'bigint' }], data: [[1]] }
        }
      })
    }

    ;(client as any).client = fakeTrino
    ;(client as any).progressTapInstalled = false
    ;(client as any).installProgressTap(fakeTrino)

    const seen: any[] = []
    const query = await client.query('SELECT 1')
    query.onProgress((p) => seen.push(p))
    await query.execute()

    const states = seen.map((p) => p.state)
    // From the initial POST response
    expect(states).toContain('QUEUED')
    // From the poll the iterator swallowed — the whole point of the tap
    expect(states).toContain('RUNNING')
    expect(seen.find((p) => p.state === 'RUNNING').percentage).toBe(50)
  })

  it('stops reporting progress once the query finishes', async () => {
    stubResults({ ...statsPage(), columns: [{ name: 'n', type: 'bigint' }], data: [[1]] })

    const query = await client.query('SELECT n FROM t')
    query.onProgress(() => { /* noop */ })
    await query.execute()

    expect((client as any).progressListeners.size).toBe(0)
  })

  it('does not call server cancel when no query id is known yet', async () => {
    const cancel = jest.fn().mockResolvedValue({})
    ;(client as any).client.cancel = cancel

    const query = await client.query('SELECT 1')
    await query.cancel()

    expect(cancel).not.toHaveBeenCalled()
  })
})

describe('TrinoClient SQL escaping', () => {
  let client: TrinoClient

  beforeEach(async () => {
    jest.clearAllMocks()
    capturedQueries.length = 0
    client = new TrinoClient(makeServer(), makeDatabase())
    await client.connect()
    capturedQueries.length = 0
  })

  it('should wrap catalog name with identifier quoting in listSchemas', async () => {
    const maliciousDb = "cat; DROP TABLE users --"
    ;(client as any).db = maliciousDb
    await client.listSchemas(null)

    const sql = capturedQueries[0]
    // Catalog name must be inside double-quote identifiers
    expect(sql).toContain('"cat; DROP TABLE users --"')
  })

  it('should wrap catalog name with identifier quoting in listTables', async () => {
    const maliciousDb = "cat; DROP TABLE users --"
    ;(client as any).db = maliciousDb
    await client.listTables(null)

    const sql = capturedQueries[0]
    // Catalog name must be inside double-quote identifiers
    expect(sql).toContain('"cat; DROP TABLE users --".information_schema')
  })

  it('should escape schema and table names in listTableColumns', async () => {
    await client.listTableColumns("test'; DROP TABLE users --", "public'; DROP TABLE users --")

    const sql = capturedQueries[0]
    // Single quotes in values must be doubled to stay inside SQL string literals
    expect(sql).toContain("public''; DROP TABLE users --")
    expect(sql).toContain("test''; DROP TABLE users --")
  })
})
