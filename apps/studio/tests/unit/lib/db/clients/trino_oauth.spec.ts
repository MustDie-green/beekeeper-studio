/**
 * @jest-environment node
 */
import http from 'http'
import { AddressInfo } from 'net'
import { TrinoClient } from '@commercial/backend/lib/db/clients/trino'
import { TrinoAuthType } from '@/lib/db/types'
import { IDbConnectionServer } from '@/lib/db/backendTypes'

describe('Trino OAuth2 flow (mock server)', () => {
  let server: http.Server
  let port: number
  let postMessageMock: jest.Mock
  let tokenPolls = 0
  const statementRequests: Array<{ auth?: string, user?: string }> = []
  const VALID_TOKENS = new Set(['test-token-1'])

  beforeAll(async () => {
    postMessageMock = jest.fn()
    ;(process as any).parentPort = { postMessage: postMessageMock }

    server = http.createServer((req, res) => {
      const base = `http://127.0.0.1:${port}`

      if (req.url === '/v1/statement' && req.method === 'POST') {
        const auth = req.headers['authorization']
        statementRequests.push({ auth, user: req.headers['x-trino-user'] as string })

        if (!auth || !VALID_TOKENS.has(auth.replace('Bearer ', ''))) {
          res.writeHead(401, {
            'WWW-Authenticate': `Bearer x_redirect_server="${base}/oauth2/redirect", x_token_server="${base}/oauth2/token"`
          })
          res.end()
          return
        }

        res.writeHead(200, { 'Content-Type': 'application/json' })
        res.end(JSON.stringify({
          id: 'query-1',
          nextUri: `${base}/v1/statement/query-1/1`,
          stats: { state: 'QUEUED' }
        }))
        return
      }

      if (req.url === '/v1/statement/query-1/1' && req.method === 'GET') {
        const auth = req.headers['authorization']
        if (!auth || !VALID_TOKENS.has(auth.replace('Bearer ', ''))) {
          res.writeHead(401)
          res.end()
          return
        }
        res.writeHead(200, { 'Content-Type': 'application/json' })
        res.end(JSON.stringify({
          id: 'query-1',
          columns: [{ name: '_col0', type: 'varchar' }],
          data: [['477']],
          stats: { state: 'FINISHED' }
        }))
        return
      }

      if (req.url === '/oauth2/token' && req.method === 'GET') {
        tokenPolls++
        res.writeHead(200, { 'Content-Type': 'application/json' })
        if (tokenPolls === 1) {
          // first poll: not signed in yet, keep polling
          res.end(JSON.stringify({ nextUri: `${base}/oauth2/token` }))
        } else {
          res.end(JSON.stringify({ token: 'test-token-1' }))
        }
        return
      }

      res.writeHead(404)
      res.end()
    })

    await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve))
    port = (server.address() as AddressInfo).port
  })

  afterAll(async () => {
    await new Promise<void>(resolve => server.close(() => resolve()))
  })

  it('performs the browser handshake and connects with the bearer token', async () => {
    const serverConfig = {
      config: {
        host: '127.0.0.1',
        port,
        ssl: false,
        user: 'v.arzhanov@example.com',
        password: null,
        readOnlyMode: false,
        trinoOptions: { authType: TrinoAuthType.OAuth2 }
      }
    } as unknown as IDbConnectionServer

    const database = { database: 'tpch', connected: false, connecting: false, namespace: '' }
    const client = new TrinoClient(serverConfig, database as any)

    await client.connect()

    // the redirect URL was sent to the main process to open in the browser
    expect(postMessageMock).toHaveBeenCalledWith({
      type: 'openExternal',
      url: `http://127.0.0.1:${port}/oauth2/redirect`
    })

    // token server was polled through nextUri until the token appeared
    expect(tokenPolls).toBe(2)

    // version query ran authenticated with the issued token
    expect(await client.versionString()).toBe('477')
    const authed = statementRequests.filter(r => r.auth === 'Bearer test-token-1')
    expect(authed.length).toBeGreaterThan(0)
    // X-Trino-User overridden with the configured user, not the OS user
    expect(authed[0].user).toBe('v.arzhanov@example.com')
  })
})
