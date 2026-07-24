import rawLog from "@bksLogger"
import { readFileSync } from "fs"
import { Agent as HttpsAgent } from "https"
import axios from "axios"
import { wait } from "@shared/lib/wait"
import { IDbConnectionDatabase, TrinoAuthType } from "@/lib/db/types"
import {
  Trino as TrinoNodeClient,
  BasicAuth,
  QueryResult,
  QueryError,
  ConnectionOptions as TrinoConnectionOptions,
  SecureContextOptions
} from 'trino-client'
import {
  BaseQueryResult,
  BasicDatabaseClient,
  ExecutionContext,
  QueryLogOptions
} from "@/lib/db/clients/BasicDatabaseClient"
import {
  BksField,
  BksFieldType,
  CancelableQuery,
  DatabaseFilterOptions,
  ExtendedTableColumn,
  FilterOptions,
  NgQueryResult,
  OrderBy,
  PrimaryKeyColumn,
  QueryProgress,
  Routine,
  SchemaFilterOptions,
  StreamResults,
  SupportedFeatures,
  TableChanges,
  TableColumn,
  TableFilter,
  TableIndex,
  TableOrView,
  TableProperties,
  TableResult,
  TableTrigger
} from "@/lib/db/models"
import { TrinoData } from "@shared/lib/dialects/trino"
import _ from "lodash"
import {
  createCancelablePromise,
  joinFilters
} from "@/common/utils"
import { buildSchemaFilter, escapeString } from "@/lib/db/clients/utils"
import {
  AlterTableSpec,
  TableKey
} from "@shared/lib/dialects/models"
import { IdentifyResult } from "sql-query-identifier/lib/defines"
import { errors } from "@/lib/errors"
import { IDbConnectionServer } from "@/lib/db/backendTypes"
import { ChangeBuilderBase } from "@shared/lib/sql/change_builder/ChangeBuilderBase"

interface ResultColumn {
  name: string
  type: string
}

interface TrinoResult extends BaseQueryResult {
  info?: any,
  length?: number,
  queryId?: string
}

const log = rawLog.scope("trino")
const knex = null
// How long to wait for the user to finish signing in on the browser
const OAUTH2_LOGIN_TIMEOUT_MS = 5 * 60 * 1000
const trinoContext = {
  getExecutionContext(): ExecutionContext {
    return null;
  },
  logQuery(_query: string, _options: QueryLogOptions, _context: ExecutionContext): Promise<number | string> {
    return null
  }
}

export class TrinoClient extends BasicDatabaseClient<TrinoResult> {
  version: string
  client: any
  supportsTransaction: boolean
  private serverUrl: string
  private connectionOptions: TrinoConnectionOptions
  // Trino's own id for the query currently in flight, needed to cancel it
  private runningQueryId: string | null = null
  // Progress callbacks keyed by Trino query id, so concurrent queries on the
  // shared client each get their own stats
  private progressListeners = new Map<string, (progress: QueryProgress) => void>()
  private progressTapInstalled = false

  constructor(server: IDbConnectionServer, database: IDbConnectionDatabase) {
    super(knex, trinoContext, server, database)
    this.dialect = "generic"
    this.readOnlyMode = server?.config?.readOnlyMode || false
  }

  rowsToObject(columns: ResultColumn[] = [], rows: any[][]= [[]]) {
    const keys = columns.map(col => col?.name).filter(c => c != null)
    return rows.map(row => _.zipObject(keys, row))
  }

  async connect(): Promise<void> {
    await super.connect()

    let url: string

    if (this.server.config.url) {
      url = this.server.config.url
    } else {
      const urlObj = new URL('http://example.com/')
      urlObj.hostname = this.server.config.host
      urlObj.port = this.server.config.port.toString()
      urlObj.protocol = this.server.config.ssl ? 'https:' : 'http:'
      url = urlObj.toString()
    }

    const sslOptions = this.buildSslOptions()

    let authOptions: Partial<TrinoConnectionOptions> = {}
    if (this.useOAuth) {
      const token = await this.fetchOAuth2Token(url, sslOptions)
      authOptions = { extraHeaders: this.oauthHeaders(token) }
    } else if ((this.server.config.user != null && this.server.config.user !== '') || (this.server.config.password != null && this.server.config.password !== '')) {
      authOptions = { auth: new BasicAuth(this.server.config.user, this.server.config.password) }
    }

    const connectionObj: TrinoConnectionOptions = {
      server: url,
      catalog: this.database.database,
      ssl: sslOptions,
      ...authOptions
    }

    this.serverUrl = url
    this.connectionOptions = connectionObj
    this.client = TrinoNodeClient.create(connectionObj)
    this.installProgressTap(this.client)
    const result = await this.driverExecuteSingle(
      "SELECT version()"
    )

    this.version = result.rows[0]['_col0']
    this.supportsTransaction = false
  }

  private get useOAuth(): boolean {
    return this.server.config.trinoOptions?.authType === TrinoAuthType.OAuth2
  }

  private buildSslOptions(): SecureContextOptions | undefined {
    if (!this.server.config.ssl) return undefined

    const { sslCaFile, sslCertFile, sslKeyFile } = this.server.config
    const ca = sslCaFile ? readFileSync(sslCaFile) : undefined
    const cert = sslCertFile ? readFileSync(sslCertFile) : undefined
    const key = sslKeyFile ? readFileSync(sslKeyFile) : undefined

    return {
      ca,
      cert,
      key,
      rejectUnauthorized: (ca || cert || key)
        ? this.server.config.sslRejectUnauthorized
        : false
    }
  }

  private oauthHeaders(token: string): Record<string, string> {
    return {
      Authorization: `Bearer ${token}`,
      // With OAuth2 the user comes from the token. Overriding the library's
      // default (the OS user) avoids impersonation errors; an empty value
      // gets stripped by the client, dropping the header entirely.
      'X-Trino-User': this.server.config.user || ''
    }
  }

  /**
   * Implements the Trino OAuth2 handshake, same as the Python client and JDBC
   * driver: an unauthenticated request returns a 401 whose WWW-Authenticate
   * header carries a URL to open in the browser (x_redirect_server) and a URL
   * to long-poll for the resulting token (x_token_server).
   */
  private async fetchOAuth2Token(serverUrl: string, ssl?: SecureContextOptions): Promise<string> {
    const httpsAgent = new HttpsAgent(ssl ?? {})
    const statementUrl = new URL('v1/statement', serverUrl).toString()

    const challenge = await axios.post(statementUrl, 'SELECT 1', {
      httpsAgent,
      validateStatus: () => true
    })

    if (challenge.status !== 401) {
      const body = typeof challenge.data === 'string' ? challenge.data : JSON.stringify(challenge.data ?? '')
      const usingHttps = statementUrl.startsWith('https:')
      const hint = usingHttps
        ? 'Check that the server has OAuth2 authentication enabled.'
        : 'Trino OAuth2 requires HTTPS — enable SSL in the connection settings.'
      throw new Error(`Expected an authentication challenge from the Trino server, got HTTP ${challenge.status}. ${hint} Server response: ${body.replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 200)}`)
    }

    const wwwAuthenticate = challenge.headers['www-authenticate'] || ''
    const redirectMatch = wwwAuthenticate.match(/x_redirect_server="([^"]+)"/i)
    const tokenMatch = wwwAuthenticate.match(/x_token_server="([^"]+)"/i)

    if (!redirectMatch || !tokenMatch) {
      throw new Error(`The Trino server did not offer OAuth2 authentication (WWW-Authenticate: ${wwwAuthenticate || 'missing'})`)
    }

    log.info('Opening browser for Trino OAuth2 sign-in')
    process.parentPort.postMessage({ type: 'openExternal', url: redirectMatch[1] })

    let tokenUrl = tokenMatch[1]
    const deadline = Date.now() + OAUTH2_LOGIN_TIMEOUT_MS

    while (Date.now() < deadline) {
      const res = await axios.get(tokenUrl, {
        httpsAgent,
        validateStatus: () => true
      })

      if (res.status !== 200) {
        throw new Error(`Trino OAuth2 token request failed with HTTP ${res.status}`)
      }

      if (res.data?.token) {
        return res.data.token
      }

      if (res.data?.error) {
        throw new Error(`Trino OAuth2 authentication failed: ${res.data.error}`)
      }

      if (res.data?.nextUri) {
        // The token server long-polls; the small delay just avoids a hot loop
        tokenUrl = res.data.nextUri
        await wait(500)
        continue
      }

      throw new Error('Unexpected response from the Trino OAuth2 token server')
    }

    throw new Error('Timed out waiting for the browser sign-in to complete')
  }

  private async refreshOAuthSession(): Promise<void> {
    const token = await this.fetchOAuth2Token(this.serverUrl, this.connectionOptions.ssl)
    this.connectionOptions = {
      ...this.connectionOptions,
      extraHeaders: this.oauthHeaders(token)
    }
    this.progressTapInstalled = false
    this.client = TrinoNodeClient.create(this.connectionOptions)
    this.installProgressTap(this.client)
  }

  async disconnect(): Promise<void> {
    // Trino client doesn't have a close method, just clean up the reference
    this.client = null
    await super.disconnect()
  }

  async versionString(): Promise<string> {
    return this.version
  }

  async alterTable(_change: AlterTableSpec): Promise<void> {
    log.info("Trino doesn't support changing data")
    return null
  }

  async getPrimaryKeys(): Promise<PrimaryKeyColumn[]> {
    log.info("Trino doesn't support primary keys")
    return await []
  }

  async getPrimaryKey(_table: string, _schema?: string): Promise<string | null> {
    log.info("Trino doesn't support primary keys")
    return null
  }

  async selectTop(
    table: string,
    offset: number,
    limit: number,
    orderBy: OrderBy[],
    filters: string | TableFilter[],
    schema: string,
    selects: string[],
  ): Promise<TableResult> {
    const columns = await this.listTableColumns(table, schema)
    let selectFields = [...selects]
    if (!selects || selects?.length === 0 || (selects?.length === 1 && selects[0] === '*')) {
      // select all columns with the column names instead of *
      selectFields = columns.map((v) => v.columnName)
    }

    const queries = this.buildSelectTopQuery(
      table,
      offset,
      limit,
      orderBy,
      filters,
      "total",
      columns,
      selectFields,
      schema
    )

    const { query } = queries
    const result = await this.driverExecuteSingle(query)
    const fields = result.columns ? result.columns.map(c => ({
      name: c.name,
      bksType: 'UNKNOWN' as BksFieldType
    })) : []
    return {
      result: result.rows || [],
      fields
    }
  }

  async selectTopSql(
    table: string,
    offset: number,
    limit: number,
    orderBy: OrderBy[],
    filters: string | TableFilter[],
    schema: string,
    selects: string[]
  ): Promise<string> {
    const columns = await this.listTableColumns(table, schema)
    const { query } = this.buildSelectTopQuery(
      table,
      offset,
      limit,
      orderBy,
      filters,
      "total",
      columns,
      selects,
      schema
    )
    return query
  }

  async getTableProperties(
    _table: string,
    _schema?: string
  ): Promise<TableProperties> {
    log.info("Trino doesn't support table properties for all databases")
    return null
  }

  async getOutgoingKeys(_table: string, _schema?: string): Promise<TableKey[]> {
    return []
  }

  async getIncomingKeys(_table: string, _schema?: string): Promise<TableKey[]> {
    return []
  }

  async listTableTriggers(
    _table: string,
    _schema?: string
  ): Promise<TableTrigger[]> {
    // Not supported
    return []
  }

  async listTableIndexes(
    _table: string,
    _schema?: string
  ): Promise<TableIndex[]> {
    log.info("Trino doesn't support table indexes in all databases it supports")
    return null
  }

  async listViews(
    _filter: FilterOptions = { schema: "public" }
  ): Promise<TableOrView[]> {
    log.info("Trino doesn't support views")
    return []
  }

  async executeApplyChanges(_changes: TableChanges): Promise<any[]> {
    log.info("Trino doesn't support changing data")
    return null
  }

  async dropElement(): Promise<void> {
    log.info("Trino doesn't support changing data")
    return null
  }

  async listDatabases(_filter?: DatabaseFilterOptions): Promise<string[]> {
    const sql = "show catalogs"
    const result = await this.driverExecuteSingle(sql)

    return result.rows.map((row) => row.Catalog)
  }

  async listSchemas(filter: SchemaFilterOptions): Promise<string[]> {
    log.info('filters in listSchemas', filter)
    const sql = `show schemas from ${this.wrapIdentifier(this.db)}`
    const result = await this.driverExecuteSingle(sql)

    return result?.rows ? result.rows.map((row) => row.Schema) : []
  }

   async listTables(filter?: FilterOptions): Promise<TableOrView[]> {
    log.info('filters in listTables', filter)
    const schemaFilter = buildSchemaFilter(filter, 'table_schema', (s) => this.wrapIdentifier(s))
    const whereClause = schemaFilter ? `WHERE ${schemaFilter}` : ''
    const sql = `select * from ${this.wrapIdentifier(this.db)}.information_schema.tables ${whereClause}`
    const result = await this.driverExecuteSingle(sql)

    return result.rows.map((row) => ({
      schema: row['table_schema'],
      name: row['table_name'],
      entityType: 'table' as const
    }))
  }

  async listTableColumns(table: string, schema: string): Promise<ExtendedTableColumn[]> {
    const sql = `
      SELECT
        *
      FROM ${this.wrapIdentifier(this.db)}.information_schema.columns
      WHERE table_schema = '${escapeString(schema)}'
        AND table_name = '${escapeString(table)}'
      ORDER BY ordinal_position
    `
    const result = await this.driverExecuteSingle(sql)
    return result.rows.map((row) => {
      // Empty string if it is not defined.
      const hasDefault = row.column_default != null

      return {
        schemaName: row.table_schema,
        tableName: row.table_name,
        columnName: row.column_name,
        dataType: row.data_type,
        ordinalPosition: row.ordinal_position,
        defaultValue: row.column_default,
        hasDefault,
        comment: row.comment,
        primaryKey: false,
        nullable: row.is_nullable,
        bksField: this.parseTableColumn(row),
      }
    })
  }

  async createDatabase(): Promise<string> {
    log.debug("Trino doesn't support creating databases")
    return null
  }

  async truncateElementSql() {
    log.debug("Trino doesn't support changing data")
    return null
  }

  async duplicateTable(): Promise<void> {
    log.debug("Trino doesn't support changing data")
    return null
  }

  async duplicateTableSql(): Promise<string> {
    log.debug("Trino doesn't support changing data")
    return null
  }

  async setElementNameSql(): Promise<string> {
    log.debug("Trino doesn't support changing data")
    return null
  }

  async getBuilder(_table: string, _schema?: string): Promise<ChangeBuilderBase> {
    log.debug("Trino doesn't support changing data")
    return null
  }

  async query(queryText: string): Promise<CancelableQuery> {
    const cancelable = createCancelablePromise(errors.CANCELED_BY_USER)
    let progressCallback: ((progress: QueryProgress) => void) | null = null
    // Tracked per query rather than on the client, so cancelling one query
    // can't target another that happens to be running on the same connection.
    let trinoQueryId: string = null

    return {
      onProgress: (callback: (progress: QueryProgress) => void): void => {
        progressCallback = callback
      },
      execute: async (): Promise<NgQueryResult[]> => {
        try {
          const data = await Promise.race([
            cancelable.wait(),
            this.executeQuery(queryText, {
              _onProgress: (p: QueryProgress) => progressCallback?.(p),
              _onQueryId: (id: string) => { trinoQueryId = id }
            }),
          ])
          if (!data) return []
          return data
        } catch (err) {
          if (cancelable.canceled) {
            err.sqlectronError = "CANCELED_BY_USER"
          }
          throw err
        } finally {
          cancelable.discard()
        }
      },
      cancel: async (): Promise<void> => {
        // Trino cancels server-side by query id, which only becomes known once
        // the first response arrives. Unblock the caller either way.
        if (trinoQueryId) {
          try {
            await this.client.cancel(trinoQueryId)
          } catch (err) {
            log.error(`Failed to cancel Trino query ${trinoQueryId}`, err)
          }
        }
        cancelable.cancel()
      },
    }
  }

  parseFields(fields: any[]) {
    return fields.map(column => ({
      dataType: column.type,
      id: column.name,
      name: column.name
    }))
  }

  async executeQuery(
    queryText: string,
    options: any = {}
  ): Promise<NgQueryResult[]> {
    const queries = queryText.trim().split(';')
    const results: NgQueryResult[] = await Promise.all(
      queries
        .filter(q => q.trim() !== '')
        .map(async q => {
          const {rows, columns} = await this.driverExecuteSingle(q, { ...options })
          const fields = rows.length === 0 ? [] : columns.map(c => ({ ...c, id: c.name }))
          return {
            fields,
            rows,
            rowCount: rows.length,
            affectedRows: 0,
            command: 'SELECT'
          } satisfies NgQueryResult
        })
    )
    return results
  }

  /**
   * trino-client's iterator only yields pages that carry rows: while a query
   * is queued, planning or still scheduling splits, it silently follows
   * nextUri without emitting anything. That is exactly the stretch where
   * progress matters, so tap the layer underneath — every polled page passes
   * through Client.request — and route each one by its query id.
   */
  private installProgressTap(trino: any): void {
    const client = trino?.client
    if (!client || typeof client.request !== 'function') {
      log.warn('Could not install the Trino progress tap; falling back to per-page stats')
      return
    }

    const original = client.request.bind(client)
    client.request = async (cfg: any) => {
      const result = await original(cfg)
      try {
        if (result?.id && result?.stats) {
          this.progressListeners.get(result.id)?.(this.toQueryProgress(result))
        }
      } catch (err) {
        // Progress is informational; never let it break query execution
        log.error('Trino progress reporting failed', err)
      }
      return result
    }
    this.progressTapInstalled = true
  }

  private toQueryProgress(r: QueryResult): QueryProgress {
    const s = r.stats
    return {
      state: s.state,
      percentage: s.progressPercentage,
      processedRows: s.processedRows,
      processedBytes: s.processedBytes,
      elapsedMillis: s.elapsedTimeMillis,
      queuedMillis: s.queuedTimeMillis,
      completedSplits: s.completedSplits,
      runningSplits: s.runningSplits,
      totalSplits: s.totalSplits,
      nodes: s.nodes,
      driverQueryId: r.id,
      infoUri: r.infoUri
    }
  }

  private buildTrinoError(error: QueryError): Error {
    // errorName is the machine code (e.g. COLUMN_NOT_FOUND, TABLE_NOT_FOUND);
    // message carries the human-readable detail with line/column info.
    const parts = [error.errorName, error.message].filter(Boolean)
    const err = new Error(parts.join(': ') || 'Trino query failed')
      ; (err as any).trinoError = error
    return err
  }

  async rawExecuteQuery(sql: string, options: any = {}): Promise<TrinoResult> {
    let trackedQueryId: string = null
    try {
      // The trino query parser doesn't particularly like semicolons. Who can blame it?
      const result: any = await this.client.query(sql.trim().replace(/;$/, ''))

      // The response to the initial POST already carries the query id, which
      // lets progress be routed correctly even with several queries in flight.
      const firstPage: QueryResult = result?.iter?.queryResult
      if (firstPage?.id) {
        trackedQueryId = firstPage.id
        this.runningQueryId = firstPage.id
        options._onQueryId?.(firstPage.id)
        if (options._onProgress) {
          this.progressListeners.set(firstPage.id, options._onProgress)
          if (firstPage.stats) options._onProgress(this.toQueryProgress(firstPage))
        }
      }

      let columns: ResultColumn[] = []
      const rows: any[] = []

      for await (const r of result as AsyncIterableIterator<QueryResult>) {
        // Trino reports query failures in the response body (HTTP 200 + an
        // `error` field), not via HTTP status, and trino-client never checks
        // for it. Without this the loop finishes empty and a failed query
        // looks like it returned zero rows instead of surfacing the error.
        if (r.error) {
          throw this.buildTrinoError(r.error)
        }

        if (r.id) {
          this.runningQueryId = r.id
          options._onQueryId?.(r.id)
        }
        // Fallback only: with the tap installed, progress already comes from
        // every polled page, including the ones the iterator skips.
        if (!this.progressTapInstalled && r.stats && options._onProgress) {
          options._onProgress(this.toQueryProgress(r))
        }

        const { data: resultData, columns: resultColumns } = r
        if (resultColumns) columns = resultColumns

        if (resultData) rows.push(...resultData)
      }

      if (rows.length === 0) {
        return {
          columns,
          rows: [],
          arrayMode: false,
          queryId: ''
        }
      }

      return {
        columns,
        rows: this.rowsToObject(columns, rows),
        arrayMode: false
      }
    } catch (err) {
      // OAuth2 tokens expire; re-run the browser sign-in once and retry
      if (this.useOAuth && err?.response?.status === 401 && !options._oauthRetried) {
        log.info('Trino OAuth2 token expired, re-authenticating')
        await this.refreshOAuthSession()
        return await this.rawExecuteQuery(sql, { ...options, _oauthRetried: true })
      }
      log.error(err)
      throw err
    } finally {
      if (trackedQueryId) this.progressListeners.delete(trackedQueryId)
    }
  }

  async supportedFeatures(): Promise<SupportedFeatures> {
    return {
      customRoutines: false,
      comments: false,
      properties: false,
      partitions: false,
      editPartitions: false,
      backups: false,
      backDirFormat: false,
      restore: false,
      indexNullsNotDistinct: false,
      transactions: this.supportsTransaction,
      filterTypes: ['standard']
    }
  }

  async listRoutines(_filter?: FilterOptions): Promise<Routine[]> {
    log.info("Trino doesn't support reoutines")
    return []
  }

  async listMaterializedViewColumns(): Promise<TableColumn[]> {
    log.info("Trino doesn't support materialized views")
    return []
  }

  async getTableReferences(
    _table: string,
    _schema?: string
  ): Promise<string[]> {
    log.info("Trino doesn't support foreign keys")
    return []
  }

  async getQuerySelectTop(
    table: string,
    limit: number,
    _schema?: string
  ): Promise<string> {
    return `SELECT * FROM ${TrinoData.wrapIdentifier(
      table
    )} LIMIT ${limit}`
  }

  async listMaterializedViews(_filter?: FilterOptions): Promise<TableOrView[]> {
    log.info("Trino doesn't support materialized views")
    return []
  }

  async listCharsets(): Promise<string[]> {
    return []
  }

  async getDefaultCharset(): Promise<string> {
    return ""
  }

  async listCollations(_charset: string): Promise<string[]> {
    return []
  }

  async createDatabaseSQL(): Promise<string> {
    throw new Error("Method not implemented.")
  }

  async getTableCreateScript(_table: string, _schema?: string): Promise<string> {
    log.debug("Trino doesn't support creating tables")
    return ''
  }

  async getViewCreateScript(_view: string, _schema?: string): Promise<string[]> {
    log.debug("Trino doesn't support view creatinon")
    return []
  }

  async getRoutineCreateScript(): Promise<string[]> {
    return []
  }

  async setTableDescription(): Promise<string> {
    log.debug("Trino doesn't support changing data")
    return ''
  }

  async truncateAllTables(_schema?: string): Promise<void> {
    log.debug("Trino doesn't support changing data")
  }

  async getTableLength(table: string, schema: string): Promise<number> {
    const result = await this.driverExecuteSingle(
      `SELECT count(*) as count FROM ${this.wrapIdentifier(this.db)}.${this.wrapIdentifier(schema)}.${this.wrapIdentifier(table)}`
    )

    const [row] = result.rows as { count: number }[]
    return row.count
  }

  // No exports of stuff since I don't think tables will be exported. Result sets, sure. Not tables
  async selectTopStream(): Promise<StreamResults> {
    return {
      columns: [],
      totalRows: 0,
      cursor: null
    }
  }

  queryStream(_query: string, _chunkSize: number): Promise<StreamResults> {
    throw new Error("Method not implemented.")
  }

  wrapIdentifier(value: string): string {
    return TrinoData.wrapIdentifier(value)
  }

  wrapDynamicLiteral(value: any): string {
    if (value == null) return 'NULL'
    if (typeof value === 'number' || /^[+-]?([0-9]*[.])?[0-9]+$/.test(value)) {
      return value.toString()
    }
    if (typeof value === 'boolean') {
      return value ? 'TRUE' : 'FALSE'
    }
    return `'${value.toString().replace(/'/g, "''")}'`
  }


  buildFilterString(filters: TableFilter[], columns = []) {
    let fullFilterString = ""

    if (filters && Array.isArray(filters) && filters.length > 0) {
      const filtersWithoutParams: string[] = []

      filters.forEach((item) => {
        const column = columns.find((c) => c.columnName === item.field)
        const field = column?.dataType?.toUpperCase().includes("BINARY")
          ? `HEX(${TrinoData.wrapIdentifier(item.field)})`
          : TrinoData.wrapIdentifier(item.field)

        const op = item.type.toUpperCase()
        const val = item.value

        // Handle IS NULL / IS NOT NULL
        if (op === "IS NULL" || op === "IS NOT NULL") {
          filtersWithoutParams.push(`${field} ${op}`)
          return
        }

        // Handle IN
        if (op === "IN" && Array.isArray(val)) {
          const values = val
            .map((v) => this.wrapDynamicLiteral(v))
            .join(", ")
          filtersWithoutParams.push(`${field} IN (${values})`)
          return
        }

        // Handle binary ops (>, <, >=, <=, =, !=, LIKE, ILIKE)
        if (
          ["=", "!=", "<", "<=", ">", ">=", "LIKE", "ILIKE"].includes(op) &&
          val != null
        ) {
          const literal = this.wrapDynamicLiteral(val)
          filtersWithoutParams.push(`${field} ${op} ${literal}`)
          return
        }
      })

      fullFilterString = "WHERE " + joinFilters(filtersWithoutParams, filters)
    }

    return {
      fullFilterString,
    }
  }

  buildSelectTopQuery(
    table: string,
    offset: number,
    limit: number,
    orderBy: OrderBy[],
    filters: string | TableFilter[],
    countTitle = "total",
    columns = [],
    selects = ["*"],
    schema
  ) {
    log.info("building selectTop for", table, offset, limit, orderBy, selects, schema)

    // Ensure sane defaults
    const safeOffset = Number.isFinite(offset) ? offset : 0
    const safeLimit = Number.isFinite(limit) ? limit : 100
    const usePagination = Number.isFinite(limit)
    const selectsArr = !Array.isArray(selects) || selects.length === 0 ? ['*'] : selects

    let rowNumberOrderClause = ""

    if (orderBy && orderBy.length > 0) {
      const orderByParts = orderBy.map((item: any) => {
        if (_.isObject(item)) {
          return `${TrinoData.wrapIdentifier(item["field"])} ${item["dir"].toUpperCase()}`
        } else {
          return TrinoData.wrapIdentifier(item)
        }
      })

      rowNumberOrderClause = "ORDER BY " + orderByParts.join(", ")
    } else {
      rowNumberOrderClause = "ORDER BY 1" // Fallback for ROW_NUMBER
    }

    let filterString = ""
    let fullFilterString = ""
    if (_.isString(filters)) {
      filterString = fullFilterString = `WHERE ${filters}`
    } else {
      const filterBlob = this.buildFilterString(filters, columns)
      filterString = filterBlob.fullFilterString
      fullFilterString = filterBlob.fullFilterString
    }

    const wrappedSelects = selectsArr.map((s) => s === '*' ? s : TrinoData.wrapIdentifier(s)).join(", ")
    const wrappedTable = `${TrinoData.wrapIdentifier(schema)}.${TrinoData.wrapIdentifier(table)}`

    // Count query remains simple
    const countSQL = `
      SELECT COUNT(*) AS ${countTitle}
      FROM ${wrappedTable}
      ${filterString}
    `

    const paginatedSQL = this.buildPaginatedQuery(wrappedTable, filterString, wrappedSelects, rowNumberOrderClause, usePagination, safeOffset, safeLimit)
    const fullSql = this.buildPaginatedQuery(TrinoData.wrapIdentifier(table), fullFilterString, wrappedSelects, rowNumberOrderClause, usePagination, safeOffset, safeLimit)

    return {
      query: paginatedSQL,
      fullQuery: fullSql,
      countQuery: countSQL,
      params: {},
    }
  }

  buildPaginatedQuery(tableRef: string, filter: string, wrappedSelects: string, rowNumberOrderClause: string, usePagination: boolean, safeOffset: number, safeLimit: number): string {
    return `
      WITH ranked AS (
        SELECT
          ${wrappedSelects},
          ROW_NUMBER() OVER (${rowNumberOrderClause}) AS rownum
        FROM ${this.wrapIdentifier(this.db)}.${tableRef}
        ${filter}
      )
      SELECT *
      FROM ranked
      ${usePagination ? `WHERE rownum > ${safeOffset} AND rownum <= ${safeOffset + safeLimit}` : ""}
    `
  }

  protected violatesReadOnly(statements: IdentifyResult[], options: any = {}) {
    // The sql-query-identifier 'generic' dialect can't parse Trino metadata
    // commands (SHOW CATALOGS, SHOW SCHEMAS FROM, DESCRIBE, ...) and marks
    // them UNKNOWN, which read-only mode would block even though they only
    // read. Filter them out before delegating to the base check.
    const readOnlyStatement = /^\s*(show|describe|desc|explain)\b/i
    const remaining = statements.filter(s => !readOnlyStatement.test(s.text))
    return (
      super.violatesReadOnly(remaining, options) ||
      (this.readOnlyMode && options.insert)
    )
  }

  parseTableColumn(column: TableColumn): BksField {
    return { name: column.columnName, bksType: "UNKNOWN" }
  }
}
