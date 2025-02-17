import { ClickHouseClient, createClient } from '@clickhouse/client';
import { formatInTimeZone } from 'date-fns-tz';
import debug from 'debug';
import { CLICKHOUSE } from 'lib/db';
import { getWebsite } from 'queries/index';
import { DEFAULT_PAGE_SIZE, OPERATORS } from './constants';
import { maxDate } from './date';
import { filtersToArray } from './params';
import { PageParams, QueryFilters, QueryOptions } from './types';

const log = debug('umami:clickhouse');

// Pool Configuration Interface
interface PoolConfig {
  min: number;
  max: number;
  acquireTimeoutMillis: number;
  createTimeoutMillis: number;
  idleTimeoutMillis: number;
  reapIntervalMillis: number;
}

// Extended ClickHouse Client Interface for Pool
interface PoolClient extends ClickHouseClient {
  lastUsed: number;
  isIdle: boolean;
}

// Date Formats Configuration
export const CLICKHOUSE_DATE_FORMATS = {
  utc: '%Y-%m-%dT%H:%i:%SZ',
  second: '%Y-%m-%d %H:%i:%S',
  minute: '%Y-%m-%d %H:%i:00',
  hour: '%Y-%m-%d %H:00:00',
  day: '%Y-%m-%d',
  month: '%Y-%m-01',
  year: '%Y-01-01',
};

// Connection Pool Implementation
class ClickHousePool {
  private clients: PoolClient[] = [];
  private config: PoolConfig;
  private connectionParams: any;

  constructor(
    config: PoolConfig = {
      min: 2,
      max: 10,
      acquireTimeoutMillis: 30000,
      createTimeoutMillis: 30000,
      idleTimeoutMillis: 30000,
      reapIntervalMillis: 1000,
    },
  ) {
    this.config = config;
    this.initialize();
  }

  private async initialize() {
    const {
      hostname,
      port,
      pathname,
      protocol,
      username = 'default',
      password,
    } = new URL(process.env.CLICKHOUSE_URL);

    this.connectionParams = {
      url: `${protocol}//${hostname}:${port}`,
      database: pathname.replace('/', ''),
      username: username,
      password: password,
      clickhouse_settings: {
        date_time_input_format: 'best_effort',
        date_time_output_format: 'iso',
      },
    };

    // Create minimum number of connections
    for (let i = 0; i < this.config.min; i++) {
      await this.createClient();
    }

    // Start the reaper
    this.startReaper();
  }

  private async createClient(): Promise<PoolClient> {
    const client = createClient(this.connectionParams) as PoolClient;
    client.lastUsed = Date.now();
    client.isIdle = true;
    this.clients.push(client);

    if (process.env.NODE_ENV !== 'production') {
      global[CLICKHOUSE] = client;
    }

    log('Created new ClickHouse client');
    return client;
  }

  private startReaper() {
    setInterval(() => {
      this.reapIdleConnections();
    }, this.config.reapIntervalMillis);
  }

  private async reapIdleConnections() {
    const now = Date.now();
    const minClients = this.config.min;

    this.clients = this.clients.filter(client => {
      const idle = now - client.lastUsed > this.config.idleTimeoutMillis;
      if (idle && this.clients.length > minClients) {
        client.close();
        log('Closed idle ClickHouse client');
        return false;
      }
      return true;
    });
  }

  async acquire(): Promise<PoolClient> {
    // Find an idle client
    let client = this.clients.find(c => c.isIdle);

    // Create new client if none available and under max
    if (!client && this.clients.length < this.config.max) {
      client = await this.createClient();
    }

    // Wait for an idle client if at max
    if (!client) {
      client = await new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error('Timeout acquiring client'));
        }, this.config.acquireTimeoutMillis);

        const checkForIdleClient = () => {
          const idleClient = this.clients.find(c => c.isIdle);
          if (idleClient) {
            clearTimeout(timeout);
            resolve(idleClient);
          } else {
            setTimeout(checkForIdleClient, 100);
          }
        };

        checkForIdleClient();
      });
    }

    client.isIdle = false;
    client.lastUsed = Date.now();
    return client;
  }

  release(client: PoolClient) {
    client.isIdle = true;
    client.lastUsed = Date.now();
  }

  async end() {
    await Promise.all(this.clients.map(client => client.close()));
    this.clients = [];
  }
}

// Create singleton pool instance
const pool = new ClickHousePool();
const enabled = Boolean(process.env.CLICKHOUSE_URL);

// Utility Functions
function getUTCString(date?: Date | string | number) {
  return formatInTimeZone(date || new Date(), 'UTC', 'yyyy-MM-dd HH:mm:ss');
}

function getDateStringSQL(data: any, unit: string = 'utc', timezone?: string) {
  if (timezone) {
    return `formatDateTime(${data}, '${CLICKHOUSE_DATE_FORMATS[unit]}', '${timezone}')`;
  }
  return `formatDateTime(${data}, '${CLICKHOUSE_DATE_FORMATS[unit]}')`;
}

function getDateSQL(field: string, unit: string, timezone?: string) {
  if (timezone) {
    return `toDateTime(date_trunc('${unit}', ${field}, '${timezone}'), '${timezone}')`;
  }
  return `toDateTime(date_trunc('${unit}', ${field}))`;
}

function getSearchSQL(column: string, param: string = 'search'): string {
  return `and positionCaseInsensitive(${column}, {${param}:String}) > 0`;
}

function mapFilter(column: string, operator: string, name: string, type: string = 'String') {
  const value = `{${name}:${type}}`;

  switch (operator) {
    case OPERATORS.equals:
      return `${column} = ${value}`;
    case OPERATORS.notEquals:
      return `${column} != ${value}`;
    case OPERATORS.contains:
      return `positionCaseInsensitive(${column}, ${value}) > 0`;
    case OPERATORS.doesNotContain:
      return `positionCaseInsensitive(${column}, ${value}) = 0`;
    default:
      return '';
  }
}

function getFilterQuery(filters: QueryFilters = {}, options: QueryOptions = {}) {
  const query = filtersToArray(filters, options).reduce((arr, { name, column, operator }) => {
    if (column) {
      arr.push(`and ${mapFilter(column, operator, name)}`);

      if (name === 'referrer') {
        arr.push('and referrer_domain != {websiteDomain:String}');
      }
    }

    return arr;
  }, []);

  return query.join('\n');
}

function getDateQuery(filters: QueryFilters = {}) {
  const { startDate, endDate, timezone } = filters;

  if (startDate) {
    if (endDate) {
      if (timezone) {
        return `and created_at between toTimezone({startDate:DateTime64},{timezone:String}) and toTimezone({endDate:DateTime64},{timezone:String})`;
      }
      return `and created_at between {startDate:DateTime64} and {endDate:DateTime64}`;
    } else {
      if (timezone) {
        return `and created_at >= toTimezone({startDate:DateTime64},{timezone:String})`;
      }
      return `and created_at >= {startDate:DateTime64}`;
    }
  }

  return '';
}

function getFilterParams(filters: QueryFilters = {}) {
  return filtersToArray(filters).reduce((obj, { name, value }) => {
    if (name && value !== undefined) {
      obj[name] = value;
    }

    return obj;
  }, {});
}

async function parseFilters(websiteId: string, filters: QueryFilters = {}, options?: QueryOptions) {
  const website = await getWebsite(websiteId);

  return {
    filterQuery: getFilterQuery(filters, options),
    dateQuery: getDateQuery(filters),
    params: {
      ...getFilterParams(filters),
      websiteId,
      startDate: maxDate(filters.startDate, new Date(website?.resetAt)),
      websiteDomain: website.domain,
    },
  };
}

// Database Operations with Connection Pool
async function rawQuery<T = unknown>(
  query: string,
  params: Record<string, unknown> = {},
): Promise<T> {
  if (process.env.LOG_QUERY) {
    log('QUERY:\n', query);
    log('PARAMETERS:\n', params);
  }

  const client = await pool.acquire();
  try {
    const resultSet = await client.query({
      query: query,
      query_params: params,
      format: 'JSONEachRow',
    });

    return (await resultSet.json()) as T;
  } finally {
    pool.release(client);
  }
}

async function insert(table: string, values: any[]) {
  const client = await pool.acquire();
  try {
    return await client.insert({
      table,
      values,
      format: 'JSONEachRow',
      clickhouse_settings: {
        date_time_input_format: 'best_effort',
      },
    });
  } finally {
    pool.release(client);
  }
}

async function pagedQuery(
  query: string,
  queryParams: { [key: string]: any },
  pageParams: PageParams = {},
) {
  const { page = 1, pageSize, orderBy, sortDescending = false } = pageParams;
  const size = +pageSize || DEFAULT_PAGE_SIZE;
  const offset = +size * (page - 1);
  const direction = sortDescending ? 'desc' : 'asc';

  const statements = [
    orderBy && `order by ${orderBy} ${direction}`,
    +size > 0 && `limit ${+size} offset ${offset}`,
  ]
    .filter(n => n)
    .join('\n');

  const count = await rawQuery(`select count(*) as num from (${query}) t`, queryParams).then(
    res => res[0].num,
  );

  const data = await rawQuery(`${query}${statements}`, queryParams);

  return { data, count, page: +page, pageSize: size, orderBy };
}

async function findUnique(data: any[]) {
  if (data.length > 1) {
    throw `${data.length} records found when expecting 1.`;
  }

  return findFirst(data);
}

async function findFirst(data: any[]) {
  return data[0] ?? null;
}

// Export Database Interface
export default {
  enabled,
  pool,
  log,
  connect: async () => {
    if (enabled) {
      return pool.acquire();
    }
    return null;
  },
  getDateStringSQL,
  getDateSQL,
  getSearchSQL,
  getFilterQuery,
  getUTCString,
  parseFilters,
  pagedQuery,
  findUnique,
  findFirst,
  rawQuery,
  insert,
};
