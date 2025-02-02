/**
 * Pageview Statistics Query Implementation
 * This module provides functionality to fetch pageview statistics with support
 * for both Prisma (relational) and ClickHouse databases.
 */

import clickhouse from 'lib/clickhouse';
import { CLICKHOUSE, PRISMA, runQuery } from 'lib/db';
import prisma from 'lib/prisma';
import { EVENT_COLUMNS, EVENT_TYPE } from 'lib/constants';
import { QueryFilters } from 'lib/types';

/**
 * Main entry point for getting pageview statistics
 * Uses strategy pattern to select appropriate database query implementation
 */
export async function getPageviewStats(...args: [websiteId: string, filters: QueryFilters]) {
  return runQuery({
    [PRISMA]: () => relationalQuery(...args),
    [CLICKHOUSE]: () => clickhouseQuery(...args),
  });
}

/**
 * Prisma (relational database) implementation for pageview statistics
 * Generates and executes SQL for traditional relational databases
 *
 * @param websiteId - Website identifier
 * @param filters - Query filters including timezone and time unit
 */
async function relationalQuery(websiteId: string, filters: QueryFilters) {
  // Extract timezone and unit from filters, defaulting to UTC and day
  const { timezone = 'utc', unit = 'day' } = filters;
  const { getDateSQL, parseFilters, rawQuery } = prisma;

  // Parse filters and get necessary query components
  const { filterQuery, joinSession, params } = await parseFilters(websiteId, {
    ...filters,
    eventType: EVENT_TYPE.pageView,
  });

  // Execute raw SQL query for pageview statistics
  return rawQuery(
    `
    select
      ${getDateSQL('website_event.created_at', unit, timezone)} x,
      count(*) y
    from website_event
      ${joinSession}
    where website_event.website_id = {{websiteId::uuid}}
      and website_event.created_at between {{startDate}} and {{endDate}}
      and event_type = {{eventType}}
      ${filterQuery}
    group by 1
    order by 1
    `,
    params,
  );
}

/**
 * ClickHouse implementation for pageview statistics
 * Provides optimized queries for the ClickHouse columnar database
 *
 * @param websiteId - Website identifier
 * @param filters - Query filters including timezone and time unit
 * @returns Array of objects containing timestamp (x) and count (y)
 */
async function clickhouseQuery(
  websiteId: string,
  filters: QueryFilters,
): Promise<{ x: string; y: number }[]> {
  const { timezone = 'utc', unit = 'day' } = filters;
  const { parseFilters, rawQuery, getDateSQL } = clickhouse;

  // Parse filters for ClickHouse syntax
  const { filterQuery, params } = await parseFilters(websiteId, {
    ...filters,
    eventType: EVENT_TYPE.pageView,
  });

  let sql = '';

  // Choose query strategy based on filters and unit
  // Use detailed table for specific column filters or minute-level granularity
  if (EVENT_COLUMNS.some(item => Object.keys(filters).includes(item)) || unit === 'minute') {
    sql = `
    select
      g.t as x,
      g.y as y
    from (
      select
        ${getDateSQL('website_event.created_at', unit, timezone)} as t,
        count(*) as y
      from website_event
      where website_id = {websiteId:UUID}
        and created_at between {startDate:DateTime64} and {endDate:DateTime64}
        and event_type = {eventType:UInt32}
        ${filterQuery}
      group by t
    ) as g
    order by t
    `;
  } else {
    // Use pre-aggregated stats for better performance
    sql = `
    select
      g.t as x,
      g.y as y
    from (
      select
        ${getDateSQL('website_event.created_at', unit, timezone)} as t,
        sum(views)as y
      from website_event_stats_hourly website_event
      where website_id = {websiteId:UUID}
        and created_at between {startDate:DateTime64} and {endDate:DateTime64}
        and event_type = {eventType:UInt32}
        ${filterQuery}
      group by t
    ) as g
    order by t
    `;
  }

  return rawQuery(sql, params);
}
