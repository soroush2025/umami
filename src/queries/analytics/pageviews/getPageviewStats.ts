import { CLICKHOUSE, PRISMA, runQuery } from 'lib/db';
import prismaUtil from 'lib/prisma'; // Rename to clarify this is utilities
import { PrismaClient } from '@prisma/client';
import clickhouse from 'lib/clickhouse';
import { EVENT_COLUMNS, EVENT_TYPE } from 'lib/constants';
import { QueryFilters } from 'lib/types';
import { startOfDay, endOfDay, parseISO } from 'date-fns';

// Create a singleton PrismaClient instance
const prisma = new PrismaClient();

/**
 * Flow of pageview statistics retrieval:
 *
 * 1. Entry point: getPageviewStats function
 *    - Accepts websiteId and filters
 *    - Uses runQuery to determine database type (Prisma/Clickhouse)
 *
 * 2. For each database type:
 *    a. Check if aggregated data can be used:
 *       - Must be daily unit
 *       - No special event column filters
 *       - Date range available in WebsiteStats
 *
 *    b. If aggregated data is usable:
 *       - Query WebsiteStats table
 *       - Return pre-calculated pageview counts
 *
 *    c. If aggregated data cannot be used:
 *       - Fall back to detailed event-level queries
 *       - Use original query logic with joins and filters
 *
 * 3. Data aggregation background process (separate from this file):
 *    - Runs daily to aggregate previous day's data
 *    - Stores in WebsiteStats table
 *    - Keeps last 30 days of detailed data
 *    - Older data only available via aggregation
 */
export async function getPageviewStats(...args: [websiteId: string, filters: QueryFilters]) {
  return runQuery({
    [PRISMA]: () => relationalQuery(...args),
    [CLICKHOUSE]: () => clickhouseQuery(...args),
  });
}

async function relationalQuery(websiteId: string, filters: QueryFilters) {
  const { timezone = 'utc', unit = 'day' } = filters;
  const { getDateSQL, parseFilters, rawQuery } = prismaUtil;

  // STEP 1: Check if we can use aggregated data
  if (unit === 'day') {
    const { startDate, endDate } = filters;
    const start = parseISO(startDate as unknown as string);
    const end = parseISO(endDate as unknown as string);

    // STEP 2a: Try to fetch from aggregated stats
    // Use prisma client to query aggregated stats
    const aggregatedStats = await prisma.websiteStats.findMany({
      where: {
        websiteId,
        startDate: {
          gte: startOfDay(start),
          lte: endOfDay(end),
        },
        period: 'daily',
      },
      orderBy: {
        startDate: 'asc',
      },
      select: {
        startDate: true,
        pageviews: true,
      },
    });

    // STEP 2b: Return aggregated data if available
    if (aggregatedStats.length > 0) {
      return aggregatedStats.map(stat => ({
        x: stat.startDate.toISOString(),
        y: stat.pageviews,
      }));
    }
  }

  // STEP 3: Fall back to detailed query if:
  // - Not using daily unit
  // - No aggregated data available
  // - Special filters required
  const { filterQuery, joinSession, params } = await parseFilters(websiteId, {
    ...filters,
    eventType: EVENT_TYPE.pageView,
  });

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

async function clickhouseQuery(
  websiteId: string,
  filters: QueryFilters,
): Promise<{ x: string; y: number }[]> {
  const { timezone = 'utc', unit = 'day' } = filters;
  const { parseFilters, rawQuery, getDateSQL } = clickhouse;

  // STEP 1: Check if we can use aggregated data
  // - Must be daily unit
  // - No special event column filters
  if (unit === 'day' && !EVENT_COLUMNS.some(item => Object.keys(filters).includes(item))) {
    const { startDate, endDate } = filters;
    const start = parseISO(startDate as unknown as string);
    const end = parseISO(endDate as unknown as string);

    // STEP 2a: Try to get data from aggregated stats
    const prismaClient = new PrismaClient();
    const aggregatedStats = await prismaClient.websiteStats.findMany({
      where: {
        websiteId,
        startDate: {
          gte: startOfDay(start),
          lte: endOfDay(end),
        },
        period: 'daily',
      },
      orderBy: {
        startDate: 'asc',
      },
      select: {
        startDate: true,
        pageviews: true,
      },
    });

    // STEP 2b: Return aggregated data if available
    if (aggregatedStats.length > 0) {
      return aggregatedStats.map(stat => ({
        x: stat.startDate.toISOString(),
        y: stat.pageviews,
      }));
    }
  }

  // STEP 3: Fall back to original clickhouse query
  // Handle cases where:
  // - Not using daily aggregation
  // - Special filters are required
  // - Data not available in aggregated form
  const { filterQuery, params } = await parseFilters(websiteId, {
    ...filters,
    eventType: EVENT_TYPE.pageView,
  });

  const sql =
    EVENT_COLUMNS.some(item => Object.keys(filters).includes(item)) || unit === 'minute'
      ? `
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
    `
      : `
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

  return rawQuery(sql, params);
}
