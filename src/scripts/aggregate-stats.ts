/**
 * Website Analytics Aggregation Script
 *
 * This script handles the aggregation of website analytics data and cleanup of old records.
 * It processes website visitor data, computes various metrics, and stores aggregated statistics
 * while maintaining data retention policies.
 */

import { PrismaClient } from '@prisma/client';
import { startOfDay, endOfDay, subDays } from 'date-fns';
import debug from 'debug';
import dotenv from 'dotenv';

// Load environment variables
dotenv.config();

// Initialize debug logger
const log = debug('umami:aggregation');

// Initialize Prisma client for database operations
const prisma = new PrismaClient();

// Configuration constant for data retention period
const RETENTION_DAYS = 30; // Keep detailed data for 30 days

/**
 * Aggregates statistics for a specific website over a given time period
 *
 * @param websiteId - Unique identifier for the website
 * @param startDate - Start date for the aggregation period
 * @param endDate - End date for the aggregation period
 */
async function aggregateWebsiteStats(websiteId: string, startDate: Date, endDate: Date) {
  log(`Aggregating data for website ${websiteId} from ${startDate} to ${endDate}`);

  // Fetch all sessions with their associated events for the specified period
  const sessions = await prisma.session.findMany({
    where: {
      websiteId,
      createdAt: {
        gte: startDate,
        lt: endDate,
      },
    },
    include: {
      websiteEvent: true,
    },
  });

  // Calculate core metrics
  const uniqueVisitors = new Set(sessions.map(s => s.id)).size;
  const pageviews = sessions.reduce((sum, session) => sum + session.websiteEvent.length, 0);

  // Calculate device-specific metrics (desktop, mobile, tablet)
  const deviceCounts = sessions.reduce((acc, session) => {
    const device = session.device?.toLowerCase() || 'unknown';
    acc[device] = (acc[device] || 0) + 1;
    return acc;
  }, {} as Record<string, number>);

  // Get top 100 URLs by visit count
  const urlData = await prisma.websiteEvent.groupBy({
    by: ['urlPath'],
    where: {
      websiteId,
      createdAt: {
        gte: startDate,
        lt: endDate,
      },
    },
    _count: true,
    orderBy: {
      _count: {
        urlPath: 'desc',
      },
    },
    take: 100,
  });

  // Get referrer statistics (where visitors came from)
  const referrerData = await prisma.websiteEvent.groupBy({
    by: ['referrerDomain'],
    where: {
      websiteId,
      createdAt: {
        gte: startDate,
        lt: endDate,
      },
      NOT: {
        referrerDomain: null,
      },
    },
    _count: true,
  });

  // Create or update aggregate statistics in the database
  await prisma.websiteStats.upsert({
    where: {
      websiteId_startDate_period: {
        websiteId,
        startDate,
        period: 'daily',
      },
    },
    create: {
      websiteId,
      startDate,
      endDate,
      period: 'daily',
      pageviews,
      sessions: sessions.length,
      uniqueVisitors,
      desktopViews: deviceCounts['desktop'] || 0,
      mobileViews: deviceCounts['mobile'] || 0,
      tabletViews: deviceCounts['tablet'] || 0,
      // Transform URL data into a key-value object
      urlData: urlData.reduce(
        (acc, { urlPath, _count }) => ({
          ...acc,
          [urlPath]: _count,
        }),
        {},
      ),
      // Transform referrer data into a key-value object
      referrerData: referrerData.reduce(
        (acc, { referrerDomain, _count }) => ({
          ...acc,
          [referrerDomain || 'direct']: _count,
        }),
        {},
      ),
    },
    update: {
      // Update uses the same values as create
      pageviews,
      sessions: sessions.length,
      uniqueVisitors,
      desktopViews: deviceCounts['desktop'] || 0,
      mobileViews: deviceCounts['mobile'] || 0,
      tabletViews: deviceCounts['tablet'] || 0,
      urlData: urlData.reduce(
        (acc, { urlPath, _count }) => ({
          ...acc,
          [urlPath]: _count,
        }),
        {},
      ),
      referrerData: referrerData.reduce(
        (acc, { referrerDomain, _count }) => ({
          ...acc,
          [referrerDomain || 'direct']: _count,
        }),
        {},
      ),
    },
  });
}

/**
 * Deletes data older than the retention period for a specific website
 *
 * @param websiteId - Unique identifier for the website
 * @param beforeDate - Date before which data should be deleted
 */
async function deleteOldData(websiteId: string, beforeDate: Date) {
  log(`Deleting old data for website ${websiteId} before ${beforeDate}`);

  // Delete old events first
  await prisma.websiteEvent.deleteMany({
    where: {
      websiteId,
      createdAt: {
        lt: beforeDate,
      },
    },
  });

  // Then delete old sessions
  await prisma.session.deleteMany({
    where: {
      websiteId,
      createdAt: {
        lt: beforeDate,
      },
    },
  });
}

/**
 * Main function that orchestrates the entire aggregation process
 * - Fetches all active websites
 * - For each website:
 *   1. Aggregates statistics for the day at RETENTION_DAYS ago
 *   2. Deletes data older than RETENTION_DAYS
 */
async function main() {
  try {
    log('Starting data aggregation...');

    // Get all non-deleted websites
    const websites = await prisma.website.findMany({
      where: {
        deletedAt: null,
      },
    });

    // Process each website
    for (const website of websites) {
      const today = new Date();
      const aggregationDate = subDays(today, RETENTION_DAYS);
      const startDate = startOfDay(aggregationDate);
      const endDate = endOfDay(aggregationDate);

      // Aggregate stats and clean up old data
      await aggregateWebsiteStats(website.id, startDate, endDate);
      await deleteOldData(website.id, startDate);

      log(`Completed processing website: ${website.name}`);
    }

    log('Aggregation completed successfully');
  } catch (error) {
    log('Error during aggregation:', error);
    throw error;
  } finally {
    // Ensure database connection is closed
    await prisma.$disconnect();
  }
}

// Execute main function if script is run directly
if (require.main === module) {
  main()
    .then(() => process.exit(0))
    .catch(error => {
      process.exit(1);
      log(error);
    });
}

export default main;
