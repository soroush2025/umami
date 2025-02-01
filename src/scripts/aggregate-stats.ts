import { PrismaClient } from '@prisma/client';
import { startOfDay, endOfDay, subDays } from 'date-fns';
import debug from 'debug';

import dotenv from 'dotenv';
dotenv.config();

const log = debug('umami:aggregation');

const prisma = new PrismaClient();

const RETENTION_DAYS = 30; // Keep detailed data for 30 days

async function aggregateWebsiteStats(websiteId: string, startDate: Date, endDate: Date) {
  log(`Aggregating data for website ${websiteId} from ${startDate} to ${endDate}`);

  // Get all sessions for the period
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

  // Calculate metrics
  const uniqueVisitors = new Set(sessions.map(s => s.id)).size;
  const pageviews = sessions.reduce((sum, session) => sum + session.websiteEvent.length, 0);

  // Calculate device metrics
  const deviceCounts = sessions.reduce((acc, session) => {
    const device = session.device?.toLowerCase() || 'unknown';
    acc[device] = (acc[device] || 0) + 1;
    return acc;
  }, {} as Record<string, number>);

  // Get URL data
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

  // Get referrer data
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

  // Create or update prisma.websiteStats
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
    update: {
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

async function deleteOldData(websiteId: string, beforeDate: Date) {
  log(`Deleting old data for website ${websiteId} before ${beforeDate}`);

  // Delete old events
  await prisma.websiteEvent.deleteMany({
    where: {
      websiteId,
      createdAt: {
        lt: beforeDate,
      },
    },
  });

  // Delete old sessions
  await prisma.session.deleteMany({
    where: {
      websiteId,
      createdAt: {
        lt: beforeDate,
      },
    },
  });
}

async function main() {
  try {
    log('Starting data aggregation...');

    const websites = await prisma.website.findMany({
      where: {
        deletedAt: null,
      },
    });

    for (const website of websites) {
      const today = new Date();
      const aggregationDate = subDays(today, RETENTION_DAYS);
      const startDate = startOfDay(aggregationDate);
      const endDate = endOfDay(aggregationDate);

      await aggregateWebsiteStats(website.id, startDate, endDate);
      await deleteOldData(website.id, startDate);

      log(`Completed processing website: ${website.name}`);
    }

    log('Aggregation completed successfully');
  } catch (error) {
    log('Error during aggregation:', error);
    throw error;
  } finally {
    await prisma.$disconnect();
  }
}

// If running directly
if (require.main === module) {
  main()
    .then(() => process.exit(0))
    .catch(error => {
      process.exit(1);
      log(error);
    });
}

export default main;
