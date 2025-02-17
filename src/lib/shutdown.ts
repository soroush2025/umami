import clickhouse from './clickhouse';
import prisma from './prisma';
import debug from 'debug';

const log = debug('umami:shutdown');

export function setupGracefulShutdown() {
  const cleanup = async () => {
    log('Gracefully shutting down...');

    // Cleanup order matters - do database connections last
    try {
      // Cleanup Kafka if enabled

      // Cleanup ClickHouse if enabled
      if (clickhouse?.enabled) {
        await clickhouse.pool.cleanup();
        log('ClickHouse connections cleaned up');
      }

      // Always cleanup Prisma last
      await prisma.client.$disconnect();
      log('Prisma disconnected');
    } catch (error) {
      log('Error during cleanup:', error);
    }

    process.exit(0);
  };

  process.on('SIGTERM', cleanup);
  process.on('SIGINT', cleanup);

  // Handle uncaught exceptions
  process.on('uncaughtException', async error => {
    log('Uncaught exception:', error);
    await cleanup();
  });

  // Handle unhandled promise rejections
  process.on('unhandledRejection', async error => {
    log('Unhandled rejection:', error);
    await cleanup();
  });

  log('Graceful shutdown handlers registered');
}
