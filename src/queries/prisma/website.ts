import { Prisma, Website } from '@prisma/client';
import { getClient } from '@umami/redis-client';
import prisma from 'lib/prisma';
import { PageResult, PageParams } from 'lib/types';
import WebsiteFindManyArgs = Prisma.WebsiteFindManyArgs;
import { ROLES } from 'lib/constants';

/**
 * Base function to find a unique website based on given criteria
 * @param criteria - Prisma find unique arguments for website query
 * @returns Promise resolving to a Website or null
 */
async function findWebsite(criteria: Prisma.WebsiteFindUniqueArgs): Promise<Website> {
  return prisma.client.website.findUnique(criteria);
}

/**
 * Retrieves a specific website by its ID
 * @param websiteId - Unique identifier of the website
 */
export async function getWebsite(websiteId: string) {
  return findWebsite({
    where: {
      id: websiteId,
    },
  });
}

/**
 * Retrieves a website using its share ID, excluding deleted websites
 * @param shareId - Public sharing identifier of the website
 */
export async function getSharedWebsite(shareId: string) {
  return findWebsite({
    where: {
      shareId,
      deletedAt: null,
    },
  });
}

/**
 * Retrieves a paginated list of websites based on search criteria
 * @param criteria - Prisma query criteria for filtering websites
 * @param pageParams - Pagination and search parameters
 * @returns Paginated result containing website array
 */
export async function getWebsites(
  criteria: WebsiteFindManyArgs,
  pageParams: PageParams,
): Promise<PageResult<Website[]>> {
  const { query } = pageParams;

  // Combine search parameters with existing criteria
  const where: Prisma.WebsiteWhereInput = {
    ...criteria.where,
    ...prisma.getSearchParameters(query, [
      {
        name: 'contains', // Search by website name
      },
      { domain: 'contains' }, // Search by domain name
    ]),
    deletedAt: null, // Exclude deleted websites
  };

  return prisma.pagedQuery('website', { ...criteria, where }, pageParams);
}

/**
 * Gets all websites accessible to a user, including team websites
 * @param userId - User's unique identifier
 * @returns Array of websites the user has access to
 */
export async function getAllWebsites(userId: string) {
  return prisma.client.website.findMany({
    where: {
      OR: [
        { userId }, // Websites owned directly by the user
        {
          team: {
            deletedAt: null,
            teamUser: {
              some: {
                userId, // Websites belonging to teams the user is part of
              },
            },
          },
        },
      ],
      deletedAt: null,
    },
  });
}

/**
 * Gets all websites where user is either the owner or a team owner
 * Primarily used for administrative purposes
 * @param userId - User's unique identifier
 */
export async function getAllUserWebsitesIncludingTeamOwner(userId: string) {
  return prisma.client.website.findMany({
    where: {
      OR: [
        { userId }, // Direct website ownership
        {
          team: {
            deletedAt: null,
            teamUser: {
              some: {
                role: ROLES.teamOwner, // Team ownership
                userId,
              },
            },
          },
        },
      ],
    },
  });
}

/**
 * Retrieves paginated list of websites owned by a specific user
 * @param userId - User's unique identifier
 * @param filters - Optional pagination and filtering parameters
 */
export async function getUserWebsites(
  userId: string,
  filters?: PageParams,
): Promise<PageResult<Website[]>> {
  return getWebsites(
    {
      where: {
        userId,
      },
      include: {
        user: {
          select: {
            username: true,
            id: true,
          },
        },
      },
    },
    {
      orderBy: 'name',
      ...filters,
    },
  );
}

/**
 * Retrieves paginated list of websites belonging to a specific team
 * @param teamId - Team's unique identifier
 * @param filters - Optional pagination and filtering parameters
 */
export async function getTeamWebsites(
  teamId: string,
  filters?: PageParams,
): Promise<PageResult<Website[]>> {
  return getWebsites(
    {
      where: {
        teamId,
      },
      include: {
        createUser: {
          select: {
            id: true,
            username: true,
          },
        },
      },
    },
    filters,
  );
}

/**
 * Creates a new website record
 * @param data - Website creation data
 */
export async function createWebsite(
  data: Prisma.WebsiteCreateInput | Prisma.WebsiteUncheckedCreateInput,
): Promise<Website> {
  return prisma.client.website.create({
    data,
  });
}

/**
 * Updates an existing website's information
 * @param websiteId - Website's unique identifier
 * @param data - Updated website data
 */
export async function updateWebsite(
  websiteId: string,
  data: Prisma.WebsiteUpdateInput | Prisma.WebsiteUncheckedUpdateInput,
): Promise<Website> {
  return prisma.client.website.update({
    where: {
      id: websiteId,
    },
    data,
  });
}

/**
 * Resets all analytics data for a website while maintaining the website record
 * Handles both database and cache clearing in cloud mode
 * @param websiteId - Website's unique identifier
 * @returns Array containing deletion results and updated website
 */
export async function resetWebsite(
  websiteId: string,
): Promise<[Prisma.BatchPayload, Prisma.BatchPayload, Website]> {
  const { client, transaction } = prisma;
  const cloudMode = !!process.env.cloudMode;

  // Execute all operations in a transaction
  return transaction([
    // Clear all analytics data
    client.eventData.deleteMany({
      where: { websiteId },
    }),
    client.sessionData.deleteMany({
      where: { websiteId },
    }),
    client.websiteEvent.deleteMany({
      where: { websiteId },
    }),
    client.session.deleteMany({
      where: { websiteId },
    }),
    // Update reset timestamp
    client.website.update({
      where: { id: websiteId },
      data: {
        resetAt: new Date(),
      },
    }),
  ]).then(async data => {
    // Handle cache clearing in cloud mode
    if (cloudMode) {
      const redis = getClient();
      await redis.set(`website:${websiteId}`, data[3]);
    }
    return data;
  });
}

/**
 * Deletes a website and all its associated data
 * In cloud mode, soft deletes the website; in self-hosted mode, hard deletes
 * @param websiteId - Website's unique identifier
 * @returns Array containing deletion results and website data
 */
export async function deleteWebsite(
  websiteId: string,
): Promise<[Prisma.BatchPayload, Prisma.BatchPayload, Website]> {
  const { client, transaction } = prisma;
  const cloudMode = !!process.env.CLOUD_MODE;

  // Execute all operations in a transaction
  return transaction([
    // Clear all associated data
    client.eventData.deleteMany({
      where: { websiteId },
    }),
    client.sessionData.deleteMany({
      where: { websiteId },
    }),
    client.websiteEvent.deleteMany({
      where: { websiteId },
    }),
    client.session.deleteMany({
      where: { websiteId },
    }),
    client.report.deleteMany({
      where: {
        websiteId,
      },
    }),
    // Handle website deletion based on mode
    cloudMode
      ? client.website.update({
          data: {
            deletedAt: new Date(), // Soft delete in cloud mode
          },
          where: { id: websiteId },
        })
      : client.website.delete({
          // Hard delete in self-hosted mode
          where: { id: websiteId },
        }),
  ]).then(async data => {
    // Clear cache in cloud mode
    if (cloudMode) {
      const redis = getClient();
      await redis.del(`website:${websiteId}`);
    }
    return data;
  });
}
