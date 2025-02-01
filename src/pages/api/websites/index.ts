// Import authentication-related functions to check user permissions
import { canCreateTeamWebsite, canCreateWebsite } from 'lib/auth';
// Import utility to generate unique identifiers
import { uuid } from 'lib/crypto';
// Import middleware functions for authentication, CORS, and validation
import { useAuth, useCors, useValidate } from 'lib/middleware';
// Import TypeScript types for API requests and pagination
import { NextApiRequestQueryBody, PageParams } from 'lib/types';
import { NextApiResponse } from 'next';
// Import utility functions for API responses
import { methodNotAllowed, ok, unauthorized } from 'next-basics';
// Import database query for website creation
import { createWebsite } from 'queries';
// Import route handler for user-specific website operations
import userWebsitesRoute from 'pages/api/users/[userId]/websites';
// Import validation library and schema
import * as yup from 'yup';
import { pageInfo } from 'lib/schema';

// Define TypeScript interface for query parameters (extends pagination parameters)
export interface WebsitesRequestQuery extends PageParams {}

// Define TypeScript interface for the request body when creating a website
export interface WebsitesRequestBody {
  name: string;
  domain: string;
  shareId: string;
  teamId: string;
}

// Define validation schemas for different HTTP methods
const schema = {
  // GET request validation schema (includes pagination parameters)
  GET: yup.object().shape({
    ...pageInfo,
  }),
  // POST request validation schema for website creation
  POST: yup.object().shape({
    name: yup.string().max(100).required(),
    domain: yup.string().max(500).required(),
    shareId: yup.string().max(50).nullable(),
    teamId: yup.string().nullable(),
  }),
};

// Main API route handler
export default async (
  req: NextApiRequestQueryBody<WebsitesRequestQuery, WebsitesRequestBody>,
  res: NextApiResponse,
) => {
  // Apply middleware
  await useCors(req, res); // Handle CORS
  await useAuth(req, res); // Verify authentication
  await useValidate(schema, req, res); // Validate request data

  // Extract user ID from authenticated request
  const {
    user: { id: userId },
  } = req.auth;

  // Handle GET requests
  if (req.method === 'GET') {
    // If no specific user ID is provided, use the authenticated user's ID
    if (!req.query.userId) {
      req.query.userId = userId;
    }

    // Forward to user-specific websites route handler
    return userWebsitesRoute(req, res);
  }

  // Handle POST requests (website creation)
  if (req.method === 'POST') {
    // Extract website creation parameters from request body
    const { name, domain, shareId, teamId } = req.body;

    // Check user permissions for website creation
    if (
      (teamId && !(await canCreateTeamWebsite(req.auth, teamId))) ||
      !(await canCreateWebsite(req.auth))
    ) {
      return unauthorized(res);
    }

    // Prepare website data for creation
    const data: any = {
      id: uuid(), // Generate unique ID
      createdBy: userId, // Set creator
      name, // Website name
      domain, // Website domain
      shareId, // Sharing identifier
      teamId, // Team association
    };

    // If not a team website, associate with individual user
    if (!teamId) {
      data.userId = userId;
    }

    // Create website in database
    const website = await createWebsite(data);

    // Return successful response with created website data
    return ok(res, website);
  }

  // Return error for unsupported HTTP methods
  return methodNotAllowed(res);
};
