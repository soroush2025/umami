import { isbot } from 'isbot';
import { NextApiRequest, NextApiResponse } from 'next';
import {
  badRequest,
  createToken,
  forbidden,
  methodNotAllowed,
  ok,
  safeDecodeURI,
  send,
} from 'next-basics';
import { COLLECTION_TYPE, HOSTNAME_REGEX, IP_REGEX } from 'lib/constants';
import { secret, visitSalt, uuid } from 'lib/crypto';
import { hasBlockedIp } from 'lib/detect';
import { useCors, useSession, useValidate } from 'lib/middleware';
import { CollectionType, YupRequest } from 'lib/types';
import { saveEvent, saveSessionData } from 'queries';
import * as yup from 'yup';

export interface CollectRequestBody {
  payload: {
    website: string;
    data?: { [key: string]: any };
    hostname?: string;
    ip?: string;
    language?: string;
    name?: string;
    referrer?: string;
    screen?: string;
    tag?: string;
    title?: string;
    url: string;
  };
  type: CollectionType;
}

export interface NextApiRequestCollect extends NextApiRequest {
  body: CollectRequestBody;
  session: {
    id: string;
    websiteId: string;
    visitId: string;
    hostname: string;
    browser: string;
    os: string;
    device: string;
    screen: string;
    language: string;
    country: string;
    subdivision1: string;
    subdivision2: string;
    city: string;
    iat: number;
  };
  headers: { [key: string]: any };
  yup: YupRequest;
}

const schema = {
  POST: yup.object().shape({
    payload: yup
      .object()
      .shape({
        data: yup.object(),
        hostname: yup.string().matches(HOSTNAME_REGEX, 'Invalid hostname format').max(100),
        ip: yup.string().matches(IP_REGEX, 'Invalid IP address format'),
        language: yup.string().max(35, 'Language code too long'),
        referrer: yup.string(),
        screen: yup.string().max(11, 'Screen resolution format invalid'),
        title: yup.string(),
        url: yup.string(),
        website: yup.string().uuid('Invalid website ID format').required('Website ID is required'),
        name: yup.string().max(50, 'Event name too long'),
        tag: yup.string().max(50, 'Tag too long').nullable(),
      })
      .required('Payload is required'),
    type: yup
      .string()
      .matches(/event|identify/i, 'Type must be either "event" or "identify"')
      .required('Type is required'),
  }),
};

export default async (req: NextApiRequestCollect, res: NextApiResponse) => {
  try {
    await useCors(req, res);

    if (req.method !== 'POST') {
      return methodNotAllowed(res, 'Method not allowed. Allowed methods: POST');
    }

    // Bot check
    if (!process.env.DISABLE_BOT_CHECK && isbot(req.headers['user-agent'])) {
      return ok(res, {
        status: 'ignored',
        reason: 'bot',
        userAgent: req.headers['user-agent'],
      });
    }

    // Validation
    try {
      await useValidate(schema, req, res);
    } catch (validationError) {
      return badRequest(
        res,
        JSON.stringify({
          error: 'Validation failed',
          details: (validationError as yup.ValidationError).errors,
          receivedPayload: req.body,
        }),
      );
    }

    // IP blocking check
    if (hasBlockedIp(req)) {
      return forbidden(
        res,
        `IP blocked: ${req.headers['x-forwarded-for'] || req.socket.remoteAddress}`,
      );
    }

    const { type, payload } = req.body;
    const { url, referrer, name: eventName, data, title, tag } = payload;
    const pageTitle = safeDecodeURI(title);

    // Session handling
    try {
      await useSession(req, res);
    } catch (sessionError) {
      return badRequest(
        res,
        JSON.stringify({
          error: 'Session creation failed',
          details: (sessionError as any).message,
        }),
      );
    }

    const session = req.session;

    if (!session?.id) {
      return badRequest(
        res,
        JSON.stringify({
          error: 'Invalid session',
          details: 'Session ID is missing',
        }),
      );
    }

    const iat = Math.floor(new Date().getTime() / 1000);

    // expire visitId after 30 minutes
    if (session.iat && iat - session.iat > 1800) {
      session.visitId = uuid(session.id, visitSalt());
    }
    session.iat = iat;

    try {
      if (type === COLLECTION_TYPE.event) {
        let [urlPath, urlQuery] = safeDecodeURI(url)?.split('?') || [];
        urlQuery = urlQuery || '';
        let [referrerPath, referrerQuery] = safeDecodeURI(referrer)?.split('?') || [];
        let referrerDomain = '';

        if (!urlPath) {
          urlPath = '/';
        }

        if (/^[\w-]+:\/\/\w+/.test(referrerPath)) {
          const refUrl = new URL(referrer);
          referrerPath = refUrl.pathname;
          referrerQuery = refUrl.search.substring(1);
          referrerDomain = refUrl.hostname.replace(/www\./, '');
        }

        if (process.env.REMOVE_TRAILING_SLASH) {
          urlPath = urlPath.replace(/(.+)\/$/, '$1');
        }

        await saveEvent({
          urlPath,
          urlQuery,
          referrerPath,
          referrerQuery,
          referrerDomain,
          pageTitle,
          eventName,
          eventData: data,
          ...session,
          sessionId: session.id,
          tag,
        });
      } else if (type === COLLECTION_TYPE.identify) {
        if (!data) {
          return badRequest(
            res,
            JSON.stringify({
              error: 'Missing data',
              details: 'Data object is required for identify events',
            }),
          );
        }

        await saveSessionData({
          websiteId: session.websiteId,
          sessionId: session.id,
          sessionData: data,
        });
      }

      const token = createToken(session, secret());
      return send(res, {
        status: 'success',
        token,
        sessionId: session.id,
        type,
      });
    } catch (processingError) {
      return badRequest(
        res,
        JSON.stringify({
          error: 'Processing failed',
          type,
          details: (processingError as Error).message,
          stack:
            process.env.NODE_ENV === 'development' ? (processingError as Error).stack : undefined,
        }),
      );
    }
  } catch (error) {
    // Global error handler
    return badRequest(
      res,
      JSON.stringify({
        error: 'Internal server error',
        details: (error as Error).message,
        stack: process.env.NODE_ENV === 'development' ? (error as Error).stack : undefined,
      }),
    );
  }
};
