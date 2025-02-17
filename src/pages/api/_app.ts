import { setupGracefulShutdown } from '../../lib/shutdown';

// Initialize any API-wide configurations
if (process.env.NODE_ENV === 'production') {
  setupGracefulShutdown();
}

// Export a dummy handler since Next.js requires it
export default function handler(req, res) {
  res.status(404).end();
}
