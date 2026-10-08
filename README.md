# P2P Matching Engine

## Runtime configuration

The engine requires explicit secrets and does not ship with a usable default:

- `CLUSTER_SECRET`: a random secret of at least 32 characters, shared only by engine nodes. Do not give this value to API clients.
- `CLIENT_API_KEYS`: a JSON object mapping each API key to its own unique secret, for example `{"trader-a":"<secret-a>","trader-b":"<secret-b>"}`. Each secret must be at least 32 characters. Configure the same mapping on every engine node.
- `NODE_ID`: this node's unique ID.
- `CLUSTER_PEERS`: comma-separated node IDs, including this node's `NODE_ID`; configure the same set on every node.
- `GRAPE_URL` and `PORT`: the node's Grape endpoint and listening port.

Start the engine after exporting those values with `npm start`. Client programs must receive only their own `API_KEY` and `API_SECRET`; the pair must match an entry in `CLIENT_API_KEYS`. Never put `CLUSTER_SECRET` in a client or commit any real secret to the repository.
